// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io::Write;
use std::fmt;
use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::net::SocketAddr;

use futures::{future, Future, Stream};
use futures::sync::mpsc;
use tokio_core::reactor::Handle;
use grpc::futures_grpc::{GrpcStreamSend, GrpcFutureSend};
use grpc::error::GrpcError;
use mio::Token;

use kvproto::raft_serverpb::*;
use kvproto::raft_serverpb_grpc::{RaftAsync, RaftAsyncClient};

use util::worker::Scheduler;
use util::transport::SendCh;
use util::buf::PipeBuffer;
use util::worker::FutureRunnable;
use super::server::ServerChannel;
use super::transport::RaftStoreRouter;
use super::snap::Task as SnapTask;
use super::errors::Result;

// RaftServer is used for receiving raft messages from other stores.
#[allow(dead_code)]
pub struct RaftServer<T: RaftStoreRouter + 'static> {
    ch: Mutex<T>,
    snap_scheduler: Mutex<Scheduler<SnapTask>>,
    token: AtomicUsize, // TODO: remove it.
}

#[allow(dead_code)]
impl<T: RaftStoreRouter + 'static> RaftServer<T> {
    pub fn new(ch: T, snap_scheduler: Scheduler<SnapTask>) -> RaftServer<T> {
        RaftServer {
            ch: Mutex::new(ch),
            snap_scheduler: Mutex::new(snap_scheduler),
            token: AtomicUsize::new(1),
        }
    }
}

impl<T: RaftStoreRouter + 'static> RaftAsync for RaftServer<T> {
    fn Raft(&self, s: GrpcStreamSend<RaftMessage>) -> GrpcFutureSend<Done> {
        let ch = self.ch.lock().unwrap().clone();
        s.for_each(move |msg| {
                ch.send_raft_msg(msg).map_err(|_| GrpcError::Other("send raft msg fail"))
            })
            .and_then(|_| future::ok::<_, GrpcError>(Done::new()))
            .boxed()
    }

    fn Snapshot(&self, s: GrpcStreamSend<SnapshotChunk>) -> GrpcFutureSend<Done> {
        let token = Token(self.token.fetch_add(1, Ordering::SeqCst));
        let sched = self.snap_scheduler.lock().unwrap().clone();
        let sched2 = sched.clone();
        s.for_each(move |mut chunk| {
                if chunk.has_message() {
                    sched.schedule(SnapTask::Register(token, chunk.take_message()))
                        .map_err(|_| GrpcError::Other("schedule snap_task fail"))
                } else if !chunk.get_data().is_empty() {
                    // TODO: Remove PipeBuffer or take good use of it.
                    let mut b = PipeBuffer::new(chunk.get_data().len());
                    b.write_all(chunk.get_data()).unwrap();
                    sched.schedule(SnapTask::Write(token, b))
                        .map_err(|_| GrpcError::Other("schedule snap_task fail"))
                } else {
                    Err(GrpcError::Other("empty chunk"))
                }
            })
            .then(move |res| {
                let res = match res {
                    Ok(_) => sched2.schedule(SnapTask::Close(token)),
                    Err(e) => {
                        error!("receive snapshot err: {}", e);
                        sched2.schedule(SnapTask::Discard(token))
                    }
                };
                future::result(res.map_err(|_| GrpcError::Other("schedule snap_task fail")))
            })
            .and_then(|_| future::ok::<_, GrpcError>(Done::new()))
            .boxed()
    }
}

// SendTask delivers a raft message to other store.
pub struct SendTask {
    pub addr: SocketAddr,
    pub msg: RaftMessage,
}

impl fmt::Display for SendTask {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "send raft message to {:?}", self.addr)
    }
}

struct Conn {
    _client: RaftAsyncClient,
    stream: mpsc::UnboundedSender<RaftMessage>,
}

impl Conn {
    fn new(addr: SocketAddr, handle: &Handle) -> Result<Conn> {
        let host = format!("{}", addr.ip());
        let client = box_try!(RaftAsyncClient::new(&*host, addr.port(), false, Default::default()));
        let (tx, rx) = mpsc::unbounded();
        handle.spawn(client.Raft(box rx.map_err(|_| GrpcError::Other("canceled")))
            .then(|_| future::ok(())));
        Ok(Conn {
            _client: client,
            stream: tx,
        })
    }
}

// SendRunner is used for sending raft messages to other stores.
pub struct SendRunner {
    snap_scheduler: Scheduler<SnapTask>,
    conns: HashMap<SocketAddr, Conn>,
}

impl SendRunner {
    pub fn new(snap_scheduler: Scheduler<SnapTask>) -> SendRunner {
        SendRunner {
            snap_scheduler: snap_scheduler,
            conns: HashMap::new(),
        }
    }

    fn get_conn(&mut self, addr: SocketAddr, handle: &Handle) -> Result<&Conn> {
        // TDOO: handle Conn::new() error.
        let conn = self.conns.entry(addr).or_insert_with(|| Conn::new(addr, handle).unwrap());
        Ok(conn)
    }

    fn send(&mut self, t: SendTask, handle: &Handle) -> Result<()> {
        let conn = try!(self.get_conn(t.addr, handle));
        box_try!(conn.stream.send(t.msg));
        Ok(())
    }
}

impl FutureRunnable<SendTask> for SendRunner {
    fn run(&mut self, t: SendTask, handle: &Handle) {
        let addr = t.addr;
        if let Err(e) = self.send(t, handle) {
            error!("send raft message error: {:?}", e);
            self.conns.remove(&addr);
        }
    }
}
