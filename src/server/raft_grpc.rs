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
use std::sync::RwLock;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::{future, Future, Stream};
use grpc::futures_grpc::{GrpcStreamSend, GrpcFutureSend};
use grpc::error::GrpcError;
use mio::Token;

use kvproto::raft_serverpb::*;
use kvproto::raft_serverpb_grpc;

use util::HandyRwLock;
use util::worker::Scheduler;
use util::buf::PipeBuffer;
use super::server::ServerChannel;
use super::transport::RaftStoreRouter;
use super::snap::Task as SnapTask;

// RaftServer is used for receiving raft messages from other stores.
#[allow(dead_code)]
pub struct RaftServer<T: RaftStoreRouter + 'static> {
    ch: ServerChannel<T>,
    snap_scheduler: RwLock<Scheduler<SnapTask>>,
    token: AtomicUsize, // TODO: remove it.
}

#[allow(dead_code)]
impl<T: RaftStoreRouter + 'static> RaftServer<T> {
    pub fn new(ch: ServerChannel<T>, snap_scheduler: Scheduler<SnapTask>) -> RaftServer<T> {
        RaftServer {
            ch: ch,
            snap_scheduler: RwLock::new(snap_scheduler),
            token: AtomicUsize::new(1),
        }
    }
}

impl<T: RaftStoreRouter + 'static> raft_serverpb_grpc::RaftAsync for RaftServer<T> {
    fn Raft(&self, s: GrpcStreamSend<RaftMessage>) -> GrpcFutureSend<Done> {
        let ch = self.ch.raft_router.clone();
        s.for_each(move |msg| {
                ch.send_raft_msg(msg).map_err(|_| GrpcError::Other("send raft msg fail"))
            })
            .and_then(|_| future::ok::<_, GrpcError>(Done::new()))
            .boxed()
    }

    fn Snapshot(&self, s: GrpcStreamSend<SnapshotChunk>) -> GrpcFutureSend<Done> {
        let token = Token(self.token.fetch_add(1, Ordering::SeqCst));
        let sched = self.snap_scheduler.rl().clone();
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
