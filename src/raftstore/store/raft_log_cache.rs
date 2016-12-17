// Copyright 2016 PingCAP, Inc.
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

/// A thread unsafe cache for raft log.
/// It's used in `PeerStorage` in order to avoid reading raft log from rocksdb.

use protobuf::Message;
use kvproto::eraftpb::Entry;

use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use util::HandyRwLock;
use raft;

pub struct RaftLogCacheCore {
    buffer_size: usize,
    start: usize,
    end: usize,
    buffer: Vec<Entry>,
}

impl RaftLogCacheCore {
    pub fn new(buffer_size: usize) -> RaftLogCacheCore {
        let v = vec![Entry::new(); buffer_size];
        RaftLogCacheCore {
            buffer_size: buffer_size,
            start: 0,
            end: 0,
            buffer: v,
        }
    }

    pub fn len(&self) -> usize {
        if self.end >= self.start {
            self.end - self.start
        } else {
            self.buffer_size - self.start + self.end
        }
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn idle_size(&self) -> usize {
        self.max_size() - self.len()
    }

    fn max_size(&self) -> usize {
        self.buffer_size - 1
    }

    fn get_entry(&self, idx: usize) -> Entry {
        self.buffer[idx].clone()
    }

    fn get_entry_index(&self, idx: usize) -> u64 {
        self.buffer[idx].get_index()
    }

    fn calc_idx(&self, index: u64) -> usize {
        let first_index = self.first_index().unwrap();
        let offset = index - first_index;
        (first_index + offset) as usize / self.buffer.len()
    }

    pub fn first_index(&self) -> raft::Result<u64> {
        if self.is_empty() {
            Err(raft::Error::Other(box_err!("empty cache")))
        } else {
            let index = self.get_entry_index(self.start);
            Ok(index)
        }
    }

    pub fn last_index(&self) -> raft::Result<u64> {
        if self.is_empty() {
            Err(raft::Error::Other(box_err!("empty cache")))
        } else {
            let idx = if self.end == 0 {
                self.buffer.len() - 1
            } else {
                self.end - 1
            };
            Ok(self.get_entry_index(idx))
        }
    }

    fn next_idx(&self, idx: usize) -> usize {
        if idx + 1 == self.buffer.len() {
            // rewind to the head of buffer
            0
        } else {
            // next slot of the buffer
            idx + 1
        }
    }

    pub fn reset(&mut self) {
        self.start = 0;
        self.end = 0;
    }

    pub fn compact_to(&mut self, index: u64) {
        if self.is_empty() {
            return;
        }
        let first_index = self.first_index().unwrap();
        if index < first_index {
            return;
        }
        let last_index = self.last_index().unwrap();
        if index > last_index {
            self.reset();
            return;
        }
        self.start = self.calc_idx(index);
    }

    pub fn truncate_to(&mut self, index: u64) {
        if self.is_empty() {
            return;
        }
        let last_index = self.last_index().unwrap();
        if index > last_index {
            return;
        }
        let first_index = self.first_index().unwrap();
        if index < first_index {
            self.reset();
            return;
        }
        self.end = self.calc_idx(index);
    }

    fn free(&mut self) {
        if self.is_empty() {
            return;
        }
        self.start = self.next_idx(self.start)
    }

    fn append_entry(&mut self, entry: Entry) {
        if self.idle_size() == 0 {
            self.free()
        }
        let idx = self.next_idx(self.end);
        self.buffer[idx] = entry;
    }

    fn append_entries(&mut self, entries: &[Entry]) {
        for e in entries {
            self.append_entry(e.clone())
        }
    }

    pub fn append(&mut self, entries: &[Entry]) {
        if entries.is_empty() {
            return;
        }

        let max_size = self.max_size();
        if max_size < entries.len() {
            // clear cache
            self.reset();
            // append as many as possible
            let start_idx = entries.len() - max_size;
            let to_append = &entries[start_idx..];
            self.append_entries(to_append);
            return;
        }

        if self.is_empty() {
            self.append_entries(entries);
            return;
        }

        let first_index = self.first_index().unwrap();
        let last_index = self.last_index().unwrap();
        let first = entries[0].get_index();

        if first < first_index || first > last_index + 1 {
            // clear cache
            self.reset();
            self.append_entries(entries);
            return;
        }

        if first != last_index + 1 {
            self.truncate_to(first);
        }

        self.append_entries(entries);
    }
}


pub struct RaftLogCache {
    core: Arc<RwLock<RaftLogCacheCore>>,
}

impl RaftLogCache {
    pub fn new(buffer_size: usize) -> RaftLogCache {
        RaftLogCache { core: Arc::new(RwLock::new(RaftLogCacheCore::new(buffer_size))) }
    }

    pub fn rl(&self) -> RwLockReadGuard<RaftLogCacheCore> {
        self.core.rl()
    }

    pub fn wl(&self) -> RwLockWriteGuard<RaftLogCacheCore> {
        self.core.wl()
    }

    pub fn entries(&self, low: u64, high: u64, max_size: u64) -> raft::Result<Vec<Entry>> {
        let core = self.rl();
        let first_index = try!(core.first_index());
        let last_index = try!(core.last_index());
        if low < first_index || high > last_index {
            return Err(raft::Error::Other(box_err!("miss cache")));
        }
        let mut res = Vec::with_capacity((high - low) as usize);
        let mut current_size = 0;
        let total = high - low;
        let mut idx = core.calc_idx(low);
        for _ in 0..total {
            let entry = core.get_entry(idx);
            current_size += Message::compute_size(&entry) as u64;
            res.push(entry);
            if current_size > max_size {
                return Ok(res);
            }
            idx = core.next_idx(idx);
        }
        Ok(res)
    }
}
