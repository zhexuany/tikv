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

use std::sync::{Mutex, MutexGuard};
use raft;

const LOG_CALL_TIMES: u64 = 500;

#[derive(Debug, Default, Clone)]
pub struct CacheStat {
    pub call: u64,
    pub valid: u64,
    pub hit: u64,
    pub miss: u64,
    pub append: u64,
    pub reset: u64,
}

pub struct RaftLogCacheCore {
    tag: String,
    buffer_size: usize,
    start: usize,
    end: usize,
    buffer: Vec<Entry>,
    stat: CacheStat,
}

impl RaftLogCacheCore {
    pub fn new(tag: String, buffer_size: usize) -> RaftLogCacheCore {
        let v = vec![Entry::new(); buffer_size];
        RaftLogCacheCore {
            tag: tag,
            buffer_size: buffer_size,
            start: 0,
            end: 0,
            buffer: v,
            stat: CacheStat::default(),
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
        let offset = (index - first_index) as usize;
        (self.start + offset) % self.buffer.len()
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
        self.stat.reset += 1;

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

        self.buffer[self.end] = entry;
        self.end = self.next_idx(self.end);
    }

    fn append_entries(&mut self, entries: &[Entry]) {
        self.stat.append += 1;
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

    pub fn entries(&mut self, low: u64, high: u64, max_size: u64) -> raft::Result<Vec<Entry>> {
        self.stat.call += 1;
        if self.stat.call % LOG_CALL_TIMES == 0 {
            info!("{} raft log cache stat: {:?}", self.tag, self.stat);
        }
        if low == high {
            return Ok(vec![]);
        }
        let first_index = try!(self.first_index());
        let last_index = try!(self.last_index());
        self.stat.valid += 1;
        if low < first_index || high > last_index + 1 {
            self.stat.miss += 1;
            return Err(raft::Error::Other(box_err!("miss cache")));
        }
        let mut res = Vec::with_capacity((high - low) as usize);
        let mut current_size = 0;
        let total = high - low;
        let mut idx = self.calc_idx(low);
        let entry = self.get_entry(idx);
        current_size += Message::compute_size(&entry) as u64;
        res.push(entry);
        for _ in 1..total {
            idx = self.next_idx(idx);
            let entry = self.get_entry(idx);
            current_size += Message::compute_size(&entry) as u64;
            if current_size > max_size {
                self.stat.hit += 1;
                return Ok(res);
            }
            res.push(entry);
        }
        self.stat.hit += 1;
        Ok(res)
    }
}


pub struct RaftLogCache {
    core: Mutex<RaftLogCacheCore>,
}

impl RaftLogCache {
    pub fn new(tag: String, buffer_size: usize) -> RaftLogCache {
        RaftLogCache { core: Mutex::new(RaftLogCacheCore::new(tag, buffer_size)) }
    }

    pub fn get(&self) -> MutexGuard<RaftLogCacheCore> {
        self.core.lock().unwrap()
    }

    pub fn entries(&self, low: u64, high: u64, max_size: u64) -> raft::Result<Vec<Entry>> {
        let mut core = self.get();
        core.entries(low, high, max_size)
    }
}


#[cfg(test)]
mod test {
    use protobuf;
    use kvproto::eraftpb::Entry;
    use super::*;

    fn new_entry(index: u64, term: u64) -> Entry {
        let mut e = Entry::new();
        e.set_index(index);
        e.set_term(term);
        e
    }

    fn size_of<T: protobuf::Message>(m: &T) -> u32 {
        m.compute_size()
    }

    #[test]
    fn test_raft_log_cache() {
        let buffer_size = 5;
        let tag = String::from("test");
        // Initialize the test cases
        let entries = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5), new_entry(6, 6)];
        let max_u64 = u64::max_value();
        let mut tests =
            vec![// check bounds
                 (2, 6, max_u64, true, vec![]),
                 (3, 8, max_u64, true, vec![]),
                 //
                 (4, 5, max_u64, false, vec![new_entry(4, 4)]),
                 (4, 6, max_u64, false, vec![new_entry(4, 4), new_entry(5, 5)]),
                 (4, 7, max_u64, false, vec![new_entry(4, 4), new_entry(5, 5), new_entry(6, 6)]),
                 // even if maxsize is zero, the first entry should be returned
                 (4, 7, 0, false, vec![new_entry(4, 4)]),
                 // limit size
                 (4,
                  7,
                  (size_of(&entries[1]) + size_of(&entries[2])) as u64,
                  false,
                  vec![new_entry(4, 4), new_entry(5, 5)])];
        // Initialize the cache
        let cache = RaftLogCache::new(tag, buffer_size);
        assert!(cache.get().is_empty());
        if let Ok(i) = cache.get().first_index() {
            panic!("unexpect first index {}", i);
        }
        if let Ok(i) = cache.get().last_index() {
            panic!("unexpect first index {}", i);
        }
        cache.get().append(&entries);
        assert!(!cache.get().is_empty());
        assert_eq!(cache.get().len(), entries.len());
        assert_eq!(cache.get().first_index(), Ok(3));
        assert_eq!(cache.get().last_index(), Ok(6));

        // Run the test cases
        for (i, (low, high, maxsize, werror, wentries)) in tests.drain(..).enumerate() {
            let r = cache.entries(low, high, maxsize);
            if werror {
                if let Ok(ents) = r {
                    panic!("#{} expect error but got {:?}", i, ents);
                }
            } else {
                if let Err(e) = r {
                    panic!("#{} unexpect error {:?}", i, e);
                }
                let ents = r.unwrap();
                if ents != wentries {
                    panic!("#{}: expect entries {:?}, got {:?}", i, wentries, ents);
                }
            }
        }

        // Ensure compaction and truncation works
        cache.get().compact_to(4);
        assert_eq!(cache.get().len(), entries.len() - 1);
        assert_eq!(cache.get().first_index(), Ok(4));
        assert_eq!(cache.get().last_index(), Ok(6));

        cache.get().truncate_to(6);
        assert_eq!(cache.get().len(), entries.len() - 2);
        assert_eq!(cache.get().first_index(), Ok(4));
        assert_eq!(cache.get().last_index(), Ok(5));

        // Append entries which outnumbers cache max_size
        let more = (buffer_size + 1) as u64;
        let last_index = cache.get().last_index().unwrap();
        let mut entries = vec![];
        for i in 0..more {
            let index = last_index + 1 + i;
            entries.push(new_entry(index, index));
        }
        cache.get().append(&entries);
        let max_size = buffer_size - 1;
        assert_eq!(cache.get().len(), max_size);
        assert_eq!(cache.get().first_index(),
                   Ok(last_index + more + 1 - max_size as u64));
        assert_eq!(cache.get().last_index(), Ok(last_index + more));
    }
}
