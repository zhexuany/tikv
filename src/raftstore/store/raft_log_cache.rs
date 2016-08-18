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

/// A thread unsafe cache for raft log
/// It's used by peer in order to avoid protobuf decoding raft log data.

use std::vec::Vec;
use kvproto::raft_cmdpb::RaftCmdRequest;
use std::collections::{HashMap, LinkedList};

pub type Uuid = Vec<u8>;

#[derive(Default, Debug)]
pub struct CacheStat {
    pub add: u64,
    pub remove: u64,
    pub hit: u64,
    pub miss: u64,
}

#[derive(Debug)]
pub struct RaftLogCache {
    max_size: usize,
    reqs: HashMap<Uuid, RaftCmdRequest>,
    uuids: LinkedList<Uuid>,
    stat: CacheStat,
}

impl RaftLogCache {
    pub fn new(max_size: usize) -> RaftLogCache {
        let mut map = HashMap::new();
        map.reserve(max_size);
        RaftLogCache {
            max_size: max_size,
            reqs: map,
            uuids: LinkedList::new(),
            stat: Default::default(),
        }
    }

    fn len(&self) -> usize {
        self.uuids.len()
    }

    fn contains(&self, uuid: &Uuid) -> bool {
        self.reqs.contains_key(uuid)
    }

    pub fn get(&mut self, uuid: Uuid) -> Option<RaftCmdRequest> {
        if !self.contains(&uuid) {
            println!("get {:?} miss", &uuid);
            self.stat.miss += 1;
            None
        } else {
            println!("get {:?} hit", &uuid);
            self.stat.hit += 1;
            assert_eq!(self.uuids.front().unwrap(), &uuid);
            // self.remove(&uuid);
            self.remove_first()
        }
    }

    pub fn put(&mut self, uuid: Uuid, req: RaftCmdRequest) {
        println!("log cache to put {:?}: {:?}", &uuid, &req);
        if self.len() >= self.max_size {
            self.remove_first();
        }
        self.stat.add += 1;
        self.uuids.push_back(uuid.to_vec());
        self.reqs.insert(uuid, req);
    }

    fn remove_first(&mut self) -> Option<RaftCmdRequest> {
        let first_uuid = self.uuids.pop_front().unwrap();
        self.stat.remove += 1;
        self.reqs.remove(&first_uuid)
    }

    pub fn clear(&mut self) {
        let count = self.len();
        self.stat.remove += count as u64;
        self.uuids.clear();
        self.reqs.clear();
    }
}
