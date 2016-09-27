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


use std::sync::atomic::{AtomicU64, Ordering, ATOMIC_U64_INIT};
use rocksdb::CompactionFilter;
use storage::mvcc::{Write, WriteType};
use storage::Key;

static COMMIT_SAFE_POINT: AtomicU64 = ATOMIC_U64_INIT;
static START_SAFE_POINT: AtomicU64 = ATOMIC_U64_INIT;

pub fn update_commit_safe_point(new_safe_point: u64) {
    COMMIT_SAFE_POINT.store(new_safe_point, Ordering::Relaxed);
}

pub fn update_start_safe_point(new_safe_point: u64) {
    START_SAFE_POINT.store(new_safe_point, Ordering::Relaxed);
}

const UPDATE_SP_INTERVAL: u32 = 4096;

pub struct WriteGCFilter {
    current_key: Vec<u8>,
    reach_too_old: bool,
    commit_safe_point: u64,
    key_count: u32,
}

impl WriteGCFilter {
    #[warn(dead_code)]
    fn new() -> WriteGCFilter {
        WriteGCFilter {
            current_key: vec![],
            reach_too_old: false,
            commit_safe_point: 0,
            key_count: 0,
        }
    }
}

impl CompactionFilter for WriteGCFilter {
    fn filter(&mut self, _: usize, key: &[u8], value: &[u8]) -> bool {
        if self.key_count >= UPDATE_SP_INTERVAL {
            self.commit_safe_point = COMMIT_SAFE_POINT.load(Ordering::Relaxed);
            self.key_count = 0;
        }
        self.key_count += 1;

        let write_key = Key::from_encoded(key.to_vec());
        let commit_ts = match write_key.decode_ts() {
            Ok(ts) => ts,
            Err(_) => return true, // delete illegal data.
        };
        let k = match write_key.truncate_ts() {
            Ok(k) => k,
            Err(_) => return true, // delete illegal data.
        };
        if k.encoded() != &self.current_key {
            self.current_key = k.encoded().clone();;
            self.reach_too_old = false;
        }
        let write = match Write::parse(value) {
            Ok(write) => write,
            Err(_) => return true, // delete illegal data.
        };

        if self.reach_too_old {
            return true;
        }

        if commit_ts <= self.commit_safe_point {
            match write.write_type {
                WriteType::Put | WriteType::Delete => {
                    self.reach_too_old = true;
                }
                WriteType::Rollback | WriteType::Lock => {}
            }

            match write.write_type {
                WriteType::Delete | WriteType::Rollback | WriteType::Lock => {
                    return true;
                }
                WriteType::Put => {}
            }
        }

        false
    }
}

pub struct DataGCFilter {
    current_key: Vec<u8>,
    reach_too_old: bool,
    start_safe_point: u64,
    key_count: u32,
}

impl DataGCFilter {
    #[warn(dead_code)]
    fn new() -> DataGCFilter {
        DataGCFilter {
            current_key: vec![],
            reach_too_old: false,
            start_safe_point: 0,
            key_count: 0,
        }
    }
}

impl CompactionFilter for DataGCFilter {
    fn filter(&mut self, _: usize, key: &[u8], value: &[u8]) -> bool {
        if self.key_count >= UPDATE_SP_INTERVAL {
            self.start_safe_point = START_SAFE_POINT.load(Ordering::Relaxed);
            self.key_count = 0;
        }
        self.key_count += 1;

        let data_key = Key::from_encoded(key.to_vec());
        let start_ts = match data_key.decode_ts() {
            Ok(ts) => ts,
            Err(_) => return true, // delete illegal data
        };
        let k = match data_key.truncate_ts() {
            Ok(k) => k,
            Err(_) => return true, // delete illegal data
        };
        if k.encoded() != &self.current_key {
            self.current_key = k.encoded().clone();
            self.reach_too_old = false;
        }

        if self.reach_too_old {
            return true;
        }

        if start_ts <= self.start_safe_point {
            self.reach_too_old = true;

            if value.is_empty() {
                return true;
            }
        }

        false
    }
}

