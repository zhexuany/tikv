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

use prometheus::{CounterVec, Histogram, exponential_buckets};

lazy_static! {
    pub static ref CHANNEL_FULL_COUNTER_VEC: CounterVec =
        register_counter_vec!(
            "tikv_channel_full_total",
            "Total number of channel full errors.",
            &["type"]
        ).unwrap();

    pub static ref PIPBUF_ENSURE_SIZE_HISTOGRAM: Histogram =
        register_histogram!(
            "tikv_pipbuf_ensure_size",
            "Histogram of ensure size for each pip buffer",
            exponential_buckets(1024.0, 2.0, 20).unwrap()
        ).unwrap();
}
