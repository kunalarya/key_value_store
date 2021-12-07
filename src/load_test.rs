use std::ops::Range;
use std::time::{Duration, Instant};

use anyhow::Result;
use crossbeam::thread;
use rand::prelude::*;
use structopt::clap::arg_enum;

use crate::store::{Blob, Store};

arg_enum! {
    #[derive(Clone, Copy, Debug)]
    pub enum LoadPattern {
        Bursty,
        Consistent,
        Unthrottled
    }
}

// TODO: Add the following to the Bursty arm above.
/// Chance that bursty load will wait.
const BURSTY_PERCENT_LONG_WAITS: f64 = 0.05;

/// Long wait range when "bursting."
const BURSTY_LONG_WAIT_RANGE_US: Range<u64> = 60_000..200_000;

/// Short wait range when "bursting."
const BURSTY_SHORT_WAIT_RANGE_US: Range<u64> = 10..20;

/// Short wait range when under consistent load.
const CONSISTENT_SHORT_WAIT_RANGE_US: Range<u64> = 1..20;

/// Split for reads vs writes (higher -> more reads).
const READ_WRITE_SPLIT: f64 = 0.10;

#[derive(Copy, Clone, Debug)]
pub struct LoadParams {
    pub threads: usize,
    pub load_pattern: LoadPattern,
    pub tot_time: Duration,
}

/// Total number of operations.
pub struct Ops(pub i64);

/// Operations per second.
pub struct OpsPerSec(f64);

/// Performance metrics for a single thread.
pub struct Stats {
    pub ops: Ops,
    pub runtime: Duration,
}

impl Stats {
    pub fn ops_per_sec(&self) -> OpsPerSec {
        OpsPerSec(self.runtime.as_secs_f64() / (self.ops.0 as f64))
    }
}

fn single_tester<S: Store>(mut store: S, load_params: LoadParams) -> Result<Stats> {
    let mut ops = 0;
    let mut rng = rand::thread_rng();

    let start = Instant::now();
    while Instant::now() - start < load_params.tot_time {
        let key = format!("Key{}", rng.gen::<u16>());

        let read_or_write = rng.gen::<f64>() > READ_WRITE_SPLIT;
        if read_or_write {
            store.put(&key, Blob::Str("foo".to_string()))?;
        } else {
            let _ = store.get(&key);
        }
        match load_params.load_pattern {
            LoadPattern::Bursty => {
                // wait a bit, then continue.
                let choose_long_wait = rng.gen::<f64>() > BURSTY_PERCENT_LONG_WAITS;
                if choose_long_wait {
                    std::thread::sleep(Duration::from_micros(
                        rng.gen_range(BURSTY_LONG_WAIT_RANGE_US),
                    ));
                } else {
                    std::thread::sleep(Duration::from_micros(
                        rng.gen_range(BURSTY_SHORT_WAIT_RANGE_US),
                    ));
                }
            }
            LoadPattern::Consistent => {
                std::thread::sleep(Duration::from_micros(
                    rng.gen_range(CONSISTENT_SHORT_WAIT_RANGE_US),
                ));
            }
            LoadPattern::Unthrottled => {}
        }
        ops += 1;
    }
    let end = Instant::now();
    Ok(Stats {
        ops: Ops(ops),
        runtime: end - start,
    })
}

pub fn load_test<S: Store>(mut store: S, load_params: LoadParams) -> Result<Vec<Stats>> {
    let results = thread::scope(|s| {
        let mut handles = Vec::with_capacity(load_params.threads);
        for _ in 0..load_params.threads {
            let thread_store = store.spawn().expect("Could not spawn store.");
            handles.push(s.spawn(|_| single_tester(thread_store, load_params)));
        }
        let mut all_stats = Vec::with_capacity(load_params.threads);
        for h in handles {
            let thread_result = h.join().expect("thread join");
            all_stats.push(thread_result.expect("test results"));
        }
        all_stats
    })
    .unwrap();
    Ok(results)
}
