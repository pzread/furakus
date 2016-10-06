//! The benchmarks for the `flow` module.
//!
//! The benchmarks need to connect to the redis server. The environment variable `TEST_REDIS_URL`
//! must be set for redis server connection before running these benchmarks. For example,
//! ```
//! TEST_REDIS_URL='redis://localhost:6379/15' cargo bench
//! ```

#![feature(test)]

extern crate dotenv;
extern crate flux;
extern crate redis;
extern crate test;

use flux::flow::*;
use redis::Connection as RedisConn;
use std::sync::{Once, ONCE_INIT};
use test::Bencher;

static FLUSHDB: Once = ONCE_INIT;

/// Since the tests will be run concurrently, we only clean the test database for the first time.
macro_rules! flushdb { ($rs:expr) => {
    FLUSHDB.call_once(|| {
        redis::Cmd::new().arg("FLUSHDB").execute($rs)
    })
} }

fn get_redis_connection() -> RedisConn {
    let redis_url: &str = env!("TEST_REDIS_URL");
    let client = redis::Client::open(redis_url).unwrap();
    client.get_connection().expect("Failed to connect to the redis server")
}

#[bench]
fn test_benchmark_sync(bench: &mut Bencher) {
    let rs = &get_redis_connection();
    flushdb!(rs);

    let flow_a = Flow::new(rs, 4 * 1024 * 1024, Some(&["ross", "alex"])).unwrap();
    let flow_b = Flow::get(rs, &flow_a.id).expect("Can't get the flow from its id.");
    let push_data = vec![1u8; 2 * 1024 * 1024];
    let mut pull_data = vec![0u8; 4 * 1024 * 1024];

    assert_eq!(flow_a.push("alex", None, &push_data), Ok(0));
    bench.iter(|| {
        assert_eq!(flow_b.pull("ross", None, &mut pull_data), Ok((0, 2 * 1024 * 1024, "alex".to_owned())));
    });
}
