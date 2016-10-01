//! The tests for the `flow` module.
//!
//! The tests need to connect to the redis server. The environment variable `TEST_REDIS_URL` must
//! be set for redis server connection before running these tests. For example,
//! ```
//! TEST_REDIS_URL='redis://localhost:6379/15' cargo test
//! ```

extern crate dotenv;
extern crate flux;
extern crate redis;

use flux::flow::*;
use redis::Connection as RedisConn;
use std::sync::{Once, ONCE_INIT};

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

#[test]
fn test_create_and_get() {
    let rs = &get_redis_connection();
    flushdb!(rs);

    let flow_a = Flow::new(rs, 2 * 1024 * 1024).unwrap();
    assert_eq!(flow_a.get_max_chunksize(), 2 * 1024 * 1024);
    let flow_b = Flow::get(rs, &flow_a.id).expect("Can't get the flow from its id.");
    assert_eq!(flow_b.get_max_chunksize(), 2 * 1024 * 1024);
}

#[test]
fn test_push_and_pop() {
    let rs = &get_redis_connection();
    flushdb!(rs);

    let flow = Flow::new(rs, 2 * 1024 * 1024).unwrap();
    let data = vec![1u8; 1000];
    assert_eq!(flow.push("alex", None, &data), Ok(0));
    assert_eq!(flow.push("bob", None, &data), Ok(1));
    assert_eq!(flow.push("alex", Some(10), &data), Ok(10));
    assert_eq!(flow.push("alex", Some(2), &data), Ok(2));
    assert_eq!(flow.push("bob", None, &data), Ok(3));
    assert_eq!(flow.push("bob", Some(1), &data), Err(Error::BadArgument));
}
