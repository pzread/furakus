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

    let flow_a = Flow::new(rs, 2 * 1024 * 1024, 1).unwrap();
    assert_eq!(flow_a.get_max_chunksize(), 2 * 1024 * 1024);
    let flow_b = Flow::get(rs, &flow_a.id).expect("Can't get the flow from its id.");
    assert_eq!(flow_b.get_max_chunksize(), 2 * 1024 * 1024);
}

#[test]
fn test_sync_push_and_pop() {
    let rs = &get_redis_connection();
    flushdb!(rs);

    let flow_a = Flow::new(rs, 2 * 1024 * 1024, 1).unwrap();
    let flow_b = Flow::get(rs, &flow_a.id).expect("Can't get the flow from its id.");
    let push_data = vec![1u8; 1000];
    let mut pull_data = vec![0u8; 2 * 1024 * 1024];

    assert_eq!(flow_a.push(None, &push_data), Ok(0));
    assert_eq!(flow_a.push(None, &push_data), Ok(1));
    assert_eq!(flow_a.push(Some(10), &push_data), Ok(10));
    assert_eq!(flow_a.push(Some(3), &push_data), Ok(3));
    assert_eq!(flow_a.push(Some(2), &push_data), Err(Error::Again));
    assert_eq!(flow_b.pull(None, &mut pull_data), Ok((0, 1000)));
    assert_eq!(flow_b.pull(None, &mut pull_data), Ok((1, 1000)));
    assert_eq!(flow_a.push(Some(1), &push_data), Err(Error::BadArgument));
    assert_eq!(flow_a.push(Some(-1), &push_data), Err(Error::BadArgument));
    assert_eq!(flow_b.pull(None, &mut pull_data), Err(Error::Again));
    assert_eq!(flow_a.push(Some(2), &push_data), Ok(2));
    assert_eq!(flow_a.push(None, &push_data), Ok(4));
    assert_eq!(flow_b.pull(Some(3), &mut pull_data), Ok((3, 1000)));
    assert_eq!(flow_a.push(None, &push_data), Err(Error::Again));
    assert_eq!(flow_b.pull(None, &mut pull_data), Ok((2, 1000)));
    assert_eq!(flow_a.push(None, &push_data), Ok(5));
    assert_eq!(flow_a.push(None, &push_data), Ok(6));
    assert_eq!(flow_b.pull(None, &mut pull_data), Ok((4, 1000)));
    assert_eq!(flow_b.pull(Some(-1), &mut pull_data), Err(Error::BadArgument));
    assert_eq!(flow_b.pull(Some(1000), &mut pull_data), Err(Error::OutOfRange));
    assert_eq!(flow_b.pull(Some(1), &mut pull_data), Err(Error::OutOfRange));
    assert_eq!(flow_b.pull(None, &mut pull_data), Ok((5, 1000)));
    assert_eq!(flow_b.pull(None, &mut pull_data), Ok((6, 1000)));
    assert_eq!(flow_b.pull(None, &mut pull_data), Err(Error::Again));
    assert_eq!(flow_a.push(None, &push_data), Ok(7));
    assert_eq!(flow_a.push(None, &push_data), Ok(8));
    assert_eq!(flow_a.push(None, &push_data), Ok(9));
}

#[test]
fn test_async_push_and_pop() {
    let rs = &get_redis_connection();
    flushdb!(rs);

    let flow_a = Flow::new(rs, 2 * 1024 * 1024, 0).unwrap();
    let flow_b = Flow::get(rs, &flow_a.id).expect("Can't get the flow from its id.");
    let push_data = vec![1u8; 1000];
    let mut pull_data = vec![0u8; 2 * 1024 * 1024];

    assert_eq!(flow_a.push(None, &push_data), Ok(0));
    assert_eq!(flow_a.push(None, &push_data), Ok(1));
    assert_eq!(flow_a.push(Some(10), &push_data), Ok(10));
    assert_eq!(flow_a.push(Some(3), &push_data), Ok(3));
    assert_eq!(flow_a.push(Some(2), &push_data), Ok(2));
    assert_eq!(flow_b.pull(None, &mut pull_data), Ok((1, 1000)));
    assert_eq!(flow_b.pull(Some(3), &mut pull_data), Ok((3, 1000)));
    assert_eq!(flow_a.push(Some(1), &push_data), Err(Error::BadArgument));
    assert_eq!(flow_a.push(Some(-1), &push_data), Err(Error::BadArgument));
    assert_eq!(flow_a.push(None, &push_data), Ok(4));
    assert_eq!(flow_b.pull(Some(2), &mut pull_data), Ok((2, 1000)));
    assert_eq!(flow_a.push(None, &push_data), Ok(5));
    assert_eq!(flow_b.pull(None, &mut pull_data), Ok((3, 1000)));
    assert_eq!(flow_a.push(None, &push_data), Ok(6));
    assert_eq!(flow_b.pull(None, &mut pull_data), Ok((4, 1000)));
    assert_eq!(flow_b.pull(Some(-1), &mut pull_data), Err(Error::BadArgument));
    assert_eq!(flow_b.pull(Some(1000), &mut pull_data), Err(Error::OutOfRange));
    assert_eq!(flow_b.pull(Some(1), &mut pull_data), Err(Error::OutOfRange));
    assert_eq!(flow_b.pull(None, &mut pull_data), Ok((4, 1000)));
    assert_eq!(flow_b.pull(Some(6), &mut pull_data), Ok((6, 1000)));
}
