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
use redis::{Connection as RedisConn, PubSub};
use std::sync::{self, Once, ONCE_INIT};
use std::{thread, time};

static FLUSHDB: Once = ONCE_INIT;

/// Since the tests will be run concurrently, we only clean the test database for the first time.
macro_rules! flushdb { ($rs:expr) => {
    FLUSHDB.call_once(|| {
        redis::Cmd::new().arg("FLUSHDB").execute($rs)
    } )
} }

fn get_client() -> redis::Client {
    let redis_url: &str = env!("TEST_REDIS_URL");
    redis::Client::open(redis_url).unwrap()
}

fn get_redis_connection() -> RedisConn {
    get_client().get_connection().expect("Failed to connect to the redis server")
}

fn get_redis_pubsub() -> PubSub {
    get_client().get_pubsub().expect("Failed to connect to the redis server")
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
    assert_eq!(flow_b.get_range(), Ok((0, 1)));
    assert_eq!(flow_b.pull(None, &mut pull_data), Ok((0, 1000)));
    assert_eq!(flow_b.pull(None, &mut pull_data), Ok((1, 1000)));
    assert_eq!(flow_b.get_range(), Err(Error::Empty));
    assert_eq!(flow_a.push(Some(1), &push_data), Err(Error::BadArgument));
    assert_eq!(flow_a.push(Some(-1), &push_data), Err(Error::BadArgument));
    assert_eq!(flow_b.pull(None, &mut pull_data), Err(Error::Empty));
    assert_eq!(flow_a.push(Some(2), &push_data), Ok(2));
    assert_eq!(flow_b.get_range(), Ok((2, 3)));
    assert_eq!(flow_a.push(None, &push_data), Ok(4));
    assert_eq!(flow_b.pull(Some(3), &mut pull_data), Ok((3, 1000)));
    assert_eq!(flow_b.pull(None, &mut pull_data), Ok((2, 1000)));
    assert_eq!(flow_a.push(None, &push_data), Ok(5));
    assert_eq!(flow_a.push(None, &push_data), Ok(6));
    assert_eq!(flow_b.pull(None, &mut pull_data), Ok((4, 1000)));
    assert_eq!(flow_b.pull(Some(-1), &mut pull_data), Err(Error::BadArgument));
    assert_eq!(flow_b.pull(Some(1000), &mut pull_data), Err(Error::OutOfRange));
    assert_eq!(flow_b.pull(Some(1), &mut pull_data), Err(Error::OutOfRange));
    assert_eq!(flow_b.pull(None, &mut pull_data), Ok((5, 1000)));
    assert_eq!(flow_b.pull(None, &mut pull_data), Ok((6, 1000)));
    assert_eq!(flow_b.pull(None, &mut pull_data), Err(Error::Empty));
    assert_eq!(flow_a.push(None, &push_data), Ok(7));
    assert_eq!(flow_a.push(None, &push_data), Ok(8));
    assert_eq!(flow_a.push(None, &push_data), Ok(9));
    assert_eq!(flow_b.get_range(), Ok((7, 10)));
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
    assert_eq!(flow_b.get_range(), Ok((1, 3)));
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

#[test]
fn test_sync_wait_and_poll() {
    flushdb!(&get_redis_connection());

    let (tx, rx) = sync::mpsc::channel();

    let a = thread::spawn(move|| {
        let rs = &get_redis_connection();
        let flow_a = Flow::new(rs, 2 * 1024 * 1024, 1).unwrap();
        let push_data = vec![1u8; 1000];
        tx.send(flow_a.id.clone()).unwrap();

        assert_eq!(flow_a.push(None, &push_data), Ok((0)));
        assert_eq!(flow_a.push(None, &push_data), Ok((1)));
        assert_eq!(flow_a.push(None, &push_data), Ok((2)));
        assert_eq!(flow_a.push(None, &push_data), Ok((3)));
        assert_eq!(flow_a.push(None, &push_data), Ok((4)));
        assert_eq!(flow_a.push(Some(7), &push_data), Ok((7)));
        thread::sleep(time::Duration::from_millis(500));
        assert_eq!(flow_a.push(None, &push_data), Ok((5)));
        thread::sleep(time::Duration::from_millis(500));
        assert_eq!(flow_a.push(None, &push_data), Ok((6)));
        // TODO Lack of timeout tests.
    } );

    let b = thread::spawn(move|| {
        let rs = &get_redis_connection();
        let flow_id: String = rx.recv().unwrap();
        let flow_b = Flow::get(rs, &flow_id).expect("Can't get the flow from its id.");
        let mut pull_data = vec![0u8; 2 * 1024 * 1024];

        thread::sleep(time::Duration::from_millis(500));
        assert_eq!(flow_b.pull(None, &mut pull_data), Ok((0, 1000)));
        assert_eq!(flow_b.pull(None, &mut pull_data), Ok((1, 1000)));
        assert_eq!(flow_b.pull(None, &mut pull_data), Ok((2, 1000)));
        assert_eq!(flow_b.pull(None, &mut pull_data), Ok((3, 1000)));
        assert_eq!(flow_b.pull(Some(5), &mut pull_data), Err(Error::OutOfRange));
        assert_eq!(flow_b.poll(get_redis_pubsub(), Some(5), Some(2)), Ok(()));
        assert_eq!(flow_b.pull(Some(5), &mut pull_data), Ok((5, 1000)));
        assert_eq!(flow_b.pull(Some(7), &mut pull_data), Err(Error::OutOfRange));
        assert_eq!(flow_b.poll(get_redis_pubsub(), Some(7), Some(2)), Ok(()));
        assert_eq!(flow_b.pull(Some(7), &mut pull_data), Ok((7, 1000)));
    } );

    a.join().unwrap();
    b.join().unwrap();
}

#[test]
fn test_async_poll() {
    flushdb!(&get_redis_connection());

    let (tx, rx) = sync::mpsc::channel();

    let a = thread::spawn(move|| {
        let rs = &get_redis_connection();
        let flow_a = Flow::new(rs, 2 * 1024 * 1024, 0).unwrap();
        let push_data = vec![1u8; 1000];
        tx.send(flow_a.id.clone()).unwrap();

        thread::sleep(time::Duration::from_millis(500));
        assert_eq!(flow_a.push(None, &push_data), Ok((0)));
        assert_eq!(flow_a.push(None, &push_data), Ok((1)));
        assert_eq!(flow_a.push(None, &push_data), Ok((2)));
        assert_eq!(flow_a.push(None, &push_data), Ok((3)));
        thread::sleep(time::Duration::from_millis(500));
        assert_eq!(flow_a.push(None, &push_data), Ok((4)));
        // TODO Lack of timeout tests.
    } );

    let b = thread::spawn(move|| {
        let rs = &get_redis_connection();
        let flow_id: String = rx.recv().unwrap();
        let flow_b = Flow::get(rs, &flow_id).expect("Can't get the flow from its id.");
        let mut pull_data = vec![0u8; 2 * 1024 * 1024];

        assert_eq!(flow_b.pull(None, &mut pull_data), Err(Error::Empty));
        assert_eq!(flow_b.poll(get_redis_pubsub(), None, Some(2)), Ok(()));
        assert_eq!(flow_b.pull(None, &mut pull_data), Ok((0, 1000)));
        assert_eq!(flow_b.poll(get_redis_pubsub(), Some(4), Some(2)), Ok(()));
        assert_eq!(flow_b.pull(Some(4), &mut pull_data), Ok((4, 1000)));
    } );

    a.join().unwrap();
    b.join().unwrap();
}
