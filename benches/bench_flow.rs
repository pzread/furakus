//! The benchmarks for the `flow` module.
//!
//! This is only supported by nightly rust now.
//!
//! These benchmarks need to connect to the redis server. The environment variable `TEST_REDIS_URL`
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
use redis::{Connection as RedisConn, PubSub};
use std::{sync, thread};
use test::Bencher;

/// Since the benchs won't be run concurrently, we clean the test database every time to reduce the
/// noise.
macro_rules! flushdb { ($rs:expr) => {
    redis::Cmd::new().arg("FLUSHDB").execute($rs)
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

fn sync_bench(bench: &mut Bencher, chunk_size: usize) {
    let rs = &get_redis_connection();
    flushdb!(rs);

    let flow_a = Flow::new(rs, chunk_size, 1).unwrap();
    let flow_b = Flow::get(rs, &flow_a.id).expect("Can't get the flow from its id.");
    let push_data = vec![1u8; chunk_size];
    let mut pull_data = vec![0u8; chunk_size];

    bench.iter(|| {
        for _ in 0..10 {
            let idx = flow_a.push(None, &push_data).unwrap();
            assert_eq!(flow_b.pull(None, &mut pull_data), Ok((idx, push_data.len())));
        }
    } );
}

#[bench]
fn test_benchmark_sync_64k(bench: &mut Bencher) {
    sync_bench(bench, 64 * 1024);
}

#[bench]
fn test_benchmark_sync_2m(bench: &mut Bencher) {
    sync_bench(bench, 2 * 1024 * 1024);
}

#[bench]
fn test_benchmark_sync_4m(bench: &mut Bencher) {
    sync_bench(bench, 4 * 1024 * 1024);
}

#[bench]
fn test_benchmark_async_64k(bench: &mut Bencher) {
    let rs = &get_redis_connection();
    flushdb!(rs);

    let flow_a = Flow::new(rs, 2 * 1024 * 1024, 0).unwrap();
    let flow_b = Flow::get(rs, &flow_a.id).expect("Can't get the flow from its id.");
    let push_data = vec![1u8; 64 * 1024];
    let mut pull_data = vec![0u8; 2 * 1024 * 1024];

    flow_a.push(None, &push_data).unwrap();
    flow_a.push(None, &push_data).unwrap();
    flow_a.push(None, &push_data).unwrap();
    bench.iter(|| {
        for _ in 0..10 {
            let idx = flow_a.push(None, &push_data).unwrap();
            assert_eq!(flow_b.pull(None, &mut pull_data), Ok((idx - 3, push_data.len())));
        }
    } );
}

#[bench]
fn test_benchmark_sync_pipe(bench: &mut Bencher) {
    flushdb!(&get_redis_connection());

    let (tx, rx) = sync::mpsc::channel();
    let (rtx, rrx) = sync::mpsc::channel();
    let step: i64 = 128;

    let a = thread::spawn(move|| {
        let rs = &get_redis_connection();
        let flow_a = Flow::new(rs, 2 * 1024 * 1024, 1).unwrap();
        let push_data = vec![1u8; 2 * 1024 * 1024];
        tx.send(flow_a.id.clone()).unwrap();

        let mut offset = 0;
        while rrx.recv().unwrap() {
            for idx in offset..(offset + step) {
                assert_eq!(flow_a.push(None, &push_data), Ok((idx)));
            }
            offset += step;
        }
    } );

    let rs = &get_redis_connection();
    let flow_id: String = rx.recv().unwrap();
    let flow_b = Flow::get(rs, &flow_id).expect("Can't get the flow from its id.");
    let mut pull_data = vec![0u8; 2 * 1024 * 1024];

    let mut offset = 0;
    bench.iter(|| {
        rtx.send(true).unwrap();
        for idx in offset..(offset + step) {
            loop {
                match flow_b.pull(None, &mut pull_data) {
                    Ok(ret) => {
                        assert_eq!(ret, (idx, 2 * 1024 * 1024));
                        break;
                    }
                    Err(err) => {
                        assert_eq!(err, Error::Empty);
                        assert_eq!(flow_b.poll(get_redis_pubsub(), None, Some(2)), Ok(()));
                    }
                }
            }
        }
        offset += step;
    } );

    rtx.send(false).unwrap();
    a.join().unwrap();
}
