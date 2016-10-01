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
use redis::{Client as RedisClient, Commands, Connection as RedisConn};

macro_rules! flushdb { ($rs:expr) => (redis::Cmd::new().arg("FLUSHDB").execute($rs)) }

fn get_redis_client() -> RedisClient {
    let redis_url: &str = env!("TEST_REDIS_URL");
    redis::Client::open(redis_url).unwrap()
}

fn get_redis_connection(client: &RedisClient) -> RedisConn {
    client.get_connection().expect("Failed to connect to the redis server")
}

#[test]
fn test_create_and_get() {
    let rs = &get_redis_connection(&get_redis_client());
    flushdb!(rs);
    let flow_a = Flow::new(rs, 2 * 1024 * 1024).unwrap();
    assert_eq!(flow_a.get_max_chunksize(), 2 * 1024 * 1024);
    let flow_b = Flow::get(rs, &flow_a.id).expect("Can't get the flow from its id.");
    assert_eq!(flow_b.get_max_chunksize(), 2 * 1024 * 1024);
    flushdb!(rs);
}
