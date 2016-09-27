//! The tests for the `flow` module.
//!
//! The tests need to connect to the redis server. The environment variable `TEST_REDIS_URL` must
//! be configured for redis server connection before running these tests.

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
    let flow = Flow::new(rs);
    Flow::get(rs, &flow.id).expect("Can't get the flow from its id.");
    flushdb!(rs);
}
