//! The `flow` module provide interface to create and control flows.
//!
//! The flow maintains how the data be buffered in the backend storage and provide interfaces to
//! access the data.
//!
//! The lifecycle of a flow starts from being created, ends by following two conditions,
//! 1. Being explicitly destroyed.
//! 2. Being removed by the redis server because the flow is idle too long.
//! We assume that the user won't handle a flow for too long. If it happens, the user will lose the
//! flow unexpectedly.

use common::*;
use redis::{self, Client as RedisClient, Commands, Connection as RedisConn, FromRedisValue,
            PipelineCommands};
use std::collections::HashMap;
use std::result::Result as StdResult;

/// Always use these macros to build a redis key.
macro_rules! rskey_flow { ($hash:expr) => { &format!("FLOW@{:x}", $hash) } }

#[derive(Debug)]
pub enum Error {
    /// Some arguments are illegal.
    BadArgument,
}

pub type Result<T> = StdResult<T, Error>;

/// The struct represents the flow.
pub struct Flow<'a> {
    rs: &'a RedisConn,
    pub id: String,
    id_hash: Hash,
    max_chunksize: usize,
}

impl<'a> Flow<'a> {
    /// Create a new `Flow`.
    pub fn new(rs: &RedisConn, max_chunksize: usize) -> Result<Flow> {
        if max_chunksize > MAX_CHUNKSIZE {
            return Err(Error::BadArgument);
        }

        let (id, id_hash) = generate_identifier();
        let flow_rskey = rskey_flow!(id_hash);
        redis::pipe()
            .hset(flow_rskey, "next_index", 0)
            .hset(flow_rskey, "tail_index", 0)
            .hset(flow_rskey, "max_chunksize", max_chunksize)
            .expire(flow_rskey, LONG_TIMEOUT)
            .execute(rs);
        Ok(Flow {
            rs: rs,
            id: id,
            id_hash: id_hash,
            max_chunksize: max_chunksize,
        })
    }

    /// Get the created `Flow` from its id.
    pub fn get<'b>(rs: &'b RedisConn, id: &str) -> Option<Flow<'b>> {
        let id_hash = Hash::get(id);
        let flow_rskey = rskey_flow!(id_hash);
        // Renew the lifetime first, so there should be no race condition.
        rs.expire::<_, i64>(flow_rskey, LONG_TIMEOUT).unwrap();
        // Try to get all metadata of the flow.
        let metadata: HashMap<String, redis::Value> = rs.hgetall(flow_rskey).unwrap();
        if metadata.is_empty() {
            None
        } else {
            let max_chunksize: usize =
                usize::from_redis_value(metadata.get("max_chunksize").unwrap()).unwrap();
            Some(Flow {
                rs: rs,
                id: id.to_string(),
                id_hash: id_hash,
                max_chunksize: max_chunksize,
            })
        }
    }

    pub fn get_max_chunksize(&self) -> usize {
        self.max_chunksize
    }

    /// Push a chunk into the flow. Return the index of the chunk in the flow.
    pub fn push(&self, provider_id: &str, index: Option<i64>, data: &[u8]) -> Result<i64> {
        if data.len() > self.max_chunksize || index.unwrap_or(0) < 0 {
            return Err(Error::BadArgument);
        }
        Ok(0)
    }

    /// Pull a chunk from the flow. Return the size of the chunk.
    pub fn pull(&self, consumer_id: &str, index: Option<i64>, data: &mut [u8]) -> Result<usize> {
        if data.len() < self.max_chunksize || index.unwrap_or(0) < 0 {
            return Err(Error::BadArgument);
        }
        Ok(0)
    }

    /// Poll and wait for the specific indexed chunk being ready.
    ///
    /// It doesn't guarantee that the chunk is always available even if this method reports the
    /// chunk is ready.
    ///
    /// Note this method needs to subscribe the redis channel. So it requires a `redis::Client`
    /// for creating the new pubsub redis connection.
    /// TODO We may extend the `r2d2_redis` to maintain a pubsub redis connection pool.
    pub fn poll(&self, client: &RedisClient, index: i64, timeout: usize) -> Result<()> {
        if index < 0 {
            return Err(Error::BadArgument);
        }
        Ok(())
    }
}
