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
macro_rules! rskey_flow_chunk {
    ($hash:expr, $index:expr) => { &format!("FLOW@{:x}@CHUNK@{}", $hash, $index) }
}
macro_rules! rskey_chunk_data { ($id:expr) => { &format!("CHUNK@{:x}", $id) } }

#[derive(Debug, PartialEq)]
pub enum Error {
    /// Some arguments are illegal.
    BadArgument,
    /// Other errors.
    Other,
}

pub type Result<T> = StdResult<T, Error>;

lazy_static! {
    /// The redis script for acquiring and inserting a chunk, and set its provider.
    ///
    /// Arguments:
    /// +. KEYS[1]: The redis key of the flow.
    /// +. ARGV[1]: The provider id.
    /// +. ARGV[2]: The specified index. If there is no specified index, set this to -1.
    /// +. ARGV[3]: The chunk id.
    ///
    /// Return:
    /// If the insertion succeeded, the index of the chunk. If the insertion failed, -1.
    static ref ACQUIRE_CHUNK_SCRIPT: redis::Script = redis::Script::new(r"
        local flow_rskey = KEYS[1]
        local provider_id = ARGV[1]
        local specific_index = tonumber(ARGV[2])
        local chunk_id = tonumber(ARGV[3])
        local flow_chunk_key = nil
        local chunk_index = nil
        if specific_index == -1 then
            local head_index = redis.call('hincrby', flow_rskey, 'head_index', 1)
            flow_chunk_key = flow_rskey .. '@CHUNK@' .. head_index
            if redis.call('hsetnx', flow_chunk_key, 'provider_id', provider_id) ~= 1 then
                error('collision')
            end
            chunk_index = head_index
        else
            local head_index = tonumber(redis.call('hget', flow_rskey, 'head_index'))
            flow_chunk_key = flow_rskey .. '@CHUNK@' .. specific_index
            if specific_index <= head_index then
                return -1
            end
            if redis.call('hsetnx', flow_chunk_key, 'provider_id', provider_id) == 0 then
                return -1
            end
            if specific_index == head_index + 1 then
                local next_index = head_index + 2
                while redis.call('exists', flow_rskey .. '@CHUNK@' .. next_index) == 1 do
                    next_index = next_index + 1
                end
                redis.call('hset', flow_rskey, 'head_index', next_index - 1)
            end
            chunk_index = specific_index
        end
        redis.call('hset', flow_chunk_key, 'chunk_id', chunk_id)
        return chunk_index
    ");
}

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
            .hset(flow_rskey, "head_index", -1)
            .hset(flow_rskey, "tail_index", 0)
            .hset(flow_rskey, "chunk_last_id", -1)
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

    /// Push a chunk into the flow. Return the chunk index.
    pub fn push(&self, provider_id: &str, index: Option<i64>, data: &[u8]) -> Result<i64> {
        if data.len() > self.max_chunksize || index.unwrap_or(0) < 0 {
            return Err(Error::BadArgument);
        }
        let flow_rskey = rskey_flow!(self.id_hash);
        let chunk_id: i64 = self.rs.incr("CHUNK_LAST_ID", 1).unwrap();
        let chunk_data_rskey = rskey_chunk_data!(chunk_id);
        // Store the chunk data first, then remove it if the insertion failed.
        // Therefore, once the insertion succeeded, the chunk will be ready.
        self.rs.set_ex::<_, _, bool>(chunk_data_rskey, data, SHORT_TIMEOUT).unwrap();
        // Try to acquire the chunk.
        ACQUIRE_CHUNK_SCRIPT
            .key(&*flow_rskey)
            .arg(provider_id)
            .arg(index.unwrap_or(-1))
            .arg(chunk_id)
            .invoke::<i64>(self.rs)
            .or(Err(Error::Other))
            .and_then(|index| {
                if index < 0 {
                    Err(Error::BadArgument)
                } else {
                    // Set chunk expiration.
                    self.rs.expire::<_, i64>(
                        rskey_flow_chunk!(self.id_hash, index), SHORT_TIMEOUT).unwrap();
                    Ok(index)
                }
            })
            .map_err(|err| {
                // The insertion failed, remove the chunk data.
                self.rs.del::<_, i64>(chunk_data_rskey).unwrap();
                err
            })
    }

    /// Pull a chunk from the flow. Return the chunk size.
    pub fn pull(&self, consumer_id: &str, index: Option<i64>, data: &mut [u8]) -> Result<usize> {
        if data.len() < self.max_chunksize || index.unwrap_or(0) < 0 {
            return Err(Error::BadArgument);
        }
        let flow_rskey = rskey_flow!(self.id_hash);
        Ok(0)
    }

    /// Poll and wait for the specific indexed chunk being ready.
    ///
    /// It doesn't guarantee that the chunk is always available even if this method reports the
    /// chunk is ready.
    pub fn poll(&self, index: i64, timeout: usize) -> Result<()> {
        if index < 0 {
            return Err(Error::BadArgument);
        }
        Ok(())
    }
}
