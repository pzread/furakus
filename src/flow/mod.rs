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
use redis::{self, Commands, Connection as RedisConn, FromRedisValue, PipelineCommands};
use std::collections::HashMap;
use std::result::Result as StdResult;

/// Always use these macros to build a redis key.
macro_rules! rskey_flow { ($hash:expr) => { &format!("FLOW@{:x}", $hash) } }
macro_rules! rskey_flow_chunk {
    ($hash:expr, $index:expr) => { &format!("FLOW@{:x}@CHUNK@{}", $hash, $index) }
}
macro_rules! rskey_chunk_data { ($id:expr) => { &format!("CHUNK@{}", $id) } }

#[derive(Debug, PartialEq)]
pub enum Error {
    /// Some arguments are illegal.
    BadArgument,
    /// OutOfRange,
    OutOfRange,
    /// Again,
    Again,
    /// Other errors.
    Other,
}

pub type Result<T> = StdResult<T, Error>;

lazy_static! {
    /// The redis script for acquiring and inserting a chunk.
    ///
    /// Once the transcation finished, the chunk is ready.
    ///
    /// Arguments:
    /// +. KEYS[1]: The redis key of the flow.
    /// +. ARGV[1]: The specified index. If there is no specified index, set this to -1.
    /// +. ARGV[2]: The chunk id.
    /// +. ARGV[3]: The pull limit.
    ///
    /// Return:
    /// If the insertion succeeded, the index of the chunk. If the insertion failed, -1. If there
    /// is no available chunk, -2.
    static ref ACQUIRE_CHUNK_SCRIPT: redis::Script = redis::Script::new(&format!(r"
        local flow_key = KEYS[1]
        local specific_index = tonumber(ARGV[1])
        local chunk_id = tonumber(ARGV[2])
        local pull_limit = tonumber(ARGV[3])
        local flow_chunk_key = nil
        local chunk_index = nil
        local ret

        ret = redis.call('hmget', flow_key, 'tail_index', 'head_index')
        local tail_index = tonumber(ret[1])
        local head_index = tonumber(ret[2])

        if tonumber(redis.call('hget', flow_key, 'avail_chunk')) <= 0 then
            -- Try slow recycling.
            if tail_index > head_index then
                return -2
            end
            local tail_flow_chunk_key = flow_key .. '@CHUNK@' .. tail_index
            ret = redis.call('hmget', tail_flow_chunk_key, 'pull_count', 'chunk_id')
            if tonumber(ret[1]) < pull_limit then
                return -2
            end
            redis.call('del', 'CHUNK@' .. ret[2], tail_flow_chunk_key)
            redis.call('hincrby', flow_key, 'avail_chunk', 1)
            redis.call('hincrby', flow_key, 'tail_index', 1)
        end

        if specific_index == -1 then
            local next_index = redis.call('hincrby', flow_key, 'head_index', 1)
            flow_chunk_key = flow_key .. '@CHUNK@' .. next_index
            if redis.call('hsetnx', flow_chunk_key, 'chunk_id', chunk_id) ~= 1 then
                error('collision')
            end
            chunk_index = next_index
        else
            flow_chunk_key = flow_key .. '@CHUNK@' .. specific_index
            if specific_index <= head_index then
                return -1
            end
            if redis.call('hsetnx', flow_chunk_key, 'chunk_id', chunk_id) == 0 then
                return -1
            end
            if specific_index == head_index + 1 then
                local next_index = head_index + 2
                while redis.call('exists', flow_key .. '@CHUNK@' .. next_index) == 1 do
                    next_index = next_index + 1
                end
                redis.call('hset', flow_key, 'head_index', next_index - 1)
            end
            chunk_index = specific_index
        end

        redis.call('hincrby', flow_key, 'avail_chunk', -1)
        redis.call('hset', flow_chunk_key, 'pull_count', 0)
        redis.call('expire', flow_chunk_key, {})
        return chunk_index
    ", SHORT_TIMEOUT));

    /// The redis script for pulling the chunk.
    ///
    /// It will update the pull count of the chunk and check if it's able to be removed.
    /// If available, it will remove the chunk metadata and ask the caller to remove the chunk
    /// data.
    ///
    /// Arguments:
    /// +. KEYS[1]: The redis key of the flow.
    /// +. ARGV[1]: The specified index. If there is no specified index, set this to -1.
    /// +. ARGV[2]: The pull limit.
    ///
    /// Return:
    /// If the pulling succeeded, (chunk index, chunk id, removable). If there is no available
    /// chunk, -1. If the specific index is out of range, -2.
    static ref PULL_SCRIPT: redis::Script = redis::Script::new(r"
        local flow_key = KEYS[1]
        local index = tonumber(ARGV[1])
        local pull_limit = tonumber(ARGV[2])

        local ret = redis.call('hmget', flow_key, 'tail_index', 'head_index')
        local tail_index = tonumber(ret[1])
        local head_index = tonumber(ret[2])
        if index == -1 then
            if tail_index > head_index then
                return -1
            end
            index = tail_index
        else
            if index < tail_index or index > head_index then
                return -2
            end
        end

        local flow_chunk_key = flow_key .. '@CHUNK@' .. index
        local chunk_id = redis.call('hget', flow_chunk_key, 'chunk_id')
        local removable = false
        if redis.call('hincrby', flow_chunk_key, 'pull_count', 1) == pull_limit then
            if index == tail_index and pull_limit > 0 then
                -- Do continuous fast recycling.
                redis.call('del', flow_chunk_key)
                redis.call('hincrby', flow_key, 'avail_chunk', 1)
                redis.call('hincrby', flow_key, 'tail_index', 1)
                removable = true
            end
        end
        return {index, chunk_id, removable}
    ");
}

/// The struct represents the flow.
pub struct Flow<'a> {
    rs: &'a RedisConn,
    pub id: String,
    id_hash: Hash,
    max_chunksize: usize,
    pull_limit: i64,
}

impl<'a> Flow<'a> {
    /// Create a new `Flow`.
    ///
    /// If `pull_limit` is 0, the flow will be asynchronous.
    pub fn new(rs: &RedisConn, max_chunksize: usize, pull_limit: i64) -> Result<Flow> {
        if max_chunksize == 0 ||  max_chunksize > MAX_CHUNKSIZE || pull_limit < 0 {
            return Err(Error::BadArgument);
        }
        let (id, id_hash) = generate_identifier();
        let flow_rskey = rskey_flow!(id_hash);
        redis::pipe()
            .hset(flow_rskey, "head_index", -1)
            .hset(flow_rskey, "tail_index", 0)
            .hset(flow_rskey, "max_chunksize", max_chunksize)
            .hset(flow_rskey, "pull_limit", pull_limit)
            .hset(flow_rskey, "avail_chunk", MAX_CHUNKSIZE * 2 / max_chunksize)
            .expire(flow_rskey, LONG_TIMEOUT)
            .execute(rs);
        Ok(Flow {
            rs: rs,
            id: id,
            id_hash: id_hash,
            max_chunksize: max_chunksize,
            pull_limit: pull_limit,
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
            let max_chunksize =
                usize::from_redis_value(metadata.get("max_chunksize").unwrap()).unwrap();
            let pull_limit =
                i64::from_redis_value(metadata.get("pull_limit").unwrap()).unwrap();
            Some(Flow {
                rs: rs,
                id: id.to_string(),
                id_hash: id_hash,
                max_chunksize: max_chunksize,
                pull_limit: pull_limit,
            })
        }
    }

    pub fn get_max_chunksize(&self) -> usize {
        self.max_chunksize
    }

    /// Push a chunk into the flow. Return the chunk index.
    pub fn push(&self, index: Option<i64>, data: &[u8]) -> Result<i64> {
        if data.len() == 0 || data.len() > self.max_chunksize || index.unwrap_or(0) < 0 {
            return Err(Error::BadArgument);
        }

        let flow_rskey = rskey_flow!(self.id_hash);
        // Get the unique chunk id.
        let chunk_id: i64 = self.rs.incr("CHUNK_LAST_ID", 1).unwrap();
        let chunk_data_rskey = rskey_chunk_data!(chunk_id);

        // Store the chunk data first, then remove it if the insertion failed.
        // Therefore, once the insertion succeeded, the chunk will be ready.
        self.rs.set_ex::<_, _, bool>(chunk_data_rskey, data, SHORT_TIMEOUT).unwrap();

        // Try to acquire the chunk.
        ACQUIRE_CHUNK_SCRIPT
            .key(&*flow_rskey)
            .arg(index.unwrap_or(-1))
            .arg(chunk_id)
            .arg(self.pull_limit)
            .invoke::<i64>(self.rs)
            .or(Err(Error::Other))
            .and_then(|index| match index {
                -2 => Err(Error::Again),
                -1 => Err(Error::BadArgument),
                _ => Ok(index)
            })
            .map_err(|err| {
                // Insertion failed, remove the chunk data.
                self.rs.del::<_, i64>(chunk_data_rskey).unwrap();
                err
            })
    }

    /// Pull a chunk from the flow. Return the chunk index and chunk size.
    pub fn pull(&self, index: Option<i64>, data: &mut [u8]) -> Result<(i64, usize)> {
        if data.len() < self.max_chunksize || index.unwrap_or(0) < 0 {
            return Err(Error::BadArgument);
        }

        // Try to get the chunk.
        let flow_rskey = rskey_flow!(self.id_hash);
        PULL_SCRIPT
            .key(&*flow_rskey)
            .arg(index.unwrap_or(-1))
            .arg(self.pull_limit)
            .invoke::<redis::Value>(self.rs)
            .or(Err(Error::Other))
            .and_then(|result| {
                redis::from_redis_value::<(i64, i64, bool)>(&result)
                    .map_err(|_| match i64::from_redis_value(&result).unwrap() {
                        -2 => Error::OutOfRange,
                        -1 => Error::Again,
                        _ => Error::Other
                    })
            })
            .and_then(|(chunk_index, chunk_id, removable)| {
                let chunk_data_rskey = rskey_chunk_data!(chunk_id);
                let chunk_data: Vec<u8> = if removable {
                    redis::pipe()
                        .get(chunk_data_rskey)
                        .del(chunk_data_rskey)
                        .query::<(Vec<u8>, i64)>(self.rs).unwrap().0
                } else {
                    self.rs.get(chunk_data_rskey).unwrap()
                };
                if chunk_data.is_empty() {
                    // The chunk has been dropped.
                    Err(Error::Other)
                } else {
                    data[..chunk_data.len()].copy_from_slice(&chunk_data);
                    Ok((chunk_index, chunk_data.len()))
                }
            })
    }

    // /// Poll and wait for the specific indexed chunk being ready.
    // ///
    // /// It doesn't guarantee that the chunk is always available even if this method reports the
    // /// chunk is ready.
    // pub fn poll(&self, index: i64, timeout: usize) -> Result<()> {
    //     if index < 0 {
    //         return Err(Error::BadArgument);
    //     }
    //     Ok(())
    // }
}
