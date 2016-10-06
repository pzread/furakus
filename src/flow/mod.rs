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
macro_rules! rskey_flow_consumer { ($hash:expr) => { &format!("FLOW@{:x}@CONSUMER", $hash) } }
macro_rules! rskey_flow_chunk {
    ($hash:expr, $index:expr) => { &format!("FLOW@{:x}@CHUNK@{}", $hash, $index) }
}
macro_rules! rskey_flow_chunk_puller {
    ($hash:expr, $index:expr) => { &format!("FLOW@{:x}@CHUNK@{}@PULLER", $hash, $index) }
}
macro_rules! rskey_chunk_data { ($id:expr) => { &format!("CHUNK@{:x}", $id) } }

#[derive(Debug, PartialEq)]
pub enum Error {
    /// Some arguments are illegal.
    BadArgument,
    /// OutOfRange,
    OutOfRange,
    /// Other errors.
    Other,
}

pub type Result<T> = StdResult<T, Error>;

lazy_static! {
    /// The redis script for acquiring and inserting a chunk, then set its provider and chunk id.
    ///
    /// Once the transcation finished, the chunk is ready.
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
            if redis.call('hsetnx', flow_chunk_key, 'chunk_id', chunk_id) ~= 1 then
                error('collision')
            end
            chunk_index = head_index
        else
            local head_index = tonumber(redis.call('hget', flow_rskey, 'head_index'))
            flow_chunk_key = flow_rskey .. '@CHUNK@' .. specific_index
            if specific_index <= head_index then
                return -1
            end
            if redis.call('hsetnx', flow_chunk_key, 'chunk_id', chunk_id) == 0 then
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

        redis.call('hset', flow_chunk_key, 'provider_id', provider_id)
        return chunk_index
    ");

    static ref PULL_SCRIPT: redis::Script = redis::Script::new(&format!(r"
        local flow_rskey = KEYS[1]
        local num_of_consumers = tonumber(ARGV[1])
        local consumer_id = ARGV[2]
        local index = tonumber(ARGV[3])
        local ret

        if num_of_consumers >= 0 then
            if redis.call('sismember', flow_rskey .. '@CONSUMER', consumer_id) == 0 then
                return -1
            end
        end
        ret = redis.call('hmget', flow_rskey, 'tail_index', 'head_index')
        local tail_index = tonumber(ret[1])
        local head_index = tonumber(ret[2])
        if index == -1 then
            index = tail_index
        else
            if index < tail_index or index > head_index then
                return -2
            end
        end
        local flow_chunk_key = flow_rskey .. '@CHUNK@' .. index
        ret = redis.call('hmget', flow_chunk_key, 'chunk_id', 'provider_id')
        local chunk_id = ret[1]
        local provider_id = ret[2]

        if num_of_consumers >= 0 then
            local flow_chunk_puller_key = flow_chunk_key .. '@PULLER'
            redis.call('sadd', flow_chunk_puller_key, consumer_id)
            redis.call('expire', flow_chunk_puller_key, {0})
        end

        return {{index, chunk_id, provider_id}}
    ", SHORT_TIMEOUT));
}

/// The struct represents the flow.
pub struct Flow<'a> {
    rs: &'a RedisConn,
    pub id: String,
    id_hash: Hash,
    max_chunksize: usize,
    num_of_consumers: i64,
}

impl<'a> Flow<'a> {
    /// Create a new `Flow`.
    pub fn new<'b>(rs: &'b RedisConn, max_chunksize: usize, consumers: Option<&[&str]>) ->
        Result<Flow<'b>> {
        if max_chunksize == 0 ||  max_chunksize > MAX_CHUNKSIZE {
            return Err(Error::BadArgument);
        }
        let (id, id_hash) = generate_identifier();
        let flow_rskey = rskey_flow!(id_hash);
        let num_of_consumers = match consumers {
            Some(consumers) => consumers.len() as i64,
            None => -1
        };
        redis::pipe()
            .hset(flow_rskey, "head_index", -1)
            .hset(flow_rskey, "tail_index", 0)
            .hset(flow_rskey, "max_chunksize", max_chunksize)
            .hset(flow_rskey, "num_of_consumers", num_of_consumers)
            .expire(flow_rskey, LONG_TIMEOUT)
            .execute(rs);
        if let Some(consumers) = consumers {
            consumers.iter()
                .fold(redis::cmd("SADD").arg(rskey_flow_consumer!(id_hash)), |cmd, consumer| {
                    cmd.arg(*consumer)
                })
                .execute(rs);
        }
        Ok(Flow {
            rs: rs,
            id: id,
            id_hash: id_hash,
            max_chunksize: max_chunksize,
            num_of_consumers: num_of_consumers,
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
            let num_of_consumers =
                i64::from_redis_value(metadata.get("num_of_consumers").unwrap()).unwrap();
            Some(Flow {
                rs: rs,
                id: id.to_string(),
                id_hash: id_hash,
                max_chunksize: max_chunksize,
                num_of_consumers: num_of_consumers,
            })
        }
    }

    pub fn get_max_chunksize(&self) -> usize {
        self.max_chunksize
    }

    /// Push a chunk into the flow. Return the chunk index.
    pub fn push(&self, provider_id: &str, index: Option<i64>, data: &[u8]) -> Result<i64> {
        if data.len() == 0 || data.len() > self.max_chunksize || index.unwrap_or(0) < 0 {
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

    /// Pull a chunk from the flow. Return the chunk index, chunk size, and provider id.
    pub fn pull(&self, consumer_id: &str, index: Option<i64>, data: &mut [u8]) ->
        Result<(i64, usize, String)> {
        if data.len() < self.max_chunksize || index.unwrap_or(0) < 0 {
            return Err(Error::BadArgument);
        }
        // Try to get the chunk.
        let flow_rskey = rskey_flow!(self.id_hash);
        PULL_SCRIPT
            .key(&*flow_rskey)
            .arg(self.num_of_consumers)
            .arg(consumer_id)
            .arg(index.unwrap_or(-1))
            .invoke::<redis::Value>(self.rs)
            .or(Err(Error::Other))
            .and_then(|result| {
                redis::from_redis_value::<(i64, i64, String)>(&result)
                    .map_err(|_| match i64::from_redis_value(&result).unwrap() {
                        -1 => Error::BadArgument,
                        -2 => Error::OutOfRange,
                        _ => Error::Other
                    })
            })
            .and_then(|(chunk_index, chunk_id, provider_id)| {
                let chunk_data_rskey = rskey_chunk_data!(chunk_id);
                let chunk_data: Vec<u8> = self.rs.get(chunk_data_rskey).unwrap();
                if chunk_data.is_empty() {
                    // The chunk has been dropped.
                    Err(Error::Other)
                } else {
                    data[..chunk_data.len()].copy_from_slice(&chunk_data);
                    Ok((chunk_index, chunk_data.len(), provider_id))
                }
            })

        /*
        if self.num_of_consumers >= 0 {
            // Check if the consumer is one of the consumers.
            if !self.rs.sismember::<_, _, bool>(
                rskey_flow_consumer!(self.id_hash), consumer_id).unwrap() {
                return Err(Error::BadArgument);
            }
        }
        let (tail_index, head_index): (i64, i64) =
            redis::cmd("hmget")
            .arg(flow_rskey)
            .arg("tail_index")
            .arg("head_index")
            .query(self.rs).unwrap();
        // Check if the index is out of range.
        let chunk_index = match index {
            Some(idx) if idx >= tail_index && idx <= head_index => idx,
            None => tail_index,
            _ => return Err(Error::OutOfRange)
        };
        let flow_chunk_key = rskey_flow_chunk!(self.id_hash, chunk_index);
        let (chunk_id, provider_id): (i64, String) =
            redis::cmd("hmget")
            .arg(flow_chunk_key)
            .arg("chunk_id")
            .arg("provider_id")
            .query(self.rs).unwrap();
        let chunk_data_rskey = rskey_chunk_data!(chunk_id);
        let chunk_data: Vec<u8> = self.rs.get(chunk_data_rskey).unwrap();
        if chunk_data.is_empty() {
            return Err(Error::Other);
        }

        if self.num_of_consumers >= 0 {
            let flow_chunk_puller_rskey = rskey_flow_chunk_puller!(self.id_hash, chunk_index);
            let (_, _): (i64, i64) = redis::pipe()
                .sadd(flow_chunk_puller_rskey, consumer_id)
                //.scard(flow_chunk_puller_rskey)
                .expire(flow_chunk_puller_rskey, SHORT_TIMEOUT)
                .query(self.rs).unwrap();
            // Check if the chunk can be dropped.
            /*if puller_count == self.num_of_consumers {
                redis::pipe()
                    .hincr(flow_rskey, "tail_index", 1)
                    .del(flow_chunk_key)
                    .del(flow_chunk_puller_rskey)
                    .del(chunk_data_rskey)
                    .execute(self.rs);
            }*/
        }

        data[..chunk_data.len()].copy_from_slice(&chunk_data);
        Ok((chunk_index, chunk_data.len(), provider_id))
        */
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
