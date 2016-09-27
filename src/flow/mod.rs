//! The `flow` module provide interface to create and control flows.
//!
//! Providers and consumers can exchange their data through the flow interface.
//! Flows maintain how these data be buffered in the backend storages and provide interfaces to
//! access these data.

use common::*;
use redis::{self, Client as RedisClient, Commands, Connection as RedisConn, PipelineCommands};

macro_rules! rskey_flow { ($hash:expr) => { &format!("FLOW@{:x}", $hash) } }

/// The flow's metadata.
pub struct Flow<'a> {
    rs: &'a RedisConn,
    pub id: String,
    id_hash: Hash,
}

impl<'a> Flow<'a> {
    /// Create a new `Flow`.
    pub fn new(rs: &RedisConn) -> Flow {
        let (id, id_hash) = generate_identifier();
        let flow_rskey = rskey_flow!(id_hash);
        redis::pipe()
            .hset(flow_rskey, "next_index", 0)
            .hset(flow_rskey, "tail_index", 0)
            .execute(rs);
        Flow {
            rs: rs,
            id: id,
            id_hash: id_hash,
        }
    }

    /// Get the created `Flow` from its `id`.
    pub fn get<'b>(rs: &'b RedisConn, id: &str) -> Option<Flow<'b>> {
        let id_hash = Hash::get(id);
        if rs.exists::<_, i64>(rskey_flow!(id_hash)).unwrap() == 0 {
            None
        } else {
            Some(Flow {
                rs: rs,
                id: id.to_string(),
                id_hash: id_hash,
            })
        }
    }
}
