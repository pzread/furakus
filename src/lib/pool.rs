use super::flow::Flow;
use byteorder::{ByteOrder, LittleEndian};
use parking_lot::RwLock;
use ring::{self, rand::SecureRandom};
use std::{
    collections::HashMap,
    sync::{Arc, Weak},
};

type SharedFlow = Arc<Flow>;
type WeekPool = Weak<RwLock<Pool>>;
pub type SharedPool = Arc<RwLock<Pool>>;

pub struct Observer {
    weak_pool: WeekPool,
    flow_id: u128,
}

impl Observer {
    pub fn on_close(&self) {
        self.weak_pool.upgrade().map(|pool| {
            pool.write().pool.remove(&self.flow_id);
        });
    }
}

pub struct Pool {
    weak_factory: WeekPool,
    pool: HashMap<u128, SharedFlow>,
}

impl Pool {
    pub fn new() -> SharedPool {
        let pool = Arc::new(RwLock::new(Pool {
            weak_factory: Weak::default(),
            pool: HashMap::new(),
        }));
        pool.write().weak_factory = Arc::downgrade(&pool);
        pool
    }

    pub fn create(&mut self) -> Option<SharedFlow> {
        let rand = ring::rand::SystemRandom::new();
        let flow_id = {
            let mut buf = [0u8; 16];
            rand.fill(&mut buf).unwrap();
            LittleEndian::read_u128(&buf)
        };
        let flow_token = {
            let mut buf = [0u8; 16];
            rand.fill(&mut buf).unwrap();
            LittleEndian::read_u128(&buf)
        };
        let observer = Observer {
            weak_pool: self.weak_factory.clone(),
            flow_id,
        };
        let flow = Arc::new(Flow::new(flow_id, flow_token, observer));
        self.pool.insert(flow.get_id(), flow.clone());
        println!("{}", self.pool.len());
        Some(flow)
    }

    pub fn get(&self, flow_id: u128) -> Option<SharedFlow> {
        self.pool.get(&flow_id).map(|flow| flow.clone())
    }
}
