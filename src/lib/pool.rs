use super::flow::Flow;
use std::{collections::HashMap, sync::Arc};

type SharedFlow = Arc<Flow>;

pub struct Pool {
    pool: HashMap<u128, SharedFlow>,
}

impl Pool {
    pub fn new() -> Self {
        Pool {
            pool: HashMap::new(),
        }
    }

    pub fn create(&mut self) -> Option<SharedFlow> {
        let flow = Arc::new(Flow::new());
        self.pool.insert(flow.get_id(), flow.clone());
        Some(flow)
    }

    pub fn get(&self, flow_id: u128) -> Option<SharedFlow> {
        self.pool.get(&flow_id).map(|flow| flow.clone())
    }
}
