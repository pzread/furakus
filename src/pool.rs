use flow::Flow;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

type SharedFlow = Arc<RwLock<Flow>>;

pub struct Pool {
    bucket: HashMap<String, SharedFlow>,
}

impl Pool {
    pub fn new() -> Arc<RwLock<Self>> {
        let pool = Pool { bucket: HashMap::new() };
        Arc::new(RwLock::new(pool))
    }

    pub fn insert(&mut self, flow_id: &str, flow_ptr: SharedFlow) {
        self.bucket.insert(flow_id.to_owned(), flow_ptr);
    }

    pub fn get(&self, flow_id: &str) -> Option<SharedFlow> {
        self.bucket.get(flow_id).map(|flow_ptr| flow_ptr.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flow::Flow;
    use std::sync::Arc;

    #[test]
    fn basic_operations() {
        let ptr = Pool::new();
        let flow_a = Flow::new(None);
        let flow_b = Flow::new(None);

        {
            let mut pool = ptr.write().unwrap();
            pool.insert("A", flow_a.clone());
            pool.insert("B", flow_b.clone());
        }

        {
            let pool = ptr.read().unwrap();
            assert!(Arc::ptr_eq(&pool.get("A").unwrap(), &flow_a));
            assert!(Arc::ptr_eq(&pool.get("B").unwrap(), &flow_b));
            assert!(!Arc::ptr_eq(&pool.get("A").unwrap(), &flow_b));
            assert!(!Arc::ptr_eq(&pool.get("B").unwrap(), &flow_a));
            assert!(!pool.get("C").is_some());
        }
    }
}
