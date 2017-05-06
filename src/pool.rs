use flow::{Flow, Observer};
use std::collections::HashMap;
use std::sync::{Arc, RwLock, Weak};

type SharedFlow = Arc<RwLock<Flow>>;

pub struct Pool {
    weakref: Weak<RwLock<Pool>>,
    bucket: HashMap<String, SharedFlow>,
}

impl Pool {
    pub fn new() -> Arc<RwLock<Self>> {
        let pool = Pool {
            weakref: Weak::new(),
            bucket: HashMap::new(),
        };
        let pool_ptr = Arc::new(RwLock::new(pool));
        pool_ptr.write().unwrap().weakref = Arc::downgrade(&pool_ptr);
        pool_ptr
    }

    pub fn insert(&mut self, flow_ptr: SharedFlow) {
        // Occupy the flow to prevent from race condition.
        let mut flow = flow_ptr.write().unwrap();
        self.bucket.insert(flow.id.to_owned(), flow_ptr.clone());
        flow.observe(self.weakref.clone());
    }

    pub fn get(&self, flow_id: &str) -> Option<SharedFlow> {
        self.bucket.get(flow_id).map(|flow_ptr| flow_ptr.clone())
    }
}

impl Observer for Weak<RwLock<Pool>> {
    fn on_close(&self, flow: &Flow) {
        if let Some(pool_ptr) = self.upgrade() {
            let mut pool = pool_ptr.write().unwrap();
            // Try to remove the flow.
            pool.bucket.remove(&flow.id).is_some();
        }
    }
}

#[cfg(test)]
mod tests {
    use flow::Flow;
    use std::sync::Arc;
    use super::*;
    use tokio::reactor::Core;

    #[test]
    fn basic_operations() {
        let ptr = Pool::new();
        let flow_a = Flow::new(None);
        let flow_b = Flow::new(None);

        let (flowa_id, flowb_id) = {
            (flow_a.read().unwrap().id.to_owned(), flow_b.read().unwrap().id.to_owned())
        };

        {
            let mut pool = ptr.write().unwrap();
            pool.insert(flow_a.clone());
            pool.insert(flow_b.clone());
        }

        {
            let pool = ptr.read().unwrap();
            assert!(Arc::ptr_eq(&pool.get(&flowa_id).unwrap(), &flow_a));
            assert!(Arc::ptr_eq(&pool.get(&flowb_id).unwrap(), &flow_b));
            assert!(!Arc::ptr_eq(&pool.get(&flowa_id).unwrap(), &flow_b));
            assert!(!Arc::ptr_eq(&pool.get(&flowb_id).unwrap(), &flow_a));
            assert!(!pool.get("C").is_some());
        }
    }

    #[test]
    fn close_recycle() {
        let mut core = Core::new().unwrap();
        let ptr = Pool::new();
        let flow = Flow::new(None);

        let flow_id = {
            flow.read().unwrap().id.to_owned()
        };

        {
            let mut pool = ptr.write().unwrap();
            pool.insert(flow.clone());
            assert!(Arc::ptr_eq(&pool.get(&flow_id).unwrap(), &flow));
        }

        {
            let fut = flow.write().unwrap().close();
            core.run(fut).is_ok();
        }

        {
            let fut = flow.read().unwrap().pull(0, Some(0));
            core.run(fut).is_err();
        }

        {
            let pool = ptr.read().unwrap();
            assert!(!pool.get(&flow_id).is_some());
        }
    }
}
