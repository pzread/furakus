use flow::{Flow, Observer};
use std::{
    collections::HashMap,
    iter,
    sync::{Arc, Mutex, RwLock, Weak},
    time::{Duration, Instant},
};

pub type SharedFlow = Arc<RwLock<Flow>>;

type LinkPointer<T> = Weak<Mutex<T>>;

struct TimeLinkNode {
    key: String,
    timestamp: Instant,
    prev: Option<LinkPointer<TimeLinkNode>>,
    next: Option<LinkPointer<TimeLinkNode>>,
}

struct Entry {
    flow: SharedFlow,
    node: Arc<Mutex<TimeLinkNode>>,
}

pub struct Pool {
    weakref: Weak<RwLock<Pool>>,
    bucket: HashMap<String, Entry>,
    pool_size: Option<usize>,
    deactive_timeout: Option<Duration>,
    timelink_head: Option<LinkPointer<TimeLinkNode>>,
    timelink_tail: Option<LinkPointer<TimeLinkNode>>,
}

impl Entry {
    fn new(flow_id: &str, flow_ptr: SharedFlow) -> Self {
        Entry {
            flow: flow_ptr,
            node: Arc::new(Mutex::new(TimeLinkNode {
                key: flow_id.to_owned(),
                timestamp: Instant::now(),
                prev: Some(Weak::new()),
                next: Some(Weak::new()),
            })),
        }
    }
}

impl TimeLinkNode {
    fn push(node: &mut TimeLinkNode, node_ptr: &Arc<Mutex<TimeLinkNode>>, pool: &mut Pool) {
        node.prev = None;
        node.next = pool.timelink_head.clone();
        let weakref = Arc::downgrade(node_ptr);
        if let Some(ref head) = pool.timelink_head {
            let head = head.upgrade().unwrap();
            head.lock().unwrap().prev = Some(weakref.clone());
        }
        pool.timelink_head = Some(weakref);
    }

    fn unlink(node: &mut TimeLinkNode, pool: &mut Pool) {
        if let Some(ref prev) = node.prev {
            // Upgrade should always success.
            let prev = prev.upgrade().unwrap();
            prev.lock().unwrap().next = node.next.clone();
        } else {
            pool.timelink_head = node.next.clone();
        }
        if let Some(ref next) = node.next {
            // Upgrade should always success.
            let next = next.upgrade().unwrap();
            next.lock().unwrap().prev = node.prev.clone();
        } else {
            pool.timelink_tail = node.prev.clone();
        }
        node.prev = Some(Weak::new());
        node.next = Some(Weak::new());
    }
}

impl Pool {
    pub fn new(pool_size: Option<usize>, deactive_timeout: Option<Duration>) -> Arc<RwLock<Self>> {
        let pool = Pool {
            weakref: Weak::new(),
            bucket: HashMap::new(),
            pool_size,
            deactive_timeout,
            timelink_head: None,
            timelink_tail: None,
        };
        let pool_ptr = Arc::new(RwLock::new(pool));
        pool_ptr.write().unwrap().weakref = Arc::downgrade(&pool_ptr);
        pool_ptr
    }

    pub fn insert(&mut self, flow_ptr: SharedFlow) -> Result<(), ()> {
        if let Some(pool_size) = self.pool_size {
            if self.bucket.len() >= pool_size {
                self.sanitize_bucket();
                if self.bucket.len() >= pool_size {
                    return Err(());
                }
            }
        }
        {
            // Occupy the flow to prevent from race condition.
            let mut flow = flow_ptr.write().unwrap();
            let node_ptr = {
                self.bucket
                    .entry(flow.id.to_owned())
                    .or_insert(Entry::new(&flow.id, flow_ptr.clone()))
                    .node
                    .clone()
            };
            {
                let mut node = node_ptr.lock().unwrap();
                TimeLinkNode::push(&mut node, &node_ptr, self);
            }
            flow.observe(self.weakref.clone());
        }
        Ok(())
    }

    pub fn get(&self, flow_id: &str) -> Option<SharedFlow> {
        self.bucket.get(flow_id).map(|entry| entry.flow.clone())
    }

    pub fn remove(&mut self, flow_id: &str) -> Result<(), ()> {
        self.bucket.remove(flow_id).ok_or(()).and_then(|entry| {
            let mut node = entry.node.lock().unwrap();
            TimeLinkNode::unlink(&mut node, self);
            Ok(())
        })
    }

    fn sanitize_bucket(&mut self) {
        let deactive_timeout = match self.deactive_timeout {
            Some(deactive_timeout) => deactive_timeout,
            None => return,
        };

        let droplist = iter::repeat(())
            .scan(self.timelink_tail.clone(), |tail_weakref, _| {
                tail_weakref
                    .as_ref()
                    .and_then(|weakref| weakref.upgrade())
                    .and_then(|tail_ptr| {
                        let node = tail_ptr.lock().unwrap();
                        if node.timestamp.elapsed() <= deactive_timeout {
                            None
                        } else {
                            *tail_weakref = node.prev.clone();
                            Some(node.key.to_owned())
                        }
                    })
            })
            .collect::<Vec<String>>();

        for key in droplist.iter() {
            // Try to remove dead flows.
            self.remove(key).is_ok();
        }
    }
}

impl Observer for Weak<RwLock<Pool>> {
    fn on_active(&self, flow: &Flow) {
        if let Some(pool_ptr) = self.upgrade() {
            // TODO minimize the pool write lock.
            let mut pool = pool_ptr.write().unwrap();
            let node_ptr = match pool.bucket.get(&flow.id) {
                Some(entry) => entry.node.clone(),
                None => return,
            };
            {
                let mut node = node_ptr.lock().unwrap();
                node.timestamp = Instant::now();
                TimeLinkNode::unlink(&mut node, &mut pool);
                TimeLinkNode::push(&mut node, &node_ptr, &mut pool);
            }
        }
    }

    fn on_close(&self, flow: &Flow) {
        if let Some(pool_ptr) = self.upgrade() {
            let mut pool = pool_ptr.write().unwrap();
            // Try to remove the flow.
            pool.remove(&flow.id).is_ok();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flow;
    use std::thread;
    use tokio::reactor::Core;

    const FLOW_CONFIG: flow::Config = flow::Config {
        length: None,
        meta_capacity: 16777216,
        data_capacity: 16777216,
        keepcount: Some(1),
        preserve_mode: false,
    };

    #[test]
    fn basic_operations() {
        let ptr = Pool::new(None, None);
        let flow_a = Flow::new(FLOW_CONFIG);
        let flow_b = Flow::new(FLOW_CONFIG);
        let flow_c = Flow::new(FLOW_CONFIG);
        let flowa_id = flow_a.read().unwrap().id.to_owned();
        let flowb_id = flow_b.read().unwrap().id.to_owned();
        let flowc_id = flow_c.read().unwrap().id.to_owned();
        {
            let mut pool = ptr.write().unwrap();
            assert_eq!(pool.insert(flow_a.clone()), Ok(()));
            assert_eq!(pool.insert(flow_b.clone()), Ok(()));
            assert_eq!(pool.insert(flow_c.clone()), Ok(()));
        }
        {
            let pool = ptr.read().unwrap();
            assert!(Arc::ptr_eq(&pool.get(&flowa_id).unwrap(), &flow_a));
            assert!(Arc::ptr_eq(&pool.get(&flowb_id).unwrap(), &flow_b));
            assert!(!Arc::ptr_eq(&pool.get(&flowa_id).unwrap(), &flow_b));
            assert!(!Arc::ptr_eq(&pool.get(&flowb_id).unwrap(), &flow_a));
            assert!(pool.get("C").is_none());
        }
        {
            let mut pool = ptr.write().unwrap();
            assert_eq!(pool.remove(&flowb_id), Ok(()));
            assert_eq!(pool.remove(&flowa_id), Ok(()));
            assert_eq!(pool.remove(&flowc_id), Ok(()));
        }
        {
            let pool = ptr.read().unwrap();
            assert!(pool.get(&flowa_id).is_none());
            assert!(pool.get(&flowb_id).is_none());
            assert!(pool.get(&flowc_id).is_none());
        }
    }

    #[test]
    fn close_recycle() {
        let mut core = Core::new().unwrap();
        let ptr = Pool::new(None, None);
        let flow = Flow::new(FLOW_CONFIG);
        let flow_id = flow.read().unwrap().id.to_owned();
        {
            let mut pool = ptr.write().unwrap();
            pool.insert(flow.clone()).unwrap();
            assert!(Arc::ptr_eq(&pool.get(&flow_id).unwrap(), &flow));
        }
        {
            let fut = flow.write().unwrap().close();
            core.run(fut).unwrap();
        }
        {
            let fut = flow.read().unwrap().pull(0, Some(0));
            core.run(fut).is_err();
        }
        assert!(ptr.read().unwrap().get(&flow_id).is_none());
    }

    #[test]
    fn dropped() {
        let mut core = Core::new().unwrap();
        let flow = Flow::new(FLOW_CONFIG);
        {
            let ptr = Pool::new(None, None);
            let flow_id = flow.read().unwrap().id.to_owned();
            {
                let mut pool = ptr.write().unwrap();
                pool.insert(flow.clone()).unwrap();
                pool.remove(&flow_id).unwrap();
            }
            assert!(ptr.read().unwrap().get(&flow_id).is_none());
            {
                let fut = flow.write().unwrap().close();
                core.run(fut).unwrap();
            }
        }
        {
            let fut = flow.read().unwrap().pull(0, Some(0));
            assert_eq!(core.run(fut), Err(flow::Error::Eof));
        }
    }

    #[test]
    fn overload_size() {
        let ptr = Pool::new(Some(1), None);
        let flow_a = Flow::new(FLOW_CONFIG);
        let flow_b = Flow::new(FLOW_CONFIG);
        {
            let mut pool = ptr.write().unwrap();
            assert_eq!(pool.insert(flow_a.clone()), Ok(()));
            assert_eq!(pool.insert(flow_b.clone()), Err(()));
        }
    }

    #[test]
    fn overload_time() {
        let mut core = Core::new().unwrap();
        let ptr = Pool::new(Some(3), Some(Duration::from_secs(6)));
        let flow_a = Flow::new(FLOW_CONFIG);
        let flow_b = Flow::new(FLOW_CONFIG);
        let flow_c = Flow::new(FLOW_CONFIG);
        let flow_d = Flow::new(FLOW_CONFIG);
        let flow_e = Flow::new(FLOW_CONFIG);
        let flow_f = Flow::new(FLOW_CONFIG);
        {
            let mut pool = ptr.write().unwrap();
            assert_eq!(pool.insert(flow_a.clone()), Ok(()));
            assert_eq!(pool.insert(flow_b.clone()), Ok(()));
            assert_eq!(pool.insert(flow_c.clone()), Ok(()));
        }
        thread::sleep(Duration::from_secs(4));
        {
            let fut = flow_a.write().unwrap().push("Hello".into());
            core.run(fut).unwrap();
        }
        thread::sleep(Duration::from_secs(4));
        {
            let mut pool = ptr.write().unwrap();
            assert_eq!(pool.insert(flow_d.clone()), Ok(()));
            assert_eq!(pool.insert(flow_e.clone()), Ok(()));
            assert_eq!(pool.insert(flow_f.clone()), Err(()));
        }
    }
}
