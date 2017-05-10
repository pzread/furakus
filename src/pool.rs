use flow::{Flow, Observer};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock, Weak};

type SharedFlow = Arc<RwLock<Flow>>;

type LinkPointer<T> = Weak<Mutex<T>>;

struct TimeLinkNode {
    key: String,
    prev: Option<LinkPointer<TimeLinkNode>>,
    next: Option<LinkPointer<TimeLinkNode>>,
}

struct Entry {
    flow: SharedFlow,
    timelink: Arc<Mutex<TimeLinkNode>>,
}

pub struct Pool {
    weakref: Weak<RwLock<Pool>>,
    bucket: HashMap<String, Entry>,
    timelink_head: Option<LinkPointer<TimeLinkNode>>,
    timelink_tail: Option<LinkPointer<TimeLinkNode>>,
}

impl Entry {
    fn new(flow_id: &str, flow_ptr: SharedFlow) -> Self {
        Entry {
            flow: flow_ptr,
            timelink: Arc::new(Mutex::new(TimeLinkNode {
                                              key: flow_id.to_owned(),
                                              prev: Some(Weak::new()),
                                              next: Some(Weak::new()),
                                          })),
        }
    }
}

impl TimeLinkNode {
    fn push(timelink: &Arc<Mutex<TimeLinkNode>>, pool: &mut Pool) {
        let mut link = timelink.lock().unwrap();
        link.prev = None;
        link.next = pool.timelink_head.clone();
        let link_ptr = Arc::downgrade(timelink);
        if let Some(ref head) = pool.timelink_head {
            let head = head.upgrade().unwrap();
            head.lock().unwrap().prev = Some(link_ptr.clone());
        }
        pool.timelink_head = Some(link_ptr);
    }

    fn unlink(timelink: &Arc<Mutex<TimeLinkNode>>, pool: &mut Pool) {
        let mut link = timelink.lock().unwrap();
        if let Some(ref prev) = link.prev {
            // Upgrade should always success.
            let prev = prev.upgrade().unwrap();
            prev.lock().unwrap().next = link.next.clone();
        } else {
            pool.timelink_head = link.next.clone();
        }
        if let Some(ref next) = link.next {
            // Upgrade should always success.
            let next = next.upgrade().unwrap();
            next.lock().unwrap().prev = link.prev.clone();
        } else {
            pool.timelink_tail = link.prev.clone();
        }
        link.prev = Some(Weak::new());
        link.next = Some(Weak::new());
    }
}

impl Pool {
    pub fn new() -> Arc<RwLock<Self>> {
        let pool = Pool {
            weakref: Weak::new(),
            bucket: HashMap::new(),
            timelink_head: None,
            timelink_tail: None,
        };
        let pool_ptr = Arc::new(RwLock::new(pool));
        pool_ptr.write().unwrap().weakref = Arc::downgrade(&pool_ptr);
        pool_ptr
    }

    pub fn insert(&mut self, flow_ptr: SharedFlow) {
        // Occupy the flow to prevent from race condition.
        let mut flow = flow_ptr.write().unwrap();
        let timelink = {
            self.bucket
                .entry(flow.id.to_owned())
                .or_insert(Entry::new(&flow.id, flow_ptr.clone()))
                .timelink
                .clone()
        };
        flow.observe(self.weakref.clone());
        TimeLinkNode::push(&timelink, self);
    }

    pub fn get(&self, flow_id: &str) -> Option<SharedFlow> {
        self.bucket.get(flow_id).map(|entry| entry.flow.clone())
    }

    pub fn remove(&mut self, flow_id: &str) -> Result<(), ()> {
        self.bucket
            .remove(flow_id)
            .ok_or(())
            .and_then(|entry| {
                TimeLinkNode::unlink(&entry.timelink, self);
                Ok(())
            })
    }
}

impl Observer for Weak<RwLock<Pool>> {
    fn on_active(&self, flow: &Flow) {
        if let Some(pool_ptr) = self.upgrade() {
            // TODO minimize the pool write lock.
            let mut pool = pool_ptr.write().unwrap();
            let timelink = match pool.bucket.get(&flow.id) {
                Some(entry) => entry.timelink.clone(),
                None => return,
            };
            TimeLinkNode::unlink(&timelink, &mut pool);
            TimeLinkNode::push(&timelink, &mut pool);
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
    use flow::Flow;
    use std::sync::Arc;
    use tokio::reactor::Core;

    #[test]
    fn basic_operations() {
        let ptr = Pool::new();
        let flow_a = Flow::new(None);
        let flow_b = Flow::new(None);
        let flow_c = Flow::new(None);

        let (flowa_id, flowb_id, flowc_id) = {
            (flow_a.read().unwrap().id.to_owned(),
             flow_b.read().unwrap().id.to_owned(),
             flow_c.read().unwrap().id.to_owned())
        };

        {
            let mut pool = ptr.write().unwrap();
            pool.insert(flow_a.clone());
            pool.insert(flow_b.clone());
            pool.insert(flow_c.clone());
        }

        {
            let pool = ptr.read().unwrap();
            assert!(Arc::ptr_eq(&pool.get(&flowa_id).unwrap(), &flow_a));
            assert!(Arc::ptr_eq(&pool.get(&flowb_id).unwrap(), &flow_b));
            assert!(!Arc::ptr_eq(&pool.get(&flowa_id).unwrap(), &flow_b));
            assert!(!Arc::ptr_eq(&pool.get(&flowb_id).unwrap(), &flow_a));
            assert!(!pool.get("C").is_some());
        }

        {
            let mut pool = ptr.write().unwrap();
            assert_eq!(pool.remove(&flowb_id), Ok(()));
            assert_eq!(pool.remove(&flowa_id), Ok(()));
            assert_eq!(pool.remove(&flowc_id), Ok(()));
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
