use super::{
    flow::Flow,
    time_heap::{Entry as TimeEntry, Observer as TimeObserver, TimeHeap},
};
use byteorder::{ByteOrder, LittleEndian};
use parking_lot::{Mutex, RwLock};
use ring::{self, rand::SecureRandom};
use std::{
    collections::HashMap,
    sync::{Arc, Weak},
    time::Duration,
};

pub type SharedPool = Arc<Pool>;

struct Entry {
    flow_id: u128,
    pool: Weak<Pool>,
    time_entry: Weak<Mutex<TimeEntry<Observer>>>,
}

#[derive(Clone)]
pub struct Observer(Weak<Entry>);

impl TimeObserver for Observer {
    fn on_removed(self) {
        self.remove();
    }
}

impl Observer {
    pub fn on_update(&self) {
        let entry = match self.0.upgrade() {
            Some(entry) => entry,
            None => return,
        };
        let pool = match entry.pool.upgrade() {
            Some(pool) => pool,
            None => return,
        };
        pool.update(entry);
    }

    pub fn on_close(&self) {
        self.remove();
    }

    fn remove(&self) {
        let entry = match self.0.upgrade() {
            Some(entry) => entry,
            None => return,
        };
        let pool = match entry.pool.upgrade() {
            Some(pool) => pool,
            None => return,
        };
        pool.remove(entry);
    }
}

pub struct Pool {
    time_heap: TimeHeap<Observer>,
    flow_map: RwLock<HashMap<u128, (Arc<Entry>, Arc<Flow>)>>,
}

impl Pool {
    pub fn new() -> SharedPool {
        Arc::new(Pool {
            time_heap: TimeHeap::new(Duration::from_secs(300)),
            flow_map: RwLock::new(HashMap::new()),
        })
    }

    pub fn create(pool: &SharedPool) -> Option<Arc<Flow>> {
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
        let flow = pool.time_heap.create(|time_entry| {
            let entry = Arc::new(Entry {
                flow_id,
                pool: Arc::downgrade(pool),
                time_entry,
            });
            let observer = Observer(Arc::downgrade(&entry));
            let flow = Arc::new(Flow::new(flow_id, flow_token, observer.clone()));
            pool.flow_map
                .write()
                .insert(flow.get_id(), (entry, flow.clone()));
            (observer, flow)
        });
        Some(flow)
    }

    pub fn get(&self, flow_id: u128) -> Option<Arc<Flow>> {
        self.flow_map
            .read()
            .get(&flow_id)
            .map(|(_, flow)| flow.clone())
    }

    fn update(&self, entry: Arc<Entry>) {
        entry.time_entry.upgrade().map(|time_entry| {
            self.time_heap.update(time_entry);
        });
    }

    fn remove(&self, entry: Arc<Entry>) {
        if self.flow_map.write().remove(&entry.flow_id).is_some() {
            entry.time_entry.upgrade().map(|time_entry| {
                self.time_heap.remove(time_entry);
            });
        }
    }
}
