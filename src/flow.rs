use bytes::Bytes;
use futures::{future, Future};
use futures::sync::oneshot;
use std::collections::{HashMap, VecDeque};
use std::mem;
use std::sync::{Arc, Mutex, RwLock, Weak};
use utils::BoxedFuture;
use uuid::Uuid;

pub const REF_SIZE: usize = 32768;

#[derive(Debug, PartialEq)]
pub enum Error {
    Invalid,
    Dropped,
    NotReady,
    Eof,
    Other,
}

#[derive(Debug)]
pub enum Chunk {
    Data(u64, Bytes),
    Eof(u64),
}

impl Chunk {
    fn data(data: Bytes) -> Self {
        Chunk::Data(0, data)
    }

    fn eof() -> Self {
        Chunk::Eof(0)
    }

    fn count(&self) -> u64 {
        match *self {
            Chunk::Data(count, ..) => count,
            Chunk::Eof(count, ..) => count,
        }
    }

    fn len(&self) -> u64 {
        match *self {
            Chunk::Data(_, ref data) => data.len() as u64,
            Chunk::Eof(..) => 0,
        }
    }
}

type SharedChunk = Arc<Mutex<Chunk>>;

pub trait Observer: Send + Sync + 'static {
    fn on_active(&self, _flow: &Flow) {}
    fn on_close(&self, _flow: &Flow) {}
}

#[derive(Clone, Debug, PartialEq)]
enum State {
    Streaming,
    Stop,
    Closed,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Config {
    pub length: Option<u64>,
    pub meta_capacity: u64,
    pub data_capacity: u64,
    pub keepcount: Option<u64>,
    pub preserve_mode: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Statistic {
    pub pushed: u64,
    pub dropped: u64,
}

pub struct Flow {
    weakref: Weak<RwLock<Flow>>,
    pub id: String,
    config: Config,
    statistic: Statistic,
    state: State,
    next_index: u64,
    tail_index: u64,
    sanitize_index: u64,
    bucket: HashMap<u64, SharedChunk>,
    bucket_capacity: u64,
    waiting_push: VecDeque<(u64, u64, oneshot::Sender<()>)>,
    waiting_pull: Arc<Mutex<HashMap<u64, Vec<oneshot::Sender<SharedChunk>>>>>,
    observers: Vec<Box<Observer>>,
}

type FlowFuture<T> = Box<Future<Item = T, Error = Error> + Send>;

impl Flow {
    pub fn new(config: Config) -> Arc<RwLock<Self>> {
        lazy_static! {
            static ref META_SIZE: u64 = (mem::size_of::<Mutex<Chunk>>() +
                                         mem::size_of::<Arc<Mutex<Chunk>>>()) as u64;
        }
        let bucket_capacity = if config.meta_capacity == 0 {
            0
        } else {
            (config.meta_capacity - 1) / *META_SIZE + 1
        };
        let flow = Flow {
            weakref: Weak::new(),
            id: Uuid::new_v4().simple().to_string(),
            config,
            statistic: Statistic {
                pushed: 0,
                dropped: 0,
            },
            state: State::Streaming,
            next_index: 0,
            tail_index: 0,
            sanitize_index: 0,
            bucket: HashMap::new(),
            bucket_capacity,
            waiting_push: VecDeque::new(),
            waiting_pull: Arc::new(Mutex::new(HashMap::new())),
            observers: Vec::new(),
        };
        let flow_ptr = Arc::new(RwLock::new(flow));
        flow_ptr.write().unwrap().weakref = Arc::downgrade(&flow_ptr);
        flow_ptr
    }

    pub fn get_config(&self) -> &Config {
        &self.config
    }

    pub fn get_range(&self) -> (u64, u64) {
        (self.tail_index, self.next_index)
    }

    pub fn get_statistic(&self) -> &Statistic {
        &self.statistic
    }

    pub fn observe<T: Observer>(&mut self, observer: T) {
        self.observers.push(Box::new(observer));
    }

    fn update_state(&mut self, new_state: State) -> Result<(), Error> {
        match new_state {
            State::Streaming if self.state == State::Streaming => {
                self.state = State::Streaming;
                Ok(())
            }
            State::Stop if self.state == State::Streaming => {
                self.state = State::Stop;
                Ok(())
            }
            State::Closed if self.state == State::Streaming || self.state == State::Stop => {
                self.state = State::Closed;
                for observer in self.observers.iter() {
                    observer.on_close(&self);
                }
                Ok(())
            }
            _ => Err(Error::Invalid),
        }
    }

    fn check_overflow(&self) -> bool {
        if self.statistic.pushed - self.statistic.dropped > self.config.data_capacity {
            return true;
        }
        if self.bucket.len() as u64 > self.bucket_capacity {
            return true;
        }
        false
    }

    fn acquire_chunk(&mut self, chunk: Chunk) -> Result<(u64, u64, u64), Error> {
        let chunk_len = chunk.len() as u64;
        if chunk_len > self.config.data_capacity {
            return Err(Error::Invalid);
        }

        // The order of following code is important. First check and return immediately if failed,
        // then update consistently.

        let new_pushed = match chunk {
            // EOF chunk ignores any overflow check.
            Chunk::Eof(..) => self.statistic.pushed,
            _ => {
                // Check if the flow is already overflow. Return if failed.
                if self.check_overflow() {
                    return Err(Error::NotReady);
                }
                // Check statistic. Return if failed.
                match self.statistic.pushed.checked_add(chunk_len) {
                    Some(new_pushed) => {
                        if let Some(length) = self.config.length {
                            // Check if the length is over.
                            if new_pushed > length {
                                return Err(Error::Invalid);
                            }
                        }
                        new_pushed
                    }
                    None => return Err(Error::Invalid),
                }
            }
        };
        // Check and update state. Return if failed.
        if self.update_state(match chunk {
            Chunk::Data(..) => State::Streaming,
            Chunk::Eof(..) => State::Stop,
        }).is_err()
        {
            return Err(Error::Invalid);
        }

        // Get chunk range.
        let chunk_start = self.statistic.pushed;
        let chunk_end = new_pushed;

        // Update statistic.
        self.statistic.pushed = new_pushed;

        // Acquire the chunk index.
        let chunk_index = self.next_index;
        self.next_index += 1;

        let shared_chunk = Arc::new(Mutex::new(chunk));
        // Insert the chunk.
        self.bucket.insert(chunk_index, shared_chunk.clone());

        // Wake up the waiting pulls.
        if let Some(waits) = self.waiting_pull.lock().unwrap().remove(&chunk_index) {
            for wait in waits {
                // Don't unwrap, since the receiver can early quit.
                wait.send(shared_chunk.clone()).is_ok();
            }
        }

        for observer in self.observers.iter() {
            observer.on_active(&self);
        }

        Ok((chunk_index, chunk_start, chunk_end))
    }

    fn sanitize_buffer(&mut self) {
        let next_index = self.next_index;
        let keepcount = self.config.keepcount.unwrap_or(0);
        while self.sanitize_index < next_index {
            let closed = {
                // Get should always success.
                let chunk = self.bucket.get(&self.sanitize_index).unwrap().lock().unwrap();
                if chunk.count() < keepcount {
                    break;
                }
                match *chunk {
                    Chunk::Eof(..) => true,
                    _ => false,
                }
            };
            // Try to close the flow.
            if closed {
                self.update_state(State::Closed).is_ok();
            }
            self.sanitize_index += 1;
        }

        // Remain one buffer chunk in preserve mode.
        if !self.config.preserve_mode || next_index - self.sanitize_index <= 1 {
            // If there isn't overflow, benignly keep chunks alive.
            while self.tail_index < self.sanitize_index && self.check_overflow() {
                {
                    let chunk_len =
                        self.bucket.get(&self.tail_index).unwrap().lock().unwrap().len();
                    // Update statistic.
                    self.statistic.dropped += chunk_len;
                }
                // Remove should always success.
                self.bucket.remove(&self.tail_index).unwrap();
                self.tail_index += 1;
            }
        }

        // Get the offset of tail.
        let tail_offset = self.statistic.dropped;
        loop {
            if let Some(wait) = self.waiting_push.front_mut() {
                // Check if the waiting chunk has been dropped.
                if wait.0 >= self.tail_index {
                    if wait.1 - tail_offset > self.config.data_capacity {
                        break;
                    }
                    if (wait.0 - self.tail_index + 1) > self.bucket_capacity {
                        break;
                    }
                }
            } else {
                break;
            };
            // Remove should always success.
            let wait = self.waiting_push.pop_front().unwrap();
            // Don't unwrap, since the receiver can early quit.
            wait.2.send(()).is_ok();
        }
    }

    pub fn push(&mut self, data: Bytes) -> FlowFuture<u64> {
        let chunk = Chunk::data(data);
        // Acquire the chunk. Return if failed.
        let (chunk_index, chunk_end) = match self.acquire_chunk(chunk) {
            Ok((chunk_index, _, chunk_end)) => (chunk_index, chunk_end),
            Err(err) => return future::err(err).boxed2(),
        };

        // Get the overflow status first.
        let is_overflow = self.check_overflow();

        // Atomically, automatically stop the flow if it reached the end.
        if let Some(length) = self.config.length {
            if self.statistic.pushed >= length {
                // Try to stop the flow.
                self.acquire_chunk(Chunk::eof()).is_ok();
            }
        }

        // Block for overflow.
        let fut = if is_overflow {
            let (tx, rx) = oneshot::channel();
            self.waiting_push.push_back((chunk_index, chunk_end, tx));
            rx.map(move |_| chunk_index).map_err(|_| Error::Other).boxed2()
        } else {
            future::ok(chunk_index).boxed2()
        };

        // Try to sanitize the buffer.
        self.sanitize_buffer();

        fut
    }

    pub fn close(&mut self) -> FlowFuture<()> {
        future::result(self.acquire_chunk(Chunk::eof()).map(|_| ())).boxed2()
    }

    pub fn pull(&self, chunk_index: u64, timeout: Option<u64>) -> FlowFuture<Bytes> {
        // Clone the chunk if exists.
        let chunk = self.bucket.get(&chunk_index).map(|chunk| chunk.clone());

        // Try to get the chunk.
        let fut = if let Some(chunk) = chunk {
            future::ok(chunk).boxed2()
        } else {
            if self.state != State::Streaming {
                future::err(Error::Eof).boxed2()
            } else if chunk_index < self.next_index {
                future::err(Error::Dropped).boxed2()
            } else if timeout == Some(0) {
                future::err(Error::NotReady).boxed2()
            } else {
                let (tx, rx) = oneshot::channel();
                let mut waiting_pull = self.waiting_pull.lock().unwrap();
                let waits = waiting_pull.entry(chunk_index).or_insert(Vec::new());
                waits.push(tx);
                rx.map_err(|_| Error::Other).boxed2()
            }
        };

        let flow_ref = self.weakref.clone();
        let keepcount = self.config.keepcount;
        fut.and_then(move |chunk| {
            let flow_ptr = match flow_ref.upgrade() {
                Some(ptr) => ptr.clone(),
                None => return future::err(Error::Other),
            };

            let (count, result) = {
                let mut chunk = chunk.lock().unwrap();
                let (count, result) = match *chunk {
                    Chunk::Data(ref mut count, ref data) => (count, Ok(data.clone())),
                    Chunk::Eof(ref mut count) => (count, Err(Error::Eof)),
                };
                *count += 1;
                (*count, result)
            };

            // Fast check if we need to sanitize.
            if match (&keepcount, &result) {
                (&None, &Err(Error::Eof)) => true,
                (&Some(keepcount), _) if count >= keepcount => true,
                _ => false,
            } {
                let mut flow = flow_ptr.write().unwrap();
                flow.sanitize_buffer();
            }

            future::result(result)
        }).boxed2()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FLOW_CONFIG: Config = Config {
        length: None,
        meta_capacity: 16777216,
        data_capacity: 16777216,
        keepcount: Some(1),
        preserve_mode: false,
    };

    macro_rules! sync_assert_eq {
        ($a:expr, $b:expr) => {
            let fut = $a;
            assert_eq!(fut.wait(), $b);
        }
    }

    #[test]
    fn basic_operations() {
        let ptr = Flow::new(FLOW_CONFIG);
        sync_assert_eq!(ptr.write().unwrap().push(vec![1u8; 1234].into()), Ok(0));
        sync_assert_eq!(ptr.write().unwrap().push("hello".into()), Ok(1));
        sync_assert_eq!(ptr.read().unwrap().pull(0, Some(0)), Ok(vec![1u8; 1234].into()));
        sync_assert_eq!(ptr.read().unwrap().pull(1, Some(0)), Ok("hello".into()));
        sync_assert_eq!(ptr.read().unwrap().pull(100, Some(0)), Err(Error::NotReady));
    }

    #[test]
    fn fixed_length_flow() {
        let ptr = Flow::new(Config {
            length: Some(10),
            meta_capacity: 16777216,
            data_capacity: 16777216,
            keepcount: Some(1),
            preserve_mode: false,
        });
        sync_assert_eq!(ptr.write().unwrap().push("hello".into()), Ok(0));
        sync_assert_eq!(ptr.write().unwrap().push("world".into()), Ok(1));
        sync_assert_eq!(ptr.write().unwrap().push("!".into()), Err(Error::Invalid));
        sync_assert_eq!(ptr.read().unwrap().pull(2, Some(0)), Err(Error::Eof));
    }

    #[test]
    fn close_flow() {
        let ptr = Flow::new(FLOW_CONFIG);
        sync_assert_eq!(ptr.write().unwrap().push("hello".into()), Ok(0));
        sync_assert_eq!(ptr.read().unwrap().pull(0, Some(0)), Ok("hello".into()));
        sync_assert_eq!(ptr.read().unwrap().pull(2, Some(0)), Err(Error::NotReady));
        sync_assert_eq!(ptr.write().unwrap().close(), Ok(()));
        sync_assert_eq!(ptr.write().unwrap().push("hello".into()), Err(Error::Invalid));
        sync_assert_eq!(ptr.read().unwrap().pull(2, Some(0)), Err(Error::Eof));
        sync_assert_eq!(ptr.read().unwrap().pull(1, Some(0)), Err(Error::Eof));
        sync_assert_eq!(ptr.write().unwrap().close(), Err(Error::Invalid));

        let ptr = Flow::new(Config {
            length: Some(10),
            meta_capacity: 16777216,
            data_capacity: 16777216,
            keepcount: Some(1),
            preserve_mode: false,
        });
        sync_assert_eq!(ptr.write().unwrap().push("hello".into()), Ok(0));
        sync_assert_eq!(ptr.write().unwrap().close(), Ok(()));
        sync_assert_eq!(ptr.write().unwrap().close(), Err(Error::Invalid));
        sync_assert_eq!(ptr.write().unwrap().push("world".into()), Err(Error::Invalid));
        sync_assert_eq!(ptr.read().unwrap().pull(1, Some(0)), Err(Error::Eof));
    }

    #[test]
    fn dropped_chunk() {
        let ptr = Flow::new(Config {
            length: None,
            meta_capacity: REF_SIZE as u64 * 2,
            data_capacity: REF_SIZE as u64 * 2,
            keepcount: Some(2),
            preserve_mode: false,
        });
        let payload1 = vec![0u8; REF_SIZE];
        let payload2 = vec![1u8; REF_SIZE];
        let payload3 = vec![2u8; REF_SIZE];

        sync_assert_eq!(ptr.write().unwrap().push(payload1.clone().into()), Ok(0));
        sync_assert_eq!(ptr.write().unwrap().push(payload2.clone().into()), Ok(1));
        let fut = ptr.write().unwrap().push(payload3.clone().into());
        sync_assert_eq!(ptr.read().unwrap().pull(1, Some(0)), Ok(payload2.clone().into()));
        sync_assert_eq!(ptr.read().unwrap().pull(1, Some(0)), Ok(payload2.clone().into()));
        sync_assert_eq!(ptr.read().unwrap().pull(0, Some(0)), Ok(payload1.clone().into()));
        sync_assert_eq!(ptr.read().unwrap().pull(0, Some(0)), Ok(payload1.clone().into()));
        sync_assert_eq!(fut, Ok(2));
        sync_assert_eq!(ptr.read().unwrap().pull(0, Some(0)), Err(Error::Dropped));
        sync_assert_eq!(ptr.read().unwrap().pull(1, Some(0)), Ok(payload2.clone().into()));
        sync_assert_eq!(ptr.write().unwrap().push(payload3.clone().into()), Ok(3));
        sync_assert_eq!(ptr.read().unwrap().pull(1, Some(0)), Err(Error::Dropped));
        assert_eq!(ptr.read().unwrap().get_range(), (2, 4));
        assert_eq!(
            ptr.read().unwrap().get_statistic(),
            &Statistic {
                pushed: (payload1.len() + payload2.len() + payload3.len() * 2) as u64,
                dropped: (payload1.len() + payload2.len()) as u64,
            }
        );
    }

    #[test]
    fn preserve_dropped_chunk() {
        let ptr = Flow::new(Config {
            length: None,
            meta_capacity: REF_SIZE as u64 * 2,
            data_capacity: REF_SIZE as u64 * 2,
            keepcount: Some(1),
            preserve_mode: true,
        });
        let payload1 = vec![0u8; REF_SIZE];
        let payload2 = vec![1u8; REF_SIZE];
        let payload3 = vec![2u8; REF_SIZE + 1];

        sync_assert_eq!(ptr.write().unwrap().push(payload1.clone().into()), Ok(0));
        sync_assert_eq!(ptr.write().unwrap().push(payload2.clone().into()), Ok(1));
        let fut = ptr.write().unwrap().push(payload3.clone().into());
        sync_assert_eq!(ptr.read().unwrap().pull(0, Some(0)), Ok(payload1.clone().into()));
        sync_assert_eq!(ptr.read().unwrap().pull(0, Some(0)), Ok(payload1.clone().into()));
        sync_assert_eq!(ptr.write().unwrap().push(payload3.clone().into()), Err(Error::NotReady));
        sync_assert_eq!(ptr.read().unwrap().pull(1, Some(0)), Ok(payload2.clone().into()));
        sync_assert_eq!(fut, Ok(2));
        sync_assert_eq!(ptr.read().unwrap().pull(0, Some(0)), Err(Error::Dropped));
        sync_assert_eq!(ptr.read().unwrap().pull(1, Some(0)), Err(Error::Dropped));
        assert_eq!(ptr.read().unwrap().get_range(), (2, 3));
        assert_eq!(
            ptr.read().unwrap().get_statistic(),
            &Statistic {
                pushed: (payload1.len() + payload2.len() + payload3.len()) as u64,
                dropped: (payload1.len() + payload2.len()) as u64,
            }
        );
    }

    #[test]
    fn waiting_pull() {
        let ptr = Flow::new(FLOW_CONFIG);
        sync_assert_eq!(ptr.read().unwrap().pull(0, Some(0)), Err(Error::NotReady));
        let fut = ptr.read().unwrap().pull(1, None);
        let fut1 = ptr.write().unwrap().push("ello".into());
        let fut2 = ptr.write().unwrap().push("hello".into());
        sync_assert_eq!(fut.join3(fut1, fut2), Ok(("hello".into(), 0, 1)));
    }

    #[test]
    fn waiting_push() {
        let ptr = Flow::new(FLOW_CONFIG);
        let payload = vec![0u8; REF_SIZE];

        sync_assert_eq!(ptr.write().unwrap().push("A".into()), Ok(0));
        for idx in 1..(FLOW_CONFIG.data_capacity / REF_SIZE as u64) {
            sync_assert_eq!(ptr.write().unwrap().push(payload.clone().into()), Ok(idx));
        }
        let base_idx = FLOW_CONFIG.data_capacity / REF_SIZE as u64;
        sync_assert_eq!(ptr.write().unwrap().push("D".into()), Ok(base_idx));

        let fut = ptr.write().unwrap().push(vec![0u8; REF_SIZE].into());
        sync_assert_eq!(ptr.write().unwrap().push("C".into()), Err(Error::NotReady));
        sync_assert_eq!(ptr.write().unwrap().close(), Ok(()));
        sync_assert_eq!(ptr.read().unwrap().pull(0, Some(0)), Ok("A".into()));
        sync_assert_eq!(ptr.read().unwrap().pull(1, Some(0)), Ok(payload.clone().into()));
        sync_assert_eq!(fut, Ok(base_idx + 1));
    }

    #[test]
    fn waiting_meta() {
        let ptr = Flow::new(Config {
            length: None,
            meta_capacity: 4096,
            data_capacity: 65536,
            keepcount: Some(1),
            preserve_mode: false,
        });

        for _ in 0..4096 {
            ptr.write().unwrap().push("A".into());
        }

        sync_assert_eq!(ptr.write().unwrap().push("B".into()), Err(Error::NotReady));

        let (_, next_index) = ptr.read().unwrap().get_range();
        for idx in 0..next_index {
            sync_assert_eq!(ptr.read().unwrap().pull(idx, Some(0)), Ok("A".into()));
        }

        let fut = ptr.write().unwrap().push("B".into());
        sync_assert_eq!(ptr.read().unwrap().pull(next_index, Some(0)), Ok("B".into()));
        sync_assert_eq!(fut, Ok(next_index));
    }

    #[test]
    fn outlive() {
        let fut = {
            let ptr = Flow::new(FLOW_CONFIG);
            sync_assert_eq!(ptr.write().unwrap().push("A".into()), Ok(0));
            let flow = ptr.read().unwrap();
            flow.pull(0, Some(0))
        };
        sync_assert_eq!(fut, Err(Error::Other));

        let fut = {
            let ptr = Flow::new(Config {
                length: None,
                meta_capacity: 16777216,
                data_capacity: 1,
                keepcount: Some(1),
                preserve_mode: false,
            });
            sync_assert_eq!(ptr.write().unwrap().push("A".into()), Ok(0));
            let mut flow = ptr.write().unwrap();
            flow.push("B".into())
        };
        sync_assert_eq!(fut, Err(Error::Other));
    }

    #[test]
    fn observer() {
        let ptr = Flow::new(FLOW_CONFIG);

        #[derive(Clone)]
        struct Ob(pub Arc<Mutex<u32>>);

        impl Ob {
            fn new() -> Self {
                Ob(Arc::new(Mutex::new(0)))
            }
        }

        impl Observer for Ob {
            fn on_active(&self, _flow: &Flow) {
                let mut flag = self.0.lock().unwrap();
                *flag += 1;
            }

            fn on_close(&self, _flow: &Flow) {
                let mut flag = self.0.lock().unwrap();
                *flag += 1;
            }
        }

        let ob1 = Ob::new();
        let ob2 = Ob::new();
        {
            ptr.write().unwrap().observe(ob1.clone());
            ptr.write().unwrap().observe(ob2.clone());
        }

        sync_assert_eq!(ptr.write().unwrap().close(), Ok(()));
        sync_assert_eq!(ptr.read().unwrap().pull(0, Some(0)), Err(Error::Eof));
        assert_eq!(*ob1.0.lock().unwrap(), 2);
        assert_eq!(*ob2.0.lock().unwrap(), 2);
    }

    #[test]
    fn nonblocking() {
        #[derive(Clone)]
        struct Ob(pub Arc<Mutex<bool>>);

        impl Ob {
            fn new() -> Self {
                Ob(Arc::new(Mutex::new(false)))
            }
        }

        impl Observer for Ob {
            fn on_close(&self, _flow: &Flow) {
                let mut flag = self.0.lock().unwrap();
                *flag = true;
            }
        }

        fn run_test(ptr: Arc<RwLock<Flow>>) {
            let payload = vec![0u8; REF_SIZE];
            let ob1 = Ob::new();
            ptr.write().unwrap().observe(ob1.clone());

            for idx in 0..16 {
                sync_assert_eq!(ptr.write().unwrap().push(payload.clone().into()), Ok(idx));
            }
            sync_assert_eq!(ptr.write().unwrap().push("world".into()), Ok(16));

            sync_assert_eq!(ptr.read().unwrap().pull(0, Some(0)), Err(Error::Dropped));
            sync_assert_eq!(ptr.read().unwrap().pull(1, Some(0)), Ok(payload.clone().into()));

            sync_assert_eq!(ptr.write().unwrap().close(), Ok(()));
            sync_assert_eq!(ptr.read().unwrap().pull(1, Some(0)), Ok(payload.clone().into()));
            sync_assert_eq!(ptr.write().unwrap().push("!".into()), Err(Error::Invalid));
            assert_eq!(*ob1.0.lock().unwrap(), false);
            sync_assert_eq!(ptr.read().unwrap().pull(17, Some(0)), Err(Error::Eof));
            assert_eq!(*ob1.0.lock().unwrap(), true);
        }

        let ptr = Flow::new(Config {
            length: None,
            meta_capacity: REF_SIZE as u64 * 16,
            data_capacity: REF_SIZE as u64 * 16,
            keepcount: None,
            preserve_mode: false,
        });
        run_test(ptr);

        let ptr = Flow::new(Config {
            length: None,
            meta_capacity: REF_SIZE as u64 * 16,
            data_capacity: REF_SIZE as u64 * 16,
            keepcount: None,
            preserve_mode: true,
        });
        run_test(ptr);
    }

    #[test]
    fn get_config() {
        let config = Config {
            length: Some(18446744073709551615),
            meta_capacity: 4096,
            data_capacity: 65536,
            keepcount: Some(18446744073709551615),
            preserve_mode: false,
        };
        let ptr = Flow::new(config.clone());
        assert_eq!(ptr.read().unwrap().get_config(), &config);

        let config = Config {
            length: None,
            meta_capacity: 18446744073709551615,
            data_capacity: 18446744073709551615,
            keepcount: None,
            preserve_mode: false,
        };
        let ptr = Flow::new(config.clone());
        assert_eq!(ptr.read().unwrap().get_config(), &config);
    }

    #[test]
    fn sanitize() {
        let ptr = Flow::new(Config {
            length: None,
            meta_capacity: 16777216,
            data_capacity: REF_SIZE as u64 * 2,
            keepcount: Some(1),
            preserve_mode: false,
        });
        let payload1 = vec![0u8; REF_SIZE + 1];
        let payload2 = vec![1u8; REF_SIZE + 2];
        sync_assert_eq!(ptr.write().unwrap().push(payload1.clone().into()), Ok(0));
        let fut = ptr.write().unwrap().push(payload2.clone().into());
        sync_assert_eq!(ptr.read().unwrap().pull(0, Some(0)), Ok(payload1.clone().into()));
        sync_assert_eq!(fut, Ok(1));

        let payload3 = vec![2u8; REF_SIZE];
        let ptr = Flow::new(Config {
            length: None,
            meta_capacity: 0,
            data_capacity: 16777216,
            keepcount: None,
            preserve_mode: false,
        });
        for idx in 0..100 {
            sync_assert_eq!(ptr.write().unwrap().push(payload3.clone().into()), Ok(idx));
        }
        assert_eq!(ptr.read().unwrap().get_range(), (100, 100));

        let ptr = Flow::new(Config {
            length: None,
            meta_capacity: 0,
            data_capacity: 16777216,
            keepcount: None,
            preserve_mode: true,
        });
        for idx in 0..100 {
            sync_assert_eq!(ptr.write().unwrap().push(payload3.clone().into()), Ok(idx));
        }
        assert_eq!(ptr.read().unwrap().get_range(), (100, 100));
    }

    #[test]
    fn chunk_size() {
        let ptr = Flow::new(Config {
            length: None,
            meta_capacity: 16777216,
            data_capacity: 0,
            keepcount: Some(1),
            preserve_mode: false,
        });
        let payload = vec![0u8; 0];
        sync_assert_eq!(ptr.write().unwrap().push(payload.clone().into()), Ok(0));
        sync_assert_eq!(ptr.write().unwrap().push(vec![0u8; 1].into()), Err(Error::Invalid));
        sync_assert_eq!(ptr.write().unwrap().close(), Ok(()));
        sync_assert_eq!(ptr.read().unwrap().pull(0, Some(0)), Ok(payload.clone().into()));
        sync_assert_eq!(ptr.read().unwrap().pull(1, Some(0)), Err(Error::Eof));
    }
}
