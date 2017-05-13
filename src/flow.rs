use futures::{Future, future};
use futures::sync::oneshot;
use std::collections::{HashMap, VecDeque};
use std::mem;
use std::sync::{Arc, Mutex, RwLock, Weak};
use uuid::Uuid;

#[derive(Debug, PartialEq)]
pub enum Error {
    Invalid,
    Dropped,
    NotReady,
    Eof,
    Other,
}

pub const MAX_SIZE: usize = 1048576;

#[derive(Debug)]
pub enum Chunk {
    Data(u64, Vec<u8>),
    Eof(u64),
}

impl Chunk {
    fn data(data: Vec<u8>) -> Self {
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

pub trait Observer {
    fn on_active(&self, _flow: &Flow) {}
    fn on_close(&self, _flow: &Flow) {}
}

#[derive(Clone, Debug, PartialEq)]
enum State {
    Streaming,
    Stop,
    Closed,
}

pub struct Config {
    pub length: Option<u64>,
    pub meta_capacity: u64,
    pub data_capacity: u64,
    pub keepcount: Option<u64>,
}

pub struct Statistic {
    pub pushed: u64,
    pub dropped: u64,
    pub buffered: u64,
}

pub struct Flow {
    weakref: Weak<RwLock<Flow>>,
    pub id: String,
    config: Config,
    statistic: Statistic,
    state: State,
    next_index: u64,
    tail_index: u64,
    bucket: HashMap<u64, SharedChunk>,
    waiting_push: VecDeque<(u64, oneshot::Sender<()>)>,
    waiting_pull: Arc<Mutex<HashMap<u64, Vec<oneshot::Sender<SharedChunk>>>>>,
    observers: Vec<Box<Observer + Send + Sync + 'static>>,
}

type FlowFuture<T> = future::BoxFuture<T, Error>;

impl Flow {
    pub fn new(config: Config) -> Arc<RwLock<Self>> {
        let flow = Flow {
            weakref: Weak::new(),
            id: Uuid::new_v4().simple().to_string(),
            config,
            statistic: Statistic {
                pushed: 0,
                dropped: 0,
                buffered: 0,
            },
            state: State::Streaming,
            next_index: 0,
            tail_index: 0,
            bucket: HashMap::new(),
            waiting_push: VecDeque::new(),
            waiting_pull: Arc::new(Mutex::new(HashMap::new())),
            observers: Vec::new(),
        };
        let flow_ptr = Arc::new(RwLock::new(flow));
        flow_ptr.write().unwrap().weakref = Arc::downgrade(&flow_ptr);
        flow_ptr
    }

    pub fn get_range(&self) -> (u64, u64) {
        (self.tail_index, self.next_index)
    }

    pub fn observe<T: Observer + Sync + Send + 'static>(&mut self, observer: T) {
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
        lazy_static! {   
            static ref META_SIZE: u64 = (mem::size_of::<Chunk>() +
                                         mem::size_of::<Mutex<Chunk>>() +
                                         mem::size_of::<Arc<Mutex<Chunk>>>()) as u64;
        }
        (self.statistic.buffered > self.config.data_capacity) ||
        ((self.bucket.len() as u64 * *META_SIZE) > self.config.meta_capacity)
    }

    fn acquire_chunk(&mut self, chunk: Chunk) -> FlowFuture<u64> {
        let chunk_len = chunk.len() as u64;

        // The order of following code is important. We first check and return immediately if
        // failed, then update consistently.

        // Check if the flow is already overflow. Return if failed.
        if self.check_overflow() {
            return future::err(Error::NotReady).boxed();
        }
        // Check statistic. Return if failed.
        let new_pushed = match self.statistic.pushed.checked_add(chunk_len) {
            Some(new_pushed) => {
                if let Some(length) = self.config.length {
                    // Check if the length is over.
                    if new_pushed > length {
                        return future::err(Error::Invalid).boxed();
                    }
                }
                new_pushed
            }
            None => return future::err(Error::Invalid).boxed(),
        };
        let new_buffered = match self.statistic.buffered.checked_add(chunk_len) {
            Some(new_buffered) => new_buffered,
            None => return future::err(Error::Invalid).boxed(),
        };

        // Check and update state. Return if failed.
        if self.update_state(match chunk {
                                 Chunk::Data(..) => State::Streaming,
                                 Chunk::Eof(..) => State::Stop,
                             })
               .is_err() {
            return future::err(Error::Invalid).boxed();
        }
        // Update statistic.
        self.statistic.pushed = new_pushed;
        self.statistic.buffered = new_buffered;

        // Acquire the chunk index.
        let chunk_index = self.next_index;
        self.next_index += 1;

        let shared_chunk = Arc::new(Mutex::new(chunk));
        // Insert the chunk.
        self.bucket.insert(chunk_index, shared_chunk.clone());

        // Check if we need to block for overflow.
        let fut = if self.check_overflow() {
            let (tx, rx) = oneshot::channel();
            self.waiting_push.push_back((chunk_len, tx));
            rx.map_err(|_| Error::Other).boxed()
        } else {
            future::ok(()).boxed()
        };

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

        if self.config.keepcount == None {
            // TODO Build a fast path for non-blocking flow.
            self.sanitize_buffer();
        }

        fut.map(move |_| chunk_index).boxed()
    }

    fn sanitize_buffer(&mut self) {
        let mut tail_index = self.tail_index;
        let next_index = self.next_index;
        let keepcount = self.config.keepcount.unwrap_or(0);

        while tail_index < next_index {
            // If there is no waiting push and the flow is streaming, benignly keep chunks alive.
            if self.state == State::Streaming && self.waiting_push.len() == 0 {
                break;
            }

            let closed = {
                // Get should always success.
                let chunk = self.bucket.get(&tail_index).unwrap().lock().unwrap();
                if chunk.count() < keepcount {
                    break;
                }

                self.statistic.buffered -= chunk.len();
                self.statistic.dropped += chunk.len();

                let mut remain = chunk.len();
                while remain > 0 {
                    let new_len = if let Some(wait) = self.waiting_push.front_mut() {
                        let (new_remain, new_len) = if remain >= wait.0 {
                            (remain - wait.0, 0)
                        } else {
                            (0, wait.0 - remain)
                        };
                        remain = new_remain;
                        wait.0 = new_len;
                        wait.0
                    } else {
                        break;
                    };
                    if new_len == 0 {
                        // Remove should always success.
                        let wait = self.waiting_push.pop_front().unwrap();
                        // Don't unwrap, since the receiver can early quit.
                        wait.1.send(()).is_ok();
                    }
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

            // Remove should always success.
            self.bucket.remove(&tail_index).unwrap();

            tail_index += 1;
        }

        self.tail_index = tail_index;
    }

    pub fn push(&mut self, data: &[u8]) -> FlowFuture<u64> {
        if data.len() > MAX_SIZE {
            return future::err(Error::Invalid).boxed();
        }
        self.acquire_chunk(Chunk::data(data.to_vec()))
    }

    pub fn close(&mut self) -> FlowFuture<()> {
        self.acquire_chunk(Chunk::eof()).map(|_| ()).boxed()
    }

    pub fn pull(&self, chunk_index: u64, timeout: Option<u64>) -> FlowFuture<Vec<u8>> {
        // Clone the chunk if exists.
        let chunk = self.bucket.get(&chunk_index).map(|chunk| chunk.clone());

        // Try to get the chunk.
        let fut = if let Some(chunk) = chunk {
            future::ok(chunk).boxed()
        } else {
            if self.state != State::Streaming {
                future::err(Error::Eof).boxed()
            } else if chunk_index < self.next_index {
                future::err(Error::Dropped).boxed()
            } else if let Some(0) = timeout {
                future::err(Error::NotReady).boxed()
            } else {
                let (tx, rx) = oneshot::channel();
                let mut waiting_pull = self.waiting_pull.lock().unwrap();
                let waits = waiting_pull.entry(chunk_index).or_insert(Vec::new());
                waits.push(tx);
                rx.map_err(|_| Error::Other).boxed()
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
                        Chunk::Data(ref mut count, ref data) => (count, Ok(data.to_vec())),
                        Chunk::Eof(ref mut count) => (count, Err(Error::Eof)),
                    };
                    *count += 1;
                    (*count, result)
                };

                // Fast check if we need to sanitize.
                if let Some(keepcount) = keepcount {
                    if count >= keepcount {
                        let mut flow = flow_ptr.write().unwrap();
                        flow.sanitize_buffer();
                    }
                }

                future::result(result)
            })
            .boxed()
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
        sync_assert_eq!(ptr.write().unwrap().push(&[1u8; 1234]), Ok(0));
        sync_assert_eq!(ptr.write().unwrap().push(&[2u8; MAX_SIZE]), Ok(1));
        sync_assert_eq!(ptr.write().unwrap().push(b"hello"), Ok(2));
        sync_assert_eq!(ptr.write().unwrap().push(&[2u8; MAX_SIZE + 1]), Err(Error::Invalid));
        sync_assert_eq!(ptr.read().unwrap().pull(0, Some(0)), Ok(Vec::from(&[1u8; 1234] as &[u8])));
        sync_assert_eq!(ptr.read().unwrap().pull(2, Some(0)), Ok(Vec::from(b"hello" as &[u8])));
        sync_assert_eq!(ptr.read().unwrap().pull(100, Some(0)), Err(Error::NotReady));
    }

    #[test]
    fn fixed_size_flow() {
        let ptr = Flow::new(Config {
                                length: Some(10),
                                meta_capacity: 16777216,
                                data_capacity: 16777216,
                                keepcount: Some(1),
                            });
        sync_assert_eq!(ptr.write().unwrap().push(b"hello"), Ok(0));
        sync_assert_eq!(ptr.write().unwrap().push(b"world"), Ok(1));
        sync_assert_eq!(ptr.write().unwrap().push(b"!"), Err(Error::Invalid));
    }

    #[test]
    fn close_flow() {
        let ptr = Flow::new(FLOW_CONFIG);
        sync_assert_eq!(ptr.write().unwrap().push(b"hello"), Ok(0));
        sync_assert_eq!(ptr.read().unwrap().pull(0, Some(0)), Ok(Vec::from(b"hello" as &[u8])));
        sync_assert_eq!(ptr.read().unwrap().pull(2, Some(0)), Err(Error::NotReady));
        sync_assert_eq!(ptr.write().unwrap().close(), Ok(()));
        sync_assert_eq!(ptr.write().unwrap().push(b"hello"), Err(Error::Invalid));
        sync_assert_eq!(ptr.read().unwrap().pull(2, Some(0)), Err(Error::Eof));
        sync_assert_eq!(ptr.read().unwrap().pull(1, Some(0)), Err(Error::Eof));
        sync_assert_eq!(ptr.write().unwrap().close(), Err(Error::Invalid));
    }

    #[test]
    fn dropped_chunk() {
        let ptr = Flow::new(Config {
                                length: None,
                                meta_capacity: (MAX_SIZE * 2) as u64,
                                data_capacity: (MAX_SIZE * 2) as u64,
                                keepcount: Some(2),
                            });
        let payload1 = vec![0u8; MAX_SIZE];
        let payload2 = vec![1u8; MAX_SIZE];
        let payload3 = vec![2u8; MAX_SIZE];

        sync_assert_eq!(ptr.write().unwrap().push(&payload1), Ok(0));
        sync_assert_eq!(ptr.write().unwrap().push(&payload2), Ok(1));
        let fut = ptr.write().unwrap().push(&payload3);
        sync_assert_eq!(ptr.read().unwrap().pull(0, Some(0)), Ok(payload1.to_owned()));
        sync_assert_eq!(ptr.read().unwrap().pull(0, Some(0)), Ok(payload1));
        sync_assert_eq!(fut, Ok(2));
        sync_assert_eq!(ptr.read().unwrap().pull(0, Some(0)), Err(Error::Dropped));
        let fut = ptr.write().unwrap().push(&payload3);
        sync_assert_eq!(ptr.read().unwrap().pull(1, Some(0)), Ok(payload2.to_owned()));
        sync_assert_eq!(ptr.read().unwrap().pull(1, Some(0)), Ok(payload2));
        sync_assert_eq!(fut, Ok(3));
        sync_assert_eq!(ptr.read().unwrap().pull(1, Some(0)), Err(Error::Dropped));
        assert_eq!(ptr.read().unwrap().get_range(), (2, 4));
    }

    #[test]
    fn waiting_pull() {
        let ptr = Flow::new(FLOW_CONFIG);
        sync_assert_eq!(ptr.read().unwrap().pull(0, Some(0)), Err(Error::NotReady));
        let fut = ptr.read().unwrap().pull(1, None);
        let fut1 = ptr.write().unwrap().push(b"ello");
        let fut2 = ptr.write().unwrap().push(b"hello");
        sync_assert_eq!(fut.join3(fut1, fut2), Ok((Vec::from(b"hello" as &[u8]), 0, 1)));
    }

    #[test]
    fn waiting_push() {
        let ptr = Flow::new(FLOW_CONFIG);
        let payload = vec![0u8; MAX_SIZE];

        sync_assert_eq!(ptr.write().unwrap().push(b"A"), Ok(0));
        for idx in 1..(FLOW_CONFIG.data_capacity / MAX_SIZE as u64) {
            sync_assert_eq!(ptr.write().unwrap().push(&payload), Ok(idx));
        }
        let base_idx = FLOW_CONFIG.data_capacity / MAX_SIZE as u64;
        sync_assert_eq!(ptr.write().unwrap().push(b"D"), Ok(base_idx));

        let fut = ptr.write().unwrap().push(&[0u8; MAX_SIZE]);
        sync_assert_eq!(ptr.write().unwrap().push(b"C"), Err(Error::NotReady));
        sync_assert_eq!(ptr.read().unwrap().pull(0, Some(0)), Ok(Vec::from(b"A" as &[u8])));
        sync_assert_eq!(ptr.read().unwrap().pull(1, Some(0)), Ok(payload));
        sync_assert_eq!(fut, Ok(base_idx + 1));
    }

    #[test]
    fn waiting_meta() {
        let ptr = Flow::new(Config {
                                length: None,
                                meta_capacity: 4096,
                                data_capacity: 65536,
                                keepcount: Some(1),
                            });

        for _ in 0..4096 {
            ptr.write().unwrap().push(b"A");
        }

        sync_assert_eq!(ptr.write().unwrap().push(b"B"), Err(Error::NotReady));

        let next_index = {
            ptr.read().unwrap().get_range().1
        };
        for idx in 0..next_index {
            sync_assert_eq!(ptr.read().unwrap().pull(idx, Some(0)), Ok(Vec::from(b"A" as &[u8])));
        }

        let fut = ptr.write().unwrap().push(b"B");
        sync_assert_eq!(ptr.read().unwrap().pull(next_index, Some(0)),
                        Ok(Vec::from(b"B" as &[u8])));
        sync_assert_eq!(fut, Ok(next_index));
    }

    #[test]
    fn outlive() {
        let fut = {
            let ptr = Flow::new(FLOW_CONFIG);
            sync_assert_eq!(ptr.write().unwrap().push(b"A"), Ok(0));
            let flow = ptr.read().unwrap();
            flow.pull(0, Some(0))
        };
        sync_assert_eq!(fut, Err(Error::Other));
    }

    #[test]
    fn observer() {
        let ptr = Flow::new(FLOW_CONFIG);

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

        let ob1 = Ob::new();
        let ob2 = Ob::new();
        {
            ptr.write().unwrap().observe(ob1.clone());
            ptr.write().unwrap().observe(ob2.clone());
        }

        sync_assert_eq!(ptr.write().unwrap().close(), Ok(()));
        sync_assert_eq!(ptr.read().unwrap().pull(0, Some(0)), Err(Error::Eof));
        assert_eq!(*ob1.0.lock().unwrap(), true);
        assert_eq!(*ob2.0.lock().unwrap(), true);
    }

    #[test]
    fn nonblocking() {
        let ptr = Flow::new(Config {
                                length: None,
                                meta_capacity: (MAX_SIZE * 16) as u64,
                                data_capacity: (MAX_SIZE * 16) as u64,
                                keepcount: None,
                            });
        let payload = vec![0u8; MAX_SIZE];

        for idx in 0..16 {
            sync_assert_eq!(ptr.write().unwrap().push(&payload), Ok(idx));
        }
        sync_assert_eq!(ptr.write().unwrap().push(b"world"), Ok(16));

        sync_assert_eq!(ptr.read().unwrap().pull(0, Some(0)), Err(Error::Dropped));
        sync_assert_eq!(ptr.read().unwrap().pull(1, Some(0)), Ok(payload.to_owned()));
        sync_assert_eq!(ptr.read().unwrap().pull(1, Some(0)), Ok(payload.to_owned()));
    }
}
