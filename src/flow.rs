use futures::{Future, future};
use futures::sync::oneshot;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use uuid::Uuid;

#[derive(Debug, PartialEq)]
pub enum Error {
    Invalid,
    NotFound,
    Dropped,
    NotReady,
    Eof,
    Other,
}

pub const MAX_SIZE: usize = 65536;
pub const MAX_CAPACITY: u64 = 16777216;

#[derive(Clone, Debug)]
pub enum Chunk {
    Data(u64, Vec<u8>),
    Eof(u64),
}

type SharedChunk = Arc<RwLock<Chunk>>;

#[derive(Clone, Debug)]
enum State {
    Streaming,
    Stop,
}

pub struct Config {
    pub length: Option<u64>,
    pub capacity: u64,
}

pub struct Statistic {
    pub pushed: u64,
    pub dropped: u64,
}

pub struct Flow {
    pub id: String,
    pub config: Config,
    pub statistic: Statistic,
    state: State,
    next_index: u64,
    pub tail_index: u64,
    buffered: u64,
    chunk_bucket: HashMap<u64, SharedChunk>,
    wait_list: Arc<Mutex<HashMap<u64, Vec<oneshot::Sender<SharedChunk>>>>>,
}

type FlowFuture<T> = future::BoxFuture<T, Error>;

impl Flow {
    pub fn new(length: Option<u64>) -> Self {
        Flow {
            id: Uuid::new_v4().simple().to_string(),
            config: Config {
                length,
                capacity: MAX_CAPACITY,
            },
            statistic: Statistic {
                pushed: 0,
                dropped: 0,
            },
            state: State::Streaming,
            next_index: 0,
            tail_index: 0,
            buffered: 0,
            chunk_bucket: HashMap::new(),
            wait_list: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn is_streaming(&self) -> bool {
        match self.state {
            State::Streaming => true,
            _ => false,
        }
    }

    fn acquire_chunk(&mut self, chunk: Chunk) -> FlowFuture<u64> {
        let shared_chunk = Arc::new(RwLock::new(chunk));
        let chunk_index = self.next_index;
        self.next_index += 1;
        // Wake up the waiting pulls.
        {
            let mut wait_list = self.wait_list.lock().unwrap();
            if let Some(waits) = wait_list.remove(&chunk_index) {
                for wait in waits {
                    // Don't unwrap, since the receiver can early quit.
                    wait.send(shared_chunk.clone()).is_ok();
                }
            }
        }
        // Insert the chunk.
        self.chunk_bucket.insert(chunk_index, shared_chunk);
        future::ok(chunk_index).boxed()
    }

    pub fn close(&mut self) -> FlowFuture<()> {
        if self.is_streaming() {
            self.state = State::Stop;
            self.acquire_chunk(Chunk::Eof(1)).map(|_| ()).boxed()
        } else {
            future::err(Error::Invalid).boxed()
        }
    }

    pub fn push(&mut self, data: &[u8]) -> FlowFuture<u64> {
        if data.len() > MAX_SIZE {
            return future::err(Error::Invalid).boxed();
        }
        match self.statistic.pushed.checked_add(data.len() as u64) {
            Some(new_pushed) => {
                if let Some(length) = self.config.length {
                    // Check if the length is over.
                    if new_pushed > length {
                        return future::err(Error::Invalid).boxed();
                    }
                }
                self.statistic.pushed = new_pushed;
            }
            None => return future::err(Error::Invalid).boxed(),
        };
        self.acquire_chunk(Chunk::Data(1, data.to_vec()))
    }

    fn deref_chunk(shared_chunk: &SharedChunk) -> Result<Vec<u8>, Error> {
        let mut chunk = shared_chunk.write().unwrap();
        match *chunk {
            Chunk::Data(ref mut count, ref data) => {
                *count += 1;
                Ok(data.to_vec())
            }
            Chunk::Eof(ref mut count) => {
                *count += 1;
                Err(Error::Eof)
            }
        }
    }

    pub fn pull(&self, chunk_index: u64, timeout: Option<u64>) -> FlowFuture<Vec<u8>> {
        if let Some(chunk) = self.chunk_bucket.get(&chunk_index) {
            return future::result(Flow::deref_chunk(chunk)).boxed();
        } else if chunk_index < self.next_index {
            return future::err(Error::Dropped).boxed();
        } else if let Some(0) = timeout {
            return future::err(Error::NotReady).boxed();
        }
        let (tx, rx) = oneshot::channel();
        {
            let mut wait_list = self.wait_list.lock().unwrap();
            let waits = wait_list.entry(chunk_index).or_insert(Vec::new());
            waits.push(tx);
        }
        rx.map_err(|_| Error::Other)
            .and_then(|chunk| future::result(Flow::deref_chunk(&chunk)))
            .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_flow_operations() {
        let mut flow = Flow::new(None);
        assert_eq!(flow.push(&[1u8; 1234]).wait(), Ok(0));
        assert_eq!(flow.push(&[2u8; MAX_SIZE]).wait(), Ok(1));
        assert_eq!(flow.push(b"hello").wait(), Ok(2));
        assert_eq!(flow.push(&[2u8; MAX_SIZE + 1]).wait(), Err(Error::Invalid));
        assert_eq!(flow.pull(2, Some(0)).wait(), Ok(Vec::from(b"hello" as &[u8])));
        assert_eq!(flow.pull(100, Some(0)).wait(), Err(Error::NotReady));
    }

    #[test]
    fn fixed_size_flow() {
        let mut flow = Flow::new(Some(10));
        assert_eq!(flow.push(b"hello").wait(), Ok(0));
        assert_eq!(flow.push(b"world").wait(), Ok(1));
        assert_eq!(flow.push(b"!").wait(), Err(Error::Invalid));
    }

    #[test]
    fn pull_chunk() {
        let mut flow = Flow::new(None);
        assert_eq!(flow.pull(0, Some(0)).wait(), Err(Error::NotReady));
        assert_eq!(flow.pull(1, None).join3(flow.push(b"ello"), flow.push(b"hello")).wait(),
                   Ok((Vec::from(b"hello" as &[u8]), 0, 1)));
    }

    #[test]
    fn close_flow() {
        let mut flow = Flow::new(None);
        assert_eq!(flow.push(b"hello").wait(), Ok(0));
        assert_eq!(flow.pull(0, Some(0)).wait(), Ok(Vec::from(b"hello" as &[u8])));
        assert_eq!(flow.close().wait(), Ok(()));
        assert_eq!(flow.pull(1, Some(0)).wait(), Err(Error::Eof));
        assert_eq!(flow.close().wait(), Err(Error::Invalid));
    }
}
