use std::collections::HashMap;
use std::result::Result as StdResult;
use std::time::Instant;
use uuid::Uuid;
use futures::{Future, Stream, future, stream};
use futures::sync::oneshot;

#[derive(Debug, PartialEq)]
pub enum Error {
    Invalid,
    NotFound,
    Dropped,
    NotReady,
    Other,
}

pub type Result<T> = StdResult<T, Error>;

pub const MAX_SIZE: usize = 65536;

#[derive(Clone, Debug)]
struct Chunk {
    data: Vec<u8>,
}

pub struct Flow {
    pub id: String,
    pub size: Option<u64>,
    active_stamp: Instant,
    next_index: u64,
    tail_index: u64,
    chunk_bucket: HashMap<u64, Chunk>,
    wait_list: HashMap<u64, Vec<oneshot::Sender<Chunk>>>,
    stat_size: u64,
}

type FlowFuture<T> = future::BoxFuture<T, Error>;

impl Flow {
    pub fn new(size: Option<u64>) -> Self {
        Flow {
            id: Uuid::new_v4().simple().to_string(),
            size: size,
            active_stamp: Instant::now(),
            next_index: 0,
            tail_index: 0,
            chunk_bucket: HashMap::new(),
            wait_list: HashMap::new(),
            stat_size: 0,
        }
    }

    pub fn push(&mut self, data: &[u8]) -> FlowFuture<u64> {
        if data.len() > MAX_SIZE {
            return future::err(Error::Invalid).boxed();
        }
        if let Some(size) = self.size {
            if (data.len() as u64) + self.stat_size > size {
                return future::err(Error::Invalid).boxed();
            }
        }

        let chunk_index = self.next_index;
        self.next_index += 1;
        let chunk = Chunk { data: data.to_vec() };
        // Wake up the waiting fetch.
        if let Some(waits) = self.wait_list.remove(&chunk_index) {
            for wait in waits {
                wait.send(chunk.clone()).unwrap();
            }
        }
        // Insert the chunk.
        self.chunk_bucket.insert(chunk_index, chunk);

        self.stat_size += data.len() as u64;
        future::ok(chunk_index).boxed()
    }

    pub fn fetch(&self, chunk_index: u64) -> Result<&[u8]> {
        if let Some(chunk) = self.chunk_bucket.get(&chunk_index) {
            Ok(&chunk.data)
        } else {
            Err(Error::NotFound)
        }
    }

    pub fn pull(&mut self, chunk_index: u64, timeout: Option<u64>) -> FlowFuture<Vec<u8>> {
        if let Some(chunk) = self.chunk_bucket.get(&chunk_index) {
            return future::ok(chunk.data.clone()).boxed();
        } else if chunk_index < self.next_index {
            return future::err(Error::Dropped).boxed();
        } else if let Some(0) = timeout {
            return future::err(Error::NotReady).boxed();
        }
        let (tx, rx) = oneshot::channel();
        let waits = self.wait_list.entry(chunk_index).or_insert(Vec::new());
        waits.push(tx);
        rx.and_then(|chunk| {
            future::ok(chunk.data)
        }).or_else(|_| {
            future::err(Error::Other)
        }).boxed()
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
    }
}
