use futures::{Future, future};
use futures::sync::oneshot;
use std::collections::HashMap;
use std::time::Instant;
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

#[derive(Clone, Debug)]
pub enum Chunk {
    Data(Vec<u8>),
    Eof,
}

pub struct Flow {
    pub id: String,
    pub size: Option<u64>,
    active_stamp: Instant,
    next_index: u64,
    pub tail_index: u64,
    chunk_bucket: HashMap<u64, Chunk>,
    wait_list: HashMap<u64, Vec<oneshot::Sender<Chunk>>>,
    pub stat_pushed: u64,
    pub stat_dropped: u64,
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
            stat_pushed: 0,
            stat_dropped: 0,
        }
    }

    fn push_chunk(&mut self, chunk: Chunk) -> FlowFuture<u64> {
        let chunk_index = self.next_index;
        self.next_index += 1;
        // Wake up the waiting fetch.
        if let Some(waits) = self.wait_list.remove(&chunk_index) {
            for wait in waits {
                wait.send(chunk.clone()).unwrap();
            }
        }
        // Insert the chunk.
        self.chunk_bucket.insert(chunk_index, chunk);
        future::ok(chunk_index).boxed()
    }

    pub fn close(&mut self) -> FlowFuture<()> {
        self.push_chunk(Chunk::Eof).map(|_| ()).boxed()
    }

    pub fn push(&mut self, data: &[u8]) -> FlowFuture<u64> {
        if data.len() > MAX_SIZE {
            return future::err(Error::Invalid).boxed();
        }
        if let Some(size) = self.size {
            if (data.len() as u64) + self.stat_pushed > size {
                return future::err(Error::Invalid).boxed();
            }
        }
        self.stat_pushed += data.len() as u64;
        self.push_chunk(Chunk::Data(data.to_vec()))
    }

    pub fn pull(&mut self, chunk_index: u64, timeout: Option<u64>) -> FlowFuture<Vec<u8>> {
        if let Some(chunk) = self.chunk_bucket.get(&chunk_index) {
            return match *chunk {
                           Chunk::Data(ref data) => future::ok(data.clone()),
                           Chunk::Eof => future::err(Error::Eof),
                       }
                       .boxed();
        } else if chunk_index < self.next_index {
            return future::err(Error::Dropped).boxed();
        } else if let Some(0) = timeout {
            return future::err(Error::NotReady).boxed();
        }
        let (tx, rx) = oneshot::channel();
        let waits = self.wait_list.entry(chunk_index).or_insert(Vec::new());
        waits.push(tx);
        rx.map_err(|_| Error::Other)
            .and_then(|chunk| match chunk {
                          Chunk::Data(ref data) => future::ok(data.clone()),
                          Chunk::Eof => future::err(Error::Eof),
                      })
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
    }
}
