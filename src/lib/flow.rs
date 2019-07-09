use super::pool::Observer;
use bytes::Bytes;
use futures::channel::oneshot;
use parking_lot::{RwLock, RwLockUpgradableReadGuard, RwLockWriteGuard};
use std::{
    collections::VecDeque,
    sync::atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering},
};

const EMPTY_BLOCK_INDEX: u64 = std::u64::MAX;
const BUFFER_LENGTH: u64 = 16;
const BUFFER_STATE_OPEN: u8 = 0;
const BUFFER_STATE_CLOSING: u8 = 1;
const BUFFER_STATE_CLOSED: u8 = 2;

#[derive(Debug)]
pub enum Error {
    Closed,
    Dropped,
    Invalid,
    NotReady,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for Error {
    fn description(&self) -> &str {
        "Flow Error"
    }
}

struct Block {
    index: u64,
    chunks: VecDeque<Bytes>,
    waits: Vec<oneshot::Sender<()>>,
}

struct PushGuard<'a>(&'a AtomicBool);

impl<'a> PushGuard<'a> {
    fn lock(buffer: &Buffer) -> Result<PushGuard, Error> {
        if !buffer.is_open() {
            return Err(Error::Closed);
        }
        if buffer
            .pushing
            .compare_and_swap(false, true, Ordering::SeqCst)
        {
            return Err(Error::NotReady);
        }
        Ok(PushGuard(&buffer.pushing))
    }
}

impl<'a> Drop for PushGuard<'a> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

struct Buffer {
    state: AtomicU8,
    pushing: AtomicBool,
    buffer: Vec<RwLock<Block>>,
    length: AtomicUsize,
    next_index: AtomicU64,
}

impl Buffer {
    fn new() -> Self {
        let mut buffer = Vec::new();
        for _ in 0..BUFFER_LENGTH {
            buffer.push(RwLock::new(Block {
                index: EMPTY_BLOCK_INDEX,
                chunks: VecDeque::new(),
                waits: Vec::new(),
            }));
        }
        Buffer {
            state: AtomicU8::new(BUFFER_STATE_OPEN),
            pushing: AtomicBool::new(false),
            length: AtomicUsize::new(0),
            buffer,
            next_index: AtomicU64::new(0),
        }
    }

    async fn push(&self, chunks: VecDeque<Bytes>) -> Result<VecDeque<Bytes>, Error> {
        let _guard = PushGuard::lock(self)?;
        let block_index = self.next_index.fetch_add(1, Ordering::SeqCst);
        let new_chunks = {
            let mut block = self.fetch_write_block(block_index).await?;
            block.index = block_index;
            let new_chunks = std::mem::replace::<VecDeque<Bytes>>(&mut block.chunks, chunks);
            self.length.fetch_add(1, Ordering::SeqCst);
            while block.waits.len() > 0 {
                #[allow(unused_must_use)]
                {
                    block.waits.pop().unwrap().send(());
                }
            }
            new_chunks
        };
        Ok(new_chunks)
    }

    pub async fn pull(&self, block_index: u64) -> Result<VecDeque<Bytes>, Error> {
        if block_index == EMPTY_BLOCK_INDEX {
            return Err(Error::Invalid);
        }
        let chunks = {
            let block = self.fetch_read_block(block_index).await?;
            let chunks = block.chunks.clone();
            let mut block = parking_lot::RwLockUpgradableReadGuard::upgrade(block);
            block.index = EMPTY_BLOCK_INDEX;
            block.chunks.clear();
            let length = self.length.fetch_sub(1, Ordering::SeqCst) - 1;
            if !self.is_open() && length == 0 {
                self.state.store(BUFFER_STATE_CLOSED, Ordering::SeqCst);
            }
            assert!(block.waits.len() <= 1);
            block.waits.pop().map(|tx| tx.send(()));
            chunks
        };
        Ok(chunks)
    }

    pub fn close(&self) {
        self.state.store(BUFFER_STATE_CLOSING, Ordering::SeqCst);
        for block in self.buffer.iter() {
            block.write().waits.clear();
        }
        if self.length.load(Ordering::SeqCst) == 0 {
            self.state.store(BUFFER_STATE_CLOSED, Ordering::SeqCst);
        }
    }

    fn is_open(&self) -> bool {
        self.state.load(Ordering::SeqCst) == BUFFER_STATE_OPEN
    }

    fn is_closed(&self) -> bool {
        self.state.load(Ordering::SeqCst) == BUFFER_STATE_CLOSED
    }

    async fn fetch_write_block(&self, block_index: u64) -> Result<RwLockWriteGuard<Block>, Error> {
        let target = &self.buffer[(block_index % BUFFER_LENGTH) as usize];
        let rx = {
            let mut block = target.write();
            if !self.is_open() {
                return Err(Error::Closed);
            } else if block.index == EMPTY_BLOCK_INDEX {
                return Ok(block);
            }
            let (tx, rx) = oneshot::channel();
            block.waits.push(tx);
            rx
        };
        rx.await.map_err(|_| Error::Closed)?;
        let block = target.write();
        assert_eq!(block.index, EMPTY_BLOCK_INDEX);
        Ok(block)
    }

    async fn fetch_read_block(
        &self,
        block_index: u64,
    ) -> Result<RwLockUpgradableReadGuard<Block>, Error> {
        let target = &self.buffer[(block_index % BUFFER_LENGTH) as usize];
        loop {
            let rx = {
                let block = target.upgradable_read();
                if block.index == block_index {
                    return Ok(block);
                } else if block.index > block_index && block.index != EMPTY_BLOCK_INDEX {
                    return Err(Error::Dropped);
                } else if !self.is_open() {
                    return Err(Error::Closed);
                }
                let (tx, rx) = oneshot::channel();
                {
                    let mut block = parking_lot::RwLockUpgradableReadGuard::upgrade(block);
                    block.waits.push(tx);
                }
                rx
            };
            rx.await.map_err(|_| Error::Closed)?;
        }
    }
}

pub struct Flow {
    id: u128,
    token: u128,
    observer: Observer,
    buffer: Buffer,
}

impl Flow {
    pub fn new(id: u128, token: u128, observer: Observer) -> Self {
        Flow {
            id,
            token,
            observer,
            buffer: Buffer::new(),
        }
    }

    pub fn get_id(&self) -> u128 {
        self.id
    }

    pub fn get_token(&self) -> u128 {
        self.token
    }

    pub async fn push(&self, chunks: VecDeque<Bytes>) -> Result<VecDeque<Bytes>, Error> {
        let ret = self.buffer.push(chunks).await;
        if ret.is_ok() {
            self.observer.on_update();
        }
        ret
    }

    pub async fn pull(&self, block_index: u64) -> Result<VecDeque<Bytes>, Error> {
        let ret = self.buffer.pull(block_index).await;
        if ret.is_ok() {
            self.observer.on_update();
        }
        if self.buffer.is_closed() {
            self.observer.on_close();
        }
        ret
    }

    pub fn close(&self) {
        self.buffer.close();
        if self.buffer.is_closed() {
            self.observer.on_close();
        }
    }
}
