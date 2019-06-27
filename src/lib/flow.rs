use super::pool::Observer;
use bytes::Bytes;
use futures::channel::oneshot;
use parking_lot::{RwLock, RwLockUpgradableReadGuard, RwLockWriteGuard};
use std::{
    collections::VecDeque,
    sync::atomic::{AtomicU64, AtomicU8, AtomicUsize, Ordering},
};

const EMPTY_BLOCK_INDEX: u64 = std::u64::MAX;
const RING_BUFFER_LENGTH: u64 = 16;

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

struct PushGuard<'a>(&'a AtomicU8);

impl<'a> PushGuard<'a> {
    pub fn lock(flow: &Flow) -> Result<PushGuard, Error> {
        if flow
            .state
            .compare_and_swap(FLOW_STATE_IDLE, FLOW_STATE_PUSHING, Ordering::SeqCst)
            != FLOW_STATE_IDLE
        {
            return Err(Error::NotReady);
        }
        Ok(PushGuard(&flow.state))
    }
}

impl<'a> Drop for PushGuard<'a> {
    fn drop(&mut self) {
        self.0
            .compare_and_swap(FLOW_STATE_PUSHING, FLOW_STATE_IDLE, Ordering::SeqCst);
    }
}

const FLOW_STATE_IDLE: u8 = 0;
const FLOW_STATE_PUSHING: u8 = 1;
const FLOW_STATE_CLOSING: u8 = 2;

pub struct Flow {
    id: u128,
    token: u128,
    state: AtomicU8,
    observer: Observer,
    ring_buffer: Vec<RwLock<Block>>,
    next_index: AtomicU64,
    length: AtomicUsize,
}

impl Flow {
    pub fn new(id: u128, token: u128, observer: Observer) -> Self {
        let mut ring_buffer = Vec::new();
        for _ in 0..RING_BUFFER_LENGTH {
            ring_buffer.push(RwLock::new(Block {
                index: EMPTY_BLOCK_INDEX,
                chunks: VecDeque::new(),
                waits: Vec::new(),
            }));
        }
        Flow {
            id,
            token,
            state: AtomicU8::new(FLOW_STATE_IDLE),
            observer,
            ring_buffer,
            next_index: AtomicU64::new(0),
            length: AtomicUsize::new(0),
        }
    }

    pub fn get_id(&self) -> u128 {
        self.id
    }

    pub fn get_token(&self) -> u128 {
        self.token
    }

    pub async fn push(&self, chunks: VecDeque<Bytes>) -> Result<VecDeque<Bytes>, Error> {
        let _push_guard = PushGuard::lock(self)?;
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
            self.length.fetch_sub(1, Ordering::SeqCst);
            self.check_and_notify_closed();
            assert!(block.waits.len() <= 1);
            block.waits.pop().map(|tx| tx.send(()));
            chunks
        };
        Ok(chunks)
    }

    pub fn close(&self) {
        self.state.store(FLOW_STATE_CLOSING, Ordering::SeqCst);
        for block in self.ring_buffer.iter() {
            block.write().waits.clear();
        }
        self.check_and_notify_closed();
    }

    fn is_closing(&self) -> bool {
        self.state.load(Ordering::SeqCst) == FLOW_STATE_CLOSING
    }

    fn check_and_notify_closed(&self) {
        if self.length.load(Ordering::SeqCst) == 0 {
            if self.is_closing() {
                self.observer.on_close();
            }
        }
    }

    async fn fetch_write_block(&self, block_index: u64) -> Result<RwLockWriteGuard<Block>, Error> {
        let target = &self.ring_buffer[(block_index % RING_BUFFER_LENGTH) as usize];
        let rx = {
            let mut block = target.write();
            if self.is_closing() {
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
        let target = &self.ring_buffer[(block_index % RING_BUFFER_LENGTH) as usize];
        loop {
            let rx = {
                let block = target.upgradable_read();
                if block.index == block_index {
                    return Ok(block);
                } else if block.index > block_index && block.index != EMPTY_BLOCK_INDEX {
                    return Err(Error::Dropped);
                } else if self.is_closing() {
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
