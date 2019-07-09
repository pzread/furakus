use owning_ref::OwningHandle;
use parking_lot::{Mutex, MutexGuard};
use std::{
    collections::HashMap,
    mem,
    sync::{Arc, Weak},
    time::{Duration, Instant},
};

type SharedEntry<T> = Arc<Mutex<Entry<T>>>;
type SharedSlot<T> = Arc<Mutex<Slot<T>>>;
type LockedBundle<T> = (
    OwningHandle<SharedSlot<T>, MutexGuard<'static, Slot<T>>>,
    OwningHandle<SharedEntry<T>, MutexGuard<'static, Entry<T>>>,
);

pub trait Observer {
    fn on_removed(self);
}

pub enum Entry<T> {
    Alive {
        observer: Option<T>,
        timestamp: Instant,
        slot: SharedSlot<T>,
    },
    Bubble {
        slot: SharedSlot<T>,
    },
    Removed,
}

impl<T> Entry<T> {
    fn is_newer(&self, other: &Entry<T>) -> Result<bool, ()> {
        match (self, other) {
            (
                Entry::Alive {
                    timestamp: self_timestamp,
                    ..
                },
                Entry::Alive {
                    timestamp: other_timestamp,
                    ..
                },
            ) => Ok(self_timestamp > other_timestamp),
            (Entry::Alive { .. }, Entry::Bubble { .. }) => Ok(true),
            (Entry::Bubble { .. }, Entry::Alive { .. }) => Ok(false),
            (Entry::Bubble { .. }, Entry::Bubble { .. }) => Ok(false),
            _ => Err(()),
        }
    }
}

enum Slot<T> {
    Alive {
        entry: SharedEntry<T>,
        left: Option<SharedSlot<T>>,
        right: Option<SharedSlot<T>>,
    },
    Removed,
}

trait ArcMutexGuard<T> {
    fn owned_lock(self) -> OwningHandle<Arc<Mutex<T>>, MutexGuard<'static, T>>;

    fn try_owned_lock(self) -> Option<OwningHandle<Arc<Mutex<T>>, MutexGuard<'static, T>>>;
}

impl<T> ArcMutexGuard<T> for Arc<Mutex<T>> {
    fn owned_lock(self) -> OwningHandle<Arc<Mutex<T>>, MutexGuard<'static, T>> {
        OwningHandle::new_with_fn(self, |ptr| unsafe { (*ptr).lock() })
    }

    fn try_owned_lock(self) -> Option<OwningHandle<Arc<Mutex<T>>, MutexGuard<'static, T>>> {
        OwningHandle::try_new(self, |ptr| unsafe { (*ptr).try_lock().ok_or(()) }).ok()
    }
}

pub struct TimeHeap<T> {
    timeout: Duration,
    slots: Mutex<Vec<SharedSlot<T>>>,
    bubbles: Mutex<HashMap<usize, SharedEntry<T>>>,
}

impl<T: 'static + Observer> TimeHeap<T> {
    pub fn new(timeout: Duration) -> Self {
        Self {
            timeout,
            slots: Mutex::new(Vec::new()),
            bubbles: Mutex::new(HashMap::new()),
        }
    }

    pub fn create<F, U>(&self, callback: F) -> U
    where
        F: FnOnce(Weak<Mutex<Entry<T>>>) -> (T, U),
    {
        let bubble = {
            let mut bubbles = self.bubbles.lock();
            bubbles.keys().next().copied().map(|key| {
                let entry = bubbles.remove(&key).unwrap();
                (Self::lock_by_entry(entry.clone()).unwrap(), entry)
            })
        };
        if let Some((mut bundle, entry)) = bubble {
            let slot = match *bundle.1 {
                Entry::Bubble { ref slot } => slot.clone(),
                Entry::Alive { .. } | Entry::Removed => unreachable!(),
            };
            let (observer, retval) = callback(Arc::downgrade(&entry));
            *bundle.1 = Entry::Alive {
                observer: Some(observer),
                timestamp: Instant::now(),
                slot: slot.clone(),
            };
            drop(bundle);
            self.update(entry.clone());
            retval
        } else {
            loop {
                if !self.sanitize_one_timeout() {
                    break;
                }
            }
            let entry = Arc::new(Mutex::new(Entry::Removed));
            let (observer, retval) = callback(Arc::downgrade(&entry));
            let slot = Arc::new(Mutex::new(Slot::Alive {
                entry: entry.clone(),
                left: None,
                right: None,
            }));
            *entry.lock() = Entry::Alive {
                observer: Some(observer),
                timestamp: Instant::now(),
                slot: slot.clone(),
            };
            let mut slots = self.slots.lock();
            slots.push(slot.clone());
            if slots.len() > 1 {
                let position = slots.len() - 1;
                let mut slot_guard = slots[(position - 1) / 2].lock();
                let (left, right) = match *slot_guard {
                    Slot::Alive {
                        ref mut left,
                        ref mut right,
                        ..
                    } => (left, right),
                    Slot::Removed => unreachable!(),
                };
                if position % 2 == 1 {
                    *left = Some(slot);
                } else {
                    *right = Some(slot);
                }
            }
            retval
        }
    }

    pub fn remove(&self, entry: SharedEntry<T>) {
        let mut bubbles = self.bubbles.lock();
        let mut bundle = match Self::lock_by_entry(entry.clone()) {
            Some(bundle) => bundle,
            None => return,
        };
        let (observer, slot) = match *bundle.1 {
            Entry::Alive {
                ref mut observer,
                ref slot,
                ..
            } => (observer.take().unwrap(), slot.clone()),
            Entry::Bubble { .. } | Entry::Removed => return,
        };
        *bundle.1 = Entry::Bubble { slot };
        bubbles.insert((&*entry as *const _) as usize, entry);
        drop(bundle);
        drop(bubbles);
        observer.on_removed();
    }

    pub fn update(&self, curr_entry: SharedEntry<T>) {
        let mut locked_curr = match Self::lock_by_entry(curr_entry) {
            Some(locks) => locks,
            None => return,
        };
        match *locked_curr.1 {
            Entry::Alive {
                ref mut timestamp, ..
            } => *timestamp = Instant::now(),
            Entry::Bubble { .. } | Entry::Removed => return,
        }
        loop {
            let (left_slot, right_slot) = match *locked_curr.0 {
                Slot::Alive {
                    ref left,
                    ref right,
                    ..
                } => (left, right),
                Slot::Removed => return,
            };
            let mut locked_candi = match (left_slot, right_slot) {
                (Some(left_slot), None) => match Self::lock_by_slot(left_slot.clone()) {
                    Some(locked_left) => locked_left,
                    None => return,
                },
                (Some(left_slot), Some(right_slot)) => {
                    let locked_left = Self::lock_by_slot(left_slot.clone());
                    let locked_right = Self::lock_by_slot(right_slot.clone());
                    match (locked_left, locked_right) {
                        (Some(locked_left), None) => locked_left,
                        (Some(locked_left), Some(locked_right)) => {
                            if locked_left.1.is_newer(&*locked_right.1).unwrap() {
                                locked_right
                            } else {
                                locked_left
                            }
                        }
                        _ => return,
                    }
                }
                (None, Some(_)) => unreachable!(),
                (None, None) => return,
            };
            if !locked_curr.1.is_newer(&*locked_candi.1).unwrap() {
                return;
            } else {
                Self::swap_locked_entry(&mut locked_curr, &mut locked_candi).unwrap();
                locked_curr = locked_candi;
            }
        }
    }

    fn sanitize_one_timeout(&self) -> bool {
        let mut slots = self.slots.lock();
        if slots.len() == 0 {
            return false;
        }

        let mut bubbles = self.bubbles.lock();
        let mut locked_candi = Self::lock_by_slot(slots[0].clone()).unwrap();
        let observer = match *locked_candi.1 {
            Entry::Alive {
                ref mut observer,
                timestamp,
                ..
            } => {
                if timestamp.elapsed() < self.timeout {
                    return false;
                }
                observer.take()
            }
            Entry::Bubble { .. } => {
                let key = match *locked_candi.0 {
                    Slot::Alive { ref entry, .. } => (&**entry as *const _) as usize,
                    Slot::Removed => unreachable!(),
                };
                bubbles.remove(&key).unwrap();
                None
            }
            Entry::Removed => unreachable!(),
        };
        drop(bubbles);

        let mut pending_update = None;
        if slots.len() > 1 {
            let mut locked_last = Self::lock_by_slot(slots.last().unwrap().clone()).unwrap();
            pending_update = match *locked_last.0 {
                Slot::Alive { ref entry, .. } => Some(entry.clone()),
                Slot::Removed => unreachable!(),
            };
            Self::swap_locked_entry(&mut locked_candi, &mut locked_last).unwrap();
            locked_candi = locked_last;
        }
        *locked_candi.0 = Slot::Removed;
        *locked_candi.1 = Entry::Removed;
        drop(locked_candi);

        if slots.len() > 1 {
            let position = slots.len() - 1;
            let mut slot_guard = slots[(position - 1) / 2].lock();
            let (left, right) = match *slot_guard {
                Slot::Alive {
                    ref mut left,
                    ref mut right,
                    ..
                } => (left, right),
                Slot::Removed => unreachable!(),
            };
            if position % 2 == 1 {
                *left = None;
            } else {
                *right = None;
            }
        }
        slots.pop();
        drop(slots);

        if let Some(entry) = pending_update {
            self.update(entry);
        }
        if let Some(observer) = observer {
            observer.on_removed();
        }
        true
    }

    fn lock_by_entry(entry: SharedEntry<T>) -> Option<LockedBundle<T>> {
        let mut entry_guard = entry.lock();
        let locked_slot = loop {
            let slot = match *entry_guard {
                Entry::Alive { ref slot, .. } | Entry::Bubble { ref slot, .. } => slot.clone(),
                Entry::Removed => return None,
            };
            if let Some(slot_guard) = slot.try_owned_lock() {
                break slot_guard;
            }
            MutexGuard::bump(&mut entry_guard);
        };
        let locked_entry = {
            let unsafe_entry_guard =
                unsafe { std::mem::transmute::<_, MutexGuard<'static, Entry<T>>>(entry_guard) };
            OwningHandle::new_with_fn(entry.clone(), move |ptr| {
                assert_eq!(ptr, &*entry as *const Mutex<Entry<T>>);
                unsafe_entry_guard
            })
        };
        Some((locked_slot, locked_entry))
    }

    fn lock_by_slot(slot: SharedSlot<T>) -> Option<LockedBundle<T>> {
        let locked_slot = slot.owned_lock();
        let locked_entry = match *locked_slot {
            Slot::Alive { ref entry, .. } => entry.clone().owned_lock(),
            Slot::Removed => return None,
        };
        Some((locked_slot, locked_entry))
    }

    fn swap_locked_entry(
        left: &mut LockedBundle<T>,
        right: &mut LockedBundle<T>,
    ) -> Result<(), ()> {
        let left_entry_slot = match *left.1 {
            Entry::Alive { ref mut slot, .. } | Entry::Bubble { ref mut slot } => slot,
            Entry::Removed => return Err(()),
        };
        let right_entry_slot = match *right.1 {
            Entry::Alive { ref mut slot, .. } | Entry::Bubble { ref mut slot } => slot,
            Entry::Removed => return Err(()),
        };
        let left_slot_entry = match *left.0 {
            Slot::Alive { ref mut entry, .. } => entry,
            Slot::Removed => return Err(()),
        };
        let right_slot_entry = match *right.0 {
            Slot::Alive { ref mut entry, .. } => entry,
            Slot::Removed => return Err(()),
        };
        mem::swap(left_entry_slot, right_entry_slot);
        mem::swap(left_slot_entry, right_slot_entry);
        mem::swap(&mut left.1, &mut right.1);
        return Ok(());
    }
}
