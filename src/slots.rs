//! Global job slots: `max_jobs` permits shared by every registration.
//! Listeners watch availability so idle runners can report `Busy` to GitHub
//! while the hive is full, leaving new jobs queued instead of assigned to a
//! runner that cannot start them yet.
//!
//! Taking a slot is two steps: [`Slots::acquire`] waits for capacity, and
//! [`Permit::assign`] picks the concrete slot once the job (and so its repo)
//! is known, preferring a slot that already holds that repo's checkout.

use std::sync::{Arc, Mutex};

use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};

pub struct Slots {
    sem: Arc<Semaphore>,
    free: Mutex<Vec<usize>>,
    available: watch::Sender<bool>,
    total: usize,
}

/// Capacity for one job, not yet tied to a slot directory.
pub struct Permit {
    slots: Arc<Slots>,
    permit: Option<OwnedSemaphorePermit>,
}

/// A specific slot, held until dropped.
pub struct Lease {
    pub index: usize,
    slots: Arc<Slots>,
    permit: Option<OwnedSemaphorePermit>,
}

impl Slots {
    pub fn new(total: usize) -> Arc<Self> {
        let (tx, _) = watch::channel(total > 0);
        Arc::new(Self {
            sem: Arc::new(Semaphore::new(total)),
            free: Mutex::new((0..total).collect()),
            available: tx,
            total,
        })
    }

    pub async fn acquire(self: &Arc<Self>) -> Permit {
        let permit = self
            .sem
            .clone()
            .acquire_owned()
            .await
            .expect("slot semaphore is never closed");
        self.publish();
        Permit {
            slots: self.clone(),
            permit: Some(permit),
        }
    }

    pub fn subscribe(&self) -> watch::Receiver<bool> {
        self.available.subscribe()
    }

    pub fn is_available(&self) -> bool {
        self.sem.available_permits() > 0
    }

    pub fn running(&self) -> usize {
        self.total - self.sem.available_permits()
    }

    pub fn total(&self) -> usize {
        self.total
    }

    fn publish(&self) {
        let now = self.is_available();
        self.available.send_if_modified(|v| {
            if *v != now {
                *v = now;
                true
            } else {
                false
            }
        });
    }
}

impl Permit {
    /// Take a free slot, the first one `prefer` accepts if any (lowest index
    /// otherwise). A permit guarantees at least one slot is free.
    pub fn assign(mut self, prefer: impl Fn(usize) -> bool) -> Lease {
        let index = {
            let mut free = self.slots.free.lock().expect("slots mutex poisoned");
            free.sort_unstable();
            let pos = free.iter().position(|&i| prefer(i)).unwrap_or(0);
            free.remove(pos)
        };
        Lease {
            index,
            slots: self.slots.clone(),
            permit: self.permit.take(),
        }
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        if let Some(p) = self.permit.take() {
            drop(p);
            self.slots.publish();
        }
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        self.slots
            .free
            .lock()
            .expect("slots mutex poisoned")
            .push(self.index);
        drop(self.permit.take());
        self.slots.publish();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn leases_are_exclusive_and_published() {
        let slots = Slots::new(2);
        let mut rx = slots.subscribe();
        assert!(*rx.borrow());
        let a = slots.acquire().await.assign(|_| false);
        let b = slots.acquire().await.assign(|_| false);
        assert_ne!(a.index, b.index);
        assert_eq!(slots.running(), 2);
        rx.changed().await.unwrap();
        assert!(!*rx.borrow());
        drop(a);
        rx.changed().await.unwrap();
        assert!(*rx.borrow());
        let c = slots.acquire().await.assign(|_| false);
        assert_ne!(c.index, b.index);
    }

    #[tokio::test]
    async fn assign_prefers_matching_slot() {
        let slots = Slots::new(3);
        let lease = slots.acquire().await.assign(|i| i == 2);
        assert_eq!(lease.index, 2);
        let other = slots.acquire().await.assign(|i| i == 2);
        assert_eq!(other.index, 0, "falls back to the lowest free slot");
    }

    #[tokio::test]
    async fn unassigned_permit_returns_capacity() {
        let slots = Slots::new(1);
        let p = slots.acquire().await;
        assert!(!slots.is_available());
        drop(p);
        assert!(slots.is_available());
    }
}
