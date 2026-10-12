//! Global job slots: `max_jobs` permits shared by every registration.
//! Listeners watch availability so idle runners can report `Busy` to GitHub
//! while the hive is full, leaving new jobs queued instead of assigned to a
//! runner that cannot start them yet.

use std::sync::{Arc, Mutex};

use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};

pub struct Slots {
    sem: Arc<Semaphore>,
    free: Mutex<Vec<usize>>,
    available: watch::Sender<bool>,
    total: usize,
}

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
            free: Mutex::new((0..total).rev().collect()),
            available: tx,
            total,
        })
    }

    pub async fn acquire(self: &Arc<Self>) -> Lease {
        let permit = self
            .sem
            .clone()
            .acquire_owned()
            .await
            .expect("slot semaphore is never closed");
        let index = self
            .free
            .lock()
            .expect("slots mutex poisoned")
            .pop()
            .expect("a permit always has a free slot index");
        self.publish();
        Lease {
            index,
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
        let a = slots.acquire().await;
        let b = slots.acquire().await;
        assert_ne!(a.index, b.index);
        assert_eq!(slots.running(), 2);
        rx.changed().await.unwrap();
        assert!(!*rx.borrow());
        drop(a);
        rx.changed().await.unwrap();
        assert!(*rx.borrow());
        let c = slots.acquire().await;
        assert_ne!(c.index, b.index);
    }
}
