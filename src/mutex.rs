use std::sync::atomic::{AtomicBool, Ordering::*};

#[derive(Debug)]
pub struct AtomicMutex {
    locked: AtomicBool,
}

#[derive(Debug, Clone, Copy)]
pub struct AtomicMutexErr;

pub struct AtomicMutexGuard<'a> {
    mutex: &'a AtomicMutex,
}

impl AtomicMutex {
    pub const fn new() -> Self {
        Self {
            locked: AtomicBool::new(false),
        }
    }

    pub fn try_lock(&self) -> Result<AtomicMutexGuard<'_>, AtomicMutexErr> {
        if self.locked.swap(true, Acquire) {
            Err(AtomicMutexErr)
        } else {
            Ok(AtomicMutexGuard { mutex: self })
        }
    }

    pub fn lock(&self) -> AtomicMutexGuard<'_> {
        // Critical sections are short, so spin first for the uncontended/brief
        // case. But if the holder has been preempted by the OS scheduler,
        // spinning forever just burns a core; after a bounded number of spins,
        // yield the thread so the holder can be rescheduled.
        const SPIN_LIMIT: u32 = 64;
        let mut spins = 0u32;
        loop {
            if let Ok(m) = self.try_lock() {
                break m;
            }
            if spins < SPIN_LIMIT {
                spins += 1;
                // Hint to the CPU that we're spinning so it can back off (e.g.
                // yield an SMT sibling) instead of hammering the cache line.
                std::hint::spin_loop();
            } else {
                std::thread::yield_now();
            }
        }
    }
}

impl Default for AtomicMutex {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> Drop for AtomicMutexGuard<'a> {
    fn drop(&mut self) {
        let _prev = self.mutex.locked.swap(false, Release);
        debug_assert!(_prev);
    }
}
