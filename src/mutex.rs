use std::sync::{Mutex, MutexGuard};

/// Serializes every call into lwIP, which is compiled with `NO_SYS=1` and has
/// no internal locking.
///
/// A plain std mutex parks the thread under contention (futex-based on the
/// major platforms) instead of burning a core the way the userspace spin lock
/// it replaces did whenever the holder was preempted — this lock is the single
/// global serialization point for the whole stack, so contention is routine.
pub struct LwipMutex(Mutex<()>);

impl LwipMutex {
    pub const fn new() -> Self {
        LwipMutex(Mutex::new(()))
    }

    pub fn lock(&self) -> MutexGuard<'_, ()> {
        // Ignore poisoning: unwinding can't un-corrupt lwIP's global state
        // anyway, and several callers lock inside Drop, where surfacing a
        // secondary panic would abort the process.
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }
}
