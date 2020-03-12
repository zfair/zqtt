use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Type for the unique ID.
pub type UID = u64;

/// Unique ID generator.
#[derive(Clone)]
pub struct UidGen {
    gen: Arc<AtomicU64>,
}

impl UidGen {
    /// Create a new unique ID generator.
    pub fn new() -> Self {
        UidGen {
            gen: Arc::new(AtomicU64::new(1)),
        }
    }

    /// Allocate a new UID.
    pub fn allocate(&mut self) -> UID {
        self.gen.fetch_add(1, Ordering::SeqCst)
    }
}
