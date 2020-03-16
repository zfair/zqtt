use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;

pub mod codec;
pub mod topic;
pub mod uid;

/// Hash bytes via the DefaultHasher.
pub fn hash_bytes(bytes: &[u8]) -> u64 {
    let mut h = DefaultHasher::new();
    h.write(bytes);
    h.finish()
}
