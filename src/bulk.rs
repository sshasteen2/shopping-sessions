//! Lock-free bulk interning.
//!
//! Looking up millions of keys one at a time through `Mutex`-guarded shards
//! makes every thread fight over the locks. Instead, keys are first bucketed
//! by shard in parallel (each chunk keeps its own buckets), then each shard is
//! filled by exactly one thread, so every lock is taken once and never waited on.

use std::sync::Mutex;

use rayon::prelude::*;

/// Which shard a precomputed hash belongs to. Uses bits 24..40, which neither
/// hashbrown's bucket index (low bits) nor its control byte (top 7 bits) rely on,
/// so keys inside one shard still spread across that shard's table.
#[inline]
pub fn shard_of(hash: u64, nshards: usize) -> usize {
    ((hash >> 24) & 0xFFFF) as usize % nshards
}

pub trait ShardTable: Send {
    fn get_or_insert(&mut self, hash: u64, key: &[u8]) -> u32;
}

struct OutPtr(*mut u32);
unsafe impl Send for OutPtr {}
unsafe impl Sync for OutPtr {}

/// Interns `n` keys (`key(i)` for `i in 0..n`) and returns their ids in order.
pub fn intern_many<'a, T, K, H>(shards: &[Mutex<T>], n: usize, key: K, hash: H) -> Vec<u32>
where
    T: ShardTable,
    K: Fn(usize) -> &'a [u8] + Sync,
    H: Fn(&[u8]) -> u64 + Sync,
{
    let nshards = shards.len();
    let nchunks = (rayon::current_num_threads() * 4).max(1);
    let chunk = n.div_ceil(nchunks).max(1);

    // Phase 1: hash every key and bucket its index by shard, one bucket set per chunk.
    let buckets: Vec<Vec<Vec<(u32, u64)>>> = (0..n)
        .step_by(chunk)
        .collect::<Vec<_>>()
        .into_par_iter()
        .map(|lo| {
            let hi = (lo + chunk).min(n);
            let mut b: Vec<Vec<(u32, u64)>> = (0..nshards)
                .map(|_| Vec::with_capacity((hi - lo) / nshards + 16))
                .collect();
            for i in lo..hi {
                let h = hash(key(i));
                b[shard_of(h, nshards)].push((i as u32, h));
            }
            b
        })
        .collect();

    // Phase 2: one thread per shard; each index is written by exactly one shard.
    let mut out = vec![0u32; n];
    let ptr = OutPtr(out.as_mut_ptr());
    (0..nshards).into_par_iter().for_each(|s| {
        let ptr = &ptr;
        let mut table = shards[s].lock().unwrap();
        for b in &buckets {
            for &(i, h) in &b[s] {
                let id = table.get_or_insert(h, key(i as usize));
                // SAFETY: `i < n`, and each index lands in exactly one shard bucket.
                unsafe { *ptr.0.add(i as usize) = id };
            }
        }
    });
    out
}
