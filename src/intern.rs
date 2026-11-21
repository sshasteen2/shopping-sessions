use std::sync::Mutex;

use ahash::RandomState;
use hashbrown::HashTable;

use rayon::prelude::*;

use crate::bulk::{shard_of, ShardTable};

const LOCAL_BITS: u32 = 24;
const LOCAL_MASK: u32 = (1 << LOCAL_BITS) - 1;

pub struct Intern {
    /// This shard's index, stored in the high bits of every id it hands out.
    shard: u32,
    /// (hash, global id)
    table: HashTable<(u64, u32)>,
    pub bytes: Vec<Vec<u8>>,
}

impl ShardTable for Intern {
    fn get_or_insert(&mut self, hash: u64, s: &[u8]) -> u32 {
        let bytes = &self.bytes;
        if let Some(&(_, id)) = self
            .table
            .find(hash, |&(h, id)| h == hash && bytes[(id & LOCAL_MASK) as usize] == s)
        {
            return id;
        }
        let local = self.bytes.len() as u32;
        assert!(local <= LOCAL_MASK, "intern shard overflow");
        let id = (self.shard << LOCAL_BITS) | local;
        self.bytes.push(s.to_vec());
        self.table.insert_unique(hash, (hash, id), |&(h, _)| h);
        id
    }
}

pub struct ShardedIntern {
    shards: Vec<Mutex<Intern>>,
    hasher: RandomState,
}

impl ShardedIntern {
    pub fn new(n: usize) -> Self {
        let n = n.max(1);
        Self {
            shards: (0..n)
                .map(|s| {
                    Mutex::new(Intern {
                        shard: s as u32,
                        table: HashTable::new(),
                        bytes: Vec::new(),
                    })
                })
                .collect(),
            hasher: RandomState::with_seeds(0x1111_2222, 0x3333_4444, 0x5555_6666, 0x7777_8888),
        }
    }

    pub fn get(&self, s: &[u8]) -> u32 {
        let h = self.hasher.hash_one(s);
        let si = shard_of(h, self.shards.len());
        self.shards[si].lock().unwrap().get_or_insert(h, s)
    }

    /// Interns `key(i)` for every `i in 0..n`, for keys with few distinct
    /// values (products, campaigns): each chunk keeps a small local cache and
    /// only takes a shard lock the first time it sees a key.
    pub fn get_many<'a, K>(&self, n: usize, key: K) -> Vec<u32>
    where
        K: Fn(usize) -> &'a [u8] + Sync,
    {
        let mut out = vec![0u32; n];
        let chunk = n.div_ceil(rayon::current_num_threads() * 4).max(1);
        out.par_chunks_mut(chunk).enumerate().for_each(|(c, dst)| {
            let lo = c * chunk;
            let mut cache: HashTable<(u64, &[u8], u32)> = HashTable::new();
            for (j, slot) in dst.iter_mut().enumerate() {
                let s = key(lo + j);
                let h = self.hasher.hash_one(s);
                *slot = match cache.find(h, |&(ch, cs, _)| ch == h && cs == s) {
                    Some(&(_, _, id)) => id,
                    None => {
                        let si = shard_of(h, self.shards.len());
                        let id = self.shards[si].lock().unwrap().get_or_insert(h, s);
                        cache.insert_unique(h, (h, s, id), |&(ch, _, _)| ch);
                        id
                    }
                };
            }
        });
        out
    }

    pub fn hash(&self, s: &[u8]) -> u64 {
        self.hasher.hash_one(s)
    }

    pub fn freeze(self) -> FrozenIntern {
        FrozenIntern {
            shards: self
                .shards
                .into_iter()
                .map(|m| m.into_inner().unwrap())
                .collect(),
        }
    }
}

pub struct FrozenIntern {
    shards: Vec<Intern>,
}

impl FrozenIntern {
    fn split(id: u32) -> (usize, usize) {
        ((id >> LOCAL_BITS) as usize, (id & LOCAL_MASK) as usize)
    }

    pub fn get_bytes(&self, id: u32) -> &[u8] {
        let (s, l) = Self::split(id);
        &self.shards[s].bytes[l]
    }

    pub fn get_str(&self, id: u32) -> String {
        String::from_utf8_lossy(self.get_bytes(id)).into_owned()
    }
}
