use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use ahash::RandomState;
use hashbrown::HashTable;

use crate::bulk::{intern_many, shard_of, ShardTable};

struct Shard {
    /// (hash, global id, id bytes)
    table: HashTable<(u64, u32, Box<[u8]>)>,
    next: Arc<AtomicU32>,
}

impl ShardTable for Shard {
    fn get_or_insert(&mut self, hash: u64, key: &[u8]) -> u32 {
        if let Some(e) = self
            .table
            .find(hash, |(h, _, b)| *h == hash && &**b == key)
        {
            return e.1;
        }
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        self.table
            .insert_unique(hash, (hash, id, key.into()), |(h, _, _)| *h);
        id
    }
}

pub struct People {
    shards: Vec<Mutex<Shard>>,
    hasher: RandomState,
    next: Arc<AtomicU32>,
}

impl People {
    pub fn new(nshards: usize) -> Self {
        let n = nshards.max(1);
        let next = Arc::new(AtomicU32::new(0));
        Self {
            shards: (0..n)
                .map(|_| {
                    Mutex::new(Shard {
                        table: HashTable::new(),
                        next: next.clone(),
                    })
                })
                .collect(),
            hasher: RandomState::with_seeds(0x9e37_79b9, 0x7f4a_7c15, 0x41c6_ce57, 0x632b_e59b),
            next,
        }
    }

    pub fn intern(&self, id: &[u8]) -> u32 {
        let h = self.hasher.hash_one(id);
        let si = shard_of(h, self.shards.len());
        self.shards[si].lock().unwrap().get_or_insert(h, id)
    }

    /// Interns `key(i)` for every `i in 0..n` without lock contention.
    pub fn intern_many<'a, K>(&self, n: usize, key: K) -> Vec<u32>
    where
        K: Fn(usize) -> &'a [u8] + Sync,
    {
        intern_many(&self.shards, n, key, |s| self.hasher.hash_one(s))
    }

    pub fn freeze(self) -> FrozenPeople {
        let n = self.next.load(Ordering::Relaxed) as usize;
        let mut bytes = vec![Vec::new(); n];
        for sh in self.shards {
            for (_, id, b) in sh.into_inner().unwrap().table {
                bytes[id as usize] = b.into_vec();
            }
        }
        FrozenPeople {
            bytes,
            parent: (0..n as u32).collect(),
            rank: vec![0u8; n],
            min_id: (0..n as u32).collect(),
        }
    }
}

pub struct FrozenPeople {
    bytes: Vec<Vec<u8>>,
    parent: Vec<u32>,
    rank: Vec<u8>,
    min_id: Vec<u32>,
}

impl FrozenPeople {
    fn find(&mut self, mut x: u32) -> u32 {
        let mut root = x;
        while self.parent[root as usize] != root {
            root = self.parent[root as usize];
        }
        while x != root {
            let p = self.parent[x as usize];
            self.parent[x as usize] = root;
            x = p;
        }
        root
    }

    pub fn union(&mut self, a: u32, b: u32) {
        let mut ra = self.find(a);
        let mut rb = self.find(b);
        if ra == rb {
            return;
        }
        if self.rank[ra as usize] < self.rank[rb as usize] {
            std::mem::swap(&mut ra, &mut rb);
        }
        self.parent[rb as usize] = ra;
        if self.rank[ra as usize] == self.rank[rb as usize] {
            self.rank[ra as usize] += 1;
        }
        let ma = self.min_id[ra as usize];
        let mb = self.min_id[rb as usize];
        self.min_id[ra as usize] = if self.bytes[ma as usize] <= self.bytes[mb as usize] {
            ma
        } else {
            mb
        };
    }

    pub fn root(&mut self, id: u32) -> u32 {
        let r = self.find(id);
        self.min_id[r as usize]
    }

    pub fn name_of(&mut self, id: u32) -> &[u8] {
        let r = self.find(id);
        let m = self.min_id[r as usize];
        &self.bytes[m as usize]
    }

    pub fn name_str(&mut self, id: u32) -> String {
        String::from_utf8_lossy(self.name_of(id)).into_owned()
    }
}
