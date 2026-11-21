use std::hash::BuildHasher;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Mutex;

use ahash::{AHashMap, RandomState};

struct Shard {
    map: AHashMap<Vec<u8>, u32>,
    items: Vec<(u32, Vec<u8>)>,
}

pub struct People {
    shards: Vec<Mutex<Shard>>,
    hasher: RandomState,
    next: AtomicU32,
}

impl People {
    pub fn new(nshards: usize) -> Self {
        let n = nshards.max(1);
        Self {
            shards: (0..n)
                .map(|_| {
                    Mutex::new(Shard {
                        map: AHashMap::new(),
                        items: Vec::new(),
                    })
                })
                .collect(),
            hasher: RandomState::with_seeds(0x9e37_79b9, 0x7f4a_7c15, 0x41c6_ce57, 0x632b_e59b),
            next: AtomicU32::new(0),
        }
    }

    fn shard(&self, s: &[u8]) -> usize {
        (self.hasher.hash_one(s) as usize) % self.shards.len()
    }

    pub fn intern(&self, id: &[u8]) -> u32 {
        let si = self.shard(id);
        let mut sh = self.shards[si].lock().unwrap();
        if let Some(&i) = sh.map.get(id) {
            return i;
        }
        let i = self.next.fetch_add(1, Ordering::Relaxed);
        let owned = id.to_vec();
        sh.map.insert(owned.clone(), i);
        sh.items.push((i, owned));
        i
    }

    pub fn freeze(self) -> FrozenPeople {
        let n = self.next.load(Ordering::Relaxed) as usize;
        let mut bytes = vec![Vec::new(); n];
        for sh in self.shards {
            for (id, b) in sh.into_inner().unwrap().items {
                bytes[id as usize] = b;
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
