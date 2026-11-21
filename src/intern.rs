use std::sync::Mutex;

use ahash::{AHashMap, RandomState};

const LOCAL_BITS: u32 = 24;
const LOCAL_MASK: u32 = (1 << LOCAL_BITS) - 1;

#[derive(Default)]
pub struct Intern {
    map: AHashMap<Vec<u8>, u32>,
    pub bytes: Vec<Vec<u8>>,
}

impl Intern {
    pub fn get(&mut self, s: &[u8]) -> u32 {
        if let Some(&i) = self.map.get(s) {
            return i;
        }
        let i = self.bytes.len() as u32;
        let owned = s.to_vec();
        self.map.insert(owned.clone(), i);
        self.bytes.push(owned);
        i
    }

    pub fn get_str(&self, i: u32) -> String {
        String::from_utf8_lossy(&self.bytes[i as usize]).into_owned()
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
            shards: (0..n).map(|_| Mutex::new(Intern::default())).collect(),
            hasher: RandomState::with_seeds(0x1111_2222, 0x3333_4444, 0x5555_6666, 0x7777_8888),
        }
    }

    fn shard_idx(&self, s: &[u8]) -> usize {
        (self.hasher.hash_one(s) as usize) % self.shards.len()
    }

    pub fn get(&self, s: &[u8]) -> u32 {
        let si = self.shard_idx(s);
        let local = self.shards[si].lock().unwrap().get(s);
        debug_assert!(local <= LOCAL_MASK);
        ((si as u32) << LOCAL_BITS) | local
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
