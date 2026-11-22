//! IDs as 64-bit keys.
//!
//! An ID of at most 10 characters drawn from `0-9 A-Z _ a-z` is packed into a
//! u64, 6 bits per character, left-aligned and zero-padded. Characters get codes
//! in ASCII order and padding (0) sorts before every character, so comparing two
//! packed keys as integers is the same as comparing the IDs byte by byte. Short
//! generator IDs (`u`/`e`/`k` + base 36, `p123`, `c_123`, upper-cased variants)
//! all fit.
//!
//! Anything else (since scorer v5, 1 in 8 IDs carries a 32-character hex
//! suffix) is interned exactly in one of `SHARDS` independently locked shards,
//! picked by a hash of the bytes, so parallel threads rarely contend. Such a key
//! has the top bit set and compares by its bytes.

use std::cmp::Ordering;
use std::sync::Mutex;

use ahash::RandomState;
use hashbrown::HashTable;

pub const NONE: u64 = 0;
const OVER: u64 = 1 << 63;
const MAX_LEN: usize = 10;
const SHARD_BITS: u32 = 10;
const SHARDS: usize = 1 << SHARD_BITS;
const LOCAL_MASK: u64 = (1 << 32) - 1;

const fn build_code() -> [u8; 256] {
    let mut t = [0u8; 256];
    let mut c = 1u8;
    let mut b = 0usize;
    while b < 256 {
        let ok = (b >= b'0' as usize && b <= b'9' as usize)
            || (b >= b'A' as usize && b <= b'Z' as usize)
            || b == b'_' as usize
            || (b >= b'a' as usize && b <= b'z' as usize);
        if ok {
            t[b] = c;
            c += 1;
        }
        b += 1;
    }
    t
}

const fn build_decode() -> [u8; 64] {
    let mut d = [0u8; 64];
    let mut b = 0usize;
    while b < 256 {
        if CODE[b] != 0 {
            d[CODE[b] as usize] = b as u8;
        }
        b += 1;
    }
    d
}

static CODE: [u8; 256] = build_code();
static DECODE: [u8; 64] = build_decode();

/// One shard of interned long IDs: bytes live in one arena.
#[derive(Default)]
struct Shard {
    /// (hash, local index)
    table: HashTable<(u64, u32)>,
    data: Vec<u8>,
    /// starts[i]..starts[i + 1] is ID i's bytes
    starts: Vec<u32>,
}

impl Shard {
    #[inline]
    fn get(&self, i: usize) -> &[u8] {
        &self.data[self.starts[i] as usize..self.starts[i + 1] as usize]
    }
}

pub struct Ids {
    shards: Vec<Mutex<Shard>>,
    hasher: RandomState,
}

impl Default for Ids {
    fn default() -> Self {
        Self {
            shards: (0..SHARDS)
                .map(|_| {
                    Mutex::new(Shard {
                        starts: vec![0],
                        ..Default::default()
                    })
                })
                .collect(),
            hasher: RandomState::with_seeds(0x243F_6A88, 0x85A3_08D3, 0x1319_8A2E, 0x0370_7344),
        }
    }
}

impl Ids {
    /// The packed key if `s` is non-empty and fits the packed form.
    #[inline]
    pub fn packed(&self, s: &[u8]) -> Option<u64> {
        let n = s.len();
        if n == 0 || n > MAX_LEN {
            return if n == 0 { Some(NONE) } else { None };
        }
        let mut k = 0u64;
        let mut bad = false;
        for &b in s {
            let c = CODE[b as usize];
            bad |= c == 0;
            k = (k << 6) | c as u64;
        }
        if bad {
            None
        } else {
            Some(k << (6 * (MAX_LEN - n)))
        }
    }

    /// The key for an ID; `NONE` for the empty string.
    #[inline]
    pub fn key(&self, s: &[u8]) -> u64 {
        let n = s.len();
        if n == 0 {
            return NONE;
        }
        if n <= MAX_LEN {
            let mut k = 0u64;
            let mut bad = false;
            for &b in s {
                let c = CODE[b as usize];
                bad |= c == 0;
                k = (k << 6) | c as u64;
            }
            if !bad {
                return k << (6 * (MAX_LEN - n));
            }
        }
        self.over_key(s)
    }

    #[inline(never)]
    fn over_key(&self, s: &[u8]) -> u64 {
        let h = self.hasher.hash_one(s);
        // Middle bits pick the shard: hashbrown uses the low bits for buckets
        // and the top 7 for its control tags, so neither is constant per shard.
        let si = ((h >> 32) as usize) & (SHARDS - 1);
        let mut sh = self.shards[si].lock().unwrap();
        let Shard { table, data, starts } = &mut *sh;
        if let Some(&(_, i)) = table.find(h, |&(eh, i)| {
            eh == h && &data[starts[i as usize] as usize..starts[i as usize + 1] as usize] == s
        }) {
            return OVER | ((si as u64) << 32) | i as u64;
        }
        let i = (starts.len() - 1) as u32;
        data.extend_from_slice(s);
        assert!(data.len() <= u32::MAX as usize, "ID shard arena overflow");
        starts.push(data.len() as u32);
        table.insert_unique(h, (h, i), |&(eh, _)| eh);
        OVER | ((si as u64) << 32) | i as u64
    }

    pub fn write_bytes(&self, k: u64, out: &mut Vec<u8>) {
        if k & OVER != 0 {
            let si = ((k & !OVER) >> 32) as usize;
            let sh = self.shards[si].lock().unwrap();
            out.extend_from_slice(sh.get((k & LOCAL_MASK) as usize));
            return;
        }
        for i in (0..MAX_LEN).rev() {
            let c = (k >> (6 * i)) & 63;
            if c == 0 {
                break;
            }
            out.push(DECODE[c as usize]);
        }
    }

    pub fn to_vec(&self, k: u64) -> Vec<u8> {
        let mut v = Vec::with_capacity(MAX_LEN);
        self.write_bytes(k, &mut v);
        v
    }

    /// Byte-wise order of the two IDs.
    #[inline]
    pub fn cmp(&self, a: u64, b: u64) -> Ordering {
        if (a | b) & OVER == 0 {
            a.cmp(&b)
        } else {
            self.to_vec(a).cmp(&self.to_vec(b))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order_and_roundtrip() {
        let ids = Ids::default();
        let words: Vec<&[u8]> = vec![
            b"u1", b"U1", b"u10", b"u", b"_x", b"a_", b"zzzzzzzzzz", b"0", b"u1 ", b" u1",
            b"abcdefghijk", b"c_12", b"C_12", b"p9",
            b"u0123456789abcdef0123456789abcdef", b"u0123456789abcdef0123456789abcdee",
            b"e0123456789abcdef0123456789abcdef", b"u1a",
        ];
        for a in &words {
            assert_eq!(&ids.to_vec(ids.key(a))[..], *a);
            for b in &words {
                assert_eq!(ids.cmp(ids.key(a), ids.key(b)), a.cmp(b), "{:?} {:?}", a, b);
                assert_eq!(ids.key(a) == ids.key(b), a == b);
            }
        }
    }

    #[test]
    fn many_long_ids_from_threads_are_exact() {
        use rayon::prelude::*;
        let ids = Ids::default();
        let make = |i: u64| format!("u{:016x}{:016x}", i.wrapping_mul(0x9E37_79B9_7F4A_7C15), i);
        let keys: Vec<u64> = (0..200_000u64).into_par_iter().map(|i| ids.key(make(i % 50_000).as_bytes())).collect();
        for (i, &k) in keys.iter().enumerate() {
            assert_eq!(k, keys[i % 50_000]);
            assert_eq!(ids.to_vec(k), make(i as u64 % 50_000).into_bytes());
        }
        let distinct: std::collections::HashSet<u64> = keys.iter().copied().collect();
        assert_eq!(distinct.len(), 50_000);
    }
}
