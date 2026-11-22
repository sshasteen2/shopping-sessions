//! IDs as 64-bit keys.
//!
//! Short IDs (at most 10 characters from `0-9 A-Z _ a-z`) pack into a u64,
//! 6 bits per character, left-aligned. Integer order matches byte order.
//!
//! Longer IDs are interned in `SHARDS` locked shards. A lowercase prefix plus
//! 32 hex digits is stored as that prefix and a `u128`, and the shard compares
//! those two integers instead of the raw bytes. Repeated `u` + hex user ids are
//! answered from a thread-local cache of the shard key, so they do not take
//! the mutex. Uppercase and every other shape stay as bytes. Order of two
//! packed hex keys is the prefix byte, then the `u128` (lowercase nibbles sort
//! like ASCII). Every other pair is compared as bytes. Bit 62 stays clear.

use std::cell::RefCell;
use std::cmp::Ordering;
use std::sync::Mutex;

use ahash::{AHashMap, RandomState};
use hashbrown::HashTable;

pub const NONE: u64 = 0;
const OVER: u64 = 1 << 63;
const MAX_LEN: usize = 10;
const SHARD_BITS: u32 = 10;
const SHARDS: usize = 1 << SHARD_BITS;
const LOCAL_MASK: u64 = (1 << 32) - 1;
/// Set on the local index of a packed hex entry. Bit 62 of the key stays clear.
const HEX_TAG: u32 = 1 << 31;

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
static HEXDIGITS: &[u8; 16] = b"0123456789abcdef";

#[inline]
fn pack_lower_hex32(s: &[u8]) -> Option<u128> {
    let mut v = 0u128;
    for &b in s {
        let n = match b {
            b'0'..=b'9' => b - b'0',
            b'a'..=b'f' => b - b'a' + 10,
            _ => return None,
        };
        v = (v << 4) | n as u128;
    }
    Some(v)
}

#[inline]
fn mix_hex(prefix: u8, value: u128) -> u64 {
    let mut x = value as u64 ^ ((value >> 64) as u64).rotate_left(17);
    x ^= (prefix as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x ^= x >> 30;
    x = x.wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

fn push_hex32(out: &mut Vec<u8>, mut v: u128) {
    let mut buf = [0u8; 32];
    for i in (0..32).rev() {
        buf[i] = HEXDIGITS[(v & 0xf) as usize];
        v >>= 4;
    }
    out.extend_from_slice(&buf);
}

/// One shard of interned long IDs.
#[derive(Default)]
struct Shard {
    /// (hash, local index). Hex entries have `HEX_TAG` set on the index.
    table: HashTable<(u64, u32)>,
    data: Vec<u8>,
    /// starts[i]..starts[i + 1] is byte-form ID i.
    starts: Vec<u32>,
    /// Packed lowercase prefix + 32 hex digits.
    packed: Vec<(u8, u128)>,
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
        if bad { None } else { Some(k << (6 * (MAX_LEN - n))) }
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
        // Repeated user ids are cached per thread. Long event and click ids are
        // inserted by the caller in shard batches (`intern_lower_hex_batch`)
        // so this path is not taken once per row.
        if s.len() == 33 && s[0] == b'u' {
            if let Some(value) = pack_lower_hex32(&s[1..]) {
                return self.cached_user(value);
            }
        }
        self.publish_bytes(s)
    }

    /// Intern many `prefix` + 32 lowercase hex digits, taking each shard lock
    /// once. `prefix` is the first byte of every id (`k` for clicks).
    pub fn intern_lower_hex_batch(&self, prefix: u8, values: &[u128]) -> Vec<u64> {
        let mut keys = vec![0u64; values.len()];
        let mut groups: Vec<Vec<(u128, u32)>> = vec![Vec::new(); SHARDS];
        for (i, &value) in values.iter().enumerate() {
            let h = mix_hex(prefix, value);
            let si = ((h >> 32) as usize) & (SHARDS - 1);
            groups[si].push((value, i as u32));
        }
        for (si, group) in groups.iter().enumerate() {
            if group.is_empty() {
                continue;
            }
            let mut sh = self.shards[si].lock().unwrap();
            let Shard { table, packed, .. } = &mut *sh;
            for &(value, idx) in group {
                let h = mix_hex(prefix, value);
                let key = if let Some(&(_, i)) = table.find(h, |&(eh, i)| {
                    if eh != h || i & HEX_TAG == 0 {
                        return false;
                    }
                    let (p, v) = packed[(i & !HEX_TAG) as usize];
                    p == prefix && v == value
                }) {
                    OVER | ((si as u64) << 32) | i as u64
                } else {
                    let n = packed.len() as u32;
                    assert!(n & HEX_TAG == 0, "hex shard overflow");
                    packed.push((prefix, value));
                    let tagged = n | HEX_TAG;
                    table.insert_unique(h, (h, tagged), |&(eh, _)| eh);
                    OVER | ((si as u64) << 32) | tagged as u64
                };
                keys[idx as usize] = key;
            }
        }
        keys
    }

    fn cached_user(&self, value: u128) -> u64 {
        let owner = self as *const Ids as usize;
        thread_local! {
            static CACHE: RefCell<(usize, AHashMap<u128, u64>)> =
                RefCell::new((0, AHashMap::with_capacity(8192)));
        }
        if let Some(k) = CACHE.with(|c| {
            let c = c.borrow();
            if c.0 == owner { c.1.get(&value).copied() } else { None }
        }) {
            return k;
        }
        let k = self.publish_hex(b'u', value);
        CACHE.with(|c| {
            let mut c = c.borrow_mut();
            if c.0 != owner {
                c.0 = owner;
                c.1.clear();
            }
            c.1.insert(value, k);
        });
        k
    }

    fn publish_hex(&self, prefix: u8, value: u128) -> u64 {
        let h = mix_hex(prefix, value);
        let si = ((h >> 32) as usize) & (SHARDS - 1);
        let mut sh = self.shards[si].lock().unwrap();
        let Shard { table, packed, .. } = &mut *sh;
        if let Some(&(_, i)) = table.find(h, |&(eh, i)| {
            if eh != h || i & HEX_TAG == 0 {
                return false;
            }
            let (p, v) = packed[(i & !HEX_TAG) as usize];
            p == prefix && v == value
        }) {
            return OVER | ((si as u64) << 32) | i as u64;
        }
        let n = packed.len() as u32;
        assert!(n & HEX_TAG == 0, "hex shard overflow");
        packed.push((prefix, value));
        let tagged = n | HEX_TAG;
        table.insert_unique(h, (h, tagged), |&(eh, _)| eh);
        OVER | ((si as u64) << 32) | tagged as u64
    }

    fn publish_bytes(&self, s: &[u8]) -> u64 {
        let h = self.hasher.hash_one(s);
        let si = ((h >> 32) as usize) & (SHARDS - 1);
        let mut sh = self.shards[si].lock().unwrap();
        let Shard { table, data, starts, .. } = &mut *sh;
        if let Some(&(_, i)) = table.find(h, |&(eh, i)| {
            eh == h
                && i & HEX_TAG == 0
                && &data[starts[i as usize] as usize..starts[i as usize + 1] as usize] == s
        }) {
            return OVER | ((si as u64) << 32) | i as u64;
        }
        let i = (starts.len() - 1) as u32;
        assert!(i & HEX_TAG == 0, "byte shard overflow");
        data.extend_from_slice(s);
        assert!(data.len() <= u32::MAX as usize, "ID shard arena overflow");
        starts.push(data.len() as u32);
        table.insert_unique(h, (h, i), |&(eh, _)| eh);
        OVER | ((si as u64) << 32) | i as u64
    }

    fn hex_parts(&self, k: u64) -> Option<(u8, u128)> {
        if k & OVER == 0 {
            return None;
        }
        let local = (k & LOCAL_MASK) as u32;
        if local & HEX_TAG == 0 {
            return None;
        }
        let si = ((k & !OVER) >> 32) as usize;
        let sh = self.shards[si].lock().unwrap();
        Some(sh.packed[(local & !HEX_TAG) as usize])
    }

    pub fn write_bytes(&self, k: u64, out: &mut Vec<u8>) {
        if let Some((prefix, value)) = self.hex_parts(k) {
            out.push(prefix);
            push_hex32(out, value);
            return;
        }
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
            return a.cmp(&b);
        }
        match (self.hex_parts(a), self.hex_parts(b)) {
            (Some((pa, va)), Some((pb, vb))) => pa.cmp(&pb).then(va.cmp(&vb)),
            _ => self.to_vec(a).cmp(&self.to_vec(b)),
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
            b"e0123456789abcdef0123456789abcdef", b"k0123456789abcdef0123456789abcdef",
            b"U0123456789ABCDEF0123456789ABCDEF", b"u0123456789abcdef0123456789abcdeF",
            b"k0123456789abcdef0123456789abcdeF", b"u1a", b"e3\r",
        ];
        for a in &words {
            assert_eq!(&ids.to_vec(ids.key(a))[..], *a);
            for b in &words {
                assert_eq!(ids.cmp(ids.key(a), ids.key(b)), a.cmp(b), "{:?} {:?}", a, b);
                assert_eq!(ids.key(a) == ids.key(b), a == b);
            }
        }
        let lower = ids.key(b"u0123456789abcdef0123456789abcdef");
        assert_ne!(lower, ids.key(b"U0123456789ABCDEF0123456789ABCDEF"));
        assert_ne!(lower, ids.key(b"u0123456789abcdef0123456789abcdeF"));
        assert_eq!(lower & (1 << 62), 0);
        assert_ne!(lower & (HEX_TAG as u64), 0);
        assert_eq!(ids.key(b"e0123456789abcdef0123456789abcdef") & (1 << 62), 0);
    }

    #[test]
    fn many_long_ids_from_threads_are_exact() {
        use rayon::prelude::*;
        let ids = Ids::default();
        let make = |i: u64| format!("u{:016x}{:016x}", i.wrapping_mul(0x9E37_79B9_7F4A_7C15), i);
        let keys: Vec<u64> =
            (0..200_000u64).into_par_iter().map(|i| ids.key(make(i % 50_000).as_bytes())).collect();
        for (i, &k) in keys.iter().enumerate() {
            assert_eq!(k, keys[i % 50_000]);
            assert_eq!(ids.to_vec(k), make(i as u64 % 50_000).into_bytes());
        }
        let distinct: std::collections::HashSet<u64> = keys.iter().copied().collect();
        assert_eq!(distinct.len(), 50_000);
    }

    #[test]
    fn hex_keys_spread_across_shards() {
        let ids = Ids::default();
        let mut counts = [0u32; SHARDS];
        for i in 0..16_384u128 {
            let mut s = Vec::with_capacity(33);
            s.push(b'k');
            push_hex32(&mut s, i.wrapping_mul(0x9E37_79B9_7F4A_7C15_1234_5678_9ABC));
            let k = ids.key(&s);
            let si = ((k & !OVER) >> 32) as usize;
            assert!(si < SHARDS, "shard {si}");
            counts[si] += 1;
        }
        let max = counts.iter().copied().max().unwrap();
        let nonempty = counts.iter().filter(|&&c| c > 0).count();
        assert!(nonempty > SHARDS * 3 / 4, "only {nonempty} shards used");
        assert!(max < 80, "shard skew {max}");
    }

    #[test]
    fn a_second_table_does_not_reuse_the_thread_cache() {
        let hex = b"u0123456789abcdef0123456789abcdef";
        let a = Ids::default();
        let ka = a.key(hex);
        let b = Ids::default();
        assert_eq!(b.to_vec(b.key(hex)), hex);
        assert_eq!(a.to_vec(a.key(hex)), hex);
        assert_eq!(a.key(hex), ka);
    }
}
