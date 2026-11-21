//! IDs as 64-bit keys.
//!
//! An ID of at most 10 characters drawn from `0-9 A-Z _ a-z` is packed into a
//! u64, 6 bits per character, left-aligned and zero-padded. Characters get codes
//! in ASCII order and padding (0) sorts before every character, so comparing two
//! packed keys as integers is the same as comparing the IDs byte by byte. The
//! generator's IDs (`u`/`e`/`k` + base 36, `p123`, `c_123`, upper-cased
//! variants) all fit. Anything else goes to a small locked overflow table and
//! gets a key with the top bit set; those compare by their bytes.

use std::cmp::Ordering;
use std::sync::Mutex;

use ahash::AHashMap;

pub const NONE: u64 = 0;
const OVER: u64 = 1 << 63;
const MAX_LEN: usize = 10;

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

#[derive(Default)]
struct Overflow {
    map: AHashMap<Box<[u8]>, u64>,
    bytes: Vec<Box<[u8]>>,
}

#[derive(Default)]
pub struct Ids {
    over: Mutex<Overflow>,
}

impl Ids {
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

    #[cold]
    fn over_key(&self, s: &[u8]) -> u64 {
        let mut o = self.over.lock().unwrap();
        if let Some(&k) = o.map.get(s) {
            return k;
        }
        let k = OVER | o.bytes.len() as u64;
        o.bytes.push(s.into());
        o.map.insert(s.into(), k);
        k
    }

    pub fn write_bytes(&self, k: u64, out: &mut Vec<u8>) {
        if k & OVER != 0 {
            let o = self.over.lock().unwrap();
            out.extend_from_slice(&o.bytes[(k & !OVER) as usize]);
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
        ];
        for a in &words {
            assert_eq!(&ids.to_vec(ids.key(a))[..], *a);
            for b in &words {
                assert_eq!(ids.cmp(ids.key(a), ids.key(b)), a.cmp(b), "{:?} {:?}", a, b);
            }
        }
    }
}
