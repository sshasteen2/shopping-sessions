//! Hot path without a shared intern table.
//!
//! An id of at most 10 alphabet characters packs into a u64 whose integer
//! order is byte order (`ids::pack_key`). Anything else — a long hex id, an
//! upper-case hex id, a space, a symbol — is a tagged hash plus the original
//! bytes. Equal tagged hashes are confirmed by those bytes, and any order
//! that involves one (person-name minimum, click-id tie-break, short against
//! long) compares the bytes.
//!
//! Event records hit their partitions as 16-byte keys. The payload stays in
//! chunk order and is read back only for a duplicate or a survivor.
//!
//! `ENGINE=fast` is the previous engine. `ENGINE=old` is the one before that.

use std::cmp::Ordering;
use std::fs::{self, File};
use std::hash::{BuildHasherDefault, Hasher};
use std::path::{Path, PathBuf};
use std::time::Instant;

use hashbrown::{HashMap, HashTable};
use memmap2::Mmap;
use parquet::data_type::ByteArray;
use rayon::prelude::*;

use crate::csv::for_each_record;
use crate::ids::{append_packed, pack_key};
use crate::pq;
use crate::rows;
use crate::ts::{parse_ts_cached, parse_ts_str, DateCache, DAY};

const SESSION_GAP: i64 = 1_800_000;
const ATTRIB_WINDOW: i64 = 604_800_000;
const LATE_MS: i64 = 86_400_000;
const MAX_PRICE: u64 = 10_000_000;

const PBITS: u32 = 8;
const P: usize = 1 << PBITS;
const SUB_BITS: u32 = 4;
const SUB: usize = 1 << SUB_BITS;
const CHUNK: usize = 4 << 20;

/// Top bit set: this u64 is not a packed id.
const TAG: u64 = 1 << 63;
/// Bit 62 set: a chunk-local long person id from a click file, not an event file.
const CLICK_LOCAL: u64 = 1 << 62;

#[inline(always)]
fn mix(x: u64) -> u64 {
    let mut z = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

#[inline(always)]
fn part(x: u64) -> usize {
    (mix(x) >> (64 - PBITS)) as usize
}

#[inline(always)]
fn bytes_at(ptr: u64, len: u32) -> &'static [u8] {
    if len == 0 {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(ptr as *const u8, len as usize) }
    }
}

#[inline(always)]
fn id_hash(s: &[u8]) -> u64 {
    let mut h = 0x9E37_79B9_7F4A_7C15u64 ^ (s.len() as u64).wrapping_mul(0xC6A4_A793_5BD1_E995);
    let mut i = 0usize;
    while i + 8 <= s.len() {
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&s[i..i + 8]);
        h = mix(h ^ u64::from_le_bytes(buf));
        i += 8;
    }
    if i < s.len() {
        let mut buf = [0u8; 8];
        buf[..s.len() - i].copy_from_slice(&s[i..]);
        h = mix(h ^ u64::from_le_bytes(buf));
    }
    h
}

/// Packed key, or a tagged content hash when the id does not pack.
#[inline(always)]
fn content_key(s: &[u8]) -> u64 {
    match pack_key(s) {
        Some(k) => k,
        None => TAG | (id_hash(s) >> 1),
    }
}

#[inline(always)]
fn tagged(k: u64) -> bool {
    k & TAG != 0
}

/// Content hash of a long person id that is not in the link map. Bit 60 marks
/// it so it never collides with a packed key or a small global name index.
const HASH_PERSON: u64 = 1 << 60;

#[inline(always)]
fn hash_person_key(s: &[u8]) -> u64 {
    let low = (id_hash(s) >> 4) & ((1 << 59) - 1) & !HASH_PERSON;
    TAG | HASH_PERSON | low
}

/// Chunk-local long person. Kept only for the rare hash collision inside one
/// chunk, where a content hash would merge two different ids.
#[inline(always)]
fn local_person(click: bool, chunk: u32, idx: u32) -> u64 {
    let mut k = TAG | (((chunk as u64) + 1) << 32) | idx as u64;
    if click {
        k |= CLICK_LOCAL;
    }
    k
}

#[inline(always)]
fn global_person(idx: u32) -> u64 {
    TAG | idx as u64
}

#[inline(always)]
fn is_local_person(k: u64) -> bool {
    // Hash-person keys set bit 60 and are already global. Local keys do not.
    tagged(k) && (k & HASH_PERSON) == 0 && ((k >> 32) & 0x0fff_ffff) != 0
}

#[inline(always)]
fn local_click(k: u64) -> bool {
    k & CLICK_LOCAL != 0
}

#[inline(always)]
fn local_chunk(k: u64) -> usize {
    (((k >> 32) & 0x3fff_ffff) - 1) as usize
}

#[inline(always)]
fn local_index(k: u64) -> usize {
    (k & 0xffff_ffff) as usize
}

fn decode_packed(k: u64) -> Vec<u8> {
    let mut v = Vec::with_capacity(10);
    append_packed(k, &mut v);
    v
}

fn cmp_content(ka: u64, pa: u64, la: u32, kb: u64, pb: u64, lb: u32) -> Ordering {
    if !tagged(ka) && !tagged(kb) {
        return ka.cmp(&kb);
    }
    let av = if tagged(ka) { bytes_at(pa, la).to_vec() } else { decode_packed(ka) };
    let bv = if tagged(kb) { bytes_at(pb, lb).to_vec() } else { decode_packed(kb) };
    av.cmp(&bv)
}

// ---------------------------------------------------------------- lookups

struct IdLookup {
    prefix: Vec<u8>,
    dense: Vec<u16>,
    rest: Vec<(u64, Vec<u8>, u16)>,
}

const DENSE_MAX: u64 = 1 << 22;

impl IdLookup {
    #[inline]
    fn number(prefix: &[u8], s: &[u8]) -> Option<u64> {
        let n = prefix.len();
        if s.len() <= n || s.len() > n + 7 || &s[..n] != prefix {
            return None;
        }
        let digits = &s[n..];
        if digits[0] == b'0' && digits.len() > 1 {
            return None;
        }
        let mut v = 0u64;
        for &b in digits {
            let d = b.wrapping_sub(b'0');
            if d > 9 {
                return None;
            }
            v = v * 10 + d as u64;
        }
        Some(v)
    }

    fn build(items: &[(Vec<u8>, u16)]) -> Self {
        let prefix: Vec<u8> = items
            .first()
            .map(|(id, _)| id.iter().take_while(|b| !b.is_ascii_digit()).copied().collect())
            .unwrap_or_default();
        let mut dense: Vec<u16> = Vec::new();
        let mut rest: Vec<(u64, Vec<u8>, u16)> = Vec::new();
        for (id, v) in items {
            match Self::number(&prefix, id) {
                Some(num) if num < DENSE_MAX => {
                    let i = num as usize;
                    if dense.len() <= i {
                        dense.resize(i + 1, 0);
                    }
                    dense[i] = v + 1;
                }
                _ => {
                    let h = id_hash(id);
                    if let Some(pos) = rest.iter().position(|r| r.0 == h && r.1 == *id) {
                        rest[pos].2 = *v;
                    } else {
                        rest.push((h, id.clone(), *v));
                    }
                }
            }
        }
        Self { prefix, dense, rest }
    }

    #[inline]
    fn get(&self, s: &[u8]) -> Option<u16> {
        if let Some(num) = Self::number(&self.prefix, s) {
            if num < DENSE_MAX {
                let i = num as usize;
                return match self.dense.get(i) {
                    Some(&v) if v != 0 => Some(v - 1),
                    _ => None,
                };
            }
        }
        let h = id_hash(s);
        for (rh, b, v) in &self.rest {
            if *rh == h && b == s {
                return Some(*v);
            }
        }
        None
    }
}

struct Parts<T> {
    data: Vec<T>,
    off: Vec<u32>,
}

impl<T> Parts<T> {
    #[inline]
    fn slice(&self, p: usize) -> &[T] {
        &self.data[self.off[p] as usize..self.off[p + 1] as usize]
    }
}

fn partition_n<T: Copy>(v: &[T], nparts: usize, key: impl Fn(&T) -> usize) -> Parts<T> {
    let ks: Vec<u16> = v.iter().map(|x| key(x) as u16).collect();
    let mut counts = vec![0u32; nparts];
    for &k in &ks {
        counts[k as usize] += 1;
    }
    let mut off = vec![0u32; nparts + 1];
    for i in 0..nparts {
        off[i + 1] = off[i] + counts[i];
    }
    let mut pos: Vec<u32> = off[..nparts].to_vec();
    let mut data: Vec<T> = Vec::with_capacity(v.len());
    {
        let dst = data.spare_capacity_mut();
        for (x, &k) in v.iter().zip(&ks) {
            dst[pos[k as usize] as usize].write(*x);
            pos[k as usize] += 1;
        }
    }
    unsafe { data.set_len(v.len()) };
    Parts { data, off }
}

fn partition<T: Copy>(v: &[T], key: impl Fn(&T) -> usize) -> Parts<T> {
    partition_n(v, P, key)
}

#[derive(Default, Clone, Copy)]
struct MixHasher(u64);
impl Hasher for MixHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = mix(self.0 ^ b as u64);
        }
    }
    fn write_u64(&mut self, x: u64) {
        self.0 = mix(x);
    }
}
type U64Map<V> = HashMap<u64, V, BuildHasherDefault<MixHasher>>;

enum FileBuf {
    Map(Mmap),
    Owned(Vec<u8>),
}

impl FileBuf {
    fn load(p: &Path) -> Self {
        if let Ok(f) = File::open(p) {
            if f.metadata().map(|m| m.len()).unwrap_or(0) > 0 {
                if let Ok(m) = unsafe { Mmap::map(&f) } {
                    return FileBuf::Map(m);
                }
            }
        }
        FileBuf::Owned(fs::read(p).unwrap_or_default())
    }
    fn bytes(&self) -> &[u8] {
        match self {
            FileBuf::Map(m) => m,
            FileBuf::Owned(v) => v,
        }
    }
}

fn csv_files(dir: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    if let Ok(rd) = fs::read_dir(dir) {
        for ent in rd.flatten() {
            let p = ent.path();
            if p.extension().and_then(|e| e.to_str()) == Some("csv") {
                files.push(p);
            }
        }
    }
    files.sort();
    files
}

fn load_dir(dir: &Path) -> Vec<FileBuf> {
    csv_files(dir).par_iter().map(|p| FileBuf::load(p)).collect()
}

struct Chunk<'a> {
    data: &'a [u8],
    header: bool,
}

fn chunks(bufs: &[FileBuf]) -> Vec<Chunk<'_>> {
    let mut out = Vec::new();
    for b in bufs {
        let b = b.bytes();
        let mut lo = 0;
        while lo < b.len() {
            let mut hi = (lo + CHUNK).min(b.len());
            if hi < b.len() {
                hi = match memchr::memchr(b'\n', &b[hi..]) {
                    Some(i) => hi + i + 1,
                    None => b.len(),
                };
            }
            out.push(Chunk { data: &b[lo..hi], header: lo == 0 });
            lo = hi;
        }
    }
    out
}

#[inline]
fn parse_price(s: &[u8]) -> Option<u32> {
    if s.is_empty() {
        return None;
    }
    let mut n: u64 = 0;
    for &b in s {
        let d = b.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        n = n * 10 + d as u64;
        if n > MAX_PRICE {
            return None;
        }
    }
    if n < 1 { None } else { Some(n as u32) }
}

// ---------------------------------------------------------------- names

/// Packed user id -> canonical person key. Absent means the id is its own name.
struct NameTable {
    mask: usize,
    keys: Vec<u64>,
    vals: Vec<u64>,
    filter: Vec<u64>,
    fshift: u32,
}

impl NameTable {
    fn new(n: usize) -> Self {
        let cap = (n.max(1) * 2).next_power_of_two().max(16);
        let fbits = (n.max(1) * 16).next_power_of_two().max(64);
        Self {
            mask: cap - 1,
            keys: vec![0; cap],
            vals: vec![0; cap],
            filter: vec![0; fbits / 64],
            fshift: 64 - fbits.trailing_zeros(),
        }
    }
    #[inline]
    fn fbit(&self, k: u64) -> usize {
        (mix(k ^ 0x6A09_E667_F3BC_C908) >> self.fshift) as usize
    }
    #[inline]
    fn maybe(&self, k: u64) -> bool {
        let b = self.fbit(k);
        (self.filter[b >> 6] >> (b & 63)) & 1 != 0
    }
    #[inline]
    fn home(&self, k: u64) -> usize {
        (mix(k) as usize) & self.mask
    }
    fn insert(&mut self, k: u64, v: u64) {
        debug_assert!(!tagged(k) && k != 0);
        let b = self.fbit(k);
        self.filter[b >> 6] |= 1 << (b & 63);
        let mut i = self.home(k);
        loop {
            if self.keys[i] == 0 {
                self.keys[i] = k;
                self.vals[i] = v;
                return;
            }
            if self.keys[i] == k {
                self.vals[i] = v;
                return;
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline]
    fn get(&self, k: u64) -> u64 {
        if k == 0 || tagged(k) || !self.maybe(k) {
            return k;
        }
        let mut i = self.home(k);
        loop {
            let sk = self.keys[i];
            if sk == k {
                return self.vals[i];
            }
            if sk == 0 {
                return k;
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline]
    fn prefetch(&self, k: u64) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
            if !tagged(k) && k != 0 {
                let b = self.fbit(k);
                _mm_prefetch(self.filter.as_ptr().add(b >> 6) as *const i8, _MM_HINT_T0);
            }
        }
        #[cfg(not(target_arch = "x86_64"))]
        let _ = k;
    }
}

/// Long link ids, hashed and confirmed by bytes, each already resolved to its
/// person's canonical key.
struct LongMap {
    mask: usize,
    hash: Vec<u64>,
    ptr: Vec<u64>,
    len: Vec<u32>,
    canon: Vec<u64>,
}

impl LongMap {
    fn new(n: usize) -> Self {
        let cap = (n.max(1) * 2).next_power_of_two().max(16);
        Self {
            mask: cap - 1,
            hash: vec![0; cap],
            ptr: vec![0; cap],
            len: vec![0; cap],
            canon: vec![0; cap],
        }
    }
    #[inline]
    fn home(&self, h: u64) -> usize {
        (mix(h) as usize) & self.mask
    }
    fn insert(&mut self, s: &[u8], canon: u64) {
        let h = id_hash(s);
        let mut i = self.home(h);
        loop {
            if self.ptr[i] == 0 {
                // The caller keeps `s` alive (the name arena).
                self.hash[i] = h;
                self.ptr[i] = s.as_ptr() as u64;
                self.len[i] = s.len() as u32;
                self.canon[i] = canon;
                return;
            }
            if self.hash[i] == h && bytes_at(self.ptr[i], self.len[i]) == s {
                self.canon[i] = canon;
                return;
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline]
    fn get(&self, s: &[u8]) -> Option<u64> {
        let h = id_hash(s);
        let mut i = self.home(h);
        loop {
            if self.ptr[i] == 0 {
                return None;
            }
            if self.hash[i] == h && self.len[i] as usize == s.len() && bytes_at(self.ptr[i], self.len[i]) == s {
                return Some(self.canon[i]);
            }
            i = (i + 1) & self.mask;
        }
    }
}

struct Globals {
    names: Vec<Vec<u8>>,
    index: U64Map<Vec<u32>>,
}

impl Globals {
    fn intern(&mut self, s: &[u8]) -> u64 {
        let h = id_hash(s);
        if let Some(ixs) = self.index.get(&h) {
            for &i in ixs {
                if self.names[i as usize] == s {
                    return global_person(i);
                }
            }
        }
        let i = self.names.len() as u32;
        self.names.push(s.to_vec());
        self.index.entry(h).or_default().push(i);
        global_person(i)
    }
}

struct LocalNames {
    /// Content-hash key -> the first bytes, still pointing at the input map.
    seen: U64Map<(u64, u32)>,
    /// Different strings that landed on one content hash. Their keys are local.
    collided: Vec<(u64, u32, u64)>,
}

impl LocalNames {
    fn new() -> Self {
        Self { seen: U64Map::default(), collided: Vec::new() }
    }
    fn intern(&mut self, click: bool, chunk: u32, s: &[u8]) -> u64 {
        let key = hash_person_key(s);
        if let Some(&(ptr, len)) = self.seen.get(&key) {
            if bytes_at(ptr, len) == s {
                return key;
            }
            for (p, l, k) in &self.collided {
                if bytes_at(*p, *l) == s {
                    return *k;
                }
            }
            let k = local_person(click, chunk, self.collided.len() as u32);
            self.collided.push((s.as_ptr() as u64, s.len() as u32, k));
            return k;
        }
        self.seen.insert(key, (s.as_ptr() as u64, s.len() as u32));
        key
    }
}

fn person_key(s: &[u8], names: &NameTable, longs: &LongMap, local: &mut LocalNames, click: bool, chunk: u32) -> u64 {
    if s.is_empty() {
        return 0;
    }
    if let Some(k) = pack_key(s) {
        return names.get(k);
    }
    if let Some(k) = longs.get(s) {
        return k;
    }
    local.intern(click, chunk, s)
}

fn remap_person(k: u64, ev_remap: &[Vec<u64>], ck_remap: &[Vec<u64>]) -> u64 {
    if !is_local_person(k) {
        return k;
    }
    let table = if local_click(k) { ck_remap } else { ev_remap };
    table[local_chunk(k)][local_index(k)]
}

// ---------------------------------------------------------------- records

/// One event, scanned and partitioned as a single record so dedup never
/// chases a side array. Long event ids keep a pointer at their bytes.
#[derive(Clone, Copy)]
struct Ev {
    eid: u64,
    person: u64,
    ingest: i64,
    eid_ptr: u64,
    ts: u32,
    pc: u32,
    valid: u32,
    eid_len: u32,
}

#[derive(Clone, Copy)]
struct Ev2 {
    person: u64,
    ts: u32,
    pc: u32,
}

impl Ev2 {
    #[inline]
    fn price(&self) -> i64 {
        (self.pc & 0x00FF_FFFF) as i64
    }
    #[inline]
    fn cat(&self) -> usize {
        (self.pc >> 24) as usize
    }
}

#[derive(Clone, Copy)]
struct Ck {
    person: u64,
    cid: u64,
    cid_ptr: u64,
    ts: u32,
    camp: u32,
    cid_len: u32,
    _pad: u32,
}

struct URow {
    person: u64,
    day: i32,
    sessions: i64,
    events: i64,
    revenue: i64,
    top: Option<u16>,
}

struct EvChunk {
    parts: Parts<Ev>,
    local: LocalNames,
}

struct CkChunk {
    rows: Vec<Ck>,
    local: LocalNames,
}

fn mark(t0: Instant, last: &mut Instant, label: &str) {
    let now = Instant::now();
    eprintln!(
        "phase {label:>16} {:>7.0} ms  (total {:>7.0} ms)",
        now.duration_since(*last).as_secs_f64() * 1000.0,
        now.duration_since(t0).as_secs_f64() * 1000.0
    );
    *last = now;
}

struct CampAgg {
    nd: usize,
    v: Vec<[i64; 4]>,
}

impl CampAgg {
    fn new(ncamp: usize, nd: usize) -> Self {
        Self { nd, v: vec![[0; 4]; (ncamp + 1) * nd] }
    }
    #[inline]
    fn at(&mut self, camp: usize, d: usize) -> &mut [i64; 4] {
        &mut self.v[camp * self.nd + d]
    }
    fn add(mut self, o: CampAgg) -> CampAgg {
        for (a, b) in self.v.iter_mut().zip(o.v) {
            for k in 0..4 {
                a[k] += b[k];
            }
        }
        self
    }
}

struct Work {
    rows: Vec<URow>,
    agg: CampAgg,
    counts: Vec<u32>,
    in_active: Vec<bool>,
    active: Vec<u32>,
    cat_rev: Vec<i64>,
    cat_touched: Vec<u16>,
}

#[derive(Clone, Copy)]
struct End {
    key: u64,
    ptr: u64,
    len: u32,
    slot: u32,
}

fn eid_same(a: &Ev, b: &Ev) -> bool {
    a.eid == b.eid
        && (!tagged(a.eid) || (a.eid_len == b.eid_len && bytes_at(a.eid_ptr, a.eid_len) == bytes_at(b.eid_ptr, b.eid_len)))
}

fn same_cid(a: &Ck, b: &Ck) -> bool {
    a.cid == b.cid
        && (!tagged(a.cid)
            || (a.cid_len == b.cid_len && bytes_at(a.cid_ptr, a.cid_len) == bytes_at(b.cid_ptr, b.cid_len)))
}

pub fn run(n: usize) {
    let t0 = Instant::now();
    let mut last = t0;
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into()));
    let out = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into()));

    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(data.join("manifest.json")).unwrap()).expect("manifest");
    let win_start = parse_ts_str(manifest["window_start"].as_str().unwrap()).unwrap();
    let win_end = parse_ts_str(manifest["window_end"].as_str().unwrap()).unwrap();
    let click_lo = win_start - 7 * DAY;
    let day_lo = win_start.div_euclid(DAY);
    let day_hi = (win_end - 1).div_euclid(DAY);
    let nd = (day_hi - day_lo + 1).max(1) as usize;
    eprintln!("rogue engine: ncpu={n}");
    if win_end - click_lo > u32::MAX as i64 {
        return crate::engine::run();
    }

    let mut cat_names: Vec<Vec<u8>> = Vec::new();
    let mut cat_idx: std::collections::HashMap<Vec<u8>, u16> = Default::default();
    let mut product_items: Vec<(Vec<u8>, u16)> = Vec::new();
    {
        let raw = fs::read(data.join("products.csv")).unwrap_or_default();
        for_each_record(&raw, 2, |row| {
            if row[0].is_empty() {
                return;
            }
            let c = *cat_idx.entry(row[1].to_vec()).or_insert_with(|| {
                cat_names.push(row[1].to_vec());
                (cat_names.len() - 1) as u16
            });
            product_items.push((row[0].to_vec(), c));
        });
    }
    let unknown = *cat_idx.entry(b"unknown".to_vec()).or_insert_with(|| {
        cat_names.push(b"unknown".to_vec());
        (cat_names.len() - 1) as u16
    });
    let products = IdLookup::build(&product_items);
    let ncat = cat_names.len();
    if ncat > 256 {
        return crate::engine::run();
    }
    let mut cat_rank = vec![0u32; ncat];
    {
        let mut order: Vec<usize> = (0..ncat).collect();
        order.sort_by(|&a, &b| cat_names[a].cmp(&cat_names[b]));
        for (r, &c) in order.iter().enumerate() {
            cat_rank[c] = r as u32;
        }
    }

    let mut camp_ids: Vec<Vec<u8>> = Vec::new();
    let mut camp_channels: Vec<Vec<u8>> = Vec::new();
    let mut camp_items: Vec<(Vec<u8>, u16)> = Vec::new();
    {
        let raw = fs::read(data.join("campaigns.csv")).unwrap_or_default();
        let mut seen: U64Map<Vec<Vec<u8>>> = U64Map::default();
        for_each_record(&raw, 2, |row| {
            if row[0].is_empty() {
                return;
            }
            let h = id_hash(row[0]);
            let list = seen.entry(h).or_default();
            if list.iter().any(|s| s.as_slice() == row[0]) {
                return;
            }
            list.push(row[0].to_vec());
            let idx = camp_ids.len() as u16;
            camp_ids.push(row[0].to_vec());
            camp_channels.push(row[1].to_vec());
            camp_items.push((row[0].to_vec(), idx));
        });
    }
    let campaigns = IdLookup::build(&camp_items);
    let ncamp = camp_ids.len();
    if ncamp > u16::MAX as usize {
        return crate::engine::run();
    }
    let organic = ncamp as u32;
    mark(t0, &mut last, "lookups");

    // ------------------------------------------------------------ links
    let link_bufs = load_dir(&data.join("links"));
    let link_chunks = chunks(&link_bufs);
    let per_chunk: Vec<Vec<(u64, u64, u32)>> = link_chunks
        .par_iter()
        .map(|c| {
            let mut v = Vec::new();
            rows::for_each_row::<2>(c.data, c.header, |f| {
                if f[0].is_empty() || f[1].is_empty() {
                    return;
                }
                for field in [f[0], f[1]] {
                    let key = pack_key(field).unwrap_or_else(|| TAG | (id_hash(field) >> 1));
                    v.push((key, field.as_ptr() as u64, field.len() as u32));
                }
            });
            v
        })
        .collect();
    let mut ends: Vec<End> = Vec::new();
    for part in per_chunk {
        for (key, ptr, len) in part {
            let slot = ends.len() as u32;
            ends.push(End { key, ptr, len, slot });
        }
    }
    // Integer sort. Packed ids group exactly; long ids that share a hash are
    // split by the byte compare, and the linear scan below uniques on bytes.
    ends.par_sort_unstable_by(|a, b| {
        a.key.cmp(&b.key).then_with(|| {
            if a.key == b.key && tagged(a.key) {
                bytes_at(a.ptr, a.len).cmp(bytes_at(b.ptr, b.len))
            } else {
                Ordering::Equal
            }
        })
    });
    let mut reps: Vec<End> = Vec::new();
    let mut pid = vec![0u32; ends.len()];
    for e in &ends {
        // Equal packed keys are the same bytes. Tagged keys share a hash, so
        // confirm with the bytes; a different hash cannot be the same id.
        let same = match reps.last() {
            Some(r) if r.key == e.key => !tagged(e.key) || bytes_at(r.ptr, r.len) == bytes_at(e.ptr, e.len),
            _ => false,
        };
        if !same {
            reps.push(*e);
        }
        pid[e.slot as usize] = (reps.len() - 1) as u32;
    }
    let nids = reps.len();
    let mut parent: Vec<u32> = (0..nids as u32).collect();
    fn find(parent: &mut [u32], mut x: u32) -> u32 {
        while parent[x as usize] != x {
            let p = parent[x as usize];
            parent[x as usize] = parent[p as usize];
            x = p;
        }
        x
    }
    for pair in pid.chunks_exact(2) {
        let ra = find(&mut parent, pair[0]);
        let rb = find(&mut parent, pair[1]);
        if ra != rb {
            let (lo, hi) = if ra < rb { (ra, rb) } else { (rb, ra) };
            parent[hi as usize] = lo;
        }
    }
    for i in 0..nids {
        parent[i] = find(&mut parent, i as u32);
    }
    let cmp_end = |a: &End, b: &End| -> Ordering {
        if !tagged(a.key) && !tagged(b.key) {
            a.key.cmp(&b.key)
        } else {
            bytes_at(a.ptr, a.len).cmp(bytes_at(b.ptr, b.len))
        }
    };
    let mut best: Vec<u32> = (0..nids as u32).collect();
    for i in 0..nids {
        let r = parent[i] as usize;
        if cmp_end(&reps[i], &reps[best[r] as usize]) == Ordering::Less {
            best[r] = i as u32;
        }
    }
    // Own every distinct long byte string and every canonical name that does
    // not pack. Packed canonical names are recovered from the key at output.
    let mut globals = Globals { names: Vec::new(), index: U64Map::default() };
    let mut root_canon = vec![u64::MAX; nids];
    let mut canon_key = vec![0u64; nids];
    for i in 0..nids {
        let r = parent[i] as usize;
        if root_canon[r] == u64::MAX {
            let b = &reps[best[r] as usize];
            root_canon[r] = if !tagged(b.key) {
                b.key
            } else {
                globals.intern(bytes_at(b.ptr, b.len))
            };
        }
        canon_key[i] = root_canon[r];
    }
    let mut nremap = 0usize;
    let mut nlong = 0usize;
    for i in 0..nids {
        if tagged(reps[i].key) {
            nlong += 1;
        } else if canon_key[i] != reps[i].key {
            nremap += 1;
        }
    }
    let mut names = NameTable::new(nremap);
    let mut longs = LongMap::new(nlong);
    // Only long ids need owned bytes. Packed ids compare and decode as integers.
    let mut arena: Vec<u8> = Vec::new();
    let mut pending: Vec<(u32, u32, u64)> = Vec::with_capacity(nlong);
    for i in 0..nids {
        if !tagged(reps[i].key) {
            if canon_key[i] != reps[i].key {
                names.insert(reps[i].key, canon_key[i]);
            }
        } else {
            let mine = bytes_at(reps[i].ptr, reps[i].len);
            let off = arena.len() as u32;
            arena.extend_from_slice(mine);
            pending.push((off, mine.len() as u32, canon_key[i]));
        }
    }
    let arena: &'static [u8] = Box::leak(arena.into_boxed_slice());
    for (off, len, canon) in pending {
        longs.insert(&arena[off as usize..off as usize + len as usize], canon);
    }
    drop(ends);
    drop(pid);
    drop(reps);
    drop(link_chunks);
    drop(link_bufs);
    mark(t0, &mut last, "links");

    // ------------------------------------------------------------ events
    let event_bufs = load_dir(&data.join("events"));
    let ev_chunks = chunks(&event_bufs);
    let ev_out: Vec<EvChunk> = ev_chunks
        .par_iter()
        .enumerate()
        .map(|(ci, c)| {
            let ci = ci as u32;
            let mut rows: Vec<Ev> = Vec::with_capacity(c.data.len() / 80 + 16);
            let mut local = LocalNames::new();
            let mut dc = DateCache::default();
            scan_fields::<7>(c.data, c.header, |f| {
                if f[0].is_empty() {
                    return;
                }
                let Some(ingest) = parse_ts_cached(f[3], &mut dc) else {
                    return;
                };
                let eid_s = f[0];
                let eid = content_key(eid_s);
                let user = person_key(f[1], &names, &longs, &mut local, false, ci);
                let kind: u32 = match f[4] {
                    b"view" => 0,
                    b"cart" => 1,
                    b"purchase" => 2,
                    _ => 3,
                };
                let mut ok = !f[1].is_empty() && kind != 3 && !f[5].is_empty();
                let mut ts = 0u32;
                if ok {
                    match parse_ts_cached(f[2], &mut dc) {
                        Some(t) if t >= win_start && t < win_end && ingest - t <= LATE_MS => {
                            ts = (t - win_start) as u32;
                        }
                        _ => ok = false,
                    }
                }
                let mut price = 0u32;
                if ok && kind == 2 {
                    match parse_price(f[6]) {
                        Some(p) => price = p,
                        None => ok = false,
                    }
                }
                let cat = if ok { products.get(f[5]).unwrap_or(unknown) } else { 0 };
                rows.push(Ev {
                    eid,
                    person: user,
                    ingest,
                    eid_ptr: if tagged(eid) { eid_s.as_ptr() as u64 } else { 0 },
                    ts,
                    pc: price | (cat as u32) << 24,
                    valid: ok as u32,
                    eid_len: if tagged(eid) { eid_s.len() as u32 } else { 0 },
                });
            });
            let parts = partition_n(&rows, P * SUB, |e| (mix(e.eid) >> (64 - PBITS - SUB_BITS)) as usize);
            EvChunk { parts, local }
        })
        .collect();
    // Content-hash person keys are already global. Only a hash collision gets
    // a chunk-local key, and that is rewritten to a owned global name.
    let mut ev_remap: Vec<Vec<u64>> = Vec::with_capacity(ev_out.len());
    let mut hash_seen: U64Map<(u64, u32)> = U64Map::default();
    for ch in &ev_out {
        let mut map = Vec::with_capacity(ch.local.collided.len());
        for (ptr, len, _) in &ch.local.collided {
            map.push(globals.intern(bytes_at(*ptr, *len)));
        }
        ev_remap.push(map);
        for (&k, &(ptr, len)) in &ch.local.seen {
            hash_seen.entry(k).or_insert((ptr, len));
        }
    }
    mark(t0, &mut last, "parse_events");

    const DEDUP_SEED: u64 = 0xA5A5_5A5A_DEAD_BEEF;
    let ev_by_person: Vec<Parts<Ev2>> = (0..P)
        .into_par_iter()
        .map(|p| {
            let total: usize = (0..SUB)
                .map(|s| ev_out.iter().map(|b| b.parts.slice(p * SUB + s).len()).sum::<usize>())
                .sum();
            let mut kept: Vec<Ev2> = Vec::with_capacity(total);
            let mut table: HashTable<Ev> = HashTable::with_capacity(total / SUB * 2 + 64);
            for sp in p * SUB..(p + 1) * SUB {
                table.clear();
                for ch in &ev_out {
                    for e in ch.parts.slice(sp) {
                        let h = mix(e.eid ^ DEDUP_SEED);
                        match table.find_mut(h, |x| eid_same(x, e)) {
                            Some(x) => {
                                if e.ingest < x.ingest {
                                    *x = *e;
                                }
                            }
                            None => {
                                table.insert_unique(h, *e, |x| mix(x.eid ^ DEDUP_SEED));
                            }
                        }
                    }
                }
                for e in table.iter() {
                    if e.valid == 0 {
                        continue;
                    }
                    kept.push(Ev2 {
                        person: remap_person(e.person, &ev_remap, &[]),
                        ts: e.ts + (win_start - click_lo) as u32,
                        pc: e.pc,
                    });
                }
            }
            drop(table);
            partition(&kept, |e| part(e.person))
        })
        .collect();
    mark(t0, &mut last, "dedup_events");

    // ------------------------------------------------------------ clicks
    let click_bufs = load_dir(&data.join("clicks"));
    let ck_out: Vec<CkChunk> = chunks(&click_bufs)
        .par_iter()
        .enumerate()
        .map(|(ci, c)| {
            let ci = ci as u32;
            let mut v: Vec<Ck> = Vec::with_capacity(c.data.len() / 50 + 8);
            let mut local = LocalNames::new();
            let mut dc = DateCache::default();
            scan_fields::<4>(c.data, c.header, |f| {
                if f[0].is_empty() || f[1].is_empty() {
                    return;
                }
                let Some(camp) = campaigns.get(f[2]) else { return };
                let Some(ts) = parse_ts_cached(f[3], &mut dc) else { return };
                if ts < click_lo || ts >= win_end {
                    return;
                }
                let cid_s = f[0];
                let cid = content_key(cid_s);
                v.push(Ck {
                    person: person_key(f[1], &names, &longs, &mut local, true, ci),
                    cid,
                    cid_ptr: if tagged(cid) { cid_s.as_ptr() as u64 } else { 0 },
                    ts: (ts - click_lo) as u32,
                    camp: camp as u32,
                    cid_len: if tagged(cid) { cid_s.len() as u32 } else { 0 },
                    _pad: 0,
                });
            });
            CkChunk { rows: v, local }
        })
        .collect();
    let mut ck_remap: Vec<Vec<u64>> = Vec::with_capacity(ck_out.len());
    for ch in &ck_out {
        let mut map = Vec::with_capacity(ch.local.collided.len());
        for (ptr, len, _) in &ch.local.collided {
            map.push(globals.intern(bytes_at(*ptr, *len)));
        }
        ck_remap.push(map);
        for (&k, &(ptr, len)) in &ch.local.seen {
            hash_seen.entry(k).or_insert((ptr, len));
        }
    }
    // Person keys on clicks may still be chunk-local. Rewrite before sessions
    // so they match the global keys on events. Packed and link-resolved keys
    // pass through.
    let ck_buckets: Vec<Parts<Ck>> = ck_out
        .par_iter()
        .map(|ch| {
            let mut data = ch.rows.clone();
            for c in &mut data {
                c.person = remap_person(c.person, &ev_remap, &ck_remap);
            }
            partition(&data, |c| part(c.person))
        })
        .collect();
    mark(t0, &mut last, "parse_clicks");

    let cid_cmp = |a: &Ck, b: &Ck| -> Ordering {
        a.ts.cmp(&b.ts).then_with(|| cmp_content(a.cid, a.cid_ptr, a.cid_len, b.cid, b.cid_ptr, b.cid_len))
    };
    let new_work = || Work {
        rows: Vec::new(),
        agg: CampAgg::new(ncamp, nd),
        counts: vec![0; ncamp + 1],
        in_active: vec![false; ncamp + 1],
        active: Vec::new(),
        cat_rev: vec![0; ncat],
        cat_touched: Vec::new(),
    };
    let work = (0..P)
        .into_par_iter()
        .fold(new_work, |mut w, q| {
            let ev_parts: Vec<&[Ev2]> = ev_by_person.iter().map(|x| x.slice(q)).collect();
            let ck_parts: Vec<&[Ck]> = ck_buckets.iter().map(|x| x.slice(q)).collect();
            const LOCAL_SEED: u64 = 0x3C6E_F372_FE94_F82B;
            let total: usize = ev_parts.iter().map(|v| v.len()).sum();
            let mut table: HashTable<(u64, u32)> = HashTable::with_capacity(total / 16 + 16);
            let mut persons: Vec<u64> = Vec::new();
            let mut counts: Vec<u32> = Vec::new();
            let mut lids: Vec<u32> = Vec::with_capacity(total);
            for v in &ev_parts {
                for e in v.iter() {
                    let h = mix(e.person ^ LOCAL_SEED);
                    let lid = match table.find(h, |x| x.0 == e.person) {
                        Some(x) => x.1,
                        None => {
                            let l = persons.len() as u32;
                            persons.push(e.person);
                            counts.push(0);
                            table.insert_unique(h, (e.person, l), |x| mix(x.0 ^ LOCAL_SEED));
                            l
                        }
                    };
                    counts[lid as usize] += 1;
                    lids.push(lid);
                }
            }
            let np = persons.len();
            let mut off = vec![0u32; np + 1];
            for i in 0..np {
                off[i + 1] = off[i] + counts[i];
            }
            let mut pos: Vec<u32> = off[..np].to_vec();
            let mut ev: Vec<Ev2> = Vec::with_capacity(total);
            {
                let dst = ev.spare_capacity_mut();
                let mut nwritten = 0;
                for v in &ev_parts {
                    for e in v.iter() {
                        let l = lids[nwritten] as usize;
                        dst[pos[l] as usize].write(*e);
                        pos[l] += 1;
                        nwritten += 1;
                    }
                }
            }
            unsafe { ev.set_len(total) };
            drop(ev_parts);
            drop(lids);

            let mut cl: Vec<(u32, Ck)> = Vec::new();
            let mut ccounts = vec![0u32; np + 1];
            for v in &ck_parts {
                for c in v.iter() {
                    let h = mix(c.person ^ LOCAL_SEED);
                    if let Some(x) = table.find(h, |x| x.0 == c.person) {
                        ccounts[x.1 as usize] += 1;
                        cl.push((x.1, *c));
                    }
                }
            }
            drop(ck_parts);
            let mut coff = vec![0u32; np + 1];
            for i in 0..np {
                coff[i + 1] = coff[i] + ccounts[i];
            }
            let mut cpos: Vec<u32> = coff[..np].to_vec();
            let mut ck: Vec<Ck> = Vec::with_capacity(cl.len());
            {
                let dst = ck.spare_capacity_mut();
                for &(l, c) in &cl {
                    dst[cpos[l as usize] as usize].write(c);
                    cpos[l as usize] += 1;
                }
            }
            unsafe { ck.set_len(cl.len()) };
            drop(cl);

            for r in 0..np {
                let evs = &mut ev[off[r] as usize..off[r + 1] as usize];
                evs.sort_unstable_by_key(|e| e.ts);
                let cks = &mut ck[coff[r] as usize..coff[r + 1] as usize];
                let mut nck = cks.len();
                if nck > 1 {
                    cks.sort_unstable_by(cid_cmp);
                    let mut wr = 1usize;
                    for rd in 1..nck {
                        if cks[rd].ts != cks[wr - 1].ts || !same_cid(&cks[rd], &cks[wr - 1]) {
                            cks[wr] = cks[rd];
                            wr += 1;
                        }
                    }
                    nck = wr;
                }
                person_pass(persons[r], evs, &cks[..nck], &mut w, click_lo, day_lo, organic, &cat_rank);
            }
            w
        })
        .reduce(new_work, |mut a, b| {
            a.rows.extend(b.rows);
            a.agg = a.agg.add(b.agg);
            a
        });
    mark(t0, &mut last, "sessions");

    let rows = work.rows;
    let gnames = &globals.names;
    let nfiles = n.max(1).min(rows.len().max(1));
    let per = rows.len().div_ceil(nfiles).max(1);
    let udir = out.join("user_daily");
    fs::create_dir_all(&udir).ok();
    let cat_names = &cat_names;
    let write_part = |i: usize, part_rows: &[URow]| {
        let mut u_user = Vec::with_capacity(part_rows.len());
        let mut u_day = Vec::with_capacity(part_rows.len());
        let mut u_sess = Vec::with_capacity(part_rows.len());
        let mut u_ev = Vec::with_capacity(part_rows.len());
        let mut u_rev = Vec::with_capacity(part_rows.len());
        let mut u_cat = Vec::with_capacity(part_rows.len());
        for r in part_rows {
            let name = if r.person & HASH_PERSON != 0 {
                let (ptr, len) = hash_seen[&r.person];
                bytes_at(ptr, len).to_vec()
            } else if tagged(r.person) {
                gnames[(r.person & 0xffff_ffff) as usize].clone()
            } else {
                decode_packed(r.person)
            };
            u_user.push(ByteArray::from(name));
            u_day.push(r.day);
            u_sess.push(r.sessions);
            u_ev.push(r.events);
            u_rev.push(r.revenue);
            u_cat.push(r.top.map(|c| ByteArray::from(cat_names[c as usize].clone())));
        }
        pq::write_user_daily(&udir.join(format!("part-{i}.parquet")), u_user, u_day, u_sess, u_ev, u_rev, u_cat);
    };
    rayon::scope(|s| {
        if rows.is_empty() {
            s.spawn(|_| write_part(0, &[]));
        }
        for (i, c) in rows.chunks(per).enumerate() {
            let write_part = &write_part;
            s.spawn(move |_| write_part(i, c));
        }
        s.spawn(|_| {
            let mut agg = work.agg;
            let mut c_id = Vec::new();
            let mut c_ch = Vec::new();
            let mut c_day = Vec::new();
            let mut c_sess = Vec::new();
            let mut c_purch = Vec::new();
            let mut c_rev = Vec::new();
            let mut c_ast = Vec::new();
            for c in 0..=ncamp {
                for d in 0..nd {
                    let v = *agg.at(c, d);
                    let assisted = if c == ncamp { 0 } else { v[3] };
                    if v[0] == 0 && assisted == 0 {
                        continue;
                    }
                    if c == ncamp {
                        c_id.push(ByteArray::from(b"organic".to_vec()));
                        c_ch.push(ByteArray::from(b"organic".to_vec()));
                    } else {
                        c_id.push(ByteArray::from(camp_ids[c].clone()));
                        c_ch.push(ByteArray::from(camp_channels[c].clone()));
                    }
                    c_day.push((day_lo + d as i64) as i32);
                    c_sess.push(v[0]);
                    c_purch.push(v[1]);
                    c_rev.push(v[2]);
                    c_ast.push(assisted);
                }
            }
            fs::create_dir_all(out.join("campaign_daily")).ok();
            pq::write_campaign_daily(
                &out.join("campaign_daily").join("part-0.parquet"),
                c_id,
                c_ch,
                c_day,
                c_sess,
                c_purch,
                c_rev,
                c_ast,
            );
        });
    });
    mark(t0, &mut last, "write");
    let _keep = (&event_bufs, &click_bufs);
    std::process::exit(0);
}

fn person_pass(
    person: u64,
    ev: &[Ev2],
    clicks: &[Ck],
    w: &mut Work,
    base: i64,
    day_lo: i64,
    organic: u32,
    cat_rank: &[u32],
) {
    let (i, j) = (0usize, ev.len());
    let (mut lo, mut hi) = (0usize, 0usize);
    let mut cur_day: i64 = i64::MIN;
    let (mut u_sess, mut u_ev, mut u_rev) = (0i64, 0i64, 0i64);
    let mut e = i;
    while e < j {
        let start = ev[e].ts as i64;
        let mut s_end = e + 1;
        let mut prev = start;
        while s_end < j && ev[s_end].ts as i64 - prev <= SESSION_GAP {
            prev = ev[s_end].ts as i64;
            s_end += 1;
        }
        let day = (base + start).div_euclid(DAY);
        let d = (day - day_lo) as usize;
        let mut credit = organic;
        if !clicks.is_empty() {
            while hi < clicks.len() && (clicks[hi].ts as i64) <= start {
                let c = clicks[hi].camp as usize;
                w.counts[c] += 1;
                if !w.in_active[c] {
                    w.in_active[c] = true;
                    w.active.push(c as u32);
                }
                hi += 1;
            }
            let lo_ts = start - ATTRIB_WINDOW;
            while lo < hi && (clicks[lo].ts as i64) < lo_ts {
                w.counts[clicks[lo].camp as usize] -= 1;
                lo += 1;
            }
            if lo < hi {
                let t = clicks[hi - 1].ts;
                let mut c = hi - 1;
                while c > lo && clicks[c - 1].ts == t {
                    c -= 1;
                }
                credit = clicks[c].camp;
                let counts = &w.counts;
                let in_active = &mut w.in_active;
                w.active.retain(|&a| {
                    let keep = counts[a as usize] > 0;
                    if !keep {
                        in_active[a as usize] = false;
                    }
                    keep
                });
                for &a in &w.active {
                    w.agg.at(a as usize, d)[3] += 1;
                }
            }
        }
        let mut s_rev = 0i64;
        for x in &ev[e..s_end] {
            if x.pc & 0x00FF_FFFF != 0 {
                s_rev += x.price();
            }
        }
        let s_events = (s_end - e) as i64;
        {
            let a = w.agg.at(credit as usize, d);
            a[0] += 1;
            if s_rev > 0 {
                a[1] += 1;
            }
            a[2] += s_rev;
        }
        if day != cur_day {
            if cur_day != i64::MIN {
                flush_user(w, person, cur_day, u_sess, u_ev, u_rev, cat_rank);
            }
            cur_day = day;
            u_sess = 0;
            u_ev = 0;
            u_rev = 0;
        }
        if s_rev > 0 {
            for x in &ev[e..s_end] {
                if x.pc & 0x00FF_FFFF != 0 {
                    let c = x.cat();
                    if w.cat_rev[c] == 0 {
                        w.cat_touched.push(c as u16);
                    }
                    w.cat_rev[c] += x.price();
                }
            }
        }
        u_sess += 1;
        u_ev += s_events;
        u_rev += s_rev;
        e = s_end;
    }
    if cur_day != i64::MIN {
        flush_user(w, person, cur_day, u_sess, u_ev, u_rev, cat_rank);
    }
    for &a in &w.active {
        w.counts[a as usize] = 0;
        w.in_active[a as usize] = false;
    }
    w.active.clear();
}

#[allow(clippy::too_many_arguments)]
fn flush_user(w: &mut Work, person: u64, day: i64, sessions: i64, events: i64, revenue: i64, cat_rank: &[u32]) {
    let mut top: Option<(u16, i64)> = None;
    for &c in &w.cat_touched {
        let r = w.cat_rev[c as usize];
        let better = match top {
            None => true,
            Some((bc, br)) => r > br || (r == br && cat_rank[c as usize] < cat_rank[bc as usize]),
        };
        if better {
            top = Some((c, r));
        }
        w.cat_rev[c as usize] = 0;
    }
    w.cat_touched.clear();
    w.rows.push(URow {
        person,
        day: day as i32,
        sessions,
        events,
        revenue,
        top: if revenue > 0 { top.map(|(c, _)| c) } else { None },
    });
}

/// Same dialect as `rows::for_each_row`, inlined into the event and click
/// parsers. The equivalence test compares it with that reference on random
/// buffers, including quoted fields and rows that cross 64-byte blocks.
#[inline(always)]
pub fn scan_fields<'a, const N: usize>(d: &'a [u8], header: bool, mut f: impl FnMut([&'a [u8]; N])) {
    let mut start = 0usize;
    if header {
        match memchr::memchr(b'\n', d) {
            Some(i) => start = i + 1,
            None => return,
        }
    }
    let len = d.len();
    let mut fields: [&'a [u8]; N] = [&[]; N];
    let mut nf = 0usize;
    let mut fs = start;
    let mut q0 = usize::MAX;
    let mut nq = 0u32;
    let mut bad = false;
    let mut line_start = start;
    let mut blk = start;
    while blk < len {
        let (dm, qm) = field_masks(d, blk);
        let mut m = dm | qm;
        while m != 0 {
            let p = blk + m.trailing_zeros() as usize;
            m &= m - 1;
            match d[p] {
                b'"' => {
                    nq += 1;
                    if nq == 1 {
                        q0 = p;
                    }
                }
                b',' | b'\n' => {
                    let eol = d[p] == b'\n';
                    let field: &'a [u8] = if nq == 0 {
                        &d[fs..p]
                    } else if nq == 2 && q0 == fs && p >= fs + 2 && d[p - 1] == b'"' {
                        &d[fs + 1..p - 1]
                    } else {
                        bad = true;
                        &d[fs..fs]
                    };
                    if nf < N {
                        fields[nf] = field;
                    }
                    nf += 1;
                    if eol {
                        if !bad && nf == N && p > line_start {
                            f(fields);
                        }
                        nf = 0;
                        bad = false;
                        line_start = p + 1;
                    }
                    fs = p + 1;
                    nq = 0;
                }
                _ => {}
            }
        }
        blk += 64;
    }
    if line_start < len {
        let p = len;
        let field: &'a [u8] = if nq == 0 {
            &d[fs..p]
        } else if nq == 2 && q0 == fs && p >= fs + 2 && d[p - 1] == b'"' {
            &d[fs + 1..p - 1]
        } else {
            bad = true;
            &d[fs..fs]
        };
        if nf < N {
            fields[nf] = field;
        }
        nf += 1;
        if !bad && nf == N && p > line_start {
            f(fields);
        }
    }
}

#[inline(always)]
fn field_masks(d: &[u8], at: usize) -> (u64, u64) {
    if at + 64 <= d.len() {
        unsafe { masks64(d.as_ptr().add(at)) }
    } else {
        let mut buf = [0u8; 64];
        buf[..d.len() - at].copy_from_slice(&d[at..]);
        unsafe { masks64(buf.as_ptr()) }
    }
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
unsafe fn masks64(p: *const u8) -> (u64, u64) {
    use std::arch::x86_64::*;
    let comma = _mm_set1_epi8(b',' as i8);
    let nl = _mm_set1_epi8(b'\n' as i8);
    let quote = _mm_set1_epi8(b'"' as i8);
    let mut dm = 0u64;
    let mut qm = 0u64;
    for i in 0..4 {
        let v = _mm_loadu_si128(p.add(16 * i) as *const __m128i);
        let delim = _mm_or_si128(_mm_cmpeq_epi8(v, comma), _mm_cmpeq_epi8(v, nl));
        dm |= (_mm_movemask_epi8(delim) as u32 as u64) << (16 * i);
        qm |= (_mm_movemask_epi8(_mm_cmpeq_epi8(v, quote)) as u32 as u64) << (16 * i);
    }
    (dm, qm)
}

#[cfg(not(target_arch = "x86_64"))]
#[inline(always)]
unsafe fn masks64(p: *const u8) -> (u64, u64) {
    let mut dm = 0u64;
    let mut qm = 0u64;
    for i in 0..64 {
        let b = *p.add(i);
        dm |= ((b == b',' || b == b'\n') as u64) << i;
        qm |= ((b == b'"') as u64) << i;
    }
    (dm, qm)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packed_keys_roundtrip_and_sort_as_bytes() {
        let mut ids = vec![
            b"u1".to_vec(),
            b"U1".to_vec(),
            b"u10".to_vec(),
            b"u2".to_vec(),
            b"u".to_vec(),
            b"k0".to_vec(),
            b"zzzzzzzzzz".to_vec(),
            b"c_12".to_vec(),
            b"C_12".to_vec(),
            b"_x".to_vec(),
            b"0".to_vec(),
        ];
        let mut rng = 0x1111_2222_3333_4444u64;
        let alpha = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz";
        for _ in 0..200 {
            rng = rng.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1);
            let n = (rng % 10) as usize + 1;
            let mut s = vec![0u8; n];
            for b in &mut s {
                rng = rng.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1);
                *b = alpha[(rng as usize) % alpha.len()];
            }
            ids.push(s);
        }
        for s in &ids {
            let k = pack_key(s).unwrap();
            assert!(!tagged(k), "{s:?}");
            assert_eq!(decode_packed(k), *s);
        }
        let mut by_bytes = ids.clone();
        by_bytes.sort();
        let mut by_key = ids.clone();
        by_key.sort_by_key(|s| pack_key(s).unwrap());
        assert_eq!(by_bytes, by_key);
    }

    #[test]
    fn short_long_and_uppercase_order_is_bytewise() {
        let samples: &[&[u8]] = &[
            b"u2",
            b"u00000000000000000000000000000000",
            b"U0123456789ABCDEF0123456789ABCDEF",
            b"u0123456789abcdef0123456789abcdef",
            b"u0123456789abcdef0123456789abcdee",
            b"e2",
            b"k",
            b" u1",
            b"u1 ",
        ];
        for a in samples {
            for b in samples {
                let ka = content_key(a);
                let kb = content_key(b);
                let (pa, la) = if tagged(ka) { (a.as_ptr() as u64, a.len() as u32) } else { (0, 0) };
                let (pb, lb) = if tagged(kb) { (b.as_ptr() as u64, b.len() as u32) } else { (0, 0) };
                assert_eq!(cmp_content(ka, pa, la, kb, pb, lb), a.cmp(b), "{a:?} vs {b:?}");
                if a != b && ka == kb {
                    // A tagged collision still has to compare unequal via bytes.
                    assert!(tagged(ka));
                    assert_ne!(*a, *b);
                }
            }
        }
        assert!(pack_key(b"u2").is_some());
        assert!(pack_key(b"u0123456789abcdef0123456789abcdef").is_none());
        assert!(pack_key(b"U0123456789ABCDEF0123456789ABCDEF").is_none());
        assert!(pack_key(b" u1").is_none());
    }

    #[test]
    fn scan_fields_matches_reference_tokenizer() {
        let mut rng = 0x0bad_f00d_1234_5678u64;
        let mut next = || {
            rng = rng.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1);
            rng
        };
        for _ in 0..200 {
            let mut buf = Vec::new();
            let lines = (next() % 30) + 1;
            for _ in 0..lines {
                let nfields = (next() % 9) + 1;
                for f in 0..nfields {
                    if f > 0 {
                        buf.push(b',');
                    }
                    let quoted = next() % 5 == 0;
                    if quoted {
                        buf.push(b'"');
                    }
                    let n = (next() % 24) as usize;
                    for _ in 0..n {
                        let b = match next() % 16 {
                            0 => b',',
                            1 => b'"',
                            2 => b'\n',
                            _ => b'a' + (next() % 26) as u8,
                        };
                        // A raw comma/quote/newline inside a field is still
                        // just a byte; both scanners see the same buffer.
                        buf.push(b);
                    }
                    if quoted {
                        buf.push(b'"');
                    }
                }
                if next() % 7 != 0 {
                    buf.push(b'\n');
                }
            }
            for n in [2usize, 4, 7] {
                let (a, b) = match n {
                    2 => (collect::<2>(&buf), collect_ref::<2>(&buf)),
                    4 => (collect::<4>(&buf), collect_ref::<4>(&buf)),
                    _ => (collect::<7>(&buf), collect_ref::<7>(&buf)),
                };
                assert_eq!(a, b);
            }
        }
    }

    fn collect<const N: usize>(buf: &[u8]) -> Vec<Vec<Vec<u8>>> {
        let mut out = Vec::new();
        scan_fields::<N>(buf, false, |f| out.push(f.iter().map(|s| s.to_vec()).collect()));
        out
    }
    fn collect_ref<const N: usize>(buf: &[u8]) -> Vec<Vec<Vec<u8>>> {
        let mut out = Vec::new();
        rows::for_each_row::<N>(buf, false, |f| out.push(f.iter().map(|s| s.to_vec()).collect()));
        out
    }
}
