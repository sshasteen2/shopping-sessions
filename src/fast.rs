//! Partitioned engine.
//!
//! 1. Parse every file in parallel chunks. IDs become u64 keys (see `ids.rs`);
//!    each event is scattered by event_id into one of `P` partitions, each
//!    valid click by person.
//! 2. Per event_id partition: dedup (smallest ingest_ts), drop invalid, then
//!    scatter the survivors by person.
//! 3. Per person partition: sort, sessionize, credit and assist, and build the
//!    user_daily rows and campaign/day counters. No shared state.
//! 4. Write user_daily as several Parquet files in parallel.

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
use crate::ids::{Ids, NONE};
use crate::pq;
use crate::rows;
use crate::ts::{parse_ts_cached, parse_ts_str, DateCache, DAY};

const SESSION_GAP: i64 = 1_800_000;
const ATTRIB_WINDOW: i64 = 604_800_000;
const LATE_MS: i64 = 86_400_000;
const MAX_PRICE: u64 = 10_000_000;

const PBITS: u32 = 8;
const P: usize = 1 << PBITS;
/// Each event_id partition splits into SUB sub-partitions, so a dedup table
/// (a few thousand entries) stays in L2.
const SUB_BITS: u32 = 4;
const SUB: usize = 1 << SUB_BITS;
const CHUNK: usize = 4 << 20;

#[inline]
fn mix(x: u64) -> u64 {
    let mut z = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

#[inline]
fn part(x: u64) -> usize {
    (mix(x) >> (64 - PBITS)) as usize
}

/// Small open-addressing map from an ID key to a value, hashed with one
/// multiply; key 0 (the empty ID) marks a free slot and is never stored.
struct KeyMap<V: Copy> {
    shift: u32,
    keys: Vec<u64>,
    vals: Vec<V>,
}

impl<V: Copy + Default> KeyMap<V> {
    fn new(n: usize) -> Self {
        let cap = (n * 4).next_power_of_two().max(16);
        Self {
            shift: 64 - cap.trailing_zeros(),
            keys: vec![0; cap],
            vals: vec![V::default(); cap],
        }
    }
    #[inline]
    fn home(&self, k: u64) -> usize {
        (k.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> self.shift) as usize
    }
    /// Inserts or overwrites; returns false when the key was already present.
    fn insert(&mut self, k: u64, v: V) -> bool {
        debug_assert!(k != 0);
        let mask = self.keys.len() - 1;
        let mut i = self.home(k);
        loop {
            if self.keys[i] == k {
                self.vals[i] = v;
                return false;
            }
            if self.keys[i] == 0 {
                self.keys[i] = k;
                self.vals[i] = v;
                return true;
            }
            i = (i + 1) & mask;
        }
    }
    #[inline]
    fn get(&self, k: u64) -> Option<V> {
        if k == 0 {
            return None;
        }
        let mask = self.keys.len() - 1;
        let mut i = self.home(k);
        loop {
            let sk = self.keys[i];
            if sk == k {
                return Some(self.vals[i]);
            }
            if sk == 0 {
                return None;
            }
            i = (i + 1) & mask;
        }
    }
}

/// ID -> small value lookup with a cache-sized fast path. IDs of the form
/// `<prefix><number>` (the prefix of the first ID, a canonical number: no
/// leading zeros, below DENSE_MAX) index a dense array; every other ID goes
/// through a `KeyMap`. Exact: the number form is only used when the string
/// is canonical, so `p0123` never matches `p123`.
struct IdLookup {
    prefix: Vec<u8>,
    dense: Vec<u16>, // 0 = absent, else value + 1
    rest: KeyMap<u16>,
}

const DENSE_MAX: u64 = 1 << 22;

impl IdLookup {
    /// The canonical number after `prefix`, if `s` has that form.
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

    fn build(items: &[(Vec<u8>, u16)], ids: &Ids) -> Self {
        let prefix: Vec<u8> = items
            .first()
            .map(|(id, _)| id.iter().take_while(|b| !b.is_ascii_digit()).copied().collect())
            .unwrap_or_default();
        let mut dense: Vec<u16> = Vec::new();
        let mut rest = KeyMap::new(items.len());
        for (id, v) in items {
            match Self::number(&prefix, id) {
                Some(num) if num < DENSE_MAX => {
                    let i = num as usize;
                    if dense.len() <= i {
                        dense.resize(i + 1, 0);
                    }
                    // Later rows win, as with a map insert.
                    dense[i] = v + 1;
                }
                _ => {
                    rest.insert(ids.key(id), *v);
                }
            }
        }
        Self { prefix, dense, rest }
    }

    #[inline]
    fn get(&self, s: &[u8], ids: &Ids) -> Option<u16> {
        if let Some(num) = Self::number(&self.prefix, s) {
            if num < DENSE_MAX {
                let i = num as usize;
                return match self.dense.get(i) {
                    Some(&v) if v != 0 => Some(v - 1),
                    _ => None,
                };
            }
        }
        self.rest.get(ids.key(s))
    }
}

/// Records grouped by partition: partition `p` is `data[off[p]..off[p + 1]]`.
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

/// Stable counting sort of `v` into `P` partitions. Writing a few hundred
/// thousand records into one cache-sized array is far cheaper than pushing
/// each into one of `P` separate growing vectors.
fn partition<T: Copy>(v: &[T], key: impl Fn(&T) -> usize) -> Parts<T> {
    partition_n(v, P, key)
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
    // SAFETY: every one of the v.len() slots was written exactly once.
    unsafe { data.set_len(v.len()) };
    Parts { data, off }
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

// ---------------------------------------------------------------- input files

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

/// A run of whole lines in one file; `header` when it starts the file.
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
            out.push(Chunk {
                data: &b[lo..hi],
                header: lo == 0,
            });
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
            // keep scanning for non-digits is unnecessary: invalid either way
            return None;
        }
    }
    if n < 1 {
        None
    } else {
        Some(n as u32)
    }
}

// ---------------------------------------------------------------- records

/// A parsed event, 32 bytes. `ing_cat` holds ingest_ts (ms from window start,
/// clamped to 48 bits) above a 16-bit category; `pk` holds the price in its
/// low 24 bits, the kind in bits 24-25 and "valid" in bit 31.
#[derive(Clone, Copy)]
struct Ev {
    eid: u64,
    person: u64,
    ing_cat: u64,
    ts: u32,
    pk: u32,
}

impl Ev {
    #[inline]
    fn ingest(&self) -> i64 {
        (self.ing_cat as i64) >> 16
    }
}

/// Open-addressing map from each linked ID to its person's name; key 0 marks
/// an empty slot (no ID has key 0). An unlinked ID is its own person. Slots are
/// `[key, value]` pairs in one u64 array; inserts claim a key slot with a
/// compare-and-swap, so the table fills in parallel. Reads only start once all
/// inserts are done.
struct NameTable {
    mask: usize,
    slots: Vec<u64>,
    /// One bit per hashed key: clear means the key is certainly not stored,
    /// so most lookups (IDs that are their own person's name, or unlinked)
    /// never touch the big table.
    filter: Vec<u64>,
    fshift: u32,
}

impl NameTable {
    fn new(n: usize) -> Self {
        let cap = (n * 2).next_power_of_two().max(16);
        let fbits = (n * 16).next_power_of_two().max(64);
        Self {
            mask: cap - 1,
            slots: vec![0; cap * 2],
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
    fn prefetch_filter(&self, k: u64) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
            _mm_prefetch(self.filter.as_ptr().add(self.fbit(k) >> 6) as *const i8, _MM_HINT_T0);
        }
        #[cfg(not(target_arch = "x86_64"))]
        let _ = k;
    }
    #[inline]
    fn home(&self, k: u64) -> usize {
        (mix(k) as usize) & self.mask
    }
    fn atomics(&self) -> &[std::sync::atomic::AtomicU64] {
        // SAFETY: AtomicU64 has the same size and alignment as u64.
        unsafe { &*(self.slots.as_slice() as *const [u64] as *const [std::sync::atomic::AtomicU64]) }
    }
    fn insert(&self, k: u64, v: u64) {
        use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
        let b = self.fbit(k);
        // SAFETY: AtomicU64 has the same size and alignment as u64.
        let fw = unsafe { &*(self.filter.as_ptr().add(b >> 6) as *const AtomicU64) };
        fw.fetch_or(1 << (b & 63), Relaxed);
        let a = self.atomics();
        let mut i = self.home(k);
        loop {
            match a[2 * i].compare_exchange(0, k, Relaxed, Relaxed) {
                Ok(_) => {
                    a[2 * i + 1].store(v, Relaxed);
                    return;
                }
                Err(cur) if cur == k => {
                    a[2 * i + 1].store(v, Relaxed);
                    return;
                }
                Err(_) => i = (i + 1) & self.mask,
            }
        }
    }
    #[inline]
    fn prefetch(&self, k: u64) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
            _mm_prefetch(self.slots.as_ptr().add(2 * self.home(k)) as *const i8, _MM_HINT_T0);
        }
        #[cfg(not(target_arch = "x86_64"))]
        let _ = k;
    }

    /// `resolve` for parsed events, skipping invalid ones.
    fn resolve_valid(&self, items: &mut [Ev]) {
        const AHEAD: usize = 16;
        for i in 0..items.len() {
            if i + 2 * AHEAD < items.len() {
                self.prefetch_filter(items[i + 2 * AHEAD].person);
            }
            if i + AHEAD < items.len() && items[i + AHEAD].pk >> 31 != 0 && self.maybe(items[i + AHEAD].person) {
                self.prefetch(items[i + AHEAD].person);
            }
            if items[i].pk >> 31 != 0 {
                items[i].person = self.get(items[i].person);
            }
        }
    }

    /// Replaces each user key (via `key`) with its person's name, keeping many
    /// table reads in flight by prefetching `AHEAD` items in advance.
    fn resolve<T>(&self, items: &mut [T], key: impl Fn(&mut T) -> &mut u64) {
        const AHEAD: usize = 16;
        for i in 0..items.len() {
            if i + 2 * AHEAD < items.len() {
                let k = *key(&mut items[i + 2 * AHEAD]);
                self.prefetch_filter(k);
            }
            if i + AHEAD < items.len() {
                let k = *key(&mut items[i + AHEAD]);
                if self.maybe(k) {
                    self.prefetch(k);
                }
            }
            let k = key(&mut items[i]);
            *k = self.get(*k);
        }
    }

    #[inline]
    fn get(&self, k: u64) -> u64 {
        if !self.maybe(k) {
            return k;
        }
        let mut i = self.home(k);
        loop {
            let (sk, sv) = (self.slots[2 * i], self.slots[2 * i + 1]);
            if sk == k {
                return sv;
            }
            if sk == 0 {
                return k;
            }
            i = (i + 1) & self.mask;
        }
    }
}

/// A surviving event, 16 bytes. `ts` is ms from `click_lo` (the start of the
/// click window, 7 days before the event window); `pc` holds the price in its
/// low 24 bits (0 unless a purchase) and the category in the top 8.
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

/// A valid click, 24 bytes; `ts` is ms from `click_lo`.
#[derive(Clone, Copy)]
struct Ck {
    person: u64,
    cid: u64,
    ts: u32,
    camp: u32,
}

struct URow {
    person: u64,
    day: i32,
    sessions: i64,
    events: i64,
    revenue: i64,
    top: Option<u16>,
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

/// Campaign/day counters: sessions, purchasing, revenue, assisted.
struct CampAgg {
    nd: usize,
    v: Vec<[i64; 4]>,
}

impl CampAgg {
    fn new(ncamp: usize, nd: usize) -> Self {
        Self {
            nd,
            v: vec![[0; 4]; (ncamp + 1) * nd],
        }
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

/// Per-thread scratch for the person pass.
struct Work {
    rows: Vec<URow>,
    agg: CampAgg,
    counts: Vec<u32>,
    in_active: Vec<bool>,
    active: Vec<u32>,
    cat_rev: Vec<i64>,
    cat_touched: Vec<u16>,
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
    eprintln!("fast engine: ncpu={n}");
    if win_end - click_lo > u32::MAX as i64 {
        // Times are stored as u32 offsets from the click window's start.
        return crate::engine::run();
    }

    let ids = Ids::default();

    // Products → category index; categories get a rank by bytes for tie-breaks.
    let mut cat_names: Vec<Vec<u8>> = Vec::new();
    let mut cat_idx: std::collections::HashMap<Vec<u8>, u16> = Default::default();
    let mut products: U64Map<u16> = U64Map::default();
    let mut product_items: Vec<(Vec<u8>, u16)> = Vec::new();
    {
        let bytes = fs::read(data.join("products.csv")).unwrap_or_default();
        for_each_record(&bytes, 2, |row| {
            if row[0].is_empty() {
                return;
            }
            let c = *cat_idx.entry(row[1].to_vec()).or_insert_with(|| {
                cat_names.push(row[1].to_vec());
                (cat_names.len() - 1) as u16
            });
            products.insert(ids.key(row[0]), c);
            product_items.push((row[0].to_vec(), c));
        });
    }
    let unknown = *cat_idx.entry(b"unknown".to_vec()).or_insert_with(|| {
        cat_names.push(b"unknown".to_vec());
        (cat_names.len() - 1) as u16
    });
    let products: IdLookup = IdLookup::build(&product_items, &ids);
    let ncat = cat_names.len();
    if ncat > 256 {
        // Categories are stored in 8 bits.
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

    // Campaigns → index; `organic` is index ncamp.
    let mut camp_ids: Vec<Vec<u8>> = Vec::new();
    let mut camp_channels: Vec<Vec<u8>> = Vec::new();
    let mut campaigns: U64Map<u32> = U64Map::default();
    {
        let bytes = fs::read(data.join("campaigns.csv")).unwrap_or_default();
        for_each_record(&bytes, 2, |row| {
            if row[0].is_empty() {
                return;
            }
            let k = ids.key(row[0]);
            if campaigns.contains_key(&k) {
                return;
            }
            campaigns.insert(k, camp_ids.len() as u32);
            camp_ids.push(row[0].to_vec());
            camp_channels.push(row[1].to_vec());
        });
    }
    let campaigns: IdLookup = {
        let items: Vec<(Vec<u8>, u16)> =
            camp_ids.iter().enumerate().map(|(i, id)| (id.clone(), i as u16)).collect();
        IdLookup::build(&items, &ids)
    };
    let ncamp = camp_ids.len();
    if ncamp > u16::MAX as usize {
        // Campaign indexes are stored in 16 bits.
        return crate::engine::run();
    }
    let organic = ncamp as u32;
    mark(t0, &mut last, "lookups");

    // ------------------------------------------------------------ links
    let link_bufs = load_dir(&data.join("links"));
    let pairs: Vec<(u64, u64)> = chunks(&link_bufs)
        .par_iter()
        .flat_map_iter(|c| {
            let mut v = Vec::new();
            rows::for_each_row::<2>(c.data, c.header, |f| {
                if f[0].is_empty() || f[1].is_empty() {
                    return;
                }
                v.push((ids.key(f[0]), ids.key(f[1])));
            });
            v
        })
        .collect();
    std::mem::forget(link_bufs);
    // names: every linked ID -> its person's name key.
    let names: NameTable = {
        // Dense IDs by sorting the link endpoints (no hash map).
        let mut ends: Vec<(u64, u32)> = pairs
            .iter()
            .enumerate()
            .flat_map(|(i, &(a, b))| [(a, 2 * i as u32), (b, 2 * i as u32 + 1)])
            .collect();
        ends.par_sort_unstable_by_key(|e| e.0);
        let mut keys: Vec<u64> = Vec::new();
        let mut pid = vec![0u32; ends.len()];
        for &(k, pos) in &ends {
            if keys.last() != Some(&k) {
                keys.push(k);
            }
            pid[pos as usize] = (keys.len() - 1) as u32;
        }
        drop(ends);
        let mut parent: Vec<u32> = (0..keys.len() as u32).collect();
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
        // Flatten so every entry points at its root, then the smallest key per root.
        for i in 0..keys.len() {
            let r = find(&mut parent, i as u32);
            parent[i] = r;
        }
        let mut min_key: Vec<u64> = keys.clone();
        for i in 0..keys.len() {
            let r = parent[i] as usize;
            if ids.cmp(keys[i], min_key[r]) == Ordering::Less {
                min_key[r] = keys[i];
            }
        }
        let remap = keys.iter().zip(&parent).filter(|&(&k, &r)| min_key[r as usize] != k).count();
        let t = NameTable::new(remap);
        keys.par_iter().zip(parent.par_iter()).for_each(|(&k, &r)| {
            let name = min_key[r as usize];
            if name != k {
                t.insert(k, name);
            }
        });
        t
    };
    std::mem::forget(pairs);
    mark(t0, &mut last, "links");

    // ------------------------------------------------------------ events
    let event_bufs = load_dir(&data.join("events"));
    let ev_chunks = chunks(&event_bufs);
    let ev_buckets: Vec<Parts<Ev>> = ev_chunks
        .par_iter()
        .map(|c| {
            let mut b: Vec<Ev> = Vec::with_capacity(c.data.len() / 70 + 16);
            let mut dc = DateCache::default();
            rows::for_each_row::<7>(c.data, c.header, |f| {
                if f[0].is_empty() {
                    return;
                }
                let Some(ingest) = parse_ts_cached(f[3], &mut dc) else {
                    return;
                };
                let user = ids.key(f[1]);
                let eid = ids.key(f[0]);
                let kind: u32 = match f[4] {
                    b"view" => 0,
                    b"cart" => 1,
                    b"purchase" => 2,
                    _ => 3,
                };
                let mut ok = user != NONE && kind != 3 && !f[5].is_empty();
                let mut ts = 0u32;
                if ok {
                    match parse_ts_cached(f[2], &mut dc) {
                        Some(t) if t >= win_start && t < win_end && ingest - t <= LATE_MS => {
                            ts = (t - win_start) as u32
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
                let cat = if ok {
                    products.get(f[5], &ids).unwrap_or(unknown)
                } else {
                    0
                };
                let ing = (ingest - win_start).clamp(-(1i64 << 47), (1i64 << 47) - 1);
                b.push(Ev {
                    eid,
                    person: user,
                    ing_cat: ((ing as u64) << 16) | cat as u64,
                    ts,
                    pk: price | (kind << 24) | ((ok as u32) << 31),
                });
            });
            names.resolve_valid(&mut b);
            partition_n(&b, P * SUB, |e| (mix(e.eid) >> (64 - PBITS - SUB_BITS)) as usize)
        })
        .collect();
    std::mem::forget(ev_chunks);
    mark(t0, &mut last, "parse_events");

    // Dedup per event_id partition, then scatter survivors by person. The table
    // hashes with a different seed than `part`, whose top bits are constant here.
    const DEDUP_SEED: u64 = 0xA5A5_5A5A_DEAD_BEEF;
    let ev_by_person: Vec<Parts<Ev2>> = (0..P)
        .into_par_iter()
        .map(|p| {
            let total: usize = (0..SUB)
                .map(|s| ev_buckets.iter().map(|b| b.slice(p * SUB + s).len()).sum::<usize>())
                .sum();
            let mut kept: Vec<Ev2> = Vec::with_capacity(total);
            let mut table: HashTable<Ev> = HashTable::with_capacity(total / SUB * 2 + 64);
            for sp in p * SUB..(p + 1) * SUB {
                table.clear();
                for b in &ev_buckets {
                    for e in b.slice(sp) {
                        let h = mix(e.eid ^ DEDUP_SEED);
                        match table.find_mut(h, |x| x.eid == e.eid) {
                            Some(x) => {
                                if e.ingest() < x.ingest() {
                                    *x = *e;
                                }
                            }
                            None => {
                                table.insert_unique(h, *e, |x| mix(x.eid ^ DEDUP_SEED));
                            }
                        }
                    }
                }
                kept.extend(table.iter().filter(|e| e.pk >> 31 != 0).map(|e| Ev2 {
                    person: e.person,
                    ts: e.ts + (win_start - click_lo) as u32,
                    pc: (e.pk & 0x00FF_FFFF) | (((e.ing_cat & 0xFF) as u32) << 24),
                }));
            }
            drop(table);
            partition(&kept, |e| part(e.person))
        })
        .collect();
    rayon::spawn(move || {
        ev_buckets.into_par_iter().for_each(drop);
        drop(event_bufs);
    });
    mark(t0, &mut last, "dedup_events");

    // ------------------------------------------------------------ clicks
    let click_bufs = load_dir(&data.join("clicks"));
    let ck_buckets: Vec<Parts<Ck>> = chunks(&click_bufs)
        .par_iter()
        .map(|c| {
            let mut v: Vec<Ck> = Vec::with_capacity(c.data.len() / 45 + 8);
            let mut dc = DateCache::default();
            rows::for_each_row::<4>(c.data, c.header, |f| {
                if f[0].is_empty() || f[1].is_empty() {
                    return;
                }
                let user = ids.key(f[1]);
                let Some(camp) = campaigns.get(f[2], &ids) else {
                    return;
                };
                let camp = camp as u32;
                let Some(ts) = parse_ts_cached(f[3], &mut dc) else {
                    return;
                };
                if ts < click_lo || ts >= win_end {
                    return;
                }
                v.push(Ck {
                    person: user,
                    cid: ids.key(f[0]),
                    ts: (ts - click_lo) as u32,
                    camp,
                });
            });
            names.resolve(&mut v, |c| &mut c.person);
            partition(&v, |c| part(c.person))
        })
        .collect();
    rayon::spawn(move || drop(click_bufs));
    mark(t0, &mut last, "parse_clicks");

    // ------------------------------------------------------------ per person
    let cid_cmp = |a: &Ck, b: &Ck| -> Ordering {
        a.person
            .cmp(&b.person)
            .then(a.ts.cmp(&b.ts))
            .then_with(|| ids.cmp(a.cid, b.cid))
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
            // Local person ids in first-seen order; the hash seed differs from
            // `part`, whose top bits are the same for everyone in this partition.
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
            // Counting sort of events by person.
            let mut off = vec![0u32; np + 1];
            for i in 0..np {
                off[i + 1] = off[i] + counts[i];
            }
            let mut pos: Vec<u32> = off[..np].to_vec();
            let mut ev: Vec<Ev2> = Vec::with_capacity(total);
            {
                let dst = ev.spare_capacity_mut();
                let mut n = 0;
                for v in &ev_parts {
                    for e in v.iter() {
                        let l = lids[n] as usize;
                        dst[pos[l] as usize].write(*e);
                        pos[l] += 1;
                        n += 1;
                    }
                }
            }
            // SAFETY: the counting sort wrote every one of the `total` slots.
            unsafe { ev.set_len(total) };
            drop(ev_parts);
            drop(lids);

            // Clicks of people with events, grouped the same way.
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
            // SAFETY: every slot was written above.
            unsafe { ck.set_len(cl.len()) };
            drop(cl);

            for r in 0..np {
                let evs = &mut ev[off[r] as usize..off[r + 1] as usize];
                evs.sort_unstable_by_key(|e| e.ts);
                let cks = &mut ck[coff[r] as usize..coff[r + 1] as usize];
                let mut nck = cks.len();
                if nck > 1 {
                    cks.sort_unstable_by(cid_cmp);
                    // Dedup identical copies in place.
                    let mut wr = 1;
                    for rd in 1..nck {
                        if cks[rd].ts != cks[wr - 1].ts || cks[rd].cid != cks[wr - 1].cid {
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
    rayon::spawn(move || {
        drop(ev_by_person);
        drop(ck_buckets);
    });
    mark(t0, &mut last, "sessions");

    // ------------------------------------------------------------ output
    let rows = work.rows;
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
            u_user.push(ByteArray::from(ids.to_vec(r.person)));
            u_day.push(r.day);
            u_sess.push(r.sessions);
            u_ev.push(r.events);
            u_rev.push(r.revenue);
            u_cat.push(r.top.map(|c| ByteArray::from(cat_names[c as usize].clone())));
        }
        pq::write_user_daily(
            &udir.join(format!("part-{i}.parquet")),
            u_user,
            u_day,
            u_sess,
            u_ev,
            u_rev,
            u_cat,
        );
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
    // Every output file is closed; skip freeing gigabytes of buffers on the way out.
    std::process::exit(0);
}

/// Sessions, credit, assists and user/day rows for one person.
/// `ev` is sorted by ts; `clicks` by (ts, click_id) and deduped.
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
    {
        let (i, j) = (0usize, ev.len());
        let (mut lo, mut hi) = (0usize, 0usize);

        // Current user/day accumulator.
        let mut cur_day: i64 = i64::MIN;
        let (mut u_sess, mut u_ev, mut u_rev) = (0i64, 0i64, 0i64);

        let mut e = i;
        while e < j {
            // One session: [e, s_end)
            let start = ev[e].ts as i64;
            let mut s_end = e + 1;
            let mut prev = start;
            while s_end < j && ev[s_end].ts as i64 - prev <= SESSION_GAP {
                prev = ev[s_end].ts as i64;
                s_end += 1;
            }
            let day = (base + start).div_euclid(DAY);
            let d = (day - day_lo) as usize;

            // Credit and assists from clicks in [start - 7d, start].
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

            // Session totals.
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
        // Reset click scratch for the next person.
        for &a in &w.active {
            w.counts[a as usize] = 0;
            w.in_active[a as usize] = false;
        }
        w.active.clear();
    }
}

/// Emits one user/day row and clears the day's category revenue.
#[allow(clippy::too_many_arguments)]
fn flush_user(
    w: &mut Work,
    person: u64,
    day: i64,
    sessions: i64,
    events: i64,
    revenue: i64,
    cat_rank: &[u32],
) {
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
