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
use crate::ts::{parse_ts, parse_ts_str, DAY};

const SESSION_GAP: i64 = 1_800_000;
const ATTRIB_WINDOW: i64 = 604_800_000;
const LATE_MS: i64 = 86_400_000;
const MAX_PRICE: u64 = 10_000_000;

const PBITS: u32 = 8;
const P: usize = 1 << PBITS;
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

/// Calls `f` on every line of the chunk (without its `\n`), skipping the header.
#[inline]
fn for_each_line<'a>(c: &Chunk<'a>, mut f: impl FnMut(&'a [u8])) {
    let d = c.data;
    let mut pos = 0;
    if c.header {
        match memchr::memchr(b'\n', d) {
            Some(i) => pos = i + 1,
            None => return,
        }
    }
    while pos < d.len() {
        let end = match memchr::memchr(b'\n', &d[pos..]) {
            Some(i) => pos + i,
            None => d.len(),
        };
        f(&d[pos..end]);
        pos = end + 1;
    }
}

/// Splits one line into exactly `N` fields under the spec's dialect.
#[inline]
fn split_line<'a, const N: usize>(line: &'a [u8], out: &mut [&'a [u8]; N]) -> bool {
    let n = line.len();
    if n == 0 {
        return false;
    }
    let mut i = 0;
    let mut f = 0;
    loop {
        if f == N {
            return false;
        }
        if i < n && line[i] == b'"' {
            let start = i + 1;
            let Some(j) = memchr::memchr(b'"', &line[start..]) else {
                return false;
            };
            let end = start + j;
            out[f] = &line[start..end];
            f += 1;
            i = end + 1;
            if i == n {
                break;
            }
            if line[i] != b',' {
                return false;
            }
        } else {
            let start = i;
            while i < n {
                let b = line[i];
                if b == b',' {
                    break;
                }
                if b == b'"' {
                    return false;
                }
                i += 1;
            }
            out[f] = &line[start..i];
            f += 1;
            if i == n {
                break;
            }
        }
        // line[i] is a comma
        i += 1;
        if i == n {
            if f == N {
                return false;
            }
            out[f] = &line[n..];
            f += 1;
            break;
        }
    }
    f == N
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

#[derive(Clone, Copy)]
struct Ev {
    eid: u64,
    ingest: i64,
    user: u64,
    ts: i64,
    price: u32,
    cat: u16,
    kind: u8,
    ok: bool,
}

#[derive(Clone, Copy)]
struct Ev2 {
    person: u64,
    ts: i64,
    price: u32,
    cat: u16,
    kind: u8,
}

#[derive(Clone, Copy)]
struct Ck {
    person: u64,
    ts: i64,
    cid: u64,
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

    let ids = Ids::default();

    // Products → category index; categories get a rank by bytes for tie-breaks.
    let mut cat_names: Vec<Vec<u8>> = Vec::new();
    let mut cat_idx: std::collections::HashMap<Vec<u8>, u16> = Default::default();
    let mut products: U64Map<u16> = U64Map::default();
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
        });
    }
    let unknown = *cat_idx.entry(b"unknown".to_vec()).or_insert_with(|| {
        cat_names.push(b"unknown".to_vec());
        (cat_names.len() - 1) as u16
    });
    let ncat = cat_names.len();
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
    let ncamp = camp_ids.len();
    let organic = ncamp as u32;
    mark(t0, &mut last, "lookups");

    // ------------------------------------------------------------ links
    let link_bufs = load_dir(&data.join("links"));
    let pairs: Vec<(u64, u64)> = chunks(&link_bufs)
        .par_iter()
        .flat_map_iter(|c| {
            let mut v = Vec::new();
            for_each_line(c, |line| {
                let mut f: [&[u8]; 2] = [&[]; 2];
                if !split_line(line, &mut f) || f[0].is_empty() || f[1].is_empty() {
                    return;
                }
                v.push((ids.key(f[0]), ids.key(f[1])));
            });
            v
        })
        .collect();
    std::mem::forget(link_bufs);
    // name_of[key] = the person's name key, for every linked ID.
    let name_of: U64Map<u64> = {
        let mut keys: Vec<u64> = pairs.iter().flat_map(|&(a, b)| [a, b]).collect();
        keys.par_sort_unstable();
        keys.dedup();
        let mut idx: U64Map<u32> = U64Map::default();
        idx.reserve(keys.len());
        for (i, &k) in keys.iter().enumerate() {
            idx.insert(k, i as u32);
        }
        let mut parent: Vec<u32> = (0..keys.len() as u32).collect();
        fn find(parent: &mut [u32], mut x: u32) -> u32 {
            while parent[x as usize] != x {
                let p = parent[x as usize];
                parent[x as usize] = parent[p as usize];
                x = p;
            }
            x
        }
        for &(a, b) in &pairs {
            let ra = find(&mut parent, idx[&a]);
            let rb = find(&mut parent, idx[&b]);
            if ra != rb {
                let (lo, hi) = if ra < rb { (ra, rb) } else { (rb, ra) };
                parent[hi as usize] = lo;
            }
        }
        // Smallest key per root.
        let mut min_key: Vec<u64> = keys.clone();
        for i in 0..keys.len() {
            let r = find(&mut parent, i as u32) as usize;
            if ids.cmp(keys[i], min_key[r]) == Ordering::Less {
                min_key[r] = keys[i];
            }
        }
        let mut m: U64Map<u64> = U64Map::default();
        m.reserve(keys.len());
        for i in 0..keys.len() {
            let r = find(&mut parent, i as u32) as usize;
            m.insert(keys[i], min_key[r]);
        }
        m
    };
    std::mem::forget(pairs);
    let person_of = |user: u64| -> u64 { name_of.get(&user).copied().unwrap_or(user) };
    mark(t0, &mut last, "links");

    // ------------------------------------------------------------ events
    let event_bufs = load_dir(&data.join("events"));
    let ev_chunks = chunks(&event_bufs);
    let ev_buckets: Vec<Vec<Vec<Ev>>> = ev_chunks
        .par_iter()
        .map(|c| {
            let cap = c.data.len() / 70 / P + 8;
            let mut b: Vec<Vec<Ev>> = (0..P).map(|_| Vec::with_capacity(cap)).collect();
            for_each_line(c, |line| {
                let mut f: [&[u8]; 7] = [&[]; 7];
                if !split_line(line, &mut f) || f[0].is_empty() {
                    return;
                }
                let Some(ingest) = parse_ts(f[3]) else {
                    return;
                };
                let eid = ids.key(f[0]);
                let user = ids.key(f[1]);
                let kind: u8 = match f[4] {
                    b"view" => 0,
                    b"cart" => 1,
                    b"purchase" => 2,
                    _ => 255,
                };
                let mut ok = user != NONE && kind != 255 && !f[5].is_empty();
                let mut ts = 0;
                if ok {
                    match parse_ts(f[2]) {
                        Some(t) if t >= win_start && t < win_end && ingest - t <= LATE_MS => {
                            ts = t
                        }
                        _ => ok = false,
                    }
                }
                let mut price = 0;
                if ok && kind == 2 {
                    match parse_price(f[6]) {
                        Some(p) => price = p,
                        None => ok = false,
                    }
                }
                let cat = if ok {
                    products.get(&ids.key(f[5])).copied().unwrap_or(unknown)
                } else {
                    0
                };
                b[part(eid)].push(Ev {
                    eid,
                    ingest,
                    user,
                    ts,
                    price,
                    cat,
                    kind,
                    ok,
                });
            });
            b
        })
        .collect();
    std::mem::forget(ev_chunks);
    mark(t0, &mut last, "parse_events");

    // Dedup per event_id partition, then scatter survivors by person.
    let ev_by_person: Vec<Vec<Vec<Ev2>>> = (0..P)
        .into_par_iter()
        .map(|p| {
            let total: usize = ev_buckets.iter().map(|b| b[p].len()).sum();
            // (eid, ingest, chunk, index)
            let mut table: HashTable<(u64, i64, u32, u32)> = HashTable::with_capacity(total);
            for (ci, b) in ev_buckets.iter().enumerate() {
                for (i, e) in b[p].iter().enumerate() {
                    let h = mix(e.eid);
                    match table.find_mut(h, |x| x.0 == e.eid) {
                        Some(x) => {
                            if e.ingest < x.1 {
                                *x = (e.eid, e.ingest, ci as u32, i as u32);
                            }
                        }
                        None => {
                            table.insert_unique(h, (e.eid, e.ingest, ci as u32, i as u32), |x| {
                                mix(x.0)
                            });
                        }
                    }
                }
            }
            let cap = total / P + 8;
            let mut outb: Vec<Vec<Ev2>> = (0..P).map(|_| Vec::with_capacity(cap)).collect();
            for &(_, _, ci, i) in table.iter() {
                let e = &ev_buckets[ci as usize][p][i as usize];
                if !e.ok {
                    continue;
                }
                let person = person_of(e.user);
                outb[part(person)].push(Ev2 {
                    person,
                    ts: e.ts,
                    price: e.price,
                    cat: e.cat,
                    kind: e.kind,
                });
            }
            outb
        })
        .collect();
    std::mem::forget(ev_buckets);
    std::mem::forget(event_bufs);
    mark(t0, &mut last, "dedup_events");

    // ------------------------------------------------------------ clicks
    let click_bufs = load_dir(&data.join("clicks"));
    let ck_buckets: Vec<Vec<Vec<Ck>>> = chunks(&click_bufs)
        .par_iter()
        .map(|c| {
            let cap = c.data.len() / 45 / P + 8;
            let mut b: Vec<Vec<Ck>> = (0..P).map(|_| Vec::with_capacity(cap)).collect();
            for_each_line(c, |line| {
                let mut f: [&[u8]; 4] = [&[]; 4];
                if !split_line(line, &mut f) || f[0].is_empty() || f[1].is_empty() {
                    return;
                }
                let Some(&camp) = campaigns.get(&ids.key(f[2])) else {
                    return;
                };
                let Some(ts) = parse_ts(f[3]) else {
                    return;
                };
                if ts < click_lo || ts >= win_end {
                    return;
                }
                let person = person_of(ids.key(f[1]));
                b[part(person)].push(Ck {
                    person,
                    ts,
                    cid: ids.key(f[0]),
                    camp,
                });
            });
            b
        })
        .collect();
    std::mem::forget(click_bufs);
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
            let total: usize = ev_by_person.iter().map(|b| b[q].len()).sum();
            let mut ev: Vec<Ev2> = Vec::with_capacity(total);
            for b in &ev_by_person {
                ev.extend_from_slice(&b[q]);
            }
            ev.sort_unstable_by_key(|e| (e.person, e.ts));
            let ctotal: usize = ck_buckets.iter().map(|b| b[q].len()).sum();
            let mut ck: Vec<Ck> = Vec::with_capacity(ctotal);
            for b in &ck_buckets {
                ck.extend_from_slice(&b[q]);
            }
            ck.sort_unstable_by(cid_cmp);
            ck.dedup_by(|a, b| a.person == b.person && a.ts == b.ts && a.cid == b.cid);
            person_pass(&ev, &ck, &mut w, day_lo, organic, &cat_rank);
            w
        })
        .reduce(new_work, |mut a, b| {
            a.rows.extend(b.rows);
            a.agg = a.agg.add(b.agg);
            a
        });
    std::mem::forget(ev_by_person);
    std::mem::forget(ck_buckets);
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

/// Sessions, credit, assists and user/day rows for one person partition.
/// `ev` is sorted by (person, ts); `ck` by (person, ts, click_id) and deduped.
fn person_pass(ev: &[Ev2], ck: &[Ck], w: &mut Work, day_lo: i64, organic: u32, cat_rank: &[u32]) {
    let mut i = 0;
    let mut k = 0;
    while i < ev.len() {
        let person = ev[i].person;
        let mut j = i + 1;
        while j < ev.len() && ev[j].person == person {
            j += 1;
        }
        while k < ck.len() && ck[k].person < person {
            k += 1;
        }
        let ka = k;
        while k < ck.len() && ck[k].person == person {
            k += 1;
        }
        let clicks = &ck[ka..k];
        let (mut lo, mut hi) = (0usize, 0usize);

        // Current user/day accumulator.
        let mut cur_day: i64 = i64::MIN;
        let (mut u_sess, mut u_ev, mut u_rev) = (0i64, 0i64, 0i64);

        let mut e = i;
        while e < j {
            // One session: [e, s_end)
            let start = ev[e].ts;
            let mut s_end = e + 1;
            let mut prev = start;
            while s_end < j && ev[s_end].ts - prev <= SESSION_GAP {
                prev = ev[s_end].ts;
                s_end += 1;
            }
            let day = start.div_euclid(DAY);
            let d = (day - day_lo) as usize;

            // Credit and assists from clicks in [start - 7d, start].
            let mut credit = organic;
            if !clicks.is_empty() {
                while hi < clicks.len() && clicks[hi].ts <= start {
                    let c = clicks[hi].camp as usize;
                    w.counts[c] += 1;
                    if !w.in_active[c] {
                        w.in_active[c] = true;
                        w.active.push(c as u32);
                    }
                    hi += 1;
                }
                let lo_ts = start - ATTRIB_WINDOW;
                while lo < hi && clicks[lo].ts < lo_ts {
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
                if x.kind == 2 {
                    s_rev += x.price as i64;
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
                    if x.kind == 2 {
                        let c = x.cat as usize;
                        if w.cat_rev[c] == 0 {
                            w.cat_touched.push(c as u16);
                        }
                        w.cat_rev[c] += x.price as i64;
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
        i = j;
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
