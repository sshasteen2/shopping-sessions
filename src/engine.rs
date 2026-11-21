use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use ahash::AHashMap;
use memmap2::Mmap;
use parquet::data_type::ByteArray;
use rayon::prelude::*;

use crate::csv::for_each_record;
use crate::intern::ShardedIntern;
use crate::pq;
use crate::ts::{parse_ts, parse_ts_str, DAY};
use crate::uf::People;

const SESSION_GAP: i64 = 1_800_000;
const ATTRIB_WINDOW: i64 = 604_800_000;
const LATE_MS: i64 = 86_400_000;
const MAX_PRICE: i64 = 10_000_000;
const EMPTY: u32 = u32::MAX;

#[derive(Clone, Copy)]
struct SliceRef {
    file: u32,
    off: u32,
    len: u32,
}

struct FileBuf {
    map: Option<Mmap>,
    owned: Vec<u8>,
}

impl FileBuf {
    fn load(p: &Path) -> Self {
        if let Ok(f) = File::open(p) {
            if f.metadata().map(|m| m.len()).unwrap_or(0) > 0 {
                if let Ok(m) = unsafe { Mmap::map(&f) } {
                    return Self {
                        map: Some(m),
                        owned: Vec::new(),
                    };
                }
            }
        }
        Self {
            map: None,
            owned: fs::read(p).unwrap_or_default(),
        }
    }

    fn as_bytes(&self) -> &[u8] {
        match &self.map {
            Some(m) => m.as_ref(),
            None => &self.owned,
        }
    }
}

fn slice_at(bufs: &[FileBuf], k: SliceRef) -> &[u8] {
    let b = bufs[k.file as usize].as_bytes();
    &b[k.off as usize..k.off as usize + k.len as usize]
}

fn field_ref(fi: u32, bytes: &[u8], field: &[u8]) -> SliceRef {
    SliceRef {
        file: fi,
        off: (field.as_ptr() as usize).wrapping_sub(bytes.as_ptr() as usize) as u32,
        len: field.len() as u32,
    }
}

#[derive(Clone, Copy)]
struct EventRec {
    hash: u64,
    eid: SliceRef,
    user: u32,
    ts: i64,
    ingest: i64,
    kind: u8,
    product: u32,
    price: i64,
}

#[derive(Clone, Copy)]
struct ClickRec {
    hash: u64,
    cid: SliceRef,
    user: u32,
    campaign: u32,
    ts: i64,
}

struct Session {
    person: u32,
    start: i64,
    day: i32,
    events: i64,
    revenue: i64,
    cat_rev: Vec<(u32, i64)>,
    credit: Option<u32>,
    assists: Vec<u32>,
}

fn ncpu() -> usize {
    std::env::var("NCPU")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n: &usize| n > 0)
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        })
}

fn data_dir() -> PathBuf {
    PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into()))
}

fn out_dir() -> PathBuf {
    PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into()))
}

fn csv_files(dir: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let Ok(rd) = fs::read_dir(dir) else {
        return files;
    };
    for ent in rd.flatten() {
        let p = ent.path();
        if p.extension().and_then(|e| e.to_str()) == Some("csv") {
            files.push(p);
        }
    }
    files.sort();
    files
}

fn parse_kind(s: &[u8]) -> Option<u8> {
    match s {
        b"view" => Some(0),
        b"cart" => Some(1),
        b"purchase" => Some(2),
        _ => None,
    }
}

fn parse_price(s: &[u8]) -> Option<i64> {
    if s.is_empty() || !s.iter().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let mut n: i64 = 0;
    for &b in s {
        n = n.saturating_mul(10).saturating_add((b - b'0') as i64);
        if n > MAX_PRICE {
            return None;
        }
    }
    if n < 1 {
        None
    } else {
        Some(n)
    }
}

fn event_valid(e: &EventRec, win_lo: i64, win_hi: i64) -> bool {
    if e.user == EMPTY || e.product == EMPTY {
        return false;
    }
    if e.ts == i64::MIN || e.ts < win_lo || e.ts >= win_hi {
        return false;
    }
    if e.kind > 2 {
        return false;
    }
    if e.kind == 2 && (e.price < 1 || e.price > MAX_PRICE) {
        return false;
    }
    if e.ingest.saturating_sub(e.ts) > LATE_MS {
        return false;
    }
    true
}

fn add_cat(v: &mut Vec<(u32, i64)>, cat: u32, rev: i64) {
    if let Some(e) = v.iter_mut().find(|e| e.0 == cat) {
        e.1 += rev;
    } else {
        v.push((cat, rev));
    }
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

pub fn run() {
    let t0 = Instant::now();
    let mut last = t0;
    let n = ncpu();
    rayon::ThreadPoolBuilder::new()
        .num_threads(n)
        .build_global()
        .ok();
    let data = data_dir();
    let out = out_dir();

    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(data.join("manifest.json")).unwrap()).expect("manifest");
    let win_start = parse_ts_str(manifest["window_start"].as_str().unwrap()).unwrap();
    let win_end = parse_ts_str(manifest["window_end"].as_str().unwrap()).unwrap();
    let click_lo = win_start - 7 * DAY;
    eprintln!(
        "window: {} to {} (ncpu={})",
        manifest["window_start"], manifest["window_end"], n
    );

    let nshards = n.max(8);
    let strs = Arc::new(ShardedIntern::new(nshards));
    let unknown = strs.get(b"unknown");
    let organic = strs.get(b"organic");

    let mut products: AHashMap<u32, u32> = AHashMap::new();
    {
        let bytes = fs::read(data.join("products.csv")).unwrap_or_default();
        for_each_record(&bytes, 2, |row| {
            if row[0].is_empty() {
                return;
            }
            products.insert(strs.get(row[0]), strs.get(row[1]));
        });
    }
    let mut campaigns: AHashMap<u32, u32> = AHashMap::new();
    {
        let bytes = fs::read(data.join("campaigns.csv")).unwrap_or_default();
        for_each_record(&bytes, 2, |row| {
            if row[0].is_empty() {
                return;
            }
            campaigns.insert(strs.get(row[0]), strs.get(row[1]));
        });
    }
    mark(t0, &mut last, "lookups");

    let people = People::new(nshards);
    let mut pairs: Vec<(u32, u32)> = Vec::new();
    for path in csv_files(&data.join("links")) {
        let bytes = fs::read(path).unwrap_or_default();
        for_each_record(&bytes, 2, |row| {
            if row[0].is_empty() || row[1].is_empty() {
                return;
            }
            pairs.push((people.intern(row[0]), people.intern(row[1])));
        });
    }
    mark(t0, &mut last, "links");

    let event_paths = csv_files(&data.join("events"));
    let event_bufs = Arc::new(
        event_paths
            .par_iter()
            .map(|p| FileBuf::load(p))
            .collect::<Vec<_>>(),
    );
    let mut events: Vec<EventRec> = (0..event_bufs.len())
        .into_par_iter()
        .flat_map(|fi| {
            let bytes = event_bufs[fi].as_bytes();
            let mut local = Vec::new();
            for_each_record(bytes, 7, |row| {
                if row[0].is_empty() {
                    return;
                }
                let Some(ingest) = parse_ts(row[3]) else {
                    return;
                };
                local.push(EventRec {
                    hash: strs.hash(row[0]),
                    eid: field_ref(fi as u32, bytes, row[0]),
                    user: if row[1].is_empty() {
                        EMPTY
                    } else {
                        people.intern(row[1])
                    },
                    ts: parse_ts(row[2]).unwrap_or(i64::MIN),
                    ingest,
                    kind: parse_kind(row[4]).unwrap_or(255),
                    product: if row[5].is_empty() {
                        EMPTY
                    } else {
                        strs.get(row[5])
                    },
                    price: parse_price(row[6]).unwrap_or(0),
                });
            });
            local
        })
        .collect();
    mark(t0, &mut last, "parse_events");

    events.par_sort_unstable_by(|a, b| {
        a.hash
            .cmp(&b.hash)
            .then_with(|| slice_at(&event_bufs, a.eid).cmp(slice_at(&event_bufs, b.eid)))
    });
    {
        let mut w = 0usize;
        for r in 0..events.len() {
            let same = w > 0
                && events[r].hash == events[w - 1].hash
                && slice_at(&event_bufs, events[r].eid) == slice_at(&event_bufs, events[w - 1].eid);
            if !same {
                if w != r {
                    events[w] = events[r];
                }
                w += 1;
            } else if events[r].ingest < events[w - 1].ingest {
                events[w - 1] = events[r];
            }
        }
        events.truncate(w);
    }
    events.retain(|e| event_valid(e, win_start, win_end));
    drop(event_bufs);
    mark(t0, &mut last, "dedup_events");

    let click_paths = csv_files(&data.join("clicks"));
    let click_bufs = Arc::new(
        click_paths
            .par_iter()
            .map(|p| FileBuf::load(p))
            .collect::<Vec<_>>(),
    );
    let mut clicks: Vec<ClickRec> = (0..click_bufs.len())
        .into_par_iter()
        .flat_map(|fi| {
            let bytes = click_bufs[fi].as_bytes();
            let mut local = Vec::new();
            for_each_record(bytes, 4, |row| {
                if row[0].is_empty() {
                    return;
                }
                local.push(ClickRec {
                    hash: strs.hash(row[0]),
                    cid: field_ref(fi as u32, bytes, row[0]),
                    user: if row[1].is_empty() {
                        EMPTY
                    } else {
                        people.intern(row[1])
                    },
                    campaign: strs.get(row[2]),
                    ts: parse_ts(row[3]).unwrap_or(i64::MIN),
                });
            });
            local
        })
        .collect();
    mark(t0, &mut last, "parse_clicks");

    clicks.par_sort_unstable_by(|a, b| {
        a.hash
            .cmp(&b.hash)
            .then_with(|| slice_at(&click_bufs, a.cid).cmp(slice_at(&click_bufs, b.cid)))
    });
    {
        let mut w = 0usize;
        for r in 0..clicks.len() {
            let same = w > 0
                && clicks[r].hash == clicks[w - 1].hash
                && slice_at(&click_bufs, clicks[r].cid) == slice_at(&click_bufs, clicks[w - 1].cid);
            if !same {
                if w != r {
                    clicks[w] = clicks[r];
                }
                w += 1;
            }
        }
        clicks.truncate(w);
    }
    clicks.retain(|c| {
        c.user != EMPTY
            && c.ts != i64::MIN
            && c.ts >= click_lo
            && c.ts < win_end
            && campaigns.contains_key(&c.campaign)
    });
    mark(t0, &mut last, "dedup_clicks");

    let mut people = people.freeze();
    for (a, b) in pairs {
        people.union(a, b);
    }
    let strs = Arc::try_unwrap(strs).unwrap_or_else(|_| panic!("strs unique"));
    let strs = strs.freeze();

    struct E2 {
        person: u32,
        ts: i64,
        kind: u8,
        category: u32,
        price: i64,
    }
    let mut e2: Vec<E2> = events
        .iter()
        .map(|e| E2 {
            person: people.root(e.user),
            ts: e.ts,
            kind: e.kind,
            category: products.get(&e.product).copied().unwrap_or(unknown),
            price: if e.kind == 2 { e.price } else { 0 },
        })
        .collect();
    events.clear();
    e2.par_sort_unstable_by_key(|e| (e.person, e.ts));
    mark(t0, &mut last, "sort_events");

    let mut sessions: Vec<Session> = Vec::new();
    {
        let mut i = 0;
        while i < e2.len() {
            let person = e2[i].person;
            let mut j = i + 1;
            while j < e2.len() && e2[j].person == person {
                j += 1;
            }
            let mut start = e2[i].ts;
            let mut prev = e2[i].ts;
            let mut s = Session {
                person,
                start,
                day: start.div_euclid(DAY) as i32,
                events: 0,
                revenue: 0,
                cat_rev: Vec::new(),
                credit: None,
                assists: Vec::new(),
            };
            for k in i..j {
                if e2[k].ts - prev > SESSION_GAP {
                    sessions.push(s);
                    start = e2[k].ts;
                    s = Session {
                        person,
                        start,
                        day: start.div_euclid(DAY) as i32,
                        events: 0,
                        revenue: 0,
                        cat_rev: Vec::new(),
                        credit: None,
                        assists: Vec::new(),
                    };
                }
                prev = e2[k].ts;
                s.events += 1;
                if e2[k].kind == 2 {
                    s.revenue += e2[k].price;
                    add_cat(&mut s.cat_rev, e2[k].category, e2[k].price);
                }
            }
            sessions.push(s);
            i = j;
        }
    }
    drop(e2);
    mark(t0, &mut last, "sessionize");

    struct C2 {
        person: u32,
        ts: i64,
        cid: SliceRef,
        campaign: u32,
    }
    let mut c2: Vec<C2> = clicks
        .iter()
        .map(|c| C2 {
            person: people.root(c.user),
            ts: c.ts,
            cid: c.cid,
            campaign: c.campaign,
        })
        .collect();
    clicks.clear();
    c2.par_sort_unstable_by(|a, b| {
        a.person.cmp(&b.person).then(a.ts.cmp(&b.ts)).then_with(|| {
            slice_at(&click_bufs, a.cid).cmp(slice_at(&click_bufs, b.cid))
        })
    });
    mark(t0, &mut last, "sort_clicks");

    {
        let mut k = 0usize;
        let mut i = 0usize;
        while i < sessions.len() {
            let person = sessions[i].person;
            let mut j = i + 1;
            while j < sessions.len() && sessions[j].person == person {
                j += 1;
            }
            while k < c2.len() && c2[k].person < person {
                k += 1;
            }
            let mut kend = k;
            while kend < c2.len() && c2[kend].person == person {
                kend += 1;
            }
            let list = &c2[k..kend];
            for s in &mut sessions[i..j] {
                if list.is_empty() {
                    continue;
                }
                let lo = s.start - ATTRIB_WINDOW;
                let left = list.partition_point(|c| c.ts < lo);
                let right = list.partition_point(|c| c.ts <= s.start);
                if left >= right {
                    continue;
                }
                let window = &list[left..right];
                let last_ts = window.last().unwrap().ts;
                let cred = window
                    .iter()
                    .filter(|c| c.ts == last_ts)
                    .min_by_key(|c| slice_at(&click_bufs, c.cid))
                    .unwrap();
                s.credit = Some(cred.campaign);
                s.assists.clear();
                for c in window {
                    if !s.assists.contains(&c.campaign) {
                        s.assists.push(c.campaign);
                    }
                }
            }
            i = j;
            k = kend;
        }
    }
    drop(c2);
    drop(click_bufs);
    mark(t0, &mut last, "credit");

    #[derive(Default)]
    struct UserDay {
        sessions: i64,
        events: i64,
        revenue: i64,
        cat_rev: Vec<(u32, i64)>,
    }
    let mut user_days: AHashMap<(u32, i32), UserDay> = AHashMap::new();
    for s in &sessions {
        let ud = user_days.entry((s.person, s.day)).or_default();
        ud.sessions += 1;
        ud.events += s.events;
        ud.revenue += s.revenue;
        for &(cat, rev) in &s.cat_rev {
            add_cat(&mut ud.cat_rev, cat, rev);
        }
    }

    let mut ukeys: Vec<(u32, i32)> = user_days.keys().copied().collect();
    ukeys.sort_unstable();
    let mut u_user = Vec::with_capacity(ukeys.len());
    let mut u_day = Vec::with_capacity(ukeys.len());
    let mut u_sess = Vec::with_capacity(ukeys.len());
    let mut u_ev = Vec::with_capacity(ukeys.len());
    let mut u_rev = Vec::with_capacity(ukeys.len());
    let mut u_cat: Vec<Option<ByteArray>> = Vec::with_capacity(ukeys.len());
    for (person, day) in ukeys {
        let ud = &user_days[&(person, day)];
        u_user.push(ByteArray::from(people.name_of(person).to_vec()));
        u_day.push(day);
        u_sess.push(ud.sessions);
        u_ev.push(ud.events);
        u_rev.push(ud.revenue);
        if ud.revenue == 0 {
            u_cat.push(None);
        } else {
            let mut best: Option<(u32, i64)> = None;
            for &(cat, rev) in &ud.cat_rev {
                match best {
                    None => best = Some((cat, rev)),
                    Some((bcat, brev)) => {
                        if rev > brev || (rev == brev && strs.get_bytes(cat) < strs.get_bytes(bcat))
                        {
                            best = Some((cat, rev));
                        }
                    }
                }
            }
            u_cat.push(best.map(|(c, _)| ByteArray::from(strs.get_bytes(c).to_vec())));
        }
    }
    pq::write_user_daily(
        &out.join("user_daily").join("part-0.parquet"),
        u_user,
        u_day,
        u_sess,
        u_ev,
        u_rev,
        u_cat,
    );
    mark(t0, &mut last, "write_user");

    #[derive(Default)]
    struct CampDay {
        sessions: i64,
        purchasing: i64,
        revenue: i64,
        assisted: i64,
    }
    let mut camp_days: AHashMap<(u32, i32), CampDay> = AHashMap::new();
    for s in &sessions {
        let camp = s.credit.unwrap_or(organic);
        let cd = camp_days.entry((camp, s.day)).or_default();
        cd.sessions += 1;
        if s.revenue > 0 {
            cd.purchasing += 1;
        }
        cd.revenue += s.revenue;
        for &a in &s.assists {
            camp_days.entry((a, s.day)).or_default().assisted += 1;
        }
    }

    let mut ckeys: Vec<(u32, i32)> = camp_days.keys().copied().collect();
    ckeys.sort_unstable();
    let mut c_id = Vec::new();
    let mut c_ch = Vec::new();
    let mut c_day = Vec::new();
    let mut c_sess = Vec::new();
    let mut c_purch = Vec::new();
    let mut c_rev = Vec::new();
    let mut c_ast = Vec::new();
    for (cid, day) in ckeys {
        let cd = &camp_days[&(cid, day)];
        if cd.sessions == 0 && cd.assisted == 0 {
            continue;
        }
        let channel = if cid == organic {
            organic
        } else {
            campaigns.get(&cid).copied().unwrap_or(cid)
        };
        let assisted = if cid == organic { 0 } else { cd.assisted };
        c_id.push(ByteArray::from(strs.get_bytes(cid).to_vec()));
        c_ch.push(ByteArray::from(strs.get_bytes(channel).to_vec()));
        c_day.push(day);
        c_sess.push(cd.sessions);
        c_purch.push(cd.purchasing);
        c_rev.push(cd.revenue);
        c_ast.push(assisted);
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
    mark(t0, &mut last, "write_camp");
}
