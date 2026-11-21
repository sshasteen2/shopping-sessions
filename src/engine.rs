use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use ahash::{AHashMap, AHashSet};
use arrow::array::{Date32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rayon::prelude::*;

use crate::csv::for_each_record;
use crate::intern::ShardedIntern;
use crate::ts::{parse_ts, parse_ts_str, DAY};
use crate::uf::People;

const SESSION_GAP: i64 = 1_800_000;
const ATTRIB_WINDOW: i64 = 604_800_000;
const LATE_MS: i64 = 86_400_000;
const MAX_PRICE: i64 = 10_000_000;
const EMPTY: u32 = u32::MAX;

struct Event {
    user: u32,
    ts: i64,
    ingest: i64,
    kind: u8,
    product: u32,
    price: i64,
}

struct Click {
    click_id: u32,
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
    cat_rev: AHashMap<u32, i64>,
    credit: Option<u32>,
    assists: AHashSet<u32>,
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

fn event_valid(e: &Event, win_lo: i64, win_hi: i64) -> bool {
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

fn write_parquet(path: &Path, batch: RecordBatch) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).ok();
    }
    let file = File::create(path).expect("create parquet");
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer =
        ArrowWriter::try_new(file, batch.schema(), Some(props)).expect("arrow writer");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close parquet");
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
            let p = strs.get(row[0]);
            let c = strs.get(row[1]);
            products.insert(p, c);
        });
    }
    let mut campaigns: AHashMap<u32, u32> = AHashMap::new();
    {
        let bytes = fs::read(data.join("campaigns.csv")).unwrap_or_default();
        for_each_record(&bytes, 2, |row| {
            if row[0].is_empty() {
                return;
            }
            let id = strs.get(row[0]);
            let ch = strs.get(row[1]);
            campaigns.insert(id, ch);
        });
    }
    mark(t0, &mut last, "lookups");

    let mut people = People::default();
    for path in csv_files(&data.join("links")) {
        let bytes = fs::read(path).unwrap_or_default();
        for_each_record(&bytes, 2, |row| {
            if row[0].is_empty() || row[1].is_empty() {
                return;
            }
            let a = people.intern(row[0]);
            let b = people.intern(row[1]);
            people.union(a, b);
        });
    }
    mark(t0, &mut last, "links");

    let people = Mutex::new(people);
    let kept_shards: Vec<Mutex<AHashMap<u32, Event>>> = (0..nshards)
        .map(|_| Mutex::new(AHashMap::new()))
        .collect();
    csv_files(&data.join("events")).into_par_iter().for_each(|path| {
        let bytes = fs::read(&path).unwrap_or_default();
        for_each_record(&bytes, 7, |row| {
            if row[0].is_empty() {
                return;
            }
            let Some(ingest) = parse_ts(row[3]) else {
                return;
            };
            let eid = strs.get(row[0]);
            let shard = (eid >> 24) as usize;
            let mut map = kept_shards[shard].lock().unwrap();
            if let Some(old) = map.get(&eid) {
                if old.ingest <= ingest {
                    return;
                }
            }
            let user = if row[1].is_empty() {
                EMPTY
            } else {
                people.lock().unwrap().intern(row[1])
            };
            let product = if row[5].is_empty() {
                EMPTY
            } else {
                strs.get(row[5])
            };
            map.insert(
                eid,
                Event {
                    user,
                    ts: parse_ts(row[2]).unwrap_or(i64::MIN),
                    ingest,
                    kind: parse_kind(row[4]).unwrap_or(255),
                    product,
                    price: parse_price(row[6]).unwrap_or(0),
                },
            );
        });
    });
    mark(t0, &mut last, "parse_events");

    let mut events: Vec<Event> = kept_shards
        .into_iter()
        .flat_map(|m| m.into_inner().unwrap().into_values())
        .filter(|e| event_valid(e, win_start, win_end))
        .collect();
    mark(t0, &mut last, "validate_events");

    let click_shards: Vec<Mutex<AHashMap<u32, Click>>> = (0..nshards)
        .map(|_| Mutex::new(AHashMap::new()))
        .collect();
    csv_files(&data.join("clicks")).into_par_iter().for_each(|path| {
        let bytes = fs::read(&path).unwrap_or_default();
        for_each_record(&bytes, 4, |row| {
            if row[0].is_empty() {
                return;
            }
            let cid = strs.get(row[0]);
            let shard = (cid >> 24) as usize;
            let mut map = click_shards[shard].lock().unwrap();
            map.entry(cid).or_insert_with(|| Click {
                click_id: cid,
                user: if row[1].is_empty() {
                    EMPTY
                } else {
                    people.lock().unwrap().intern(row[1])
                },
                campaign: strs.get(row[2]),
                ts: parse_ts(row[3]).unwrap_or(i64::MIN),
            });
        });
    });
    mark(t0, &mut last, "parse_clicks");

    let mut clicks: Vec<Click> = click_shards
        .into_iter()
        .flat_map(|m| m.into_inner().unwrap().into_values())
        .filter(|c| {
            c.user != EMPTY
                && c.ts != i64::MIN
                && c.ts >= click_lo
                && c.ts < win_end
                && campaigns.contains_key(&c.campaign)
        })
        .collect();
    mark(t0, &mut last, "validate_clicks");

    let mut people = people.into_inner().unwrap();
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
    e2.sort_unstable_by_key(|e| (e.person, e.ts));
    mark(t0, &mut last, "sort_events");

    #[derive(Clone, Copy, PartialEq, Eq, Hash)]
    struct SessKey {
        person: u32,
        start: i64,
    }
    let mut sessions: AHashMap<SessKey, Session> = AHashMap::new();
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
            for k in i..j {
                if e2[k].ts - prev > SESSION_GAP {
                    start = e2[k].ts;
                }
                prev = e2[k].ts;
                let day = start.div_euclid(DAY) as i32;
                let s = sessions.entry(SessKey { person, start }).or_insert_with(|| {
                    Session {
                        person,
                        start,
                        day,
                        events: 0,
                        revenue: 0,
                        cat_rev: AHashMap::new(),
                        credit: None,
                        assists: AHashSet::new(),
                    }
                });
                s.events += 1;
                if e2[k].kind == 2 {
                    s.revenue += e2[k].price;
                    *s.cat_rev.entry(e2[k].category).or_insert(0) += e2[k].price;
                }
            }
            i = j;
        }
    }
    drop(e2);
    mark(t0, &mut last, "sessionize");

    struct C2 {
        person: u32,
        ts: i64,
        click_id: u32,
        campaign: u32,
    }
    let mut c2: Vec<C2> = clicks
        .iter()
        .map(|c| C2 {
            person: people.root(c.user),
            ts: c.ts,
            click_id: c.click_id,
            campaign: c.campaign,
        })
        .collect();
    clicks.clear();
    c2.sort_unstable_by(|a, b| {
        a.person
            .cmp(&b.person)
            .then(a.ts.cmp(&b.ts))
            .then_with(|| strs.get_bytes(a.click_id).cmp(strs.get_bytes(b.click_id)))
    });
    let mut clicks_by: AHashMap<u32, Vec<C2>> = AHashMap::new();
    for c in c2 {
        clicks_by.entry(c.person).or_default().push(c);
    }
    mark(t0, &mut last, "sort_clicks");

    for s in sessions.values_mut() {
        let Some(list) = clicks_by.get(&s.person) else {
            continue;
        };
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
            .min_by_key(|c| strs.get_bytes(c.click_id))
            .unwrap();
        s.credit = Some(cred.campaign);
        for c in window {
            s.assists.insert(c.campaign);
        }
    }
    mark(t0, &mut last, "credit");

    #[derive(Default)]
    struct UserDay {
        sessions: i64,
        events: i64,
        revenue: i64,
        cat_rev: AHashMap<u32, i64>,
    }
    let mut user_days: AHashMap<(u32, i32), UserDay> = AHashMap::new();
    for s in sessions.values() {
        let ud = user_days.entry((s.person, s.day)).or_default();
        ud.sessions += 1;
        ud.events += s.events;
        ud.revenue += s.revenue;
        for (&cat, &rev) in &s.cat_rev {
            *ud.cat_rev.entry(cat).or_insert(0) += rev;
        }
    }

    let mut u_user = Vec::new();
    let mut u_day = Vec::new();
    let mut u_sess = Vec::new();
    let mut u_ev = Vec::new();
    let mut u_rev = Vec::new();
    let mut u_cat: Vec<Option<String>> = Vec::new();
    let mut ukeys: Vec<(u32, i32)> = user_days.keys().copied().collect();
    ukeys.sort_unstable();
    for (person, day) in ukeys {
        let ud = &user_days[&(person, day)];
        u_user.push(people.name_str(person));
        u_day.push(day);
        u_sess.push(ud.sessions);
        u_ev.push(ud.events);
        u_rev.push(ud.revenue);
        if ud.revenue == 0 {
            u_cat.push(None);
        } else {
            let mut best: Option<(u32, i64)> = None;
            for (&cat, &rev) in &ud.cat_rev {
                match best {
                    None => best = Some((cat, rev)),
                    Some((bcat, brev)) => {
                        if rev > brev
                            || (rev == brev && strs.get_bytes(cat) < strs.get_bytes(bcat))
                        {
                            best = Some((cat, rev));
                        }
                    }
                }
            }
            u_cat.push(best.map(|(c, _)| strs.get_str(c)));
        }
    }
    let user_schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("events", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("top_category", DataType::Utf8, true),
    ]));
    let user_batch = RecordBatch::try_new(
        user_schema,
        vec![
            Arc::new(StringArray::from(u_user)),
            Arc::new(Date32Array::from(u_day)),
            Arc::new(Int64Array::from(u_sess)),
            Arc::new(Int64Array::from(u_ev)),
            Arc::new(Int64Array::from(u_rev)),
            Arc::new(StringArray::from(u_cat)),
        ],
    )
    .expect("user batch");
    write_parquet(&out.join("user_daily").join("part-0.parquet"), user_batch);
    mark(t0, &mut last, "write_user");

    #[derive(Default)]
    struct CampDay {
        sessions: i64,
        purchasing: i64,
        revenue: i64,
        assisted: i64,
    }
    let mut camp_days: AHashMap<(u32, i32), CampDay> = AHashMap::new();
    for s in sessions.values() {
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

    let mut c_id = Vec::new();
    let mut c_ch = Vec::new();
    let mut c_day = Vec::new();
    let mut c_sess = Vec::new();
    let mut c_purch = Vec::new();
    let mut c_rev = Vec::new();
    let mut c_ast = Vec::new();
    let mut ckeys: Vec<(u32, i32)> = camp_days.keys().copied().collect();
    ckeys.sort_unstable();
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
        c_id.push(strs.get_str(cid));
        c_ch.push(strs.get_str(channel));
        c_day.push(day);
        c_sess.push(cd.sessions);
        c_purch.push(cd.purchasing);
        c_rev.push(cd.revenue);
        c_ast.push(assisted);
    }
    let camp_schema = Arc::new(Schema::new(vec![
        Field::new("campaign_id", DataType::Utf8, false),
        Field::new("channel", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("purchasing_sessions", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("assisted_sessions", DataType::Int64, false),
    ]));
    let camp_batch = RecordBatch::try_new(
        camp_schema,
        vec![
            Arc::new(StringArray::from(c_id)),
            Arc::new(StringArray::from(c_ch)),
            Arc::new(Date32Array::from(c_day)),
            Arc::new(Int64Array::from(c_sess)),
            Arc::new(Int64Array::from(c_purch)),
            Arc::new(Int64Array::from(c_rev)),
            Arc::new(Int64Array::from(c_ast)),
        ],
    )
    .expect("camp batch");
    write_parquet(
        &out.join("campaign_daily").join("part-0.parquet"),
        camp_batch,
    );
    mark(t0, &mut last, "write_camp");
}
