mod csv;
mod ts;
mod uf;

use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ahash::{AHashMap, AHashSet};
use arrow::array::{Date32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rayon::prelude::*;

use crate::csv::{field_str, records};
use crate::ts::{parse_ts, parse_ts_str, DAY};
use crate::uf::People;

const SESSION_GAP: i64 = 1_800_000;
const ATTRIB_WINDOW: i64 = 604_800_000; // 7 days
const LATE_MS: i64 = 86_400_000; // 24h
const MAX_PRICE: i64 = 10_000_000;

#[derive(Clone)]
struct Event {
    event_id: Vec<u8>,
    user: Vec<u8>,
    ts: i64,
    ingest: i64,
    kind: u8, // 0 view 1 cart 2 purchase
    product: Vec<u8>,
    price: i64,
}

#[derive(Clone)]
struct Click {
    click_id: Vec<u8>,
    user: Vec<u8>,
    campaign: Vec<u8>,
    ts: i64,
}

struct Session {
    person: u32,
    start: i64,
    day: i32,
    events: i64,
    revenue: i64,
    cat_rev: AHashMap<Vec<u8>, i64>,
    credit: Option<Vec<u8>>, // campaign_id
    assists: AHashSet<Vec<u8>>,
}

fn ncpu() -> usize {
    std::env::var("NCPU")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n: &usize| n > 0)
        .unwrap_or_else(num_cpus_fallback)
}

fn num_cpus_fallback() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
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

fn read_all(path: &Path) -> Vec<u8> {
    fs::read(path).unwrap_or_default()
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

fn load_lookup(path: &Path, key_fields: usize) -> AHashMap<Vec<u8>, Vec<u8>> {
    let data = read_all(path);
    let mut map = AHashMap::new();
    for row in records(&data, key_fields) {
        if row.len() != key_fields {
            continue;
        }
        if row[0].is_empty() {
            continue;
        }
        map.insert(row[0].clone(), row[1].clone());
    }
    map
}

fn parse_event_row(row: &[Vec<u8>]) -> Option<Event> {
    // event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents
    if row.len() != 7 {
        return None;
    }
    if row[0].is_empty() {
        return None;
    }
    let ingest = parse_ts(&row[3])?;
    Some(Event {
        event_id: row[0].clone(),
        user: row[1].clone(),
        ts: parse_ts(&row[2]).unwrap_or(i64::MIN), // keep raw; validate after dedup
        ingest,
        kind: parse_kind(&row[4]).unwrap_or(255),
        product: row[5].clone(),
        price: parse_price(&row[6]).unwrap_or(0),
        // stash invalid markers: ts MIN, kind 255, empty user/product handled later
    })
}

fn event_valid(e: &Event, win_lo: i64, win_hi: i64) -> bool {
    if e.user.is_empty() || e.product.is_empty() {
        return false;
    }
    if e.ts == i64::MIN || e.ts < win_lo || e.ts >= win_hi {
        return false;
    }
    if e.kind > 2 {
        return false;
    }
    if e.kind == 2 {
        // purchase needs a valid price stored; parse_price None became 0
        if e.price < 1 || e.price > MAX_PRICE {
            return false;
        }
        // distinguish "invalid price" vs valid: we stored 0 for invalid.
        // view/cart ignore price. purchase with price 0 is invalid.
    }
    if e.ingest.saturating_sub(e.ts) > LATE_MS {
        return false;
    }
    true
}

fn parse_click_row(row: &[Vec<u8>]) -> Option<Click> {
    // click_id,user_id,campaign_id,ts
    if row.len() != 4 || row[0].is_empty() {
        return None;
    }
    Some(Click {
        click_id: row[0].clone(),
        user: row[1].clone(),
        campaign: row[2].clone(),
        ts: parse_ts(&row[3]).unwrap_or(i64::MIN),
    })
}

fn click_valid(c: &Click, win_lo: i64, win_hi: i64, campaigns: &AHashMap<Vec<u8>, Vec<u8>>) -> bool {
    if c.user.is_empty() {
        return false;
    }
    if c.ts == i64::MIN || c.ts < win_lo || c.ts >= win_hi {
        return false;
    }
    campaigns.contains_key(&c.campaign)
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

fn main() {
    let n = ncpu();
    rayon::ThreadPoolBuilder::new()
        .num_threads(n)
        .build_global()
        .ok();

    let data = data_dir();
    let out = out_dir();

    let manifest: serde_json::Value =
        serde_json::from_slice(&read_all(&data.join("manifest.json"))).expect("manifest");
    let win_start = parse_ts_str(manifest["window_start"].as_str().unwrap()).unwrap();
    let win_end = parse_ts_str(manifest["window_end"].as_str().unwrap()).unwrap();
    let click_lo = win_start - 7 * DAY;
    eprintln!(
        "window: {} to {} (ncpu={})",
        manifest["window_start"], manifest["window_end"], n
    );

    let products = load_lookup(&data.join("products.csv"), 2);
    let campaigns = load_lookup(&data.join("campaigns.csv"), 2);

    let mut people = People::default();

    // links
    for path in csv_files(&data.join("links")) {
        let bytes = read_all(&path);
        for row in records(&bytes, 2) {
            let a = field_str(&row[0]);
            let b = field_str(&row[1]);
            let (Some(a), Some(b)) = (a, b) else {
                continue;
            };
            let ia = people.intern(a.as_bytes());
            let ib = people.intern(b.as_bytes());
            people.union(ia, ib);
        }
    }

    // events: parse all files
    let event_files = csv_files(&data.join("events"));
    let parsed_events: Vec<Event> = event_files
        .par_iter()
        .flat_map_iter(|path| {
            let bytes = read_all(path);
            records(&bytes, 7)
                .into_iter()
                .filter_map(|row| parse_event_row(&row))
        })
        .collect();

    // dedup event_id → min ingest_ts (before validation)
    let mut kept: AHashMap<Vec<u8>, Event> = AHashMap::new();
    for e in parsed_events {
        match kept.get(&e.event_id) {
            Some(old) if old.ingest <= e.ingest => {}
            _ => {
                kept.insert(e.event_id.clone(), e);
            }
        }
    }

    let mut events: Vec<Event> = kept
        .into_values()
        .filter(|e| event_valid(e, win_start, win_end))
        .collect();

    // clicks
    let click_files = csv_files(&data.join("clicks"));
    let parsed_clicks: Vec<Click> = click_files
        .par_iter()
        .flat_map_iter(|path| {
            let bytes = read_all(path);
            records(&bytes, 4)
                .into_iter()
                .filter_map(|row| parse_click_row(&row))
        })
        .collect();

    let mut click_kept: AHashMap<Vec<u8>, Click> = AHashMap::new();
    for c in parsed_clicks {
        click_kept.entry(c.click_id.clone()).or_insert(c);
    }
    let mut clicks: Vec<Click> = click_kept
        .into_values()
        .filter(|c| click_valid(c, click_lo, win_end, &campaigns))
        .collect();

    // intern remaining ids
    for e in &events {
        people.intern(&e.user);
    }
    for c in &clicks {
        people.intern(&c.user);
    }

    #[derive(Clone)]
    struct E2 {
        person: u32,
        ts: i64,
        kind: u8,
        category: Vec<u8>,
        price: i64,
    }
    let mut e2: Vec<E2> = events
        .iter()
        .map(|e| {
            let person = {
                let id = people.intern(&e.user);
                people.root(id)
            };
            let category = products
                .get(&e.product)
                .cloned()
                .unwrap_or_else(|| b"unknown".to_vec());
            let price = if e.kind == 2 { e.price } else { 0 };
            E2 {
                person,
                ts: e.ts,
                kind: e.kind,
                category,
                price,
            }
        })
        .collect();
    events.clear();

    e2.sort_unstable_by_key(|e| (e.person, e.ts));

    // sessionize
    struct EvSess {
        person: u32,
        start: i64,
        day: i32,
        kind: u8,
        category: Vec<u8>,
        price: i64,
    }
    let mut sess_events: Vec<EvSess> = Vec::with_capacity(e2.len());
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
                sess_events.push(EvSess {
                    person,
                    start,
                    day,
                    kind: e2[k].kind,
                    category: e2[k].category.clone(),
                    price: e2[k].price,
                });
            }
            i = j;
        }
    }
    drop(e2);

    // unique sessions
    #[derive(Clone, Copy, PartialEq, Eq, Hash)]
    struct SessKey {
        person: u32,
        start: i64,
    }
    let mut sessions: AHashMap<SessKey, Session> = AHashMap::new();
    for ev in &sess_events {
        let key = SessKey {
            person: ev.person,
            start: ev.start,
        };
        let s = sessions.entry(key).or_insert_with(|| Session {
            person: ev.person,
            start: ev.start,
            day: ev.day,
            events: 0,
            revenue: 0,
            cat_rev: AHashMap::new(),
            credit: None,
            assists: AHashSet::new(),
        });
        s.events += 1;
        if ev.kind == 2 {
            s.revenue += ev.price;
            *s.cat_rev.entry(ev.category.clone()).or_insert(0) += ev.price;
        }
    }

    // clicks by person
    #[derive(Clone)]
    struct C2 {
        person: u32,
        ts: i64,
        click_id: Vec<u8>,
        campaign: Vec<u8>,
    }
    let mut c2: Vec<C2> = clicks
        .iter()
        .map(|c| {
            let id = people.intern(&c.user);
            C2 {
                person: people.root(id),
                ts: c.ts,
                click_id: c.click_id.clone(),
                campaign: c.campaign.clone(),
            }
        })
        .collect();
    clicks.clear();
    c2.sort_unstable_by(|a, b| {
        a.person
            .cmp(&b.person)
            .then(a.ts.cmp(&b.ts))
            .then(a.click_id.cmp(&b.click_id))
    });

    let mut clicks_by: AHashMap<u32, Vec<C2>> = AHashMap::new();
    for c in c2 {
        clicks_by.entry(c.person).or_default().push(c);
    }

    for s in sessions.values_mut() {
        let Some(list) = clicks_by.get(&s.person) else {
            continue;
        };
        let lo = s.start - ATTRIB_WINDOW;
        let hi = s.start;
        let left = list.partition_point(|c| c.ts < lo);
        let right = list.partition_point(|c| c.ts <= hi);
        if left >= right {
            continue;
        }
        let window = &list[left..right];
        let last_ts = window.last().unwrap().ts;
        let cred = window
            .iter()
            .filter(|c| c.ts == last_ts)
            .min_by_key(|c| &c.click_id)
            .unwrap();
        s.credit = Some(cred.campaign.clone());
        for c in window {
            s.assists.insert(c.campaign.clone());
        }
    }

    // user_daily
    #[derive(Default)]
    struct UserDay {
        sessions: i64,
        events: i64,
        revenue: i64,
        cat_rev: AHashMap<Vec<u8>, i64>,
    }
    let mut user_days: AHashMap<(u32, i32), UserDay> = AHashMap::new();
    for s in sessions.values() {
        let ud = user_days.entry((s.person, s.day)).or_default();
        ud.sessions += 1;
        ud.events += s.events;
        ud.revenue += s.revenue;
        for (cat, rev) in &s.cat_rev {
            *ud.cat_rev.entry(cat.clone()).or_insert(0) += *rev;
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
            let mut best: Option<(&[u8], i64)> = None;
            for (cat, rev) in &ud.cat_rev {
                match best {
                    None => best = Some((cat, *rev)),
                    Some((bcat, brev)) => {
                        if *rev > brev || (*rev == brev && cat.as_slice() < bcat) {
                            best = Some((cat, *rev));
                        }
                    }
                }
            }
            u_cat.push(best.map(|(c, _)| String::from_utf8_lossy(c).into_owned()));
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

    // campaign_daily
    #[derive(Default)]
    struct CampDay {
        sessions: i64,
        purchasing: i64,
        revenue: i64,
        assisted: i64,
    }
    let mut camp_days: AHashMap<(Vec<u8>, i32), CampDay> = AHashMap::new();
    let organic = b"organic".to_vec();
    for s in sessions.values() {
        let camp = s.credit.clone().unwrap_or_else(|| organic.clone());
        let cd = camp_days.entry((camp, s.day)).or_default();
        cd.sessions += 1;
        if s.revenue > 0 {
            cd.purchasing += 1;
        }
        cd.revenue += s.revenue;
        for a in &s.assists {
            camp_days.entry((a.clone(), s.day)).or_default().assisted += 1;
        }
    }

    let mut c_id = Vec::new();
    let mut c_ch = Vec::new();
    let mut c_day = Vec::new();
    let mut c_sess = Vec::new();
    let mut c_purch = Vec::new();
    let mut c_rev = Vec::new();
    let mut c_ast = Vec::new();
    let mut ckeys: Vec<(Vec<u8>, i32)> = camp_days.keys().cloned().collect();
    ckeys.sort();
    for (cid, day) in ckeys {
        let cd = &camp_days[&(cid.clone(), day)];
        if cd.sessions == 0 && cd.assisted == 0 {
            continue;
        }
        let channel = if cid == organic {
            organic.clone()
        } else {
            campaigns.get(&cid).cloned().unwrap_or_else(|| cid.clone())
        };
        let assisted = if cid == organic { 0 } else { cd.assisted };
        c_id.push(String::from_utf8_lossy(&cid).into_owned());
        c_ch.push(String::from_utf8_lossy(&channel).into_owned());
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
}
