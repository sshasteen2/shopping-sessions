# Handoff: Shopping Sessions pipeline

Read this before touching anything. It covers what the pipeline does, how to prove a change is correct, how to time it, what has been tried, and what is worth trying next.

## Ground rules

1. **Work on a branch, never `main`.** `main` is what gets submitted. Open a branch (for example `perf/<idea>`) and leave it for review.
2. **A change counts only if its output is identical.** Every change must pass all of [How to verify](#how-to-verify) before it is kept. A pipeline that is fast but wrong by one row scores nothing.
3. **Measure, don't guess.** Use alternating A/B runs (8 or more per binary) on Linux. Single runs vary by 5–20%.
4. **The deadline is fixed.** Scoring closes Sunday 10:00 AM CST. The final rebuilds the chosen commit (the best-scoring one unless changed) and runs it 3 times on about 60M new events, each run capped at 3 minutes. A run over the cap, or any wrong table, means no final score.

## Status

| Commit | What changed | Practice score |
|---|---|---|
| (start) | Earlier Rust engine | 13.88s, 11.99s |
| `7e6a62d` | Lock-free interning (old engine) | 9.22s |
| `d553779` | New partitioned engine (`src/fast.rs`) | 2.44s |
| `10c4a14` | SIMD row splitting, batched person lookups, parallel links | 1.77s |
| `d7e45d9` | Date caches, 16-byte records, leaner Parquet, fast epoch-ms | 1.44s |
| `0484ec1` | Counting-sort regrouping, sub-partitioned dedup, dense ID lookup | 1.4103s |
| `9a9f120` | Filter in front of the person-name table (~6% less CPU) | 1.4273s, submitted |

`9a9f120` is the submitted baseline. Practice score 1.4273s versus 1.4103s for `0484ec1` is a tie inside the scorer's noise, so the name-table filter stays.

Local A/B on this machine (8 vCPU, 20M events, seed 7), alternating runs, release build with `target-cpu=x86-64-v3`:

| Change | Wall median | CPU mean | Kept |
|---|---|---|---|
| LLVM `-C llvm-args=-inline-threshold=1000` | 902 → 872 ms and 876 → 857 ms | 6.1 → 5.9 s and 6.1 → 5.8 s | yes |
| Write-combining partition scatter (8 records, then a sequential flush) | 897 → 880 ms and 908 → 864 ms | 6.5 → 5.9 s and 5.97 → 5.75 s | yes |
| `MADV_HUGEPAGE` on large partition buffers | 932 → 884 ms and 906 → 866 ms | 6.28 → 5.90 s and 5.97 → 5.78 s | yes |
| Radix-sort events by `ts`, then stable group | 982 vs 983 ms | 6.4 vs 6.5 s | no |
| SWAR `HH:MM:SS` and 8-digit F3 parse | 878 vs 864 ms | 5.9 vs 5.9 s | no |
| Open-addressing dedup table (replaced hashbrown) | 799 vs 787 ms | 5.5 vs 5.4 s | no |
| Multiply hash inside hashbrown dedup | 811 vs 824 ms | 5.8 vs 5.7 s | no |
| Unrolled ID packing (microbench, 9-byte ids) | — | ~4 ns either way | no |

Each row is its own alternating A/B. Absolute times drifted between sessions (this VM moved between about 0.78s and 0.95s with no code change), so compare only within a row.

Stacked tip `6d84a39` versus `9a9f120` (`bin-base`), quiet 10-run alternating A/B on the same 20M set, 8 vCPUs: wall median **782 ms vs 786 ms** (no wall gain), CPU **5.30 s vs 5.13 s**, sys **0.52 s vs 0.42 s**. The per-change wall gaps above showed up while the machine was slower and noisier; they do not reproduce as a wall win once both binaries are in the 0.75–0.85s band. CPU and kernel time are slightly lower. One 60M run (seed 42) was 2.49s vs 2.46s wall, user CPU 15.71s vs 15.01s.

The scorer's times vary by up to ±20% between machines, so differences under about 10% cannot be read from single submissions. `9a9f120` at 1.4273s versus `0484ec1` at 1.4103s is that kind of tie.

A quiet Linux machine (Ryzen 7 8840HS, 8 threads, WSL2) runs the 20M set in about 1.2–1.3s. That is about 9.5–10.5 CPU-seconds, of which about 1.5–2s is kernel time.

The practice leaderboard's top ~20 entries (0.27–0.32s) are almost certainly replaying a precomputed answer for the fixed practice dataset. Container start plus reading 1.8 GB alone uses up about that much time. Legitimate entries appear to start around 0.5s. Don't chase the 0.3s numbers.

## How it works

- `src/main.rs` runs the new engine (`fast::run`) by default. `ENGINE=old` runs the original engine (`engine.rs`), which is kept as an independent reference.
- `fast::run` falls back to the old engine if the input breaks one of its compact formats: a click window longer than u32 milliseconds, more than 256 categories, or more than 65,535 campaigns.

`src/fast.rs` runs these phases in order. Each phase prints its time to stderr (`phase <name> <ms>`).

1. **lookups**: Loads `products.csv` and `campaigns.csv`. `IdLookup` indexes IDs of the form `<prefix><canonical number>` (such as `p123` or `c_45`) in a dense array. Any other ID goes through `KeyMap`. The number form is used only when the string is canonical, so `p0123` never matches `p123`.
2. **links**: Parses the links in parallel. It assigns dense IDs by sorting the link endpoints, runs union-find over arrays, and picks the byte-wise smallest key per person. `NameTable` stores only IDs whose person's name is a different ID, behind a 16-bit-per-key filter.
3. **parse_events**: Splits the files into ~4 MB chunks. For each chunk it:
   - splits rows with `rows::for_each_row` (SIMD comma/quote/newline masks)
   - packs IDs into u64 keys (`ids.rs`)
   - parses timestamps with `ts::parse_ts_cached`
   - builds 32-byte `Ev` records in one buffer, resolves persons in a batched, prefetched pass, then counting-sorts the buffer into 256 partitions by `mix(event_id)` (`Parts<T>`).
4. **dedup_events**: For each event_id partition (16 sub-partitions so each table fits in L2), keeps the copy with the smallest `ingest_ts`. It drops kept copies that are invalid, emits 16-byte `Ev2` records, and counting-sorts them into 256 person partitions.
5. **parse_clicks**: Parses clicks the same way, validates them at parse time (copies are identical, so this is safe), resolves persons, and partitions by person.
6. **sessions**: For each person partition:
   - groups events by person (local hash table, then counting sort) and sorts each person's events by `ts`
   - groups clicks the same way, sorts them by `(ts, click_id)` and removes duplicates
   - `person_pass`: builds sessions (gap > 30 min), picks the credited click with a two-pointer 7-day window (latest `ts`, then smallest `click_id`), counts assisting campaigns, and builds `user_daily` rows and campaign/day counters.
7. **write**: Writes `user_daily` as `NCPU` Parquet files in parallel and `campaign_daily` as one file, using the parquet crate (`pq.rs`) with no dictionaries and no statistics.

Key details:
- **IDs (`ids.rs`):** IDs of at most 10 characters from `[0-9A-Z_a-z]` pack into a u64, 6 bits per character, left-aligned. Integer order equals byte order. Anything else goes to a locked overflow table with the top bit set. Key 0 means the empty ID.
- **Timestamps (`ts.rs`):**
  - `parse_ts_cached` has a 64-slot cache of dates keyed by the `YYYY-MM-DD` bytes, and a plain u64 path for F3 values of up to 18 digits.
  - The tests compare it with `parse_ts` on about 32k generated dates plus edge cases.
- **Partitions:** `part(x) = mix(x) >> 56`. Any hash table inside a partition must use a different seed (see `DEDUP_SEED` and `LOCAL_SEED`). Otherwise the table's control bytes are all equal and lookups degrade badly. This bug once cost ~0.8s.

## How to verify

Every change must pass all of these. The reference outputs come from `ENGINE=old`, run once per dataset.

```sh
# Build (the scorer uses x86-64-v3; match it)
RUSTFLAGS="-C target-cpu=x86-64-v3" cargo build --release --locked
B=target/release/shopping-sessions
pip install polars

# 1. Unit tests (tokenizer vs reference, timestamps, ID ordering)
cargo test --release

# 2. All 36 fixtures
SCRATCH_DIR=$(mktemp -d) NCPU=8 python3 scripts/run_fixtures.py fixtures -- $B    # expect 36/36

# 3. Exact output vs the old engine on the dev set and a 20M set
cargo build --release --manifest-path generator/Cargo.toml
G=generator/target/release/generate
$G --seed 1 --events 1000000 --clicks 250000 --output dev
$G --seed 7 --events 20000000 --clicks 5000000 --output ss20m
for d in dev ss20m; do
  mkdir -p ref-$d new-$d
  ENGINE=old NCPU=8 DATA_DIR=$d OUTPUT_DIR=ref-$d SCRATCH_DIR=$(mktemp -d) $B
  NCPU=8 DATA_DIR=$d OUTPUT_DIR=new-$d SCRATCH_DIR=$(mktemp -d) $B
  python3 scripts/compare.py new-$d ref-$d    # must print MATCH
done

# 4. Final-size run (needs ~16 GB RAM free). The old engine is slow here,
#    so compare against the current main build instead.
$G --seed 42 --events 60000000 --clicks 15000000 --output ss60m
```

`scripts/compare.py` is an exact, order-independent diff. "MATCH" is the only acceptable result.

## How to time it

```sh
# Alternate two binaries (8+ runs each); prints min/median wall ms and mean CPU seconds
bash scripts/ab_linux.sh ss20m 10 ./bin-before ./bin-after
```

- Compare CPU seconds as well as wall time; CPU time is the steadier measure.
- Close other programs while timing.

To profile on Linux, build with `CARGO_PROFILE_RELEASE_DEBUG=line-tables-only`, then run:

```sh
perf record -e cpu-clock -F 5000 -o p.data $B
perf report -i p.data --no-children --sort symbol
perf annotate -i p.data --stdio -l -s <symbol>     # time per source line
```

## Where the time goes now

Quiet Linux machine, 20M events, 8 threads. All threads are busy in every phase.

| Phase | Wall ms | Share | Notes |
|---|---|---|---|
| parse_events | ~500–580 | ~42% | Per row, single-threaded with warm caches: tokenizer ~30ns, two timestamps ~10–16ns, packing the event and user IDs ~10ns, product lookup ~5ns; ~190ns per row with 8 threads |
| sessions | ~300 | ~24% | Per-person `sort_unstable_by_key(ts)` was ~1/3 of its CPU; grouping hash ~15%; counting-sort scatter ~10% |
| dedup_events | ~120 | ~10% | hashbrown `HashTable<Ev>` per sub-partition |
| parse_clicks | ~100 | ~8% | Same structure as events |
| links | ~80 | ~6% | |
| write | ~70 | ~6% | parquet crate, ~200ns per row |

## What has been tried and rejected

All of these were measured on Linux or with warm-cache benchmarks:

- **Delimiter-index tokenizer** (simdjson-style stage 1 + stage 2 over ~64 KB windows). It matched the reference exactly but was slower: 43ns per row with SSE and ~90ns with AVX2, against 30ns for the current `for_each_row`.
- **Tokenizer that walks only comma/newline bits** and counts quotes from a mask: ~50ns against ~36ns. Re-reading `d[p]` from L1 is nearly free.
- **Global "seen twice" bitset** so unique event IDs skip dedup: the atomic marking during parse cost more than the dedup it saved.
- **Custom Parquet writer** (hand-written Thrift). It was correct (read back by polars and parquet-rs) but showed no measurable wall gain, and it risks the scorer's reader being stricter. Reverted.
- **`MAP_POPULATE` on input mmaps:** neutral to slightly worse.
- **Gathering dedup input contiguously before the 16-way split:** neutral.
- **A single-entry date cache:** rows are shuffled across days, so it mostly missed. It was replaced with the 64-slot cache.
- **Larger Parquet batch and page sizes:** within noise.

## Ideas from the previous list, now measured

Tried on this 8-vCPU Xeon (x86-64-v3, rustc 1.100). Output was checked against `ENGINE=old` before any timing that was a candidate to keep. Reverted when wall time did not clearly drop.

- **Radix-sort sessions by `ts`, then stable group.** Correct (dev MATCH). Wall median 982 vs 983 ms, CPU 6.4 vs 6.5 s. The per-person sorts are already in cache; the extra streaming passes cost as much as they save. Reverted.
- **SWAR `HH:MM:SS` and 8-digit F3.** Matched `parse_ts` in unit tests and dev MATCH. Wall median 878 vs 864 ms, CPU 5.9 vs 5.9 s. Reverted.
- **28-byte packed `Ev`** (24-bit biased ingest seconds, category in the top 8, fallback to the old engine if an ingest is not whole seconds or outside ±97 days). Dev MATCH, and the fast path did not fall back on generator data. Two A/Bs: wall median 962→921 then 927→915 ms, CPU 6.37→6.23 then 6.11→5.91 s. Dedup did not speed up (misaligned 28-byte slots). Wall gain was not repeatable. Reverted.
- **Thread-local reuse** of the parse buffer and the partition scratch. Dev MATCH. Wall median 781 vs 789 ms (no gain), CPU 5.28→5.12 s, sys 0.52→0.45 s. Reverted.
- **Unrolled ID packing.** Microbench on 200k 9-byte ids: about 4 ns either way (loop 4.2 ns, unroll 3.5–4.9 ns). Not integrated. A pshufb lookup cannot cover the 256-entry code table.
- **Overlap click parsing with dedup** via `rayon::join`. Dev MATCH. Both sides are CPU-saturated, so wall median stayed 903 vs 900 ms. CPU 5.97 vs 5.7 s. Reverted.
- **64-slot direct-mapped cache in front of `NameTable::get`.** Dev MATCH. Wall median 834 vs 837 ms, CPU 5.66 vs 5.43 s. Reverted.
- **Open-addressing dedup table** and a **one-multiply hash** inside hashbrown. Both MATCH on dev. Neither beat hashbrown on wall (799 vs 787 ms, and 811 vs 824 ms). Reverted.
- **Scalar per-line CSV scan** versus the SIMD `for_each_row`: 97 ns/row vs 48 ns/row on a 300 MB event file, and the scalar scan disagreed. Not integrated.

## What is still worth trying

- A fused event parser that pulls field parsing into the SIMD delimiter loop, so LLVM stops outlining the per-row closure. The closure plus `parse_ts_cached` is still the largest self-time. Previous tokenizer rewrites were slower; this one has not been written.
- Tuning the write-combine depth (4 or 16) now that the 8-deep flush is in the tip. Only worth it if a quiet A/B shows a wall gap, which the stacked tip currently does not.

## Verification of the tip (`6d84a39`)

8 vCPUs. `ENGINE=old` references: dev seed 1, 20M seed 7, dev-size seeds 2, 3, and 11. 60M seed 42 compared to the `9a9f120` binary (the old engine was not rerun at 60M). All of these printed MATCH. `cargo test --release`: 23 passed. Fixtures: 36/36. 60M fit: about 16 GB available plus a 30 GB balloon, and both runs finished (2.49s and 2.46s).

Recommendation: practice score stays a tie around 1.42s. Submit `9a9f120` if you want the build that already scored. The tip is byte-identical on every set above and uses a little less CPU, but a quiet 10-run A/B does not show a lower wall time.
