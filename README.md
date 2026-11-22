# Shopping Sessions

A Rust pipeline for the [Builders Buildathon](https://www.joinbuilders.org/buildathon) Databricks track, **Shopping Sessions**. It turns tens of millions of messy click-stream events into two exact daily Parquet tables, and it is scored on run time. A table that is wrong by one row scores nothing.

**Result:** 1.61s on the ~20M-event practice set, down from 13.88s for the first version. Every version since has matched the official answer exactly.

## The problem

The input is a folder of CSV files: shopping events (views, carts, purchases), ad clicks, links between user IDs, plus product and campaign tables. The data is dirty on purpose:
- duplicated events, where the copy to keep is decided by ingest time
- three timestamp formats with time-zone offsets, some bad, some out of the window
- quoted fields and malformed lines
- unknown products and campaigns, bad prices, wrong-case values
- one person split across up to hundreds of linked IDs, including chains hundreds of links long and shared devices joining thousands of people

The pipeline has to:
1. clean and deduplicate the rows
2. join each person's IDs
3. build sessions (a gap of more than 30 minutes starts a new one)
4. credit each session to the latest qualifying ad click in the 7 days before it, and count assisting campaigns
5. write `user_daily` and `campaign_daily` exactly as [`rules.md`](rules.md) defines them

The scorer runs the Docker image on 8 vCPUs and 24 GB. The final rescores the fastest entries on three fresh ~60M-event datasets.

## Score history

| Commit | Change | Practice time |
|---|---|---|
| `e15033a` | First Rust pipeline, all 36 fixtures passing | 13.88s, 11.99s |
| `7e6a62d` | Lock-free bulk interning of IDs | 9.22s |
| `d553779` | New partitioned engine with IDs packed into 64-bit keys | 2.44s |
| `10c4a14` | SIMD row splitting, batched person lookups, parallel links | 1.77s |
| `d7e45d9` | Date caches, 16-byte records, leaner Parquet, fast epoch-ms | 1.44s |
| `0484ec1` | Cache-friendly regrouping, sub-partitioned dedup, dense ID lookup | 1.41s |
| `42e7549` | Fix for the scorer's new long-ID format (v5): sharded interner | 1.74s on v5 data |
| `aa39117` | Long event IDs hashed with exact byte confirmation | **1.61s on v5 data** |
| `3812282` | Long user IDs packed as 128-bit numbers | 1.62s on v5 data |

Midway through the event the organizers changed the data: 1 in 8 IDs became 33 characters, scrambled per dataset. The pipeline at that point slowed to 5.5s, because every long ID went through one shared, locked table. Sharding that table and hashing event IDs (with a byte comparison whenever two hashes are equal, so a collision can never merge two events) brought it back to 1.61s.

## How it works

The main engine is [`src/fast.rs`](src/fast.rs). It runs in phases, and every phase uses all cores:

1. **Lookups.** Loads products and campaigns. IDs of the form `<prefix><number>` (like `p123`) go in a plain array, and anything else in a hash map.
2. **Links.** Parses the ID links in parallel, joins IDs into people with union-find, and gives each person the byte-wise smallest of their IDs as their name.
3. **Parse events.** Splits the files into ~4 MB chunks and reads rows with a SIMD tokenizer ([`src/rows.rs`](src/rows.rs)). It packs IDs into integer keys, parses timestamps with a small date cache ([`src/ts.rs`](src/ts.rs)), and counting-sorts the records into 256 partitions by event ID.
4. **Dedup.** In each partition, keeps the copy with the earliest ingest time, then drops the event if that copy is invalid. Invalid kept copies are dropped, not replaced, as the rules require.
5. **Parse clicks.** Same structure as events, partitioned by person.
6. **Sessions.** For each person: sorts their events and clicks, splits sessions, finds the credited click with a two-pointer 7-day window, and counts assists.
7. **Write.** Writes `user_daily` as one Parquet file per core, plus one `campaign_daily` file.

Some details that mattered:

- **IDs as integers ([`src/ids.rs`](src/ids.rs)).** An ID of up to 10 characters from `[0-9A-Z_a-z]` packs into a u64 at 6 bits per character, so integer order equals string order. `u` plus 32 lowercase hex digits packs into 128 bits. Any other ID is stored as its exact bytes. Nothing decodes the generator's numbering: every ID is treated as an opaque string, as the rules require.
- **Hash seeds.** Rows are split into partitions by the top bits of a hash. A hash table inside a partition that uses the same hash sees those bits constant, and its lookups slow badly. Using a different seed inside partitions fixed a ~0.8s loss.
- **Fallbacks.** If an input breaks one of the compact formats (more than 256 categories, more than 65,535 campaigns, or a click window longer than u32 milliseconds), the pipeline falls back to the original, general engine ([`src/engine.rs`](src/engine.rs)). Run it directly with `ENGINE=old`.

## How correctness was checked

Every change was kept only if all of these matched exactly:

- unit tests (`cargo test --release`): the tokenizer against a reference, timestamps across ~32k dates, ID ordering
- all 36 edge-case fixtures (`scripts/run_fixtures.py`)
- the official dev answer for the 1M-event set
- the original engine's output on generated 20M- and 60M-event sets

[`scripts/compare.py`](scripts/compare.py) is an exact, order-independent comparison of two output folders. It prints `MATCH` only if every row matches.

## Build and run

```sh
# Native build (the scorer is x86-64; match its CPU level)
RUSTFLAGS="-C target-cpu=x86-64-v3" cargo build --release --locked
NCPU=8 DATA_DIR=dev OUTPUT_DIR=out SCRATCH_DIR=/tmp/scratch target/release/shopping-sessions

# Fixtures and an exact comparison
python3 scripts/run_fixtures.py fixtures -- target/release/shopping-sessions
python3 scripts/compare.py out dev-answer
```

With Docker, `make build`, `make fixtures`, `make run-dev` and `make check-dev` run the image the way the scorer does. The test scripts need Python 3.10+ with `polars` (`pip install polars`).

To generate data, build the organizers' generator in [`generator/`](generator) and run `generate --seed 1 --events 1000000 --clicks 250000 --output dev`.

For timing, [`scripts/ab_linux.sh`](scripts/ab_linux.sh) alternates two binaries over several runs and prints min and median wall time plus mean CPU time. Single runs vary by 5–20%, so alternating runs are the only way to see a few percent.

## What I'd do next

The fastest entries ran at about 0.42s. Ideas that should close part of that gap:

- radix-sort each person partition by time instead of sorting each person separately
- parse `HH:MM:SS` 8 bytes at a time
- smaller event records, and reusing buffers to cut kernel time on fresh memory
- parse clicks while events are being deduplicated

Ideas that were measured and rejected are listed in the commit history: a delimiter-index tokenizer, a "seen twice" bitset, a hand-written Parquet writer and `MAP_POPULATE`.

## Credits

- The track organizers provided the rules, the fixtures, the data generator ([`generator/`](generator)), the `Makefile` and the test scripts `run_fixtures.py`, `compare.py` and `docker_run.sh`.
- Rust crates: `ahash`, `hashbrown`, `memchr`, `memmap2`, `mimalloc`, `parquet`, `rayon`, `serde_json`.
- Built with AI coding agents (Claude Code and Cursor) doing much of the implementation, under my direction.

## License

[MIT](LICENSE)
