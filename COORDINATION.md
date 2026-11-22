# Coordination log

Shared notes between the Cursor agent and Claude Code on the v5 long-ID speed work.
Neither agent can see the other's edits except through this file.

## 1. Purpose and rules

- This file is an append-only log. Add new entries at the bottom. Do not rewrite or delete an existing entry.
- Each entry starts with `### <UTC time> - <agent name>`. Use `Cursor agent` or `Claude Code`.
- The results table grows by new rows. Do not change a row someone else wrote; add a new row if a number is revised.
- `main` is submission-only. Nobody pushes to `main`, and nobody merges into it, without the user's explicit OK.
- Work stays on the branch that owns it. Do not commit onto the other agent's branch.
- Every branch name and commit SHA in play is listed below and updated when a new commit exists.
- Before appending, run `git pull` on `coordination` and read the new entries. Adjust the plan if the other agent already covered something.
- This branch is not a submission. Do not point the scorer at it.

## 2. Current state

- `main` is `42e75499f49d95acc7ac90c24e9c4cac2f3ce6e3` ("fix: shard the long-ID interner for scorer v5's 33-char IDs"). That is the baseline.
- The scorer timed that commit at **1.7430s** on v5 practice data.
- Finals cutoff to stay in the top 30 is about **1.116s**.
- Deadline is **10:00 AM CST Sunday = 16:00 UTC Sunday 22 Nov 2026**.
- Practice set used for local A/B: v5 generator, seed 7, 20M events, 5M clicks (`/tmp/ss-data/n20m` on the Cursor VM).

## 3. Division of labor

- **Claude Code** owns the event-ID equality path used by dedup. Event IDs only need equality, not byte order, and long event IDs are nearly unique.
- **Cursor agent** owns the 128-bit representation for user and click IDs, where order matters (person-name minimum key, click-id tie-break), plus other hotspots that are not the event-ID path.
- Shared constraint: a letter plus 32 hex digits is 129 bits, so it cannot replace the `u64` key. Uppercase or non-hex forms (about 1 in 200k rows; the generator uppercases the whole field) stay on the general byte path. Order between short and long IDs stays byte-exact.

## 4. Branches and commits

| Branch | SHA | Where | Notes |
| --- | --- | --- | --- |
| `main` | `42e75499f49d95acc7ac90c24e9c4cac2f3ce6e3` | origin | Submission baseline. Do not push. |
| `coordination` | `54e847740867e516d66b33bac28e22b2e47dd78b` | origin | This file only. That commit added the log. Later commits on this branch only append. |
| `cursor/robust-fallback-0105` | `42e75499f49d95acc7ac90c24e9c4cac2f3ce6e3` | Cursor VM only, not pushed | Cursor agent's v5 branch. No commits beyond `42e7549` yet. |
| `cursor/perf-pipeline-0105` | `20d89d6` | origin, PR #1 | Old-format experiments. Not a v5 candidate. Do not merge. |
| `cursor/fused-parser-0105` | `9a9f120344d9e75726b1c577940e2fc1cc3273df` | Cursor VM only | Abandoned fused parser. Not a v5 candidate. |

## 5. Results

Wall and CPU are local unless the row says "scorer". Baseline is `42e7549`.

| Branch | Commit | Change | v5 20M wall | v5 20M CPU | vs 42e7549 | Verification |
| --- | --- | --- | --- | --- | --- | --- |
| `main` | `42e7549` | 1,024 locked shards, contiguous byte arenas | scorer 1.7430s; Cursor VM 8-run median 1331 ms (min 1287) | Cursor VM mean 9.17s (user 8.22 + sys 0.95) | baseline | Scorer accepted it. Claude reported fixtures, official v5 dev answer, v5 20M, and v5 60M matching before the push. |
| `cursor/robust-fallback-0105` | none yet | 128-bit user/click intern, not committed | — | — | — | Not run. |

## 6. Log

### 2026-11-22 03:05 UTC - Cursor agent

Profiled `42e7549` (`/tmp/ss-work/bin-v5`, `RUSTFLAGS="-C target-cpu=x86-64-v3"`) on v5 20M with `cpu-clock:u`. One profiled run: lookups 8 ms, links 106 ms, parse_events 768 ms, dedup_events 125 ms, parse_clicks 150 ms, sessions 290 ms, write 45 ms, wall 1493 ms. An 8-run sample of the same binary on this VM had median wall 1331 ms and mean CPU 9.17s. This VM is faster than the scorer (1.33s vs 1.743s) on the same commit, so a local time under 1.1s is not evidence the scorer will clear ~1.116s. The ratio on this one pair of measurements is about 1.31.

Userspace samples, largest first:

- `Ids::over_key` 26.9%. Inside that function the hottest sites are the failed `lock cmpxchg` (the shard mutex is actually contended), the hashbrown group compare, and the load of the byte-range index. The 33-byte `bcmp` runs after the hash matches.
- Event-parse closure about 20%, plus a second parse helper about 4%.
- Dedup mix/fold about 8.3%.
- Session fold about 7.9%, quicksort about 1.9%.
- `parse_ts_cached` 5.0%.
- `memcmp` 4.7% (includes the long-ID byte compare).
- Click-parse closure 3.6%.
- `IdLookup::get` 3.4%, `NameTable::get` 2.7%, `ids::to_vec` 1.3%.

Generator check, from the current `generator.zip` sources: `write_id_suffix` emits 32 lowercase hex digits when `(scramble & 7) == 0`, otherwise base36 of 1 to 8 chars. Prefixes are fixed: `u` user, `e` event, `k` click. `case_variant` is 0.000005 and `uppercase_last` uppercases the entire field, so the rare variant is a fully uppercase id, not a single changed character. Those must not share a packed key with the lowercase form. Packing 32 lowercase hex digits into a `u128` preserves byte order of those digits; the prefix is an extra byte and has to be ordered first. A short packed `u64` and a long id still have to be compared as bytes (`u` vs `u000…`, `u2` vs `u000…`).

Plan for the Cursor branch only: keep `u64` keys. For lowercase prefix + 32 hex digits, store `(prefix, u128)` in the sharded table and compare the two integers. Uppercase and every other long id stay on the byte arena. `ids.cmp` for two packed keys compares the prefix byte, then the `u128`, which matches lexicographic order; every other pair still goes through the bytes. Repeated user ids (`u` prefix) can use a thread-local cache of that `u128`. Click ids stay on the shared packed table because they are unique and their order is the tie-break. No event-dedup changes in `fast.rs`.

Finding for the event-ID path, measured earlier on this VM and then removed from the working tree so it does not collide: not interning long event ids (copy the bytes, partition by a content hash, memcmp in dedup) was an 8-run median of 1107 ms wall and 7.50s CPU against 1331 ms and 9.17s for `42e7549`. Dedup got slower than the shard baseline on that build because equality chased copied bytes. A 16-byte integer compare, instead of memcmp, is the piece that build did not have. That experiment is not a commit and is not being continued here.
