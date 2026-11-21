mod bulk;
mod csv;
mod engine;
mod fast;
mod ids;
mod intern;
mod pq;
mod rows;
mod ts;
mod uf;

#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

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

fn main() {
    // ENGINE=old runs the previous engine, for side-by-side checks.
    if std::env::var("ENGINE").as_deref() == Ok("old") {
        engine::run();
        return;
    }
    let n = ncpu();
    rayon::ThreadPoolBuilder::new()
        .num_threads(n)
        .build_global()
        .ok();
    fast::run(n);
}
