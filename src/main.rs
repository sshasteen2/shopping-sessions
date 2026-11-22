mod bulk;
mod csv;
mod engine;
mod fast;
mod ids;
mod intern;
mod pq;
mod rogue;
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
    // ENGINE=old is the original engine. ENGINE=fast is the aa39117 partitioned
    // engine. Both stay as references. The default engine is the rogue rewrite.
    let engine = std::env::var("ENGINE").ok();
    if engine.as_deref() == Some("old") {
        engine::run();
        return;
    }
    let n = ncpu();
    rayon::ThreadPoolBuilder::new()
        .num_threads(n)
        .build_global()
        .ok();
    if engine.as_deref() == Some("fast") {
        fast::run(n);
        return;
    }
    rogue::run(n);
}
