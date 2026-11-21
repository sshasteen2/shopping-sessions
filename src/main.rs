mod csv;
mod engine;
mod intern;
mod pq;
mod ts;
mod uf;

#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() {
    engine::run();
}
