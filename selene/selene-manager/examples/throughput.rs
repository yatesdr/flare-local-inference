#[path = "../tests/support/mod.rs"]
mod support;

fn main() {
    support::throughput::run(false);
}
