# Qualification

What each test suite in this crate proves, and how to run it. The Flare-side
rungs (a real GPU client against a real manager) live with the Flare client and are
only summarized here.

## Test ladder

Each rung uses the real component below it and adds one class of risk.

| Rung | Runs on | What it proves |
|---|---|---|
| Unit | Linux CPU, no GPU | Modules in isolation: tree and catalog ordering, record format, arena allocation, disk worker, configuration, peer placement, data channels, and the store's admission and eviction rules. |
| L0 (`tests/l0.rs`) | Linux CPU, no GPU | The manager's contract through its gRPC surface: tree parity, admission and refusal classification, window grants and credits, budget and Room, eviction and compaction, chain durability, crash recovery (including subprocess `SIGKILL` at partial Data, before and after rename, and during the parent write), directory modes, failed-start cleanup, and a throughput smoke test. Run against exact-byte regions in shared memory and a real local NVMe file system. |
| Peer L0 (`tests/peer_l0.rs`) | Linux CPU, no GPU | Two managers over a real TCP peer channel with mutual TLS: forwarding, remote fetch into a client region, placement across upstreams, quota behavior, grant fencing, peer loss, and a forward that fails after the storage manager goes away. |
| L4 (`tests/peer_l4.rs`) | Linux CPU, root and a privileged container | Two manager processes in separate network namespaces joined by a veth pair with `netem` delay and bandwidth limits: forwarding and remote reads under latency and a shaped link, interrupted fetches, and teardown. Ignored by default because it changes host networking state. |
| GPU rungs | One or more GPUs, a real manager process, the Flare client | Exact bytes through the whole client-to-manager path, the shared lifecycle between replicas, and distribution equivalence against native prefix reuse. These are run from the Flare repository. |

## Running

```sh
cargo test --release --locked
cargo test --release --locked --features rdma
cargo fmt --all -- --check
cargo clippy --all-targets --locked -- -D warnings
```

Set `SELENE_NVME_SCRATCH` to a disposable directory on a local NVMe file system for
the tests that need real direct I/O; each test owns and removes its own
subdirectory. The tests need room in `/dev/shm`; in a container, pass `--shm-size=4g`.

L4 needs `ip` and `tc`, root, and a privileged container:

```sh
cargo test --release --locked --test peer_l4 -- --ignored --nocapture --test-threads=1
```

The `rdma` feature needs libibverbs and a CMake toolchain at build time
(see `Dockerfile.rdma`). Its loopback unit test skips when the host has no active RDMA
device. RDMA is experimental and is not a correctness gate.

## Limitations

- The tests cover the CPU data path and shared-memory windows. They do not include GPU
  transfer, scheduling or model execution time.
- Fault gates exist only in the library's test configuration and are not exposed by the
  executable.
- RDMA and the L4 network emulation need privileged hosts and are not part of the default run.
