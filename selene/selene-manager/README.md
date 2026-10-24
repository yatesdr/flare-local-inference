# selene-manager

The Selene manager (binary `selene`): a Linux CPU-only cache service for Flare
instances sharing one host. It owns the prefix tree, record admission, deduplication, LRU eviction, row compaction,
leases, DRAM retention, NVMe writes and recovery. It never accesses GPU memory.
The client contract is [PROTOCOL.md](PROTOCOL.md), protocol version 1.

## Build and run

Use Rust 1.87 or newer (validated with 1.94.1 on Linux). Serving is async on
tokio and speaks gRPC through tonic and prost over a Unix socket; it also
depends on `blake2` and `twox-hash`.

```sh
cargo build --release --locked
mkdir -p /run/selene /var/lib/selene
target/release/selene \
  --socket /run/selene/pool.sock \
  --root /var/lib/selene \
  --memory 8G --nvme 256G --staging 64M \
  --metrics 127.0.0.1:9108
```

Run with the same effective uid and gid as the replicas. A missing
socket directory is created with permissions 0770, an existing one keeps its
mode and ownership, and the socket itself is 0660. One process exclusively
locks each root, and startup refuses to replace a live socket. A stale socket
from a killed manager is removed once startup confirms nothing is listening. The
NVMe file system must support 4096-byte O_DIRECT reads/writes, rename and
file/directory sync. Do not use a network file system.

Serving other hosts is opt-in (`--listen-peer`, `--listen-data`, `--listen-admin`,
`--upstream`; see [docs/network-operations.md](docs/network-operations.md)). The manager
prints a warning at startup when any listener is enabled, and another when RDMA is
enabled, which is experimental and cannot be combined with TLS.

Options accept bytes or binary K/M/G/T suffixes. Defaults:

| Option | Default |
|---|---|
| `--name` | `default`; names the shared-memory instance and the default socket |
| `--socket` | `/run/selene/<name>.sock` |
| `--root` | none; records stay in DRAM until evicted |
| `--memory` | 1 GiB |
| `--nvme` | 0 without `--root`; required with `--root` |
| `--staging` | 256 MiB (minimum 8 MiB) |
| `--write-budget` | 0 (unlimited) |
| `--metrics` | disabled; when set, HTTP `GET /metrics` |

The arena and staging are preallocated files under `/dev/shm/selene/<name>`,
mapped shared with THP advice and split into 4096-aligned, reference-counted
size-class extents, so the shared-memory mount must hold at least
`--memory + --staging`. Size classes round allocations to powers of two; usable
record capacity can therefore be lower than the configured mapping size.
Budgets cover cache/storage extents, not total process RSS: trees, checksum
tables, bounded frame channels and each disk worker's small aligned scratch
buffers also consume memory. NVMe admission includes the 4096-byte manager
envelope and the complete padded Flare record. All namespaces share these
budgets. Records whose rounded extent exceeds the arena stream through staging
and also charge a fixed two-window staging hold; a record that fits the arena
waits for DRAM room, and streams as well when the arena cannot make room for
it.

A DRAM publication is acknowledged before NVMe durability. A manager crash can
lose recently acknowledged records whose ancestor chains have not reached NVMe.
Durable records survive replica/manager restarts. Disk-full writes retain
non-durable DRAM copies and retry at 100 ms intervals. Clients receive Budget
retries and a Room notification when capacity becomes available. DRAM eviction
requires durability; NVMe eviction removes both copies. SIGTERM/SIGKILL ends the
process; recovery removes unfinished files and orphaned descendants on restart.

The service runs a state thread that owns the cache, a tokio gRPC runtime, an
acceptor, one ordered writer per namespace, and an optional metrics thread.
Each connection has bounded queues, four open operations per direction and two
window credits per direction. Metrics include namespace/tier inventory and hits,
Put/Get outcomes, evictions, compactions, disk bytes/time/errors, connections,
leases, open operations, cancellations, discarded frames and arena allocations.

## Docker and systemd

```sh
docker build -t selene .
docker run --rm --name selene \
  --user "$(id -u):$(id -g)" --pid=host --network=host --ipc=host \
  -v /run/selene:/run/selene \
  -v /var/lib/selene:/var/lib/selene \
  selene --root /var/lib/selene --nvme 256G --memory 8G \
  --metrics 127.0.0.1:9108
```

Create `/run/selene` and the root directory with ownership matching the runtime
uid/gid. The arena and staging live in `/dev/shm/selene` on the host, so the
manager and every Flare instance must share that mount: run all of them with
`--ipc=host` as above, or bind-mount the same directory at `/dev/shm/selene`
in each container. The host's `/dev/shm` must hold `--memory + --staging`.
Share the socket directory and a PID namespace with the instances so the peer
pid matches `SO_PEERCRED`; the example uses the host PID namespace. No GPU
flags, CUDA libraries or privileges are required, and the image has no CUDA
dependency.

`deploy/` has a compose file for one host (`compose.yaml`), one for a storage node
that serves other managers over mutual TLS (`compose-storage.yaml`), and a systemd
unit (`selene.service`). `Dockerfile.rdma` builds the experimental RDMA variant.
Network deployment is described in [docs/network-operations.md](docs/network-operations.md).

## Tests

Run on Linux. The default run executes the library tests, 68 L0 tests, 11 peer L0 tests
(two managers over TLS) and the protocol digest test, including subprocess `SIGKILL`
tests and exact-byte `/dev/shm` and real-NVMe throughput tests. The L4 network-emulation
test is ignored unless asked for, because it needs root and changes host networking
state. There are no GPU requirements. Set scratch to a disposable directory on a real
local NVMe file system; each test owns and removes its own subdirectory. See
[docs/qualification.md](docs/qualification.md) for what each suite proves.

```sh
SELENE_NVME_SCRATCH=/var/tmp/selene-scratch cargo test --release --locked
cargo test --release --locked --features rdma    # needs libibverbs and cmake to build
cargo fmt --all -- --check
cargo clippy --all-targets --locked -- -D warnings
```

The suite covers tree parity; eviction/compaction; accepted record shape and
replacement; deduplication/limits; malformed payloads; ordered chain durability;
manager crash/recovery; streaming/refill/Oversize and the admission charge;
disk-full/Budget/Room; restore windows under a full durable budget;
Cancel/EOF/replica SIGKILL and I/O lifetimes; paused Put windows and quarantine;
snapshots/deltas/Resync/namespaces; socket and shared-memory directory modes;
round-robin service and bounded cleanup; generation/op fencing; peer forwarding,
remote fetch and peer loss; and throughput. Fault gates exist only in the library's
test configuration and are not exposed by the executable.

The throughput group runs an in-process manager and one client across three
modes - DRAM through shared memory, staged streaming through shared memory, and
staged streaming on NVMe - with 128 64 KiB pages (about 8 MiB) per record and
six samples per mode, the first a warm-up. It compares the exact fetched bytes
and requires a DRAM median of at least 1 GB/s in both directions; the streamed
modes report their medians without a floor. Set `SELENE_NVME_SCRATCH` to a
disposable directory on a local NVMe file system for the NVMe mode. The test
needs room for the DRAM record in `/dev/shm`; test containers need
`--shm-size=4g`.

```sh
SELENE_NVME_SCRATCH=/var/tmp/selene-scratch cargo run --release --locked --example throughput
```

The Flare client and GPU integration validation live in the Flare repository.
