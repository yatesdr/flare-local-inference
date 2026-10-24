# selene-manager

The Selene manager (binary `selene`): a Linux, CPU-only service that lets Flare instances
share KV-cache records. Instances announce what they hold; the manager answers lookups from
that index, brokers every copy between instances (it never moves a byte through a message), keeps
its own store of records in DRAM and on local NVMe for instances to spill into, and owns the
shared-memory windows the copies go through. It never accesses GPU memory and holds no document
content in its index.

The protocol is `selene.v2` ([proto/selene.proto](proto/selene.proto)); the state machines and
fencing rules are in [PROTOCOL.md](PROTOCOL.md). Running it is in
[docs/operating.md](docs/operating.md), serving other hosts in
[docs/network-operations.md](docs/network-operations.md), and what the tests prove in
[docs/qualification.md](docs/qualification.md).

## Build and run

Use Rust 1.88 or newer (validated with 1.94.1 on Linux). Serving is async on tokio and speaks
gRPC through tonic and prost over a Unix socket; the crate also depends on `flare-selene` (by
path, for the identity functions), `blake2` and `twox-hash`.

```sh
cargo build --release --locked
mkdir -p /run/selene /var/lib/selene
target/release/selene \
  --socket /run/selene/pool.sock \
  --root /var/lib/selene \
  --memory 8G --nvme 256G --staging 512M \
  --metrics 127.0.0.1:9108
```

Run with the same effective uid and gid as the instances. A missing socket directory is created
with permissions 0770, an existing one keeps its mode and ownership, and the socket itself is
0660. One process exclusively locks each root, and startup refuses to replace a live socket. A
stale socket from a killed manager is removed once startup confirms nothing is listening. The NVMe
file system must support 4096-byte O_DIRECT reads and writes, rename and file/directory sync. Do
not use a network file system.

Options accept bytes with binary K/M/G/T suffixes, and durations with ms/s/m/h/d.

| Option | Default |
|---|---|
| `--name` | `default`; names the shared-memory instance and the default socket |
| `--socket` | `/run/selene/<name>.sock` |
| `--root` | none; records stay in DRAM until evicted |
| `--memory` | 1 GiB |
| `--nvme` | 0 without `--root`; required with `--root` |
| `--staging` | 256 MiB (minimum 8 MiB) |
| `--write-budget` | 0 (unlimited) |
| `--listen` | none; serve instances on other hosts at ADDR:PORT |
| `--tls-cert`, `--tls-key` | none; TLS for `--listen` |
| `--rdma` | off; experimental, needs the `rdma` feature, not with TLS |
| `--source-wait` | 10s; wait for remembered instances before serving |
| `--instance-retention` | 7d; forget instances not seen for this long |
| `--metrics` | disabled; when set, HTTP `GET /metrics` |
| `--config` | none; a TOML file with the same keys, flags override |

The manager prints a warning at startup when network listening is enabled and when RDMA is
enabled, and refuses to start with RDMA and TLS together. See
[docs/network-operations.md](docs/network-operations.md).

The arena and staging are preallocated files under `/dev/shm/selene/<name>`, so the shared-memory
mount must hold at least `--memory + --staging`. Budgets cover record and window extents, not total
process RSS.

## Docker and systemd

```sh
docker build -f selene-manager/Dockerfile -t selene .   # from the directory above this one
docker run --rm --name selene \
  --user "$(id -u):$(id -g)" --pid=host --network=host --ipc=host \
  -v /run/selene:/run/selene \
  -v /var/lib/selene:/var/lib/selene \
  selene --root /var/lib/selene --nvme 256G --memory 8G \
  --metrics 127.0.0.1:9108
```

Create `/run/selene` and the root directory with ownership matching the runtime uid/gid. The
manager and every instance must share `/dev/shm/selene`: run them with `--ipc=host` as above, or
bind-mount the same directory at `/dev/shm/selene` in each container. Share the socket directory
and a PID namespace with the instances so the peer pid matches `SO_PEERCRED`. No GPU flags, CUDA
libraries or privileges are required, and the image has no CUDA dependency.

`deploy/` has a compose file for one host (`compose.yaml`), one that also serves other hosts over
TLS (`compose-network.yaml`), and a systemd unit (`selene.service`). `Dockerfile.rdma` builds the
experimental RDMA variant.

## Source layout

| Path | What it holds |
|---|---|
| `src/pool/` | The pool actor: sessions, the announcement index, lookup, claims and fences, put, readiness. |
| `src/store.rs`, `src/disk.rs`, `src/tree.rs`, `src/record.rs`, `src/arena.rs` | The manager's own store: DRAM arena, NVMe records, eviction, durability, recovery. |
| `src/local.rs` | Shared-memory regions, windows and process-exit (pidfd) reclaim. |
| `src/control.rs`, `src/config.rs`, `src/main.rs` | Startup, listeners, shutdown, flags. |
| `src/data/`, `src/grants.rs`, `src/regions.rs` | The stream and RDMA data-channel modules and their tests; wired to instances in a later change. |

## Tests

Run on Linux. See [docs/qualification.md](docs/qualification.md) for what each suite proves and the
counts. Set scratch to a disposable directory on a local NVMe file system; each test owns and removes
its own subdirectory, and the test fixture never removes a directory it did not create.

```sh
SELENE_NVME_SCRATCH=/var/tmp/selene-scratch cargo test --release --locked
cargo test --release --locked --features rdma    # needs libibverbs and cmake to build
cargo fmt --all -- --check
cargo clippy --all-targets --locked -- -D warnings
```

The tests that start managers need `/dev/shm` room; test containers need `--shm-size=4g`. The
throughput smoke test (`l0_14`) puts records through the pool protocol and compares the bytes it
gets back; `cargo run --release --locked --example throughput` runs it with reported rates.

The Flare instance-side client and its GPU integration live in the Flare repository.
