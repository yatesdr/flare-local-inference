# Operating selene over a network

The manager is a Linux, CPU-only service. Flare replicas reach it over a Unix socket and move
bytes through `/dev/shm/selene`; the manager alone talks to NVMe and to remote peers.
This document covers sizing, deployment, TLS, upstreams, metrics, upgrades and the experimental
RDMA path.

## 1. Choose a topology

| Topology | Shape | Manager flags |
|---|---|---|
| S1 | One host, one or more replicas, one pool | `--root`, `--memory`, `--nvme` |
| S2 | Replicas on a near host, pool on a storage host | near: `--upstream NAME=ADDR`, `--tls-*`; storage: `--listen-peer`, `--listen-data`, `--tls-*` |
| S3 | Several storage nodes behind one near manager | near: repeated `--upstream`; storage: as S2 |

Every inference host runs a local manager. A host without one cannot use the cache. Membership is
static: adding or removing a storage node takes effect on restart. A record that changes owner is
a miss, never a migration.

## 2. Sizing

- **`--memory`** is the shared DRAM arena. Size it to the prefix working set you want to keep hot
  (for example the last 30 minutes of distinct prompts), not to the model size. Start at 8-16 GiB
  per pool.
- **`--staging`** holds windows for records larger than the arena and bytes in transit to peers.
  Keep it at least `2 × max_window_bytes` per concurrently active connection; 512 MiB is a safe
  default, 1 GiB on storage nodes. A staging region that is momentarily full refuses `BUDGET` and
  arms a Room notification; a record whose own two-window hold exceeds the whole region can never
  be admitted and is refused `OVERSIZE`.
- **`--nvme`** is the durable budget. Admission charges the envelope plus the arena extent's
  allocator size class, and staging or draining extents count while held, so leave about 10%
  headroom over the record bytes you expect.
- **`--write-budget`** optionally caps write-through per client connection (bytes/s, `0` is
  unlimited). Gets are never throttled.

**Known limit: a remote restore fetches the whole missing chain into DRAM.** A near manager
allocates every destination before the transfer, because the serving path only plans windows over
local records. A chain whose records together exceed the near's `--memory` is therefore a cache
miss - the replica recomputes it - even when the storage node could stream it in pieces. There is
no windowed remote restore yet, so size `--memory` on near hosts for the largest chain you expect
to restore. The fetch makes room by eviction like any admission, and the chain stays pinned from
the reservation through adoption and the serving Get.

The root must support `O_DIRECT`, atomic rename, `fsync` and an exclusive lock. Do not put it on
NFS.

## 3. Running

The manager and its replicas must run with the same effective uid and gid. They must share the
socket directory and the shared-memory directory at identical paths (bind mounts in containers).

At startup the manager:

1. takes an exclusive `flock` on `/dev/shm/selene/<name>/lock` (a second manager with
   the same `--name` exits);
2. creates fresh `arena-<boot_nonce>` and `staging-<boot_nonce>` files, `fallocate`s them, and
   removes older boots' files;
3. creates or reopens the socket, after checking peer credentials on every connection.

The arena and staging files are scratch; they are never reused across manager boots. A restarted
manager therefore starts with an empty DRAM tier and recovers NVMe records from the root.

## 4. TLS for peer connections

Peer control channels use mutual TLS. Generate a small CA, a server certificate for the storage
node and a client certificate for each near manager:

```sh
openssl req -x509 -newkey rsa:3072 -nodes -days 3650 \
  -keyout ca-key.pem -out ca.pem -subj '/CN=selene-ca'
# Server (SAN must contain the DNS name the near manager dials)
openssl req -newkey rsa:3072 -nodes -keyout server-key.pem -out server.csr -subj '/CN=storage'
printf 'subjectAltName=DNS:storage\n' > server.ext
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca-key.pem -CAcreateserial \
  -days 3650 -extfile server.ext -out server.pem
# Client (one per near host)
openssl req -newkey rsa:3072 -nodes -keyout client-key.pem -out client.csr -subj '/CN=near-1'
openssl x509 -req -in client.csr -CA ca.pem -CAkey ca-key.pem -CAcreateserial \
  -days 3650 -out client.pem
```

Pass `--tls-cert`, `--tls-key` and `--tls-ca` to both sides. `--listen-peer` requires all three
and `--listen-data`; `--listen-admin` also requires mTLS. TLS protects the peer control channel
only; bulk bytes travel on the data channel authenticated by a per-session token and are not
encrypted. At startup the manager prints a warning when any peer, data or admin listener is
enabled. Keep the keys out of the image and
mount them read-only (`deploy/compose-storage.yaml`).

## 5. Upstreams and placement

`--upstream NAME=ADDR[,WEIGHT]` is repeatable; weights are integers from 1 to 1,000,000 (default
1). The owner of a chain is chosen by rendezvous hashing over all configured upstreams, so a
disconnected owner still owns its share: its records miss until it returns, and they are never
silently taken over. Write-through forwards a record after it is stored locally, parent first; a
failed forward is retried and then abandoned without affecting local serving. `--retain-remote`
(default true) keeps fetched remote records in local DRAM when there is room.

Each upstream's catalog is merged into the local namespace, so replicas can look up records that
only exist upstream. A Get for such a chain fetches it into local DRAM and then serves it through
the normal window path; the fetched copy stays resident (`--retain-remote`). When an upstream
disconnects, its catalog entries are removed again and those lookups miss.

## 6. Metrics

Point Prometheus at `--metrics ADDR:PORT`. The manager exposes the `cm_` series below; the
prefix is stable even where the name is generic.

- Per namespace, per tier (`memory`, `disk`): `cm_records{namespace,tier}`,
  `cm_bytes{namespace,tier}`, `cm_hits_total{namespace,tier}`.
- Publications: `cm_puts_total{outcome}` with `ok`, `error`, `present`, `queue`, `budget`,
  `inflight` and `oversize`.
- Restores: `cm_gets_total{outcome}` with `ok` and `error`.
- NVMe: `cm_nvme_bytes_total`, `cm_nvme_seconds_total`, `cm_nvme_reserved_bytes`,
  `cm_nvme_errors_total`.
- Memory and cleanup: `cm_memory_allocated_bytes`, `cm_staging_allocated_bytes`,
  `cm_cleanup_records`.
- Work in flight: `cm_connections`, `cm_leases`, `cm_open_ops`, `cm_pending_puts`,
  `cm_pending_gets`.
- Maintenance: `cm_evictions_total`, `cm_compactions_total`, `cm_cancels_total`,
  `cm_unknown_frames_total`.
- Peers: `cm_forwarded_total`, `cm_forward_failed_total`, `cm_forward_retries_total`.

A rising `cm_puts_total{outcome="budget"}` with little eviction means the pool is genuinely full:
add DRAM, NVMe or review quotas. A rising `cm_forward_failed_total` means an upstream keeps
rejecting records; check its quota and the upstream link.

## 7. Upgrades and protocol versions

- The manager and the Flare client negotiate `PROTOCOL = 1` in `Hello`. A mismatch is refused and
  the replica stays GPU-only; upgrade both sides together.
- Field numbers are frozen after release. A change that needs new fields adds them; it never
  reuses or changes an existing number.
- Namespace directories use the 64-hex namespace id and a versioned descriptor (`FLCDSC2`).
  Roots written by the v1 protocol are refused at startup with a message naming the directory;
  there is no migration. Start a new root for this version.
- Restarting a manager drops the DRAM tier. Replicas reconnect with backoff (0.1 s to 5 s) and
  rebuild their catalogs; duplicate publications resolve as `PRESENT`.
- `--name`, `--socket`, upstream names and TLS settings are restart-only.

## 8. Experimental RDMA (Soft-RoCE included)

The default image and build never link libibverbs. Build with `--features rdma` (or use
`Dockerfile.rdma`) and select it with `--peer-transport auto|rdma`. RDMA is experimental: the
manager prints a warning at startup when it is enabled, and every RDMA log line says so. RDMA
bytes are not encrypted, so a manager refuses to start with RDMA enabled and any `--tls-*` option,
and says why. Peer listening requires TLS, so a build with the `rdma` feature serves peers only
with `--peer-transport stream` until the data channel is encrypted.

Soft-RoCE on a test host:

```sh
sudo modprobe rdma_rxe
sudo rdma link add rxe0 type rxe netdev eth0   # any active Ethernet device
ibv_devices                                    # rxe0 should appear
```

The container needs `--device /dev/infiniband` and `--ulimit memlock=-1:-1`. If local setup
fails, the manager logs one line and falls back to stream; a live connection never silently
changes transport.

## 9. Troubleshooting

| Symptom | Check |
|---|---|
| Manager exits at startup | `flock` held by another manager with the same `--name`; root not O_DIRECT-capable; v1 descriptor on the root |
| Replicas stay GPU-only | Socket path and permissions, uid/gid equality, `/dev/shm/selene` mounted at the same path in both containers |
| `BUDGET` refusals | Arenas/staging full or write budget exhausted; inspect `windows_quarantined` for a dead client's windows |
| Peer never connects | TLS SAN/CA mismatch, `--listen-data` reachable, stream token rejection in the log |
| RDMA selected but transfers fail | `ibv_devinfo` port state, `memlock` limit, rkey registration of both regions |
