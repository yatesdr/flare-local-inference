# Operating selene-manager

This runbook covers the deployment model, sizing, refusal reasons, metrics, restarts and
upgrades for `selene`. The wire contract is [PROTOCOL.md](../PROTOCOL.md) and
[proto/selene.proto](../proto/selene.proto); building, testing and containers are in
[README.md](../README.md); remote hosts are in [network-operations.md](network-operations.md).

## Deployment model

- **One manager per pool.** A pool is the set of Flare instances that share records. Run one
  `selene` for it, on a host with local NVMe if you want records to outlive the instances.
  Instances do not talk to each other; they talk to the manager, which brokers every copy.
- **The manager owns its NVMe directory exclusively.** `--root` is locked
  (`<root>/.lock`): a second manager on the same directory refuses to start. Do not put the
  directory on a network file system, share it between managers, or edit it while the manager runs.
- **Instances configure only their private tiers plus the pool address.** Each Flare instance
  keeps its own DRAM and NVMe settings for its private tiers and adds
  `--prefix-cache-pool <address>` (`unix:/path` for a manager on the same host, `host:port` for
  a remote one) and `--prefix-cache-pool-tenant`. Everything shared, the index, holds, windows,
  the manager's own store and the network, lives in the manager.
- **Never put the manager behind a load balancer.** A session is stateful: the index entries an
  instance announced, the claims in flight and the windows granted to it all live in the one
  manager process it is connected to. A balancer that spreads instances over several managers
  splits the pool, and one that moves a stream between them loses its claims. Give each pool one
  address.
- **Same host, same identity.** The manager and every instance that uses shared memory must
  run with the same effective uid and gid, share the socket directory and `/dev/shm/selene` at
  identical paths, and (in containers) share a PID namespace so peer credentials are real.
  Instances that cannot meet this still work over `--listen`, without shared memory.

## Sizing

The service is CPU-only and never touches GPU memory. Three budgets are configured
separately and shared by every scope: `--memory` (the DRAM arena), `--staging` (windows) and
`--nvme` (durable records). A fourth, `--write-budget`, limits how fast one process may admit
puts.

`--memory` is the arena for the manager's own records served without touching NVMe. Size it for
the hot working set, not for everything. Allocation uses power-of-two size classes, so a record
consumes its length rounded up and usable capacity can be materially below the mapping size.
A record whose rounded extent exceeds the whole arena is not dropped (with `--root` it streams
to NVMe) but it will never be served from DRAM; without `--root` it can never be admitted and is
refused `OVERSIZE`.

`--staging` holds the windows of copies between instances and the reads of NVMe records. A
**GPU export is accepted only after staging for the whole export has been reserved** (one
block, rounded up to the allocator's size class), so the source's GPU pin lasts only for its
device-to-host copy: with too little free staging the claim fails `NO_WINDOWS` at once and
nothing is pinned. Host-tier exports that do not fit stream with two windows at a time. Provision
staging for the largest export you expect to run concurrently times `export_slots` (default 2 per
instance), plus the quarantined bytes below. The minimum is 8 MiB and the default 256 MiB.

`--nvme` is the durable budget and includes the 4096-byte envelope per record. It must exceed the
largest record an instance can put, or the put is refused `OVERSIZE`. Keep it above `--memory`:
DRAM eviction needs durability, so a full NVMe budget stops DRAM from making room.

`--write-budget` is a token bucket in bytes per second per process, with a burst of
`max(rate, 1 MiB)`; `0` (the default) is unlimited. It gates admission of puts, not the device.

The arena and staging are preallocated files in `/dev/shm/selene/<name>`, so the shared-memory mount
must provide at least `--memory + --staging` for each manager. A container's default 64 MiB
`/dev/shm` is far too small; use `--shm-size` or the host IPC namespace. New region files are
created at every start and stale ones are reclaimed, so a killed manager never blocks a restart.

Tenancy separates keys, not memory: every instance with shared memory maps the same arena and
staging files, so any of them can read another tenant's mapped bytes. Tenants are not a security
boundary on one host. When tenants must not see each other's data, give each trust boundary its
own uid (with distinct manager names) or its own host.

## Startup

At start the manager prints a warning for each option that widens its exposure:

- `selene: warning: network listening enabled on ADDR (TLS|plaintext)` when `--listen` is set;
- `selene: warning: RDMA enabled (experimental)` when `--rdma` is set in a build that has it.

It refuses to start, and says why, when `--rdma` is combined with TLS (RDMA transfers are not
encrypted), when `--rdma` is given in a build without the `rdma` feature, when only one of
`--tls-cert` and `--tls-key` is given, or when a certificate and key do not match.

Startup order is the root lock, NVMe recovery, shared-memory reclaim, then the socket. A second
manager on the same root or the same `--name` exits at once; a stale socket is replaced only after
confirming nothing is listening.

## Readiness and restarts

The manager is `READY` (lookups, claims and puts are served) when disk recovery is complete and
every instance it remembers has re-announced its holdings or the bounded wait has run out.
Until then lookups, claims and puts answer `NOT_READY` and instances use their local tiers;
sessions, notices and resyncs are accepted so sources can sync.

- The instance ids the manager served are kept in `<root>/instances` (replaced atomically).
  Instance ids are stable across restarts of an instance, so one that restarts together with
  the manager is satisfied as soon as it syncs.
- `--source-wait` (default 10s, counted from process start) bounds the wait for an instance that
  never returns. `--instance-retention` (default 7d) forgets instances not seen for that long.
  A manager with no `--root` has nothing to remember and is ready immediately.
- Records the manager held only in DRAM are lost with it; NVMe records are recovered and start in the
  disk tier. A put is complete (and the writer may drop its copy) only at `Stored`.
- `SIGTERM` or `SIGINT` drains: new sessions, lookups, claims and puts are refused, claims in
  flight run to a terminal state (bounded by their deadlines), then sessions are closed with
  `SHUTDOWN`.

## Refusal reasons

Claims are refused with `FAILED` and a reason; puts with `PutRefused`. Instances treat every
refusal as a normal outcome and move to another candidate, another tier, or recompute.

| Reason | Meaning | Operator action |
|---|---|---|
| `NOT_READY` | The manager is recovering, waiting for sources, or draining. | None; check `cm_pool_state`. |
| `CLAIM_LIMIT` | The requester already has its claim slots in flight (default 2). | None; limits do not scale with concurrency. |
| `SOURCE_LIMIT` / `SOURCE_BUSY` | The source already serves its export slots (default 2). | None. |
| `UNKNOWN_SOURCE` / `STALE_SOURCE` | The named source or entry is gone, or the source reconnected. | None; the index catches up. |
| `NO_WINDOWS` | Staging cannot hold the export, or too much of it is quarantined. | Raise `--staging`; look at `cm_pool_quarantined_bytes`. |
| `NOT_FOUND`, `GENERATION_CHANGED`, `CHAIN_GAP`, `NO_HEADROOM` | The source no longer has it, or has no room to pin it. | None. |
| `SOURCE_TIMEOUT` | The source did not answer within `pin_deadline_ms` (default 250). | Look at the source's load. |
| `SOURCE_LOST` / `REQUESTER_LOST` | The other side's stream ended. | Look at the instance that went. |
| `COPY_ERROR` / `CHECKSUM` | A read, write or checksum failed, or the copy was not exact. | Alert on repeats; check the device. |
| `DEADLINE` / `NO_PROGRESS` | The claim ran past its deadline or stopped making progress (3s). | Look at copy throughput. |
| put `PRESENT` | Already stored and committed: success, nothing transferred. | None. |
| put `BUDGET` | Out of budget or full: the writer moves the record to its next tier at once. | Add DRAM/NVMe if sustained. |
| put `OVERSIZE` | Can never fit in this configuration. | Raise the budget or add `--root`. |
| put `QUEUE` | The writer has `max_open_puts` (4) open. | None. |
| put `INVALID` | Malformed, metadata that does not match the put, or a mode not allowed. | Alert: client and manager disagree. |
| put `IO` | A device error. | Check the NVMe and the file system. |
| put `PUT_NOT_READY` | Not ready (see Readiness). | None. |

## Quarantine

Memory is reused only when every process that could still touch it has said it stopped, or is
confirmed gone. If a process cannot say so (it reports `QUARANTINED`, or its stream ended while the
process lives and it has not reported on a new session), the windows it held stay out of service
until it exits, or until the manager does. `cm_pool_quarantined_bytes` and `ReadinessInfo` show
the total; a nonzero value that does not fall means an instance is hung, not a leak.

## Metrics

`GET /metrics` on `--metrics HOST:PORT` returns Prometheus text (`cm_` prefix, content type
`text/plain; version=0.0.4`); only that path is served.

| Series | Meaning | Alert on |
|---|---|---|
| `cm_pool_state`, `cm_pool_ready` | 1 recovering, 2 awaiting sources, 3 ready, 4 draining; and 1 when ready. | Not ready for longer than `--source-wait`. |
| `cm_pool_sessions` | Live instance sessions. | Dropping to zero while instances serve. |
| `cm_pool_sources_synced`, `cm_pool_sources_expected` | Sources with an installed snapshot; remembered ones still awaited. | `expected` that does not reach 0. |
| `cm_pool_index_entries` | Entries announced to the index. | Unbounded growth. |
| `cm_pool_claims_in_flight`, `cm_pool_claim_records`, `cm_pool_lookups_queued` | Work in flight and records kept for fences and replies. | Records that never fall. |
| `cm_pool_claims_total{state,reason}`, `cm_pool_claims_refused_total{reason}`, `cm_pool_lookups_total{status}` | Outcomes. | `COPY_ERROR`, `CHECKSUM`, `NO_WINDOWS` rising. |
| `cm_pool_puts_open` | Puts in flight. | |
| `cm_pool_quarantined_bytes` | Window bytes out of service. | Anything sustained. |
| `cm_puts_total{outcome}` | `ok`, `error`, `present`, `queue`, `budget`, `inflight`, `oversize` for the store's puts. | Any `error`; `budget` growing faster than `ok`. |
| `cm_gets_total{outcome}` | `ok`, `error` for the store's reads. | Any `error`. |
| `cm_nvme_errors_total` | NVMe read and write failures. | Any increase. |
| `cm_nvme_bytes_total`, `cm_nvme_seconds_total` | Durable bytes written and disk time. | Disk time growing faster than bytes. |
| `cm_memory_allocated_bytes`, `cm_staging_allocated_bytes` | Arena and staging in use. | Near `--memory` / `--staging`. |
| `cm_nvme_reserved_bytes` | Rounded reservations in force. | Near `--nvme`: DRAM eviction stops. |
| `cm_evictions_total`, `cm_compactions_total` | DRAM evictions and row compactions. | Hot set not staying resident. |
| `cm_hits_total{namespace,tier}`, `cm_records{namespace,tier}`, `cm_bytes{namespace,tier}` | Hits and inventory of the manager's own records. | Hit rate collapse. |

## Recovery on disk

Recovery per scope removes leftover `.*.tmp` files, validates every `.rec` envelope, header, layout,
name, length and metadata checksum, sorts parents before children, and deletes orphaned
descendants. A record that is corrupt in a detectable way is removed; one that cannot be read
because of an I/O error fails startup without deletion, so an operator can inspect or move it. If
the disk fills during a write the manager keeps the non-durable DRAM copies and retries every
100 ms; puts are refused `BUDGET` until space returns. Never free space by deleting files under a
running manager.

## Upgrades and clearing the cache

Stop the manager, replace the binary and start it with the same `--root`, `--name` and `--socket`.
Durable records are validated and reused; instances reconnect, resync and refill the DRAM tier from
their traffic. The protocol version must match the instances: a manager that does not speak the
instance's version answers `Hello` with `VERSION_MISMATCH` and the instance runs on its local tiers
until restarted. `--name`, `--socket`, `--listen` and the TLS files are restart-only.

The root holds one opaque 64-hex directory per scope plus `.lock` and `instances`. Remove a scope
directory to drop that model's durable records, or all of them to clear the pool, only while the
manager is stopped. A changed model identity, layout fingerprint or tenant is a different scope, so
the old directory is safe to delete and cannot be reused by accident.

## Troubleshooting

| Symptom | Check |
|---|---|
| Manager exits at startup | Another manager on the same root or `--name`; a root that cannot do `O_DIRECT`; a certificate and key that do not match; `--rdma` with TLS. |
| Instances run on local tiers only | The pool address, socket permissions, uid/gid equality, `/dev/shm/selene` mounted at the same path in both containers, `cm_pool_ready`. |
| `NO_WINDOWS` | `--staging`; `cm_pool_quarantined_bytes`; many large exports at once. |
| Sustained `NOT_READY` | `cm_pool_sources_expected`: a remembered instance is not coming back; it clears after `--source-wait`. |
| Quarantined bytes do not fall | A hung instance (it will clear when the process exits). |
