# Operating selene-manager

This runbook covers sizing, refusal reasons, metrics, crash recovery and
upgrades for `selene`. The client contract is [PROTOCOL.md](PROTOCOL.md); the
build, test and container instructions are in [README.md](README.md).

## Sizing

The service is CPU-only and never touches GPU memory. Three budgets are
configured separately and shared by every namespace: `--memory` (record
retention), `--staging` (streamed transfers) and `--nvme` (durable records). A
fourth, `--write-budget`, gates how fast a connection may admit Puts. Size them
from the Flare record geometry: one record is one checkpoint of one prefix, and its
length grows with context length, rank count and plane sizes.

`--memory` is the DRAM arena for records served without touching NVMe. Size it
for the hot working set, not for the whole tree: the arena holds the records
replicas restore from, while colder records stay durable on NVMe. Allocation
uses power-of-two size classes, so a record consumes its length rounded up to
the next power of two and usable capacity can be materially below the mapping
size; records of similar size converge on one class, records of very different
sizes fragment it. A record whose rounded extent exceeds the whole arena is not
dropped — with `--root` it streams to NVMe — but it will never be served from
DRAM; without `--root` it can never be admitted and is refused `OVERSIZE`.

`--staging` is a bounded scratch region. Each streamed Put reserves a fixed
hold of two of its largest windows (a page window is at most 4 MiB unless a
single page is larger; the row rides with the last page window when it fits and
is otherwise a window of its own), and disk reads draw their windows from the
same region. The minimum is 8 MiB and the default
is 256 MiB; provision at least 8-16 MiB per concurrent streamed transfer. A
streamed record whose two-window hold exceeds the whole staging region can
never be admitted, so admission refuses it `OVERSIZE`.

`--nvme` is the durable budget and includes the 4096-byte manager envelope per
record. It must exceed the largest record a replica can publish; otherwise the
Put is refused `OVERSIZE`. Size it for the durable working set, and keep it
above `--memory`: DRAM eviction requires durability, so a full NVMe budget
stops DRAM from making room. Restore windows are transient staging traffic and
are never charged to this budget, so a full cache does not refuse restores.

`--write-budget` is a token bucket in bytes per second with a burst of
`max(rate, 1 MiB)`; `0` (the default) is unlimited. Each connection has its own
bucket and a Put is admitted only while its balance is non-negative, so the
budget is admission control, not a device throttle: an admitted record is
written at full speed, and the budget limits neither in-flight writes,
compaction nor recovery. A low budget delays the admission, and therefore the
durability, of later records; setting it around the device's sustained write
rate keeps the write queue from growing.

The arena and staging are preallocated files in `/dev/shm/selene/<name>`, so
the shared-memory mount must provide at least `--memory + --staging` for each
manager. A container's default 64 MiB `/dev/shm` is far too small; use
`--shm-size` or the host IPC namespace.

Tenancy separates cache keys, not memory: every replica runs with the manager's
uid and maps the same arena and staging files, so any replica can read another
tenant's mapped bytes. Tenants are not a security boundary, and separate
managers under one uid do not change that - they map the same shared memory.
When tenants must not see each other's data, give each trust boundary its own
uid (with distinct instance names) or its own host.

## Refusal reasons

Clients retry on the transient reasons and must not retry `OVERSIZE`,
`QUOTA` or `INVALID`. The manager never stalls a connection: a `BUDGET`
refusal arms a Room notification that arrives when capacity or the budget
frees up.

| Reason | Meaning | Operator action |
|---|---|---|
| `PRESENT` | The record is already stored; no work is needed. | None. |
| `BUDGET` | A capacity or rate limit is currently exhausted: a negative write-budget balance or no DRAM room that eviction can free. The manager arms a Room notification for the same connection. | Expect retries; a Room follows. Sustained `BUDGET` with no Room means the write budget is far below ingest, or other clients hold the space. |
| `QUEUE` | The connection's open-operation limit (four per direction) or an internal queue is full. | None; retries drain. Persistent `QUEUE` means clients are not reaping completions. |
| `OVERSIZE` | The record can never be admitted in this configuration: with `--root`, record plus envelope exceeds `--nvme`; without `--root`, its rounded extent exceeds `--memory`; or its two-window staging hold exceeds `--staging`. | Raise the budget, add `--root`, or publish a smaller record. Retrying cannot succeed. |
| `QUOTA` | A namespace quota set by an operator (DRAM or NVMe, through the admin API; zero means unlimited) would be exceeded. No Room is armed. | Raise the namespace quota or purge records. Retrying cannot succeed until the quota changes. |
| `INVALID` | Malformed request, namespace-descriptor mismatch or geometry disagreement between a replica and the manager. | Alert. Check the protocol version and that replicas and manager use the same prepared package. |
| `IO` | An NVMe read or write failed. | Alert on `cm_nvme_errors_total`; check the device and file system. |
| `INFLIGHT` | A duplicate operation for the same prefix is in flight. | None; retry after it settles. |

`OVERSIZE` describes only a record that can never fit on its own. A shortage
that depends on what else must stay resident - the retained ancestry, a
protected parent, another operation's lease - is `BUDGET`, and without `Room`
the record is simply not stored.

`QUOTA` describes a namespace budget, which only an operator can change, so
it is never transient for the client. `UNSPECIFIED` is defined by the
protocol but never produced by this manager.

## Metrics

`GET /metrics` on `--metrics HOST:PORT` returns Prometheus text (`cm_` prefix,
content type `text/plain; version=0.0.4`); only that path is served. Collect
the gauge proportions against the configured budgets, not absolute values.

| Series | Meaning | Alert on |
|---|---|---|
| `cm_connections` | Live replica connections. | Dropping to zero while replicas serve. |
| `cm_puts_total{outcome}` | `ok`, `error`, `present`, `queue`, `budget`, `inflight`, `oversize`. | Any `error`; `budget` growing faster than `ok`; any `oversize`. |
| `cm_gets_total{outcome}` | `ok`, `error`. | Any `error`. |
| `cm_nvme_errors_total` | NVMe read and write failures. | Any increase. |
| `cm_nvme_bytes_total`, `cm_nvme_seconds_total` | Durable bytes written and disk time. | Disk time growing faster than bytes; write rate far from `--write-budget`. |
| `cm_memory_allocated_bytes` | Arena bytes in use. | Near `--memory`, especially with rising evictions. |
| `cm_staging_allocated_bytes` | Staging bytes in use. | Near `--staging`; streamed transfers are waiting. |
| `cm_nvme_reserved_bytes` | The rounded record reservations in force - durable or still only in DRAM - plus the staging bytes currently in use. | Near `--nvme`: DRAM eviction stops when durability cannot be admitted. |
| `cm_pending_puts`, `cm_pending_gets` | Operations queued across connections. | Sustained non-zero: clients outrun the manager. |
| `cm_evictions_total`, `cm_compactions_total` | DRAM evictions and row compactions. | Evictions frequent enough that the hot set does not stay resident. |
| `cm_hits_total{namespace,tier}`, `cm_records`, `cm_bytes` | Per-namespace hits and inventory. | Hit rate collapse, or a namespace that no longer matches a deployed package. |
| `cm_unknown_frames_total` | Frames dropped as malformed. | Any sustained increase. |
| `cm_leases`, `cm_open_ops`, `cm_cleanup_records`, `cm_cancels_total` | In-flight restores, open operations, pinned cleanup, cancellations. | Steadily growing leases or cleanup: stuck clients. |

## Restart, crash and recovery

Startup order is the root lock, NVMe recovery, shared-memory reclaim, then
socket replacement. One manager owns a root at a time: it exclusive-locks
`<root>/.lock`, and a second manager on the same root exits immediately.

The shared-memory instance directory is reclaimed after taking its per-instance
lock, so a killed manager never blocks a restart: stale region files are
removed and one line is logged (`reclaimed stale instance '<name>' (N files)`).
A stale socket is replaced only after confirming that nothing is listening.

A DRAM publication is acknowledged before NVMe durability. A crash can
therefore lose recently acknowledged records whose chains have not reached
NVMe; replicas re-publish them. Durable records survive restarts, recovered
records start in the disk tier, and DRAM is cold until replicas restore and
re-publish. SIGTERM and SIGKILL both simply end the process; there is no
shutdown flush beyond records already durable, but SIGTERM is the orderly
choice.

Recovery per namespace removes leftover `.*.tmp` files, validates every `.rec`
envelope, header, layout, name, length and metadata checksum, sorts parents
before children, and deletes orphaned descendants. A record that is corrupt in
a detectable way is removed; a record that cannot be read because of an I/O
error fails startup without deletion, so an operator can inspect or move it
deliberately. A root that predates the descriptor format fails startup rather
than being migrated.

If the disk fills during a write, the manager retains the non-durable DRAM copies
and retries every 100 ms; clients see `BUDGET` until space returns. Never free
space by deleting files under a running manager: the root lock excludes a second
manager, not an operator.

## Upgrades and clearing the cache

To upgrade, stop the manager, replace the binary and start it with the same
`<root>`, `--name` and `--socket`. Durable records are validated and reused; no
offline migration is needed and the DRAM cache warms again from replica traffic.
The wire protocol version must match the replicas. Changing `--name` selects a
different shared-memory instance and the default socket path, and does not
affect the root.

The root holds one opaque 64-hex directory per namespace plus `.lock`. Remove a
namespace directory to drop that package's durable records; remove all namespace
directories to clear the cache. Do this only while the manager is stopped;
descriptors are written by the manager and must not be edited or created by
hand. A changed identity, tenant or layout fingerprint is a different namespace,
so the previous directory is safe to delete and cannot be reused accidentally;
a descriptor whose geometry differs under the same namespace id is refused at
registration instead.

The root must be a local file system supporting 4096-byte `O_DIRECT` reads and
writes, rename, and file and directory `fsync`. Network file systems are not
supported. Run the manager with the same effective uid and gid as the replicas;
it sets the socket to 0660, creates a missing socket directory with mode 0770,
and never changes the mode or ownership of a directory that already exists. The
default `/run/selene` lives under a root-owned `/run`, so when running
unprivileged create it first with the runtime ownership and a mode the replicas
can traverse (for example 0770). In containers, share the socket volume and the
PID namespace with the replicas.
