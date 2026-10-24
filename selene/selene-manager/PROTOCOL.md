# Selene manager protocol, version 1

This document is the operating contract between Flare replicas and the selene
manager. [proto/selene.proto](proto/selene.proto) is normative for field
numbers and message layouts; this document defines the services that are served,
the stream flows, the shared-memory window model, checksums and client
obligations. `selene` serves `LocalCache` on its Unix socket. `Locate` is declared and returns
`UNIMPLEMENTED`. `PeerCache` (the remote tier, between managers) is served only
with `--listen-peer`, and `Admin` (operator statistics, pins, purges and quotas)
on the Unix socket and with `--listen-admin`; neither is part of the replica
contract described here.

## Transport and sessions

Clients connect over a gRPC channel on the manager's Unix socket. The manager
takes the peer pid from `SO_PEERCRED` and requires the peer's effective uid and
gid to match its own; run replicas and manager under the same identity and, in
containers, in the same PID namespace.

`Hello` is the first call on a connection. It registers the namespace described
by the request (creating it on first use), returns the shared-memory region
files and the negotiated limits, and assigns a `generation`. Every other RPC
carries that generation in the `x-selene-generation` metadata; a superseded
generation fails with `ABORTED`, and a missing or unknown one with
`UNAUTHENTICATED`. A new `Hello` on the same connection supersedes the previous
generation, fencing replies from older operations.

`Hello` fails with `FAILED_PRECONDITION` when the request's `protocol` is not 1.
The negotiated limits are:

| Limit | Value |
|---|---|
| Concurrent connections | 64 |
| Open operations per direction per connection | 4 |
| Unfilled or unreleased windows per direction | 2 credits, each up to 64 pages and/or the row |
| gRPC message cap | 16 MiB (payload never travels in messages) |
| Largest page window | negotiated in `HelloReply.max_window_bytes` (4 MiB) |
| Chain length per request | 65,536 nodes |
| No-progress deadline | 3 s |

A stream that makes no progress for 3 s is ended. The manager's stream task
fails it with `DEADLINE_EXCEEDED`, and the store's own timer refuses the
operation with `Refused{INVALID}`; a message that races the teardown can
receive either. Replicas treat both as a session failure and rebuild the
channel and `Hello` with backoff.

## Namespace and tenant identity

A `Descriptor` names one model package and storage geometry: the 32-byte
identity, the 16-byte layout fingerprint, the tenant, the page size in tokens
and the rank/plane layout with each record's page and row bytes. The namespace
id is BLAKE2b-256 over `selene-ns-v1`, the identity, the fingerprint, the tenant
length byte and the tenant.

The same descriptor is passed to `Hello`, `Subscribe` and `PutBegin` and must
match every time; a mismatch fails the call (`FAILED_PRECONDITION` or
`INVALID_ARGUMENT`, depending on the RPC). Different identities, tenants or
layout fingerprints — including different TP widths — are different namespaces
and never share records. A descriptor with different geometry under the same
namespace id is refused rather than mixed. `HelloReply.namespace_id` is the
32-byte id clients must use in later requests.

## Shared-memory window model

`HelloReply` names two manager-owned files with their sizes: the retention
arena and the staging region. Both sides map them `MAP_SHARED` at the returned
paths. Payload never travels in gRPC messages; only metadata and checksums do.

A `WindowGrant` identifies one window as a region and byte range in
`WindowRef{region, offset, length}`, plus the transfer id, the window ordinal
(`index`, from 0 within the call) and the record-relative offset. A window
length is a multiple of 4096 bytes and covers whole pages and/or the row. A
page window stays within the negotiated maximum, except that a single page or
the row can exceed it as a window of its own. `page_first` is the page index
within the chain (the key's full page span), not the record; a row-only grant
carries `page_first` 0 and `page_count` 0.

On `Put` the replica writes the payload into the granted range and then reports
its checksums. On `Get` the window aliases immutable stored bytes (a DRAM
record's extent) or a staging copy the manager read from NVMe; the replica
copies the bytes out before releasing the window. Neither side may
read or write outside `[offset, offset + length)`, may assume the region persists
across reconnects, or may keep a window after releasing it.

## Put stream

`Put` is a bidirectional stream. The replica sends `PutBegin` with the key,
`chain_pages`, the published parent and tail, the bucket, and the descriptor.
The manager validates the geometry and the namespace, then answers with exactly
one of:

- `Accept{transfer, first_page, parent, record_bytes}` — the selected parent
  (which may differ from the published one) and the record length. The manager
  then sends `WindowGrant` messages, at most two unfilled windows at a time.
- `Refused{reason, detail}` with `PRESENT` when the record is already stored,
  or a transient or terminal reason from the table below.

For every accepted window the replica writes the bytes and sends
`WindowFilled{transfer, index, page_xxh3[], row_xxh3, has_row}` with one 64-bit
xxh3 per page the window carried and the row checksum when it carried the row.
Once every granted window is filled, the replica sends `PutCommit` with the
metadata fragments; concatenated in `metadata_offset` order they are exactly
the 4096-byte `record::Header` followed by the padded checksum segment. The
stream ends with `Stored{bytes}` (the record length) or `Refused`.

`Present` is a successful deduplication: no bytes are transferred and the
record is available. A replica may send `Cancel` before it has sent `Commit`;
after `Commit` the operation runs to `Stored` or a refusal. A window's credit
returns when its `WindowFilled` is accepted. An unfilled window stays leased to
the connection even after a refusal, a cancellation or a no-progress failure,
because the replica may still be writing into it; it is reclaimed when the
connection closes and the writing process has exited (the manager watches the
peer pid), or when a remote session's streams end.

## Get stream

`Get` is a bidirectional stream. `GetBegin` names ascending chain nodes (at most
65,536), the number of pages to skip from the front of the first record, the
expected page count and a row flag that must be true; a Get without the row is
refused `INVALID`, as is a chain whose last node has none. The manager pins the chain
and answers `GetLeased{transfer, record_bytes[]}` — one record length per chain
node — or `Refused`.

For each record the manager sends one or more `RecordMeta` fragments (the
`Header` then the padded checksum segment, concatenated by `metadata_offset`)
with the source tier, then `WindowGrant` messages for the payload. After copying
a window out, the replica must send `WindowReleased{transfer, index}`; that
returns the credit and lets the manager reuse the window. `GetDone` arrives only
after every record and window has been delivered and released. `Cancel` may be
sent at any time.

A window already granted to a `Get` stays valid after `GetDone`, a refusal or a
cancellation until the replica releases it: the replica may still be copying its
bytes out. The manager keeps those bytes allocated, including after the peer
connection is gone, until the granting process exits. A replica that abandons a
restore must still release its windows or close the connection.

The replica validates the namespace, geometry, key, parent, metadata and every
page and row checksum before using the restored bytes. A short stream, a stale
key, a checksum failure, a deadline or a disconnect must fail the restore and
fall back to recompute; a partially received record is never usable. Restores do
not consume the durable budget: window traffic is bounded by the staging region.

## Catalog subscription

`Subscribe{namespace_id, generation, descriptor}` opens the event stream for one
connection and namespace. It begins with `SnapshotBegin{seq0}`, the current
entries with parents before children, and `SnapshotEnd`, then carries live
events. `sequence` increases by exactly one per event on that stream. `Added`
introduces a node, `Changed` reports tier, durability, row or readability
changes (never geometry), and `Removed` removes exactly one named node without
cascading. `Room` is the connection's availability notification (below).

A connection may have only one live subscription: a new `Subscribe` bumps the
epoch and ends the previous stream. Catalog events carry no generation or epoch
field; a replica associates each event stream with the generation and epoch it
subscribed with and discards events from a stream it has replaced. A sequence
gap, a decode error or an oversized backlog
requires discarding the mirror and resubscribing; the mirror is rebuilt only
from a new snapshot.

## Budget, Room and refusal reasons

A transient refusal is not a busy-wait signal. When a `Put` is refused
`BUDGET`, the manager arms that connection's requirement, and later sends
`Room{bytes}` on the connection's subscription once a retry can be admitted.
The replica retries only after `Room` or another availability change; a steady
`BUDGET` must produce no retry traffic.

| Reason | Meaning | Client action |
|---|---|---|
| `PRESENT` | The record is already stored. | Treat as success with zero transferred bytes. |
| `INFLIGHT` | A duplicate operation for the same key is in flight. | Retry after it settles. |
| `BUDGET` | The write budget or a capacity limit is exhausted. | Wait for `Room`; never spin. |
| `QUEUE` | The connection's open-operation limit or an internal queue is full. | Retry after completions drain. |
| `OVERSIZE` | The record can never be admitted in this configuration. | Report an error; retrying cannot succeed. |
| `INVALID` | Malformed request, descriptor mismatch or geometry violation. | Fail the operation; do not retry. |
| `IO` | A manager device operation failed. | Fail the operation and report the manager as unhealthy. |
| `QUOTA`, `UNSPECIFIED` | Reserved; never produced by this manager. | Treat as a protocol error. |

`OVERSIZE` is a structural impossibility of the record's own shape: its full
admission charge - the envelope plus its rounded extent, or for a streamed
record the envelope, the record and its two-window staging hold - exceeds a
configured capacity. A shortage that depends on what else must stay resident
(the retained ancestry, a protected parent, another operation's lease) is
`BUDGET`. A replica retries only on `Room`, so a shortage that never clears
simply leaves the record unstored.

## Checksums and durability

Every page and row carries a 64-bit xxh3 checksum supplied by the writer with
`WindowFilled`. The manager verifies every Put window before accepting it and a
record's metadata before committing it. On an NVMe read it does not re-verify
the payload: the stored metadata, including every page and row xxh3, is served
to the replica, which must validate every page and row it restores. Every
NVMe record is stored behind a 4096-byte envelope with a CRC32 over the
`Header` and padded checksum segment and a CRC32 over the envelope itself.
Recovery verifies both and removes detectably corrupt records; a streamed
read serves the stored metadata and payload without re-checking the envelope,
and a read failure does not remove the record.

A DRAM publication is acknowledged before it reaches NVMe, so a manager crash
can lose recently acknowledged records whose ancestor chains are not yet
durable. The replica must be prepared to republish; `Changed` events and the
catalog snapshot are the only authority on what remains stored. Durable records
survive manager restarts and are recovered into the disk tier.

## Client obligations

- Call `Hello` before any other RPC, keep the generation for the connection's
  lifetime, and rebuild the session on `ABORTED` or a transport failure.
- Map both region files after `Hello`, use only granted windows, never use a Put
  window after its `WindowFilled` is sent (an unfilled window from a failed or
  cancelled Put stays valid until the connection ends), and never use a Get
  window after releasing it.
- Validate every restored record before use and fail closed to recompute.
- Retry only transient reasons, and only after `Room` or an availability change.
- Discard catalog events from a superseded generation or subscription epoch.
- Do not exceed the negotiated open-operation and window-credit limits; a
  request beyond them is refused `QUEUE` or fails the stream.
- Treat the manager's region files and namespace directories as manager-owned:
  map the regions exactly as `Hello` instructs, and never create, truncate or
  delete them while the manager runs.

## Versioning

The protocol version is carried in `Hello` and is currently 1; a mismatch fails
the call. The manager pins the sha256 of `proto/selene.proto` in a test
(`PROTO_SHA256`), and Flare's client must carry the same schema. Message additions
that stay wire-compatible still require that update, and any change to stream sequencing, window semantics, checksums or
refusal behavior requires a new protocol version.
