//! Pool protocol, manager side: a real manager and a wire-level client.
//! Section ids (H, I, L, C, ...) refer to the protocol's test list.

mod support;

use selene_manager::proto::v2::{self, client_message as c, server_message::M};
use std::time::Duration;
use support::{
    Host,
    pool::{self, Ident, Pool, Session, chain, channel, entry, evicted, saved, spilled},
};

fn host() -> Host {
    let mut host = Host::dram_only(2 * 1024 * 1024);
    // Tight timers keep the timeout cases quick.
    host.config.pool.limits.pin_deadline_ms = 300;
    host.config.pool.limits.progress_timeout_ms = 600;
    host.config.pool.limits.resync_timeout_ms = 400;
    host.config.pool.reply_retention = Duration::from_millis(1500);
    host
}

/// Opens a session and installs an empty snapshot.
async fn source(pool: &mut Pool, ident: &Ident) -> Session {
    let mut session = pool.open(ident).await;
    assert_eq!(session.opened.status, v2::OpenStatus::OpenOk as i32);
    session.snapshot(vec![]).await;
    session
}

fn candidates_of(reply: &v2::LookupReply) -> Vec<(i32, Vec<u8>, u64)> {
    reply
        .candidates
        .iter()
        .map(|candidate| {
            (
                candidate.level,
                candidate.source.clone().unwrap().instance_id,
                candidate.key.clone().unwrap().consumed_tokens,
            )
        })
        .collect()
}

// ---------------------------------------------------------------- Hello (H)

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h1_a_v1_hello_is_a_version_mismatch() {
    let mut host = host();
    host.start();
    let mut client = v2::pool_client::PoolClient::new(channel(&host.config.socket).await);
    let reply = client
        .hello(v2::HelloRequest {
            protocol: 1,
            client_version: "old".into(),
            connected_with: 0,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(reply.status, v2::HelloStatus::VersionMismatch as i32);
    assert_eq!(reply.protocol, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h5_a_stale_incarnation_is_refused_and_a_new_hello_succeeds() {
    let mut host = host();
    host.start();
    let mut pool = Pool::connect(&host.config.socket).await;
    let ident = Ident::new("a");
    let stale = pool.hello.manager_incarnation ^ 0xff;
    let session = pool.open_with(&ident, stale).await;
    assert_eq!(session.opened.status, v2::OpenStatus::StaleManager as i32);
    let again = pool.hello_again().await;
    assert_eq!(again.manager_incarnation, pool.hello.manager_incarnation);
    let session = pool.open(&ident).await;
    assert_eq!(session.opened.status, v2::OpenStatus::OpenOk as i32);
    assert!(session.opened.resync_required);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h6_a_unix_peer_of_the_same_user_gets_shared_memory() {
    let mut host = host();
    host.start();
    let pool = Pool::connect(&host.config.socket).await;
    let shm = pool.hello.shared_memory.as_ref().expect("shared memory");
    assert!(std::path::Path::new(&shm.arena_path).exists());
    assert!(std::path::Path::new(&shm.staging_path).exists());
    assert_eq!(pool.hello.host_id.len(), 16);
    let mut pool = pool;
    let session = pool.open(&Ident::new("a")).await;
    assert_eq!(session.opened.export_slots, 2);
    assert_eq!(session.opened.claim_slots, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h7_malformed_scopes_are_refused() {
    let mut host = host();
    host.start();
    let mut pool = Pool::connect(&host.config.socket).await;
    for mutate in [
        (|scope: &mut v2::Scope| scope.tenant.clear()) as fn(&mut v2::Scope),
        |scope| scope.geometry.as_mut().unwrap().page_record_bytes = 4000,
        |scope| scope.geometry.as_mut().unwrap().row_record_bytes = 0,
        |scope| scope.geometry.as_mut().unwrap().ranks.clear(),
        |scope| scope.model_identity.truncate(5),
        // Sizes that are multiples of 4096 but disagree with the ranks.
        |scope| scope.geometry.as_mut().unwrap().page_record_bytes = 8192,
    ] {
        let mut ident = Ident::new("a");
        mutate(&mut ident.scope);
        let session = pool.open(&ident).await;
        assert_eq!(
            session.opened.status,
            v2::OpenStatus::InvalidScope as i32,
            "{:?}",
            ident.scope
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h8_scopes_never_see_each_other() {
    let mut host = host();
    host.start();
    let mut pool_a = Pool::connect(&host.config.socket).await;
    let mut pool_b = Pool::connect(&host.config.socket).await;
    let a = Ident::new("a");
    let mut b = Ident::new("b");
    b.scope = pool::scope_for("other-tenant", 7);
    let mut sa = source(&mut pool_a, &a).await;
    let mut sb = source(&mut pool_b, &b).await;
    assert_ne!(sa.opened.scope_id, sb.opened.scope_id);
    sa.notices(
        chain(&[1024], v2::Tier::Dram, 1)
            .into_iter()
            .map(saved)
            .collect(),
    )
    .await;
    let reply = sb.lookup(1, 1024, 0).await;
    assert!(reply.candidates.is_empty());
    // A claim that names the other scope's source answers like an unknown name.
    let candidate = sa.lookup(2, 1024, 0).await;
    assert!(candidate.candidates.is_empty(), "a is its own instance");
    let source_ref = v2::SourceRef {
        instance_id: a.instance_id.to_vec(),
        session_incarnation: sa.sid,
    };
    sb.claim(v2::Claim {
        claim_id: 1,
        level: v2::Level::HostDram as i32,
        source: Some(source_ref),
        tier: v2::Tier::Dram as i32,
        key: Some(pool::pkey(1024)),
        generation: 1,
        begin_tokens: 0,
        want_row: true,
        mode: v2::CopyMode::SharedWindows as i32,
        deadline_ms: 0,
        plan_id: 0,
        part: 1,
    })
    .await;
    let terminal = sb.claim_terminal(1).await;
    assert_eq!(terminal.state, v2::ClaimState::Failed as i32);
    assert_eq!(terminal.reason, v2::ClaimReason::UnknownSource as i32);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h10_a_reconnect_supersedes_the_old_stream() {
    let mut host = host();
    host.start();
    let mut pool = Pool::connect(&host.config.socket).await;
    let ident = Ident::new("a");
    let mut first = source(&mut pool, &ident).await;
    first
        .notices(
            chain(&[1024], v2::Tier::Dram, 1)
                .into_iter()
                .map(saved)
                .collect(),
        )
        .await;
    let mut second = pool.open(&ident).await;
    assert_ne!(first.sid, second.sid);
    let closing = first.closed().await.expect("closing");
    assert_eq!(closing.reason, v2::ClosingReason::ClosingSuperseded as i32);
    // The predecessor's entries stop being offered at once.
    let mut other = source(&mut pool, &Ident::new("b")).await;
    assert!(other.lookup(1, 1024, 0).await.candidates.is_empty());
    second.snapshot(chain(&[2048], v2::Tier::Dram, 1)).await;
    let reply = other.lookup(2, 2048, 0).await;
    assert_eq!(candidates_of(&reply).len(), 1);
    assert_eq!(
        reply.candidates[0].key.as_ref().unwrap().consumed_tokens,
        2048
    );
}

// ---------------------------------------------------------------- index (I)

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn i1_to_i4_notices_apply_in_order_and_gaps_demand_a_resync() {
    let mut host = host();
    host.start();
    let mut pool = Pool::connect(&host.config.socket).await;
    let mut a = pool.open(&Ident::new("a")).await;
    // I4: a notice before the first resync is refused.
    a.send_notices_at(1, vec![saved(entry(1024, None, v2::Tier::Dram, 1))])
        .await;
    assert_eq!(
        a.resync_required().await.reason,
        v2::ResyncReason::ResyncGap as i32
    );
    a.snapshot(vec![]).await;
    // I1: in order.
    let ack = a
        .notices(vec![saved(entry(1024, None, v2::Tier::Dram, 1))])
        .await;
    assert_eq!(ack.applied_through, 1);
    let mut b = source(&mut pool, &Ident::new("b")).await;
    assert_eq!(b.lookup(1, 1024, 0).await.candidates.len(), 1);
    // I2: a duplicate batch changes nothing.
    a.send_notices_at(1, vec![saved(entry(1024, None, v2::Tier::Dram, 1))])
        .await;
    let ack = a
        .take(|m| match m {
            M::NoticeAck(ack) => Some(*ack),
            _ => None,
        })
        .await;
    assert_eq!(ack.applied_through, 1);
    assert_eq!(b.lookup(2, 1024, 0).await.candidates.len(), 1);
    // I3: a gap drops the batch and demands a snapshot; later notices are ignored.
    a.send_notices_at(5, vec![saved(entry(2048, None, v2::Tier::Dram, 1))])
        .await;
    assert_eq!(
        a.resync_required().await.reason,
        v2::ResyncReason::ResyncGap as i32
    );
    a.send_notices_at(2, vec![saved(entry(2048, None, v2::Tier::Dram, 1))])
        .await;
    assert_eq!(
        b.lookup(3, 2048, 0).await.candidates.len(),
        1,
        "only the old entry"
    );
    assert!(
        b.lookup(4, 2048, 0).await.candidates.iter().all(|c| c
            .key
            .as_ref()
            .unwrap()
            .consumed_tokens
            == 1024)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn i5_to_i9_generations_spills_compactions() {
    let mut host = host();
    host.start();
    let mut pool = Pool::connect(&host.config.socket).await;
    let mut a = source(&mut pool, &Ident::new("a")).await;
    let mut b = source(&mut pool, &Ident::new("b")).await;
    // I5: a lower generation is ignored, an equal one replaces, a higher one replaces.
    let mut e = entry(1024, None, v2::Tier::Dram, 5);
    e.bytes = 100;
    a.notices(vec![saved(e.clone())]).await;
    let mut lower = e.clone();
    lower.generation = 4;
    lower.bytes = 1;
    a.notices(vec![saved(lower)]).await;
    let reply = b.lookup(1, 1024, 0).await;
    assert_eq!(reply.candidates[0].generation, 5);
    let mut higher = e.clone();
    higher.generation = 6;
    a.notices(vec![saved(higher)]).await;
    assert_eq!(b.lookup(2, 1024, 0).await.candidates[0].generation, 6);
    // I6: an eviction of an older generation does not remove a newer one.
    a.notices(vec![evicted(1024, v2::Tier::Dram, 5)]).await;
    assert_eq!(b.lookup(3, 1024, 0).await.candidates.len(), 1);
    a.notices(vec![evicted(1024, v2::Tier::Dram, 6)]).await;
    assert!(b.lookup(4, 1024, 0).await.candidates.is_empty());
    // I7: a spill adds the lower tier and removes the upper in one step.
    a.notices(vec![saved(entry(2048, None, v2::Tier::Gpu, 9))])
        .await;
    assert_eq!(
        b.lookup(5, 2048, 0).await.candidates[0].level,
        v2::Level::NeighbourGpu as i32
    );
    a.notices(vec![spilled(
        entry(2048, None, v2::Tier::Dram, 1),
        v2::Tier::Gpu,
        9,
    )])
    .await;
    let reply = b.lookup(6, 2048, 0).await;
    assert_eq!(reply.candidates.len(), 1);
    assert_eq!(reply.candidates[0].level, v2::Level::HostDram as i32);
    // I23-ish: generations are per tier, so a low DRAM generation does not shadow GPU.
    a.notices(vec![
        saved(entry(3072, None, v2::Tier::Gpu, 100)),
        saved(entry(3072, None, v2::Tier::Dram, 2)),
    ])
    .await;
    a.notices(vec![evicted(3072, v2::Tier::Dram, 2)]).await;
    let reply = b.lookup(7, 3072, 0).await;
    let at_3072: Vec<_> = reply
        .candidates
        .iter()
        .filter(|c| c.key.as_ref().unwrap().consumed_tokens == 3072)
        .collect();
    assert_eq!(at_3072.len(), 1);
    assert_eq!(at_3072[0].level, v2::Level::NeighbourGpu as i32);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn i8_a_compacted_node_is_no_longer_an_endpoint_but_the_chain_holds() {
    let mut host = host();
    host.start();
    let mut pool = Pool::connect(&host.config.socket).await;
    let mut a = source(&mut pool, &Ident::new("a")).await;
    let mut b = source(&mut pool, &Ident::new("b")).await;
    // root(1024) <- mid(2048) <- leaf(3072)
    a.notices(
        chain(&[1024, 2048, 3072], v2::Tier::Dram, 1)
            .into_iter()
            .map(saved)
            .collect(),
    )
    .await;
    assert_eq!(b.lookup(1, 3072, 0).await.candidates.len(), 3);
    // mid is compacted into leaf: leaf's parent becomes root and it owns the tail.
    let heir = entry(3072, Some(1024), v2::Tier::Dram, 1);
    a.notices(vec![v2::Notice {
        kind: Some(v2::notice::Kind::Evicted(v2::Evicted {
            key: Some(pool::pkey(2048)),
            tier: v2::Tier::Dram as i32,
            generation: 1,
            kind: v2::EvictKind::Compacted as i32,
            heir: Some(heir),
        })),
    }])
    .await;
    let reply = b.lookup(2, 3072, 0).await;
    let keys: Vec<u64> = reply
        .candidates
        .iter()
        .map(|c| c.key.as_ref().unwrap().consumed_tokens)
        .collect();
    assert_eq!(keys, vec![3072, 1024]);
    let leaf = &reply.candidates[0];
    assert_eq!(leaf.nodes.len(), 2, "root then the compacted leaf");
    assert_eq!(leaf.pages, 3072 / 64);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn i10_to_i15_snapshots_install_atomically() {
    let mut host = host();
    host.start();
    let mut pool = Pool::connect(&host.config.socket).await;
    let mut a = pool.open(&Ident::new("a")).await;
    let mut b = source(&mut pool, &Ident::new("b")).await;
    // I10: a snapshot at watermark 3 with a delta stream from 4.
    a.seq = 3;
    let installed = a
        .snapshot_at(
            vec![entry(1024, None, v2::Tier::Dram, 1)],
            3,
            vec![saved(entry(2048, None, v2::Tier::Dram, 1))],
        )
        .await;
    assert_eq!(
        (
            installed.watermark,
            installed.entries,
            installed.applied_through
        ),
        (3, 1, 4)
    );
    assert_eq!(b.lookup(1, 2048, 0).await.candidates.len(), 2);
    // A15: expected_next is W + 1 + buffered applied; 5 applies, 4 is skipped.
    a.send_notices_at(4, vec![saved(entry(3072, None, v2::Tier::Dram, 1))])
        .await;
    a.send_notices_at(5, vec![saved(entry(4096, None, v2::Tier::Dram, 1))])
        .await;
    let ack = a
        .take(|m| match m {
            M::NoticeAck(ack) if ack.applied_through >= 5 => Some(*ack),
            _ => None,
        })
        .await;
    assert_eq!(ack.applied_through, 5);
    // I14: a second begin abandons the first; only the second installs.
    a.send(c::M::ResyncBegin(v2::ResyncBegin {
        resync_id: 50,
        watermark: 5,
    }))
    .await;
    a.send(c::M::ResyncChunk(v2::ResyncChunk {
        resync_id: 50,
        chunk_index: 0,
        entries: vec![entry(5120, None, v2::Tier::Dram, 1)],
    }))
    .await;
    let installed = a
        .snapshot_at(vec![entry(6144, None, v2::Tier::Dram, 1)], 5, vec![])
        .await;
    assert_eq!(installed.entries, 1);
    let reply = b.lookup(2, 6144, 0).await;
    let keys: Vec<u64> = reply
        .candidates
        .iter()
        .map(|c| c.key.as_ref().unwrap().consumed_tokens)
        .collect();
    assert_eq!(keys, vec![6144]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn i11_to_i13_bad_resyncs() {
    let mut host = host();
    host.config.pool.limits.max_resync_buffered_notices = 2;
    host.start();
    let mut pool = Pool::connect(&host.config.socket).await;
    let mut a = source(&mut pool, &Ident::new("a")).await;
    a.notices(vec![saved(entry(1024, None, v2::Tier::Dram, 1))])
        .await;
    let mut b = source(&mut pool, &Ident::new("b")).await;
    // I11: buffered deltas must start at W + 1.
    a.send(c::M::ResyncBegin(v2::ResyncBegin {
        resync_id: 1,
        watermark: 1,
    }))
    .await;
    a.send_notices_at(3, vec![saved(entry(2048, None, v2::Tier::Dram, 1))])
        .await;
    assert_eq!(
        a.resync_required().await.reason,
        v2::ResyncReason::ResyncGap as i32
    );
    // The installed entries stay as hints.
    assert_eq!(b.lookup(1, 1024, 0).await.candidates.len(), 1);
    // I12: more buffered deltas than the bound.
    a.send(c::M::ResyncBegin(v2::ResyncBegin {
        resync_id: 2,
        watermark: 1,
    }))
    .await;
    a.send_notices_at(
        2,
        (0..3)
            .map(|n| saved(entry(4096 + n * 64, None, v2::Tier::Dram, 1)))
            .collect(),
    )
    .await;
    assert_eq!(
        a.resync_required().await.reason,
        v2::ResyncReason::ResyncOverflow as i32
    );
    // I13: a chunk hole, a wrong count and an unknown id.
    a.send(c::M::ResyncBegin(v2::ResyncBegin {
        resync_id: 3,
        watermark: 1,
    }))
    .await;
    a.send(c::M::ResyncChunk(v2::ResyncChunk {
        resync_id: 3,
        chunk_index: 1,
        entries: vec![],
    }))
    .await;
    assert_eq!(
        a.resync_required().await.reason,
        v2::ResyncReason::ResyncBad as i32
    );
    a.send(c::M::ResyncBegin(v2::ResyncBegin {
        resync_id: 4,
        watermark: 1,
    }))
    .await;
    a.send(c::M::ResyncEnd(v2::ResyncEnd {
        resync_id: 4,
        chunks: 0,
        entries: 7,
    }))
    .await;
    assert_eq!(
        a.resync_required().await.reason,
        v2::ResyncReason::ResyncBad as i32
    );
    a.send(c::M::ResyncEnd(v2::ResyncEnd {
        resync_id: 99,
        chunks: 0,
        entries: 0,
    }))
    .await;
    assert_eq!(
        a.resync_required().await.reason,
        v2::ResyncReason::ResyncBad as i32
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn i15_a_snapshot_above_the_entry_bound_overflows() {
    let mut host = host();
    host.config.pool.limits.max_resync_entries = 3;
    host.start();
    let mut pool = Pool::connect(&host.config.socket).await;
    let mut a = pool.open(&Ident::new("a")).await;
    a.send(c::M::ResyncBegin(v2::ResyncBegin {
        resync_id: 1,
        watermark: 0,
    }))
    .await;
    a.send(c::M::ResyncChunk(v2::ResyncChunk {
        resync_id: 1,
        chunk_index: 0,
        entries: (1..=4)
            .map(|n| entry(n * 1024, None, v2::Tier::Dram, 1))
            .collect(),
    }))
    .await;
    assert_eq!(
        a.resync_required().await.reason,
        v2::ResyncReason::ResyncOverflow as i32
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a13_a_stalled_resync_times_out_without_holding_anyone() {
    let mut host = host();
    host.start();
    let mut pool = Pool::connect(&host.config.socket).await;
    let mut a = pool.open(&Ident::new("a")).await;
    let mut b = pool.open(&Ident::new("b")).await;
    a.send(c::M::ResyncBegin(v2::ResyncBegin {
        resync_id: 1,
        watermark: 0,
    }))
    .await;
    // Another session's resync is unaffected while a stalls.
    b.snapshot(vec![entry(1024, None, v2::Tier::Dram, 1)]).await;
    let required = a.resync_required().await;
    assert_eq!(required.reason, v2::ResyncReason::ResyncTimeout as i32);
    let mut c_session = source(&mut pool, &Ident::new("c")).await;
    assert_eq!(c_session.lookup(1, 1024, 0).await.candidates.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn i18_and_i19_stale_incarnations_and_session_end() {
    let mut host = host();
    host.start();
    let mut pool = Pool::connect(&host.config.socket).await;
    let mut a = source(&mut pool, &Ident::new("a")).await;
    a.notices(vec![saved(entry(1024, None, v2::Tier::Dram, 1))])
        .await;
    let mut b = source(&mut pool, &Ident::new("b")).await;
    assert_eq!(b.lookup(1, 1024, 0).await.candidates.len(), 1);
    // A message with the wrong session incarnation closes the stream.
    let mut stale = pool.open(&Ident::new("s")).await;
    stale
        .send_with(
            v2::Header {
                manager_incarnation: stale.incarnation,
                session_incarnation: stale.sid + 100,
            },
            c::M::ReadinessQuery(v2::ReadinessQuery {}),
        )
        .await;
    let closing = stale.closed().await.expect("closing");
    assert_eq!(
        closing.reason,
        v2::ClosingReason::ClosingStaleSession as i32
    );
    // A message with a wrong manager incarnation is dropped silently.
    b.send_with(
        v2::Header {
            manager_incarnation: b.incarnation ^ 1,
            session_incarnation: b.sid,
        },
        c::M::ReadinessQuery(v2::ReadinessQuery {}),
    )
    .await;
    assert!(
        b.never(Duration::from_millis(200), |m| matches!(m, M::Readiness(_))
            .then_some(()))
            .await
    );
    // I19: session end removes its entries at once.
    drop(a);
    let mut removed = false;
    for id in 10..60 {
        if b.lookup(id, 1024, 0).await.candidates.is_empty() {
            removed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(removed, "the ended session's entries are gone");
}

// ---------------------------------------------------------------- lookup (L)

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l1_to_l3_ordering_trimming_and_the_requesters_own_entries() {
    let mut host = host();
    host.start();
    let mut pool = Pool::connect(&host.config.socket).await;
    let (ia, ib, ic) = (Ident::new("a"), Ident::new("b"), Ident::new("c"));
    let mut a = source(&mut pool, &ia).await;
    let mut b = source(&mut pool, &ib).await;
    let mut c_session = source(&mut pool, &ic).await;
    // a holds 4096 on GPU; b holds 3072 in DRAM and 4096 on NVMe; c asks.
    a.notices(
        chain(&[1024, 2048, 3072, 4096], v2::Tier::Gpu, 7)
            .into_iter()
            .map(saved)
            .collect(),
    )
    .await;
    b.notices(
        chain(&[1024, 2048, 3072], v2::Tier::Dram, 3)
            .into_iter()
            .chain(chain(&[1024, 2048, 3072, 4096], v2::Tier::Nvme, 4))
            .map(saved)
            .collect(),
    )
    .await;
    // L2: the requester's own entries are never offered.
    c_session
        .notices(
            chain(&[1024, 2048, 3072, 4096, 5120], v2::Tier::Dram, 1)
                .into_iter()
                .map(saved)
                .collect(),
        )
        .await;
    let reply = c_session.lookup(1, 5000, 0).await;
    let found = candidates_of(&reply);
    assert!(
        found
            .iter()
            .all(|(_, id, _)| *id != ic.instance_id.to_vec())
    );
    assert_eq!(found.len(), 8, "at most max_candidates");
    assert!(
        found.windows(2).all(|w| w[0].2 >= w[1].2),
        "longest endpoints first"
    );
    assert_eq!(found[0].2, 4096);
    let reply = c_session.lookup(2, 5000, 2048).await;
    let dram = reply
        .candidates
        .iter()
        .find(|c| {
            c.level == v2::Level::HostDram as i32 && c.key.as_ref().unwrap().consumed_tokens == 3072
        })
        .expect("a DRAM candidate");
    assert_eq!(dram.begin_tokens, 2048);
    assert_eq!(dram.pages, (3072 - 2048) / 64);
    assert_eq!(dram.skip_pages, 0);
    let reply = c_session.lookup(3, 5000, 3072).await;
    assert!(
        reply
            .candidates
            .iter()
            .all(|c| c.key.as_ref().unwrap().consumed_tokens > 3072)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l1b_records_shorter_than_a_block_do_not_crowd_out_a_long_history_s_candidates() {
    let mut host = host();
    host.start();
    let mut pool = Pool::connect(&host.config.socket).await;
    let (ia, ic) = (Ident::new("a"), Ident::new("c"));
    let mut a = source(&mut pool, &ia).await;
    let mut c_session = source(&mut pool, &ic).await;
    // Twelve short records of unrelated prompts, nearer in level than the record we want.
    let shorts: Vec<v2::IndexEntry> = (1..=12u64)
        .map(|n| chain(&[10 + n], v2::Tier::Gpu, n).remove(0))
        .collect();
    a.notices(shorts.into_iter().map(saved).collect()).await;
    a.notices(
        chain(&[1024, 2048, 3072], v2::Tier::Dram, 3)
            .into_iter()
            .map(saved)
            .collect(),
    )
    .await;
    // A history with blocks of its own is offered the long records only.
    let reply = c_session.lookup(1, 5000, 0).await;
    let found = candidates_of(&reply);
    assert_eq!(found.len(), 3, "{found:?}");
    assert!(
        found.iter().all(|(_, _, tokens)| *tokens >= 1024),
        "{found:?}"
    );
    // A history shorter than a block has only the root bucket, so it still sees them.
    let reply = c_session.lookup(2, 500, 0).await;
    assert_eq!(candidates_of(&reply).len(), 8, "capped at max_candidates");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l5_to_l8_holes_limits_and_validation() {
    let mut host = host();
    host.config.pool.limits.max_concurrent_lookups = 2;
    host.config.pool.limits.max_boundaries = 8;
    host.start();
    let mut pool = Pool::connect(&host.config.socket).await;
    let mut a = source(&mut pool, &Ident::new("a")).await;
    let mut b = source(&mut pool, &Ident::new("b")).await;
    // L5: a chain with a hole yields no candidate.
    let mut broken = chain(&[1024, 2048, 3072], v2::Tier::Dram, 1);
    broken.remove(1);
    a.notices(broken.into_iter().map(saved).collect()).await;
    let reply = b.lookup(1, 3072, 0).await;
    let keys: Vec<u64> = reply
        .candidates
        .iter()
        .map(|c| c.key.as_ref().unwrap().consumed_tokens)
        .collect();
    assert_eq!(keys, vec![1024]);
    // L6: before_tokens excludes endpoints at or above it.
    let reply = b
        .lookup_request(v2::LookupRequest {
            lookup_id: 2,
            boundaries: pool::boundaries(3072),
            before_tokens: 1024,
            resident_tokens: 0,
        })
        .await;
    assert!(reply.candidates.is_empty());
    // L8: unsorted boundaries, too many boundaries, invalid boundaries.
    for request in [
        v2::LookupRequest {
            lookup_id: 3,
            boundaries: vec![pool::pkey(2048), pool::pkey(1024)],
            before_tokens: 5000,
            resident_tokens: 0,
        },
        v2::LookupRequest {
            lookup_id: 4,
            boundaries: (0..9).map(|n| pool::pkey(n * 1024)).collect(),
            before_tokens: 90000,
            resident_tokens: 0,
        },
    ] {
        let reply = b.lookup_request(request).await;
        assert_eq!(reply.status, v2::LookupStatus::LookupInvalid as i32);
    }
    // L7: more live lookups than the bound are refused at once, not queued.
    for id in 100..110 {
        b.send(c::M::Lookup(v2::LookupRequest {
            lookup_id: id,
            boundaries: pool::boundaries(1024),
            before_tokens: 5000,
            resident_tokens: 0,
        }))
        .await;
    }
    let mut overloaded = 0;
    for id in 100..110 {
        let reply = b
            .take(|m| match m {
                M::LookupReply(reply) if reply.lookup_id == id => Some(reply.clone()),
                _ => None,
            })
            .await;
        if reply.status == v2::LookupStatus::LookupOverloaded as i32 {
            overloaded += 1;
        }
    }
    assert!(overloaded >= 1, "a burst beyond the bound is refused");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l4_instance_candidates_need_shared_memory_on_both_sides() {
    let mut host = host();
    host.start();
    let mut pool = Pool::connect(&host.config.socket).await;
    let mut a = source(&mut pool, &Ident::new("a")).await;
    let mut b = source(&mut pool, &Ident::new("b")).await;
    a.notices(
        chain(&[1024], v2::Tier::Dram, 1)
            .into_iter()
            .map(saved)
            .collect(),
    )
    .await;
    let reply = b.lookup(1, 1024, 0).await;
    assert_eq!(
        reply.candidates[0]
            .source
            .as_ref()
            .unwrap()
            .session_incarnation,
        a.sid
    );
    assert!(a.opened.export_slots > 0);
    // A source that serves no exports is never offered.
    let mut idle = Ident::new("idle");
    idle.export_slots = 0;
    let mut mute = source(&mut pool, &idle).await;
    mute.notices(
        chain(&[2048], v2::Tier::Dram, 1)
            .into_iter()
            .map(saved)
            .collect(),
    )
    .await;
    let reply = b.lookup(2, 2048, 0).await;
    assert!(
        reply
            .candidates
            .iter()
            .all(|c| c.key.as_ref().unwrap().consumed_tokens != 2048)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l9_not_ready_answers_with_a_retry_hint() {
    // A rooted manager remembers an instance that has not come back yet.
    let base = std::env::temp_dir().join(format!("selene-l9-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let ident = Ident::new("a");
    let mut config = selene_manager::store::Config::new(
        base.join("records"),
        base.join("run/m.sock"),
        2 * 1024 * 1024,
        64 * 1024 * 1024,
    );
    config.name = support::instance_name("l9");
    config.pool.source_wait = Duration::from_secs(30);
    {
        let mut host = Host::open_config(config.clone());
        host.start();
        let mut pool = Pool::connect(&host.config.socket).await;
        let mut a = source(&mut pool, &ident).await;
        assert!(a.lookup(1, 1024, 0).await.status == v2::LookupStatus::LookupOk as i32);
        a.goodbye(true).await;
        drop(a);
        host.stop();
        // Dropping the fixture would delete the store directory.
        host.base = std::env::temp_dir().join(format!("selene-l9-scratch-{}", std::process::id()));
    }
    config.metrics = None;
    config.name = support::instance_name("l9b");
    let mut host = Host::open_config(config);
    host.start();
    let mut pool = Pool::connect(&host.config.socket).await;
    let mut other = pool.open(&Ident::new("b")).await;
    other.snapshot(vec![]).await;
    let reply = other.lookup(1, 1024, 0).await;
    assert_eq!(reply.status, v2::LookupStatus::LookupNotReady as i32);
    assert!(reply.retry_after_ms > 0);
    let info = other.readiness().await;
    assert_eq!(info.state, v2::ReadinessState::AwaitingSources as i32);
    assert_eq!(info.sources_expected, 1);
    // Notices are still accepted while not ready.
    other
        .notices(vec![saved(entry(1024, None, v2::Tier::Dram, 1))])
        .await;
    // The remembered instance returns under the same id: ready as soon as it syncs.
    let mut back = pool.open(&ident.restarted()).await;
    back.snapshot(vec![]).await;
    let info = other
        .take(|m| match m {
            M::Readiness(info) if info.state == v2::ReadinessState::Ready as i32 => Some(*info),
            _ => None,
        })
        .await;
    assert_eq!(info.sources_expected, 0);
    let _ = std::fs::remove_dir_all(base);
}

// ---------------------------------------------------------------- restart barrier (R, S)

/// A rooted manager configuration whose directory outlives its fixture.
fn rooted(label: &str) -> (std::path::PathBuf, selene_manager::store::Config) {
    let base = std::env::temp_dir().join(format!("selene-{label}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let mut config = selene_manager::store::Config::new(
        base.join("records"),
        base.join("run/m.sock"),
        2 * 1024 * 1024,
        64 * 1024 * 1024,
    );
    config.name = support::instance_name(label);
    (base, config)
}

/// Runs a first manager in which `names` open, sync and leave cleanly.
async fn first_life(config: &selene_manager::store::Config, names: &[&str]) -> String {
    let mut first = config.clone();
    first.name = support::instance_name("first");
    let mut host = Host::open_config(first);
    host.start();
    let mut pool = Pool::connect(&host.config.socket).await;
    let arena = pool
        .hello
        .shared_memory
        .as_ref()
        .unwrap()
        .arena_path
        .clone();
    for name in names {
        let session = source(&mut pool, &Ident::new(name)).await;
        session.goodbye(true).await;
    }
    host.stop();
    // Dropping the fixture would delete the store directory.
    host.base = std::env::temp_dir().join(format!("selene-unused-{}", std::process::id()));
    arena
}

async fn wait_ready(session: &mut Session) -> v2::ReadinessInfo {
    session
        .take(|m| match m {
            M::Readiness(info) if info.state == v2::ReadinessState::Ready as i32 => Some(*info),
            _ => None,
        })
        .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r2_r3_and_s2_ready_when_remembered_sources_return_or_the_wait_ends() {
    let (base, mut config) = rooted("r3");
    config.pool.source_wait = Duration::from_secs(30);
    let first_arena = first_life(&config, &["x", "y"]).await;

    // Both return: READY as soon as the second snapshot installs, long before the bound.
    let mut second = config.clone();
    second.name = support::instance_name("second");
    let mut host = Host::open_config(second);
    host.start();
    let mut pool = Pool::connect(&host.config.socket).await;
    // R6: the region files of a new incarnation are new files.
    assert_ne!(
        pool.hello.shared_memory.as_ref().unwrap().arena_path,
        first_arena
    );
    assert_eq!(
        pool.hello.readiness.unwrap().state,
        v2::ReadinessState::AwaitingSources as i32
    );
    let mut watcher = pool.open(&Ident::new("w")).await;
    watcher.snapshot(vec![]).await;
    let started = std::time::Instant::now();
    let mut x = pool.open(&Ident::new("x").restarted()).await;
    x.snapshot(vec![]).await;
    assert_eq!(watcher.readiness().await.sources_expected, 1);
    let mut y = pool.open(&Ident::new("y").restarted()).await;
    y.snapshot(vec![]).await;
    let info = wait_ready(&mut watcher).await;
    assert_eq!(info.sources_expected, 0);
    assert!(started.elapsed() < Duration::from_secs(5));
    assert!(watcher.lookup(1, 1024, 0).await.status == v2::LookupStatus::LookupOk as i32);
    host.stop();
    host.base = std::env::temp_dir().join(format!("selene-unused2-{}", std::process::id()));

    // One never returns: READY at the bound.
    let mut third = config.clone();
    third.name = support::instance_name("third");
    third.pool.source_wait = Duration::from_millis(700);
    let mut host = Host::open_config(third);
    host.start();
    let mut pool = Pool::connect(&host.config.socket).await;
    let mut watcher = pool.open(&Ident::new("w")).await;
    watcher.snapshot(vec![]).await;
    let began = std::time::Instant::now();
    let mut x = pool.open(&Ident::new("x").restarted()).await;
    x.snapshot(vec![]).await;
    assert_eq!(
        watcher.lookup(1, 1024, 0).await.status,
        v2::LookupStatus::LookupNotReady as i32
    );
    wait_ready(&mut watcher).await;
    assert!(
        began.elapsed() >= Duration::from_millis(500),
        "waited for the missing source"
    );
    assert!(began.elapsed() < Duration::from_secs(5));
    host.stop();
    host.base = std::env::temp_dir().join(format!("selene-unused3-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s8_a_changed_instance_id_does_not_satisfy_the_barrier_and_ages_out() {
    let (base, mut config) = rooted("s8");
    config.pool.source_wait = Duration::from_millis(900);
    config.pool.instance_retention = Duration::from_secs(1);
    first_life(&config, &["old"]).await;
    // A different instance (its GPU set changed) returns: the old id still costs the wait.
    let mut second = config.clone();
    second.name = support::instance_name("second");
    let mut host = Host::open_config(second);
    host.start();
    let mut pool = Pool::connect(&host.config.socket).await;
    let mut other = pool.open(&Ident::new("renamed")).await;
    other.snapshot(vec![]).await;
    assert_eq!(
        other.readiness().await.state,
        v2::ReadinessState::AwaitingSources as i32
    );
    wait_ready(&mut other).await;
    host.stop();
    host.base = std::env::temp_dir().join(format!("selene-unused-s8-{}", std::process::id()));
    // Past the retention period the old id is forgotten and not waited for.
    tokio::time::sleep(Duration::from_millis(1300)).await;
    let mut third = config.clone();
    third.name = support::instance_name("third");
    let mut host = Host::open_config(third);
    host.start();
    let pool = Pool::connect(&host.config.socket).await;
    assert_eq!(
        pool.hello.readiness.unwrap().state,
        v2::ReadinessState::Ready as i32
    );
    host.stop();
    host.base = std::env::temp_dir().join(format!("selene-unused-s8b-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r13_shutdown_drains_claims_then_closes_sessions() {
    let mut host = host();
    host.config.pool.limits.progress_timeout_ms = 5000;
    host.start();
    let mut pool = Pool::connect(&host.config.socket).await;
    let mut source = source(&mut pool, &Ident::new("src")).await;
    source
        .notices(
            chain(&[1024], v2::Tier::Gpu, 5)
                .into_iter()
                .map(saved)
                .collect(),
        )
        .await;
    let mut requester = self::source(&mut pool, &Ident::new("req")).await;
    let candidate = requester.lookup(1, 1024, 0).await.candidates[0].clone();
    requester
        .claim(requester.claim_for(1, &candidate, 0, true))
        .await;
    let request = source.export_request().await;
    let records = pool::records_for(&[1024], 0, 5, true);
    source.pin_ok(&request, records, true).await;
    requester.claim_granted(1).await;
    let fill = source.export_fill().await;

    host.request_stop();
    let info = requester
        .take(|m| match m {
            M::Readiness(info) if info.state == v2::ReadinessState::Draining as i32 => Some(*info),
            _ => None,
        })
        .await;
    assert_eq!(info.state, v2::ReadinessState::Draining as i32);
    // New sessions, lookups and claims are refused while draining.
    let refused = pool.open(&Ident::new("late")).await;
    assert_eq!(refused.opened.status, v2::OpenStatus::ShuttingDown as i32);
    assert_eq!(
        requester.lookup(2, 1024, 0).await.status,
        v2::LookupStatus::LookupNotReady as i32
    );
    requester
        .claim(requester.claim_for(2, &candidate, 0, true))
        .await;
    assert_eq!(
        requester.claim_terminal(2).await.reason,
        v2::ClaimReason::NotReady as i32,
        "no new claim while draining"
    );
    // The claim in flight still completes.
    let content: Vec<u8> = (0..16)
        .flat_map(|page| pool::page_bytes(1, page))
        .chain(pool::row_bytes(1, 1024))
        .collect();
    source
        .filled(request.export_id, &fill.windows[0], &content)
        .await;
    let ready = requester.window_ready(1).await;
    requester.released(1, ready.window.unwrap().index).await;
    source
        .export_end(
            request.export_id,
            v2::ExportEndStatus::ExportComplete,
            v2::FenceOutcome::Drained,
        )
        .await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    requester.copy_result(1, v2::CopyOutcome::CopyOk).await;
    assert_eq!(
        requester.claim_terminal(1).await.state,
        v2::ClaimState::Copied as i32
    );
    requester.fence(1, v2::FenceOutcome::Drained).await;
    let closing = requester.closed().await.expect("closing");
    assert_eq!(closing.reason, v2::ClosingReason::ClosingShutdown as i32);
    host.stop();
}

// ---------------------------------------------------------------- more index and limit cases

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn i9_a_leaf_leaves_its_page_only_ancestors_until_they_are_named() {
    let mut host = host();
    host.start();
    let mut pool = Pool::connect(&host.config.socket).await;
    let mut a = source(&mut pool, &Ident::new("a")).await;
    let mut b = source(&mut pool, &Ident::new("b")).await;
    a.notices(
        chain(&[1024, 2048, 3072], v2::Tier::Dram, 1)
            .into_iter()
            .map(saved)
            .collect(),
    )
    .await;
    let keys = |reply: &v2::LookupReply| -> Vec<u64> {
        reply
            .candidates
            .iter()
            .map(|c| c.key.as_ref().unwrap().consumed_tokens)
            .collect()
    };
    // Only the leaf is named: nothing cascades in the manager.
    a.notices(vec![evicted(3072, v2::Tier::Dram, 1)]).await;
    assert_eq!(keys(&b.lookup(1, 3072, 0).await), vec![2048, 1024]);
    // The ancestors that left with it arrive as their own notices, one per node.
    a.notices(vec![
        evicted(2048, v2::Tier::Dram, 1),
        evicted(1024, v2::Tier::Dram, 1),
    ])
    .await;
    assert!(b.lookup(2, 3072, 0).await.candidates.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn x4_batch_chunk_and_chain_limits_are_enforced() {
    let mut host = host();
    host.config.pool.limits.max_notices_per_batch = 2;
    host.config.pool.limits.max_resync_chunk_entries = 2;
    host.config.pool.limits.max_chain_nodes = 2;
    host.start();
    let mut pool = Pool::connect(&host.config.socket).await;
    // More notices than a batch may carry closes the session.
    let mut a = source(&mut pool, &Ident::new("a")).await;
    a.send_notices_at(
        1,
        (1..=3)
            .map(|n| saved(entry(n * 1024, None, v2::Tier::Dram, 1)))
            .collect(),
    )
    .await;
    let closing = a.closed().await.expect("closing");
    assert_eq!(
        closing.reason,
        v2::ClosingReason::ClosingProtocolViolation as i32
    );
    // So does a resync chunk with too many entries.
    let mut b = pool.open(&Ident::new("b")).await;
    b.send(c::M::ResyncBegin(v2::ResyncBegin {
        resync_id: 1,
        watermark: 0,
    }))
    .await;
    b.send(c::M::ResyncChunk(v2::ResyncChunk {
        resync_id: 1,
        chunk_index: 0,
        entries: (1..=3)
            .map(|n| entry(n * 1024, None, v2::Tier::Dram, 1))
            .collect(),
    }))
    .await;
    let closing = b.closed().await.expect("closing");
    assert_eq!(
        closing.reason,
        v2::ClosingReason::ClosingProtocolViolation as i32
    );
    // A chain longer than the node limit is not offered.
    let mut source_session = source(&mut pool, &Ident::new("c")).await;
    source_session
        .notices(
            chain(&[1024, 2048], v2::Tier::Dram, 1)
                .into_iter()
                .map(saved)
                .collect(),
        )
        .await;
    source_session
        .notices(
            chain(&[3072], v2::Tier::Dram, 1)
                .into_iter()
                .map(|e| {
                    let mut e = e;
                    e.has_parent = true;
                    e.parent = Some(pool::pkey(2048));
                    e.tail_pages = 16;
                    saved(e)
                })
                .collect(),
        )
        .await;
    let mut d = source(&mut pool, &Ident::new("d")).await;
    let reply = d.lookup(1, 3072, 0).await;
    let keys: Vec<u64> = reply
        .candidates
        .iter()
        .map(|c| c.key.as_ref().unwrap().consumed_tokens)
        .collect();
    assert!(
        !keys.contains(&3072),
        "a three-node chain exceeds max_chain_nodes: {keys:?}"
    );
    assert!(keys.contains(&2048));
}
