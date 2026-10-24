//! Index notices and resync (PROTOCOL.md section 3).

use super::{
    Checked, Notice, Violation,
    core::Core,
    entry_from,
    index::{Entry, SourceId, Staged},
    notice_from,
};
use crate::proto::v2::{self, server_message::M};
use std::{collections::BTreeMap, time::Instant};

impl Core {
    pub(super) fn notices(&mut self, sid: u64, batch: v2::NoticeBatch) -> Checked<()> {
        let limits = self.cfg.limits.clone();
        if batch.notices.len() > limits.max_notices_per_batch as usize {
            return Err(Violation::new("notice batch exceeds max_notices_per_batch"));
        }
        let Some(session) = self.sessions.get(&sid) else {
            return Ok(());
        };
        let first = batch.first_sequence;
        if first == 0 {
            return Err(Violation::new("notice sequences start at 1"));
        }
        // Buffer while a snapshot streams.
        if let Some(staged) = &session.staged {
            let next = staged.watermark + 1 + staged.buffered.len() as u64;
            if first > next {
                self.demand_resync(sid, v2::ResyncReason::ResyncGap);
                return Ok(());
            }
            let mut parsed = Vec::new();
            for (offset, notice) in batch.notices.iter().enumerate() {
                let sequence = first + offset as u64;
                if sequence < next {
                    continue;
                }
                parsed.push((sequence, notice_from(notice)?));
            }
            let session = self.sessions.get_mut(&sid).expect("checked above");
            let staged = session.staged.as_mut().expect("checked above");
            staged.buffered.extend(parsed);
            if staged.buffered.len() > limits.max_resync_buffered_notices as usize {
                self.demand_resync(sid, v2::ResyncReason::ResyncOverflow);
            }
            return Ok(());
        }
        if session.need_resync {
            if !session.resync_requested {
                self.demand_resync(sid, v2::ResyncReason::ResyncGap);
            }
            return Ok(());
        }
        let expected = session.expected_next;
        if first > expected {
            self.demand_resync(sid, v2::ResyncReason::ResyncGap);
            return Ok(());
        }
        let skip = (expected - first) as usize;
        let mut parsed = Vec::new();
        for notice in batch.notices.iter().skip(skip) {
            parsed.push(notice_from(notice)?);
        }
        let applied = parsed.len() as u64;
        let (instance, scope) = (session.instance, session.scope);
        for notice in parsed {
            self.apply_notice(scope, SourceId::Instance(instance), notice);
        }
        let session = self.sessions.get_mut(&sid).expect("checked above");
        session.expected_next += applied;
        let applied_through = session.expected_next - 1;
        if self.scopes.get(&scope).is_some_and(|state| {
            state.index.source_len(SourceId::Instance(instance))
                > limits.max_resync_entries as usize
        }) {
            self.demand_resync(sid, v2::ResyncReason::ResyncOverflow);
            return Ok(());
        }
        self.send(sid, M::NoticeAck(v2::NoticeAck { applied_through }));
        Ok(())
    }

    pub(super) fn apply_notice(&mut self, scope: [u8; 32], source: SourceId, notice: Notice) {
        let Some(state) = self.scopes.get_mut(&scope) else {
            return;
        };
        let index = &mut state.index;
        match notice {
            Notice::Saved(entry) => {
                index.upsert(source, entry);
            }
            Notice::Spilled {
                to,
                from_tier,
                from_generation,
            } => {
                let key = to.key;
                index.upsert(source, to);
                index.remove(source, key, from_tier, from_generation);
            }
            Notice::Evicted {
                key,
                tier,
                generation,
                heir,
            } => {
                index.remove(source, key, tier, generation);
                if let Some(heir) = heir {
                    index.upsert(source, heir);
                }
            }
        }
    }

    /// Discards pending notices and staged data and asks the client for a snapshot.
    /// The entries of the last installed snapshot stay as hints.
    pub(super) fn demand_resync(&mut self, sid: u64, reason: v2::ResyncReason) {
        let Some(session) = self.sessions.get_mut(&sid) else {
            return;
        };
        session.staged = None;
        session.need_resync = true;
        session.resync_requested = true;
        let applied_through = session.expected_next.saturating_sub(1);
        self.send(
            sid,
            M::ResyncRequired(v2::ResyncRequired {
                reason: reason as i32,
                applied_through,
            }),
        );
    }

    pub(super) fn resync_begin(&mut self, sid: u64, begin: v2::ResyncBegin) -> Checked<()> {
        let Some(session) = self.sessions.get_mut(&sid) else {
            return Ok(());
        };
        session.resync_requested = false;
        session.staged = Some(Staged {
            resync_id: begin.resync_id,
            watermark: begin.watermark,
            started: Instant::now(),
            chunks: 0,
            received: 0,
            entries: BTreeMap::new(),
            buffered: Vec::new(),
        });
        Ok(())
    }

    pub(super) fn resync_chunk(&mut self, sid: u64, chunk: v2::ResyncChunk) -> Checked<()> {
        let limits = self.cfg.limits.clone();
        if chunk.entries.len() > limits.max_resync_chunk_entries as usize {
            return Err(Violation::new(
                "resync chunk exceeds max_resync_chunk_entries",
            ));
        }
        let Some(session) = self.sessions.get_mut(&sid) else {
            return Ok(());
        };
        let Some(staged) = session.staged.as_mut() else {
            self.demand_resync(sid, v2::ResyncReason::ResyncBad);
            return Ok(());
        };
        if staged.resync_id != chunk.resync_id || staged.chunks != chunk.chunk_index {
            self.demand_resync(sid, v2::ResyncReason::ResyncBad);
            return Ok(());
        }
        let mut parsed: Vec<Entry> = Vec::with_capacity(chunk.entries.len());
        for entry in &chunk.entries {
            parsed.push(entry_from(entry)?);
        }
        let session = self.sessions.get_mut(&sid).expect("checked above");
        let staged = session.staged.as_mut().expect("checked above");
        staged.chunks += 1;
        staged.received += parsed.len() as u64;
        for entry in parsed {
            let slot = (entry.key, entry.tier);
            match staged.entries.get(&slot) {
                Some(existing) if existing.generation > entry.generation => {}
                _ => {
                    staged.entries.insert(slot, entry);
                }
            }
        }
        if staged.entries.len() > limits.max_resync_entries as usize {
            self.demand_resync(sid, v2::ResyncReason::ResyncOverflow);
        }
        Ok(())
    }

    pub(super) fn resync_end(&mut self, sid: u64, end: v2::ResyncEnd) -> Checked<()> {
        let Some(session) = self.sessions.get_mut(&sid) else {
            return Ok(());
        };
        let Some(staged) = session.staged.take() else {
            self.demand_resync(sid, v2::ResyncReason::ResyncBad);
            return Ok(());
        };
        if staged.resync_id != end.resync_id
            || staged.chunks != end.chunks
            || staged.received != end.entries
        {
            self.demand_resync(sid, v2::ResyncReason::ResyncBad);
            return Ok(());
        }
        // The buffered deltas must begin at exactly W + 1: they were buffered
        // contiguously, so only an empty or well-formed buffer remains.
        let (instance, scope) = (session.instance, session.scope);
        let watermark = staged.watermark;
        let entries = staged.entries.len() as u64;
        let buffered = staged.buffered.len() as u64;
        let source = SourceId::Instance(instance);
        if let Some(state) = self.scopes.get_mut(&scope) {
            state.index.replace(source, staged.entries.into_values());
        }
        for (_, notice) in staged.buffered {
            self.apply_notice(scope, source, notice);
        }
        let session = self.sessions.get_mut(&sid).expect("checked above");
        session.need_resync = false;
        session.resync_requested = false;
        session.installed_once = true;
        session.expected_next = watermark + 1 + buffered;
        let applied_through = session.expected_next - 1;
        self.send(
            sid,
            M::ResyncInstalled(v2::ResyncInstalled {
                resync_id: end.resync_id,
                watermark,
                entries,
                applied_through,
            }),
        );
        self.mark_synced(instance);
        Ok(())
    }

    pub(super) fn resync_timers(&mut self, now: Instant) {
        let timeout = self.cfg.limits.resync_timeout();
        let stalled: Vec<u64> = self
            .sessions
            .values()
            .filter(|session| {
                session
                    .staged
                    .as_ref()
                    .is_some_and(|staged| now.duration_since(staged.started) >= timeout)
            })
            .map(|session| session.sid)
            .collect();
        for sid in stalled {
            self.demand_resync(sid, v2::ResyncReason::ResyncTimeout);
        }
    }
}
