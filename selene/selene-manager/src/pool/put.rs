//! Put: spill a record into the manager's store (PROTOCOL.md section 11).

use super::{
    Checked,
    core::{Core, ProcKey, Session},
};
use crate::proto::v2::{self, server_message::M};
use std::time::Instant;

impl Core {
    pub(super) fn put_begin(&mut self, sid: u64, m: v2::PutBegin) -> Checked<()> {
        self.send(
            sid,
            M::PutRefused(v2::PutRefused {
                put_id: m.put_id,
                reason: v2::Reason::PutNotReady as i32,
                detail: "the manager store is not served yet".into(),
            }),
        );
        Ok(())
    }

    pub(super) fn put_filled(&mut self, _sid: u64, _m: v2::PutFilled) -> Checked<()> {
        Ok(())
    }

    pub(super) fn put_commit(&mut self, _sid: u64, _m: v2::PutCommit) -> Checked<()> {
        Ok(())
    }

    pub(super) fn put_cancel(&mut self, _sid: u64, _m: v2::PutCancel) -> Checked<()> {
        Ok(())
    }

    pub(super) fn put_fence(
        &mut self,
        _instance: super::index::InstanceId,
        _nonce: super::core::Nonce,
        _put_id: u64,
        _party: super::claims::Party,
    ) {
    }

    pub(super) fn puts_lost(&mut self, _session: &Session) {}

    pub(super) fn puts_proc_exited(&mut self, _process: ProcKey) {}

    pub(super) fn puts_quiesced(&mut self, _process: ProcKey) {}

    pub(super) fn put_timers(&mut self, _now: Instant) {}
}
