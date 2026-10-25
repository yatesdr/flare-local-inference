//! The source role: serving exports of this instance's holdings.
//!
//! A GPU export is answered by the model worker, which pins the checkpoint;
//! this module then fills the manager's windows by D2H copies through the page
//! copier. A host-tier export is pinned by the store on the worker thread and
//! fills windows on a copy thread, serving the checksums persisted with the
//! records. In both, the pin is released only after every copy that reads it
//! has drained, and the export ends with exactly one `ExportEnd`.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use crate::{
    Direction, ExportChain, ExportId, ExportRefusal, Job, JobId, record::checksum,
    slab::WINDOW_PAGES,
};

use super::{
    super::{
        codec,
        exporter::{Checksums, HostPin, HostWindow},
        net::{Channel, Filled, Id},
        shared::{ExportRequest, PoolEvent},
        wire::{self, client_message::M as Out},
    },
    Engine, Owner,
    coverage::Coverage,
    hostfill::{FillDone, FillJob, FillOwner},
    ranks::{PeerPart, PeerWindow},
    requester::ExpectedRecord,
};

enum Phase {
    /// Delivered to the model worker, which has not answered.
    AtWorker,
    Pinned,
}

enum Pin {
    Gpu(ExportChain),
    Host(Arc<Mutex<Box<dyn HostPin>>>),
}

struct Export {
    phase: Phase,
    pin: Option<Pin>,
    want_row: bool,
    reserved: u64,
    deadline: Instant,
    /// The PinAck went out already (a refusal at the deadline).
    answered: bool,
    cancelled: bool,
    failed: bool,
    lost: bool,
    coverage: Option<Coverage>,
    jobs_outstanding: u32,
    /// Windows the copies of which are still running, by index.
    windows: HashMap<u32, wire::WindowSpec>,
    /// The windows go to the manager on the data channel (no shared memory), which the
    /// request named.
    reference: Option<wire::DataChannelRef>,
    /// The channel's task, until it has reported that it is gone.
    channel: Option<Channel>,
    channel_open: bool,
    /// The staging slot of each granted window until it has been written out.
    slots: HashMap<u32, u32>,
    /// Filled windows the channel has not yet written out.
    sends_outstanding: u32,
    /// The channels the request named for the peer ranks of a multi-process instance.
    rank_channels: Vec<wire::RankChannel>,
    /// The peer ranks' parts of this export.
    peers: Vec<PeerPart>,
    /// Shared-memory peers that have not yet drained their slice of a window, and the
    /// slice checksums those that have reported.
    peer_windows: HashMap<u32, PeerWindow>,
    /// Windows whose rank-0 host read is running, and the persisted checksums of those
    /// that finished before their peers did.
    host_filling: std::collections::HashSet<u32>,
    host_sums: HashMap<u32, Checksums>,
}

#[derive(Default)]
pub(crate) struct State {
    exports: HashMap<u64, Export>,
}

impl Export {
    pub(super) fn set_peers(&mut self, peers: Vec<PeerPart>) {
        self.peers = peers;
    }

    /// The transfers of the peers not yet cancelled or done, marked cancelled.
    pub(super) fn take_uncancelled_peers(&mut self) -> Vec<u64> {
        self.peers
            .iter_mut()
            .filter(|p| p.done.is_none() && !p.cancelled)
            .map(|p| {
                p.cancelled = true;
                p.xfer
            })
            .collect()
    }
}

impl State {
    pub(super) fn set_peers(&mut self, export: u64, peers: Vec<PeerPart>) {
        if let Some(run) = self.exports.get_mut(&export) {
            run.set_peers(peers);
        }
    }

    pub(super) fn take_uncancelled(&mut self, export: u64) -> Vec<u64> {
        self.exports
            .get_mut(&export)
            .map(Export::take_uncancelled_peers)
            .unwrap_or_default()
    }

    /// A shared peer reported one window. `false` when it was not expected.
    pub(super) fn peer_window_done(
        &mut self,
        export: u64,
        rank: usize,
        index: u32,
        result: std::result::Result<(), String>,
        sums: Option<crate::pool::SliceSums>,
    ) -> bool {
        let Some(run) = self.exports.get_mut(&export) else {
            return false;
        };
        let Some(window) = run.peer_windows.get_mut(&index) else {
            return false;
        };
        if !window.left.remove(&rank) {
            return false;
        }
        match (result, sums) {
            (Ok(()), Some(sums)) => window.sums.push((rank, sums)),
            (Err(why), _) => {
                eprintln!("warning: export {export}: rank {rank}: {why}");
                run.failed = true;
            }
            (Ok(()), None) => run.failed = true,
        }
        true
    }

    /// The window's spec once every peer has reported it, and whether the export failed.
    pub(super) fn window_after_peers(
        &self,
        export: u64,
        index: u32,
    ) -> (Option<wire::WindowSpec>, bool) {
        let Some(run) = self.exports.get(&export) else {
            return (None, false);
        };
        let complete = run
            .peer_windows
            .get(&index)
            .is_some_and(|w| w.left.is_empty());
        (
            complete.then(|| run.windows.get(&index).cloned()).flatten(),
            run.failed,
        )
    }

    pub(super) fn peer_done(
        &mut self,
        export: u64,
        rank: usize,
        result: std::result::Result<(), String>,
        quarantined: bool,
    ) {
        let Some(run) = self.exports.get_mut(&export) else {
            return;
        };
        if let Some(peer) = run
            .peers
            .iter_mut()
            .find(|p| p.rank == rank && p.done.is_none())
        {
            peer.done = Some(result.clone());
            peer.quarantined = quarantined;
        }
        if let Err(why) = result {
            eprintln!("warning: export {export}: rank {rank}: {why}");
            run.failed = true;
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.exports.is_empty()
    }

    pub(crate) fn busy(&self) -> bool {
        self.exports.values().any(|e| e.jobs_outstanding > 0)
    }
}

fn reason(refusal: ExportRefusal) -> wire::ClaimReason {
    match refusal {
        ExportRefusal::NotFound => wire::ClaimReason::NotFound,
        ExportRefusal::ChainGap => wire::ClaimReason::ChainGap,
        ExportRefusal::NoHeadroom => wire::ClaimReason::NoHeadroom,
        ExportRefusal::NoWindows => wire::ClaimReason::NoWindows,
        ExportRefusal::Busy => wire::ClaimReason::SourceBusy,
    }
}

impl Engine {
    fn pin_refused(&mut self, export: u64, why: wire::ClaimReason) {
        self.emit(Out::PinAck(wire::PinAck {
            export_id: export,
            result: wire::PinResult::Refused as i32,
            refusal: why as i32,
            records: Vec::new(),
            total_pages: 0,
            row: false,
        }));
    }

    pub(crate) fn on_export_request(&mut self, now: Instant, request: wire::ExportRequest) {
        let id = request.export_id;
        if self.source.exports.contains_key(&id) {
            return;
        }
        let Some(sess) = &self.sess else {
            return;
        };
        if request.session_incarnation != sess.header.session_incarnation {
            return self.pin_refused(id, wire::ClaimReason::StaleSource);
        }
        let reachable = if request.channel.is_some() {
            self.dial.is_some()
        } else {
            sess.regions.is_some()
        };
        if !reachable
            || sess.export_slots == 0
            || self.source.exports.len() >= sess.export_slots as usize
            || self.closing
        {
            return self.pin_refused(id, wire::ClaimReason::SourceBusy);
        }
        let deadline = now + Duration::from_millis(u64::from(sess.limits.pin_deadline_ms));
        let (Some(key), Some(tier)) = (
            request.key.as_ref().and_then(codec::key_from),
            codec::tier_from(request.tier),
        ) else {
            return self.pin_refused(id, wire::ClaimReason::InvalidClaim);
        };
        let export = Export {
            phase: Phase::AtWorker,
            pin: None,
            want_row: request.want_row,
            reserved: request.reserved_bytes,
            deadline,
            answered: false,
            cancelled: false,
            failed: false,
            lost: false,
            coverage: None,
            jobs_outstanding: 0,
            windows: HashMap::new(),
            reference: request.channel.clone(),
            channel: None,
            channel_open: false,
            slots: HashMap::new(),
            sends_outstanding: 0,
            rank_channels: request.rank_channels.clone(),
            peers: Vec::new(),
            peer_windows: HashMap::new(),
            host_filling: Default::default(),
            host_sums: HashMap::new(),
        };
        self.source.exports.insert(id, export);
        self.event(PoolEvent::Export(ExportRequest {
            export: ExportId(id),
            tier,
            key,
            begin_tokens: request.begin_tokens,
            want_row: request.want_row,
            deadline,
            reserved_bytes: request.reserved_bytes,
        }));
    }

    /// The worker pinned the checkpoint and describes its chain.
    pub(crate) fn on_export_pinned(&mut self, export: ExportId, chain: ExportChain) {
        let last = chain.nodes.len().saturating_sub(1);
        let records: Vec<ExpectedRecord> = chain
            .nodes
            .iter()
            .enumerate()
            .map(|(index, node)| {
                let first = if index == 0 { chain.skip_pages } else { 0 };
                ExpectedRecord {
                    key: node.key,
                    first_page: first,
                    pages: (node.pages.len() as u32).saturating_sub(first),
                    row: index == last && chain.row.is_some(),
                }
            })
            .collect();
        let row = chain.row.is_some();
        self.pinned(export, records, row, Pin::Gpu(chain));
    }

    /// The store pinned a host-tier chain for the export.
    pub(crate) fn on_host_pinned(&mut self, export: ExportId, pin: Box<dyn HostPin>) {
        let chain = pin.chain().clone();
        let records = chain
            .records
            .iter()
            .map(|r| ExpectedRecord {
                key: r.key,
                first_page: r.first_page,
                pages: r.pages,
                row: r.row,
            })
            .collect();
        self.pinned(
            export,
            records,
            chain.row,
            Pin::Host(Arc::new(Mutex::new(pin))),
        );
    }

    /// Answer an export request with the chain that was pinned for it, or
    /// release the pin if the export can no longer use it.
    fn pinned(&mut self, export: ExportId, expected: Vec<ExpectedRecord>, row: bool, pin: Pin) {
        let id = export.0;
        let release = |engine: &Engine, pin: Pin| {
            // A GPU pin is the worker's to release; a host pin releases on drop.
            if matches!(pin, Pin::Gpu(_)) {
                engine.event(PoolEvent::ExportEnded { export });
            }
        };
        let Some(run) = self.source.exports.get_mut(&id) else {
            // Nothing waits for this pin any more.
            return release(self, pin);
        };
        if !matches!(run.phase, Phase::AtWorker) {
            return;
        }
        let (page_bytes, row_bytes) = (self.st.page_record_bytes(), self.st.row_record_bytes());
        let total: u64 = expected.iter().map(|r| u64::from(r.pages)).sum();
        let bytes = total * page_bytes + if row { row_bytes } else { 0 };
        let valid = !expected.is_empty() && row == run.want_row;
        // A GPU export always has its staging reserved; a host export with none
        // reserved streams.
        let over = if matches!(pin, Pin::Gpu(_)) {
            bytes > run.reserved
        } else {
            run.reserved != 0 && bytes > run.reserved
        };
        if run.cancelled || run.lost || run.answered || !valid || over {
            let (answered, cancelled) = (run.answered, run.cancelled);
            self.source.exports.remove(&id);
            if !answered {
                let why = if cancelled {
                    wire::ClaimReason::Cancelled
                } else if valid {
                    wire::ClaimReason::NoWindows
                } else {
                    wire::ClaimReason::ChainGap
                };
                self.pin_refused(id, why);
            }
            return release(self, pin);
        }
        let records: Vec<wire::Record> = expected
            .iter()
            .map(|r| wire::Record {
                key: Some(codec::key_to(r.key)),
                first_page: r.first_page,
                pages: r.pages,
                row: r.row,
            })
            .collect();
        let rank_channels = run.rank_channels.clone();
        // A multi-process instance's peers start before the manager can send a window.
        if self.ranks.is_some() {
            let opened = match &pin {
                Pin::Gpu(chain) => {
                    self.open_export_peers(id, &records, Some(chain), &rank_channels)
                }
                Pin::Host(_) => self.open_export_peers(id, &records, None, &rank_channels),
            };
            if let Err(why) = opened {
                eprintln!("warning: export {id} refused: {why}");
                self.cancel_export_peers(id);
                self.source.exports.remove(&id);
                self.pin_refused(id, wire::ClaimReason::SourceBusy);
                return release(self, pin);
            }
        }
        let Some(run) = self.source.exports.get_mut(&id) else {
            return release(self, pin);
        };
        run.phase = Phase::Pinned;
        run.coverage = Some(Coverage::new(expected));
        run.pin = Some(pin);
        let reference = run.reference.clone();
        self.emit(Out::PinAck(wire::PinAck {
            export_id: id,
            result: wire::PinResult::Pinned as i32,
            refusal: 0,
            records,
            total_pages: total,
            row,
        }));
        if let Some(reference) = reference {
            // The manager grants this export's windows on the channel from now on.
            match self.open_channel(Id::Export(id), &reference) {
                Ok(channel) => {
                    let run = self.source.exports.get_mut(&id).expect("present");
                    run.channel = Some(channel);
                    run.channel_open = true;
                }
                Err(_) => {
                    if let Some(run) = self.source.exports.get_mut(&id) {
                        run.failed = true;
                    }
                    self.drive_export(id);
                }
            }
        }
    }

    /// The worker did not pin the checkpoint.
    pub(crate) fn on_export_refused(&mut self, export: ExportId, why: ExportRefusal) {
        let id = export.0;
        let Some(run) = self.source.exports.get(&id) else {
            return;
        };
        if !matches!(run.phase, Phase::AtWorker) {
            return;
        }
        let (answered, cancelled) = (run.answered, run.cancelled);
        self.source.exports.remove(&id);
        if !answered {
            self.pin_refused(
                id,
                if cancelled {
                    wire::ClaimReason::Cancelled
                } else {
                    reason(why)
                },
            );
        }
    }

    pub(crate) fn on_export_cancel(&mut self, cancel: wire::ExportCancel) {
        let id = cancel.export_id;
        let Some(run) = self.source.exports.get_mut(&id) else {
            // Unknown: the idempotent answer.
            self.emit(Out::ExportEnd(wire::ExportEnd {
                export_id: id,
                status: wire::ExportEndStatus::ExportReleased as i32,
                fence: wire::FenceOutcome::Drained as i32,
            }));
            return;
        };
        run.cancelled = true;
        match run.phase {
            Phase::AtWorker => {
                // A request the worker never took is withdrawn at once.
                if self.shared.withdraw_export(ExportId(id)) {
                    self.source.exports.remove(&id);
                    self.pin_refused(id, wire::ClaimReason::Cancelled);
                }
            }
            Phase::Pinned => self.drive_export(id),
        }
    }

    pub(crate) fn on_export_fill(&mut self, fill: wire::ExportFill) {
        let id = fill.export_id;
        let Some(run) = self.source.exports.get_mut(&id) else {
            return;
        };
        if !matches!(run.phase, Phase::Pinned) || run.cancelled || run.failed {
            return;
        }
        if run.reference.is_some() {
            // This export's windows are granted on its channel.
            return;
        }
        for spec in fill.windows {
            if !self.export_window(id, spec, None) {
                break;
            }
        }
        self.drive_export(id);
    }

    /// The manager granted a window of an export over the data channel; its bytes go
    /// in `slot`.
    pub(crate) fn on_export_grant(
        &mut self,
        export: u64,
        shape: super::super::frame::Shape,
        slot: u32,
    ) {
        let Some(staging) = self.net.clone() else {
            return;
        };
        let spec = wire::WindowSpec {
            index: shape.index,
            region: wire::Region::Staging as i32,
            offset: staging.offset(slot),
            length: shape.length,
            record: shape.record,
            first_page: shape.first_page,
            page_count: shape.page_count,
            row: shape.row,
        };
        let taken = self.export_window(export, spec, Some(slot));
        if !taken {
            self.free_slot(slot);
        }
        self.drive_export(export);
    }

    /// Start filling one window. `false` when it was not taken up: the export is over, or
    /// the window is wrong and the export failed. A slot, if any, then is still the
    /// caller's.
    fn export_window(&mut self, id: u64, spec: wire::WindowSpec, slot: Option<u32>) -> bool {
        let (whole_page, whole_row) = (self.st.page_record_bytes(), self.st.row_record_bytes());
        // A window of this rank's own leg holds only its slices.
        let (page_bytes, row_bytes) = match (&self.ranks, slot) {
            (Some(ranks), Some(_)) => (ranks.layout.rank(0).page_len, ranks.layout.rank(0).row_len),
            _ => (whole_page, whole_row),
        };
        let memory = self.memory();
        let Some(run) = self.source.exports.get_mut(&id) else {
            return false;
        };
        if !matches!(run.phase, Phase::Pinned) || run.cancelled || run.failed {
            return false;
        }
        let coverage = run.coverage.as_mut().expect("pinned has coverage");
        let ok = coverage.accept(&spec, page_bytes, row_bytes);
        let address = memory
            .as_ref()
            .and_then(|m| m.region(spec.region()))
            .and_then(|region| region.address(spec.offset, spec.length));
        let (Ok(_), Some(address)) = (ok, address) else {
            run.failed = true;
            self.fill_failed(id, spec.index);
            return false;
        };
        if let Some(slot) = slot {
            run.slots.insert(spec.index, slot);
        }
        let record = spec.record as usize;
        let first = spec.first_page as usize;
        let count = spec.page_count as usize;
        match run.pin.as_ref().expect("pinned") {
            Pin::Gpu(chain) => {
                let node = &chain.nodes[record];
                let mut jobs = Vec::new();
                let mut from = 0;
                while from < count || (count == 0 && jobs.is_empty()) {
                    let end = (from + WINDOW_PAGES).min(count);
                    let pages = (from..end)
                        .map(|i| (node.pages[first + i], address + i * page_bytes as usize))
                        .collect();
                    let row = (end == count && spec.row).then(|| {
                        (
                            chain.row.expect("row window"),
                            address + count * page_bytes as usize,
                        )
                    });
                    jobs.push(Job {
                        id: JobId(0),
                        window: spec.index,
                        direction: Direction::Out,
                        pages,
                        row,
                    });
                    from = end;
                    if count == 0 {
                        break;
                    }
                }
                run.jobs_outstanding += jobs.len() as u32;
                run.windows.insert(spec.index, spec);
                // The shared-memory peers write their own slices of this window in step.
                let shared: Vec<(usize, u64)> = run
                    .peers
                    .iter()
                    .filter(|p| p.shared && p.done.is_none())
                    .map(|p| (p.rank, p.xfer))
                    .collect();
                if slot.is_none() && !shared.is_empty() {
                    run.peer_windows.insert(
                        spec.index,
                        PeerWindow {
                            left: shared.iter().map(|(rank, _)| *rank).collect(),
                            sums: Vec::new(),
                        },
                    );
                }
                for job in jobs {
                    self.queue_job(
                        job,
                        Owner::Export {
                            export: id,
                            window: spec.index,
                        },
                    );
                }
                for (rank, xfer) in shared {
                    let sent = self.ranks.as_ref().is_some_and(|ranks| {
                        ranks.send_window(
                            rank,
                            super::super::rank::link::Window {
                                xfer,
                                spec: Some(spec),
                                sums: None,
                            },
                        )
                    });
                    if !sent {
                        if let Some(run) = self.source.exports.get_mut(&id) {
                            run.failed = true;
                        }
                        break;
                    }
                }
            }
            Pin::Host(pin) => {
                let pin = pin.clone();
                run.jobs_outstanding += 1;
                run.windows.insert(spec.index, spec);
                let span = HostWindow {
                    record: spec.record,
                    first_page: spec.first_page,
                    page_count: spec.page_count,
                    row: spec.row,
                };
                // Rank 0 reads its own slices; in a shared window they are scattered into
                // place beside the peers' (a channel's slot holds them packed).
                let scatter = self
                    .ranks
                    .as_ref()
                    .filter(|_| slot.is_none())
                    .map(|ranks| ranks.layout.clone());
                let shared: Vec<(usize, u64)> = run
                    .peers
                    .iter()
                    .filter(|p| p.shared && p.done.is_none())
                    .map(|p| (p.rank, p.xfer))
                    .collect();
                if self.ranks.is_some() {
                    run.host_filling.insert(spec.index);
                    if slot.is_none() && !shared.is_empty() {
                        run.peer_windows.insert(
                            spec.index,
                            PeerWindow {
                                left: shared.iter().map(|(rank, _)| *rank).collect(),
                                sums: Vec::new(),
                            },
                        );
                    }
                }
                let work: Box<dyn FnOnce(&mut [u8]) -> crate::Result<Checksums> + Send> =
                    match scatter {
                        Some(layout) => Box::new(move |dst| {
                            let mut packed = vec![
                                0u8;
                                layout.slice_bytes(0, span.page_count, span.row)
                                    as usize
                            ];
                            let sums = pin.lock().unwrap().read(&span, &mut packed)?;
                            layout.scatter(0, dst, span.page_count, span.row, &packed)?;
                            Ok(sums)
                        }),
                        None => Box::new(move |dst| pin.lock().unwrap().read(&span, dst)),
                    };
                let job = FillJob {
                    owner: FillOwner::Export(id),
                    window: spec.index,
                    address,
                    length: spec.length as usize,
                    work,
                };
                if !self.submit_fill(job) {
                    let run = self.source.exports.get_mut(&id).expect("present");
                    run.jobs_outstanding -= 1;
                    run.host_filling.remove(&spec.index);
                    run.failed = true;
                    self.fill_failed(id, spec.index);
                    return true;
                }
                for (rank, xfer) in shared {
                    let sent = self.ranks.as_ref().is_some_and(|ranks| {
                        ranks.send_window(
                            rank,
                            super::super::rank::link::Window {
                                xfer,
                                spec: Some(spec),
                                sums: None,
                            },
                        )
                    });
                    if !sent {
                        if let Some(run) = self.source.exports.get_mut(&id) {
                            run.failed = true;
                        }
                        break;
                    }
                }
            }
        }
        true
    }

    fn fill_failed(&mut self, export: u64, window: u32) {
        if self
            .source
            .exports
            .get(&export)
            .is_some_and(|run| run.reference.is_some())
        {
            // The channel carries the windows: dropping it tells the manager.
            return;
        }
        self.emit(Out::WindowFilled(wire::WindowFilled {
            export_id: export,
            index: window,
            status: wire::FillStatus::FillFailed as i32,
            page_xxh3: Vec::new(),
            row_xxh3: 0,
            has_row: false,
            rank_sums: Vec::new(),
        }));
    }

    /// A D2H job of an export drained.
    pub(crate) fn on_export_job(&mut self, export: u64, window: u32, result: crate::Result<()>) {
        let Some(run) = self.source.exports.get_mut(&export) else {
            return;
        };
        run.jobs_outstanding = run.jobs_outstanding.saturating_sub(1);
        let spec = run.windows.get(&window).cloned();
        let failed_before = run.failed;
        match (result, spec) {
            (Ok(()), Some(spec)) => {
                if self.window_still_running(export, window) {
                    return;
                }
                if !failed_before {
                    self.fill_done(export, spec, None);
                }
            }
            (Err(_), _) | (_, None) => {
                if !failed_before {
                    if let Some(run) = self.source.exports.get_mut(&export) {
                        run.failed = true;
                    }
                    self.fill_failed(export, window);
                }
            }
        }
        self.drive_export(export);
    }

    pub(super) fn fail_export_window(&mut self, export: u64, window: u32) {
        self.fill_failed(export, window);
    }

    pub(super) fn fill_done_if_idle(&mut self, export: u64, index: u32, spec: wire::WindowSpec) {
        if !self.window_still_running(export, index) {
            let supplied = self
                .source
                .exports
                .get_mut(&export)
                .and_then(|run| run.host_sums.remove(&index));
            self.fill_done(export, spec, supplied);
        }
    }

    pub(super) fn drive_export_now(&mut self, export: u64) {
        self.drive_export(export);
    }

    /// Whether another job of the same window is still queued or running.
    fn window_still_running(&self, export: u64, window: u32) -> bool {
        let owner_is = |o: &Owner| matches!(o, Owner::Export { export: e, window: w } if *e == export && *w == window);
        self.jobs.values().any(owner_is)
            || self.pending.iter().any(|(_, o)| owner_is(o))
            || self
                .source
                .exports
                .get(&export)
                .is_some_and(|run| run.host_filling.contains(&window))
            || self
                .source
                .exports
                .get(&export)
                .and_then(|run| run.peer_windows.get(&window))
                .is_some_and(|peers| !peers.left.is_empty())
    }

    /// The window's bytes are in place: tell the manager, with the persisted
    /// checksums for a host source and fresh ones for a GPU source.
    fn fill_done(&mut self, export: u64, spec: wire::WindowSpec, persisted: Option<Checksums>) {
        let page_bytes = self.st.page_record_bytes() as usize;
        let on_channel = self
            .source
            .exports
            .get(&export)
            .is_some_and(|run| run.reference.is_some());
        // What this rank hashes: a multi-process instance's rank states checksums of its own
        // slices, which start each record of a shared window and fill a channel's slot.
        let (stride, hash_len, row_len) = match &self.ranks {
            Some(ranks) => {
                let mine = ranks.layout.rank(0);
                (
                    if on_channel {
                        mine.page_len as usize
                    } else {
                        page_bytes
                    },
                    mine.page_len as usize,
                    mine.row_len as usize,
                )
            }
            None => (page_bytes, page_bytes, self.st.row_record_bytes() as usize),
        };
        let memory = self.memory();
        let sums = match persisted {
            Some(sums) => Some(sums),
            None => memory
                .as_ref()
                .and_then(|m| m.region(spec.region()))
                .and_then(|r| r.bytes(spec.offset, spec.length))
                .map(|bytes| {
                    let count = spec.page_count as usize;
                    Checksums {
                        pages: (0..count)
                            .map(|i| checksum(&bytes[i * stride..i * stride + hash_len]))
                            .collect(),
                        row: spec
                            .row
                            .then(|| checksum(&bytes[count * stride..count * stride + row_len])),
                    }
                }),
        };
        let Some(sums) = sums else {
            if let Some(run) = self.source.exports.get_mut(&export) {
                run.failed = true;
            }
            return self.fill_failed(export, spec.index);
        };
        let Some(run) = self.source.exports.get_mut(&export) else {
            return;
        };
        run.windows.remove(&spec.index);
        if run.reference.is_some() {
            // The window is written to the manager on the channel, from its slot.
            let slot = run.slots.get(&spec.index).copied();
            let sent = slot.is_some_and(|slot| {
                run.channel.as_ref().is_some_and(|channel| {
                    channel.send(Filled {
                        index: spec.index,
                        row_xxh3: sums.row,
                        page_xxh3: sums.pages.clone(),
                        slot,
                        length: spec.length,
                    })
                })
            });
            if sent {
                run.sends_outstanding += 1;
            } else {
                run.failed = true;
            }
            return;
        }
        if self.ranks.is_some() {
            // Rank 0 attests its own slice and those of the shared peers.
            let run = self.source.exports.get_mut(&export).expect("present");
            let peers = run.peer_windows.remove(&spec.index);
            let mut rank_sums = vec![wire::RankSums {
                rank: 0,
                page_xxh3: sums.pages,
                row_xxh3: sums.row.unwrap_or(0),
                has_row: sums.row.is_some(),
            }];
            for (rank, peer) in peers.map(|p| p.sums).unwrap_or_default() {
                rank_sums.push(wire::RankSums {
                    rank: rank as u32,
                    page_xxh3: peer.pages,
                    row_xxh3: peer.row.unwrap_or(0),
                    has_row: peer.row.is_some(),
                });
            }
            return self.emit(Out::WindowFilled(wire::WindowFilled {
                export_id: export,
                index: spec.index,
                status: wire::FillStatus::FillOk as i32,
                rank_sums,
                ..Default::default()
            }));
        }
        self.emit(Out::WindowFilled(wire::WindowFilled {
            export_id: export,
            index: spec.index,
            status: wire::FillStatus::FillOk as i32,
            page_xxh3: sums.pages,
            row_xxh3: sums.row.unwrap_or(0),
            has_row: sums.row.is_some(),
            rank_sums: Vec::new(),
        }));
    }

    /// A filled window of an export has left its slot.
    pub(crate) fn on_export_sent(&mut self, export: u64, index: u32, slot: u32) {
        // The slot is free whatever became of the export: the task is done with it.
        if let Some(run) = self.source.exports.get_mut(&export) {
            run.sends_outstanding = run.sends_outstanding.saturating_sub(1);
            run.slots.remove(&index);
        }
        self.free_slot(slot);
        self.drive_export(export);
    }

    /// An export's transfer failed.
    pub(crate) fn net_export_failed(&mut self, export: u64, _error: String) {
        if let Some(run) = self.source.exports.get_mut(&export) {
            run.failed = true;
            self.drive_export(export);
        }
    }

    /// An export's task is gone and its connection closed.
    pub(crate) fn net_export_closed(&mut self, export: u64) {
        let Some(run) = self.source.exports.get_mut(&export) else {
            return;
        };
        run.channel = None;
        run.channel_open = false;
        // Nothing sends from a slot any more.
        run.sends_outstanding = 0;
        let slots: Vec<u32> = run.slots.drain().map(|(_, slot)| slot).collect();
        let complete = run.coverage.as_ref().is_some_and(|c| c.complete());
        if !complete && !run.cancelled && !run.lost {
            run.failed = true;
        }
        for slot in slots {
            self.free_slot(slot);
        }
        self.drive_export(export);
    }

    /// End the export if it is over and nothing reads its pages any more.
    fn drive_export(&mut self, id: u64) {
        let Some(run) = self.source.exports.get_mut(&id) else {
            return;
        };
        if run.jobs_outstanding > 0 || !matches!(run.phase, Phase::Pinned) {
            return;
        }
        let complete = run.coverage.as_ref().is_some_and(|c| c.complete());
        if !(complete || run.cancelled || run.failed || run.lost) {
            return;
        }
        // Windows still on their way to the manager are let out first, unless the export
        // is being abandoned. The task is then stopped, and nothing is ended while it
        // may still use a slot.
        let abandoned = run.cancelled || run.failed || run.lost;
        if run.sends_outstanding > 0 && !abandoned {
            return;
        }
        if let Some(channel) = &run.channel {
            channel.stop();
        }
        if run.channel_open {
            return;
        }
        // The peer ranks end their parts too: after their copies drained.
        if abandoned {
            self.cancel_export_peers(id);
        }
        let run = self.source.exports.get_mut(&id).expect("present");
        if run.peers.iter().any(|peer| peer.done.is_none()) {
            return;
        }
        let status = if run.failed {
            wire::ExportEndStatus::ExportFailed
        } else if complete && !run.cancelled && !run.lost {
            wire::ExportEndStatus::ExportComplete
        } else {
            wire::ExportEndStatus::ExportReleased
        };
        let run = self.source.exports.remove(&id).expect("present");
        if matches!(run.pin, Some(Pin::Gpu(_))) {
            self.event(PoolEvent::ExportEnded {
                export: ExportId(id),
            });
        }
        drop(run);
        self.emit(Out::ExportEnd(wire::ExportEnd {
            export_id: id,
            status: status as i32,
            fence: wire::FenceOutcome::Drained as i32,
        }));
    }

    /// Collect finished host fills and answer export requests the worker
    /// missed the deadline on.
    pub(crate) fn source_tick(&mut self, now: Instant) {
        for FillDone {
            owner,
            window,
            result,
        } in self.collect_fills()
        {
            let FillOwner::Export(export) = owner else {
                self.on_put_fill(owner, window, result);
                continue;
            };
            let Some(run) = self.source.exports.get_mut(&export) else {
                continue;
            };
            run.jobs_outstanding = run.jobs_outstanding.saturating_sub(1);
            run.host_filling.remove(&window);
            match (result, run.windows.get(&window).cloned()) {
                (Ok(sums), Some(spec)) => {
                    if self.window_still_running(export, window) {
                        // The peers' slices are not all in; the last of them completes it.
                        if let Some(run) = self.source.exports.get_mut(&export) {
                            run.host_sums.insert(window, sums);
                        }
                    } else {
                        self.fill_done(export, spec, Some(sums));
                    }
                }
                _ => {
                    run.failed = true;
                    self.fill_failed(export, window);
                }
            }
            self.drive_export(export);
        }
        let late: Vec<u64> = self
            .source
            .exports
            .iter()
            .filter(|(_, r)| matches!(r.phase, Phase::AtWorker) && !r.answered && now >= r.deadline)
            .map(|(id, _)| *id)
            .collect();
        for id in late {
            // The manager has given up; the worker's late answer is discarded
            // and its pin, if any, released.
            if let Some(run) = self.source.exports.get_mut(&id) {
                run.answered = true;
            }
            self.pin_refused(id, wire::ClaimReason::SourceBusy);
        }
    }

    /// The session is gone: pins are released after the copies drain, and the
    /// `ExportEnd` is owed to the next session. Requests the worker has not
    /// answered are dropped; a late pin is released when the worker answers.
    pub(crate) fn source_lost(&mut self) {
        let ids: Vec<u64> = self.source.exports.keys().copied().collect();
        for id in ids {
            let Some(run) = self.source.exports.get_mut(&id) else {
                continue;
            };
            run.lost = true;
            if matches!(run.phase, Phase::AtWorker) {
                self.shared.withdraw_export(ExportId(id));
                self.source.exports.remove(&id);
            } else {
                self.drive_export(id);
            }
        }
    }

    pub(crate) fn source_close(&mut self) {
        let ids: Vec<u64> = self.source.exports.keys().copied().collect();
        for id in ids {
            if let Some(run) = self.source.exports.get_mut(&id) {
                run.cancelled = true;
            }
            self.on_export_cancel(wire::ExportCancel {
                export_id: id,
                reason: 0,
            });
        }
    }

    pub(crate) fn source_quarantine(&mut self) {
        let ids: Vec<u64> = self.source.exports.keys().copied().collect();
        for id in ids {
            self.emit(Out::ExportEnd(wire::ExportEnd {
                export_id: id,
                status: wire::ExportEndStatus::ExportFailed as i32,
                fence: wire::FenceOutcome::Quarantined as i32,
            }));
        }
        // The pins stay: their copies may still be reading the pages.
        std::mem::forget(std::mem::take(&mut self.source.exports));
    }
}
