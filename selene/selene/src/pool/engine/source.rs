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
        shared::{ExportRequest, PoolEvent},
        wire::{self, client_message::M as Out},
    },
    Engine, Owner,
    coverage::Coverage,
    hostfill::{FillDone, FillJob, FillOwner},
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
}

#[derive(Default)]
pub(crate) struct State {
    exports: HashMap<u64, Export>,
}

impl State {
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
        ExportRefusal::GenerationChanged => wire::ClaimReason::GenerationChanged,
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
        if sess.regions.is_none()
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
        };
        self.source.exports.insert(id, export);
        self.event(PoolEvent::Export(ExportRequest {
            export: ExportId(id),
            tier,
            key,
            generation: request.generation,
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
        let generations: Vec<u64> = chain.nodes.iter().map(|n| n.generation).collect();
        let row = chain.row.is_some();
        self.pinned(export, records, generations, row, Pin::Gpu(chain));
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
        let generations = chain.records.iter().map(|r| r.generation).collect();
        self.pinned(
            export,
            records,
            generations,
            chain.row,
            Pin::Host(Arc::new(Mutex::new(pin))),
        );
    }

    /// Answer an export request with the chain that was pinned for it, or
    /// release the pin if the export can no longer use it.
    fn pinned(
        &mut self,
        export: ExportId,
        expected: Vec<ExpectedRecord>,
        generations: Vec<u64>,
        row: bool,
        pin: Pin,
    ) {
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
        let records = expected
            .iter()
            .zip(generations)
            .map(|(r, generation)| wire::Record {
                key: Some(codec::key_to(r.key)),
                generation,
                first_page: r.first_page,
                pages: r.pages,
                row: r.row,
            })
            .collect();
        run.phase = Phase::Pinned;
        run.coverage = Some(Coverage::new(expected));
        run.pin = Some(pin);
        self.emit(Out::PinAck(wire::PinAck {
            export_id: id,
            result: wire::PinResult::Pinned as i32,
            refusal: 0,
            records,
            total_pages: total,
            row,
        }));
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
        let (page_bytes, row_bytes) = (self.st.page_record_bytes(), self.st.row_record_bytes());
        let regions = self.sess.as_ref().and_then(|s| s.regions.clone());
        let Some(run) = self.source.exports.get_mut(&id) else {
            return;
        };
        if !matches!(run.phase, Phase::Pinned) || run.cancelled || run.failed {
            return;
        }
        let Some(regions) = regions else {
            run.failed = true;
            return self.drive_export(id);
        };
        for spec in fill.windows {
            let run = self.source.exports.get_mut(&id).expect("present");
            if run.failed {
                break;
            }
            let coverage = run.coverage.as_mut().expect("pinned has coverage");
            let ok = coverage.accept(&spec, page_bytes, row_bytes);
            let region = (spec.region() == wire::Region::Staging).then_some(&regions.staging);
            let address = region.and_then(|r| r.address(spec.offset, spec.length));
            let (Ok(_), Some(address)) = (ok, address) else {
                run.failed = true;
                self.fill_failed(id, spec.index);
                break;
            };
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
                    for job in jobs {
                        self.queue_job(
                            job,
                            Owner::Export {
                                export: id,
                                window: spec.index,
                            },
                        );
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
                    let job = FillJob {
                        owner: FillOwner::Export(id),
                        window: spec.index,
                        address,
                        length: spec.length as usize,
                        work: Box::new(move |dst| pin.lock().unwrap().read(&span, dst)),
                    };
                    if !self.submit_fill(job) {
                        let run = self.source.exports.get_mut(&id).expect("present");
                        run.jobs_outstanding -= 1;
                        run.failed = true;
                        self.fill_failed(id, spec.index);
                        break;
                    }
                }
            }
        }
        self.drive_export(id);
    }

    fn fill_failed(&mut self, export: u64, window: u32) {
        self.emit(Out::WindowFilled(wire::WindowFilled {
            export_id: export,
            index: window,
            status: wire::FillStatus::FillFailed as i32,
            page_xxh3: Vec::new(),
            row_xxh3: 0,
            has_row: false,
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

    /// Whether another job of the same window is still queued or running.
    fn window_still_running(&self, export: u64, window: u32) -> bool {
        let owner_is = |o: &Owner| matches!(o, Owner::Export { export: e, window: w } if *e == export && *w == window);
        self.jobs.values().any(owner_is) || self.pending.iter().any(|(_, o)| owner_is(o))
    }

    /// The window's bytes are in place: tell the manager, with the persisted
    /// checksums for a host source and fresh ones for a GPU source.
    fn fill_done(&mut self, export: u64, spec: wire::WindowSpec, persisted: Option<Checksums>) {
        let page_bytes = self.st.page_record_bytes() as usize;
        let sums = match persisted {
            Some(sums) => Some(sums),
            None => self
                .sess
                .as_ref()
                .and_then(|s| s.regions.as_ref())
                .and_then(|r| r.staging.bytes(spec.offset, spec.length))
                .map(|bytes| {
                    let count = spec.page_count as usize;
                    Checksums {
                        pages: (0..count)
                            .map(|i| checksum(&bytes[i * page_bytes..(i + 1) * page_bytes]))
                            .collect(),
                        row: spec.row.then(|| checksum(&bytes[count * page_bytes..])),
                    }
                }),
        };
        let Some(sums) = sums else {
            if let Some(run) = self.source.exports.get_mut(&export) {
                run.failed = true;
            }
            return self.fill_failed(export, spec.index);
        };
        self.emit(Out::WindowFilled(wire::WindowFilled {
            export_id: export,
            index: spec.index,
            status: wire::FillStatus::FillOk as i32,
            page_xxh3: sums.pages,
            row_xxh3: sums.row.unwrap_or(0),
            has_row: sums.row.is_some(),
        }));
        if let Some(run) = self.source.exports.get_mut(&export) {
            run.windows.remove(&spec.index);
        }
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
            match (result, run.windows.get(&window).cloned()) {
                (Ok(sums), Some(spec)) => self.fill_done(export, spec, Some(sums)),
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
