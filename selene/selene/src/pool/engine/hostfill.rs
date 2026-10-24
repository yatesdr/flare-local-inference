//! The copy thread that fills windows from host sources: pinned host records
//! for exports, and bytes a store or a probe supplies for puts.
//!
//! Started on first use only, so an instance that never serves a host-tier
//! export or writes a host-tier put has no such thread.

use std::{
    sync::{Arc, mpsc},
    thread,
};

use tokio::sync::Notify;

use crate::Result;

use super::super::exporter::Checksums;

/// Whose window a fill is for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FillOwner {
    Export(u64),
    Put(u64),
}

/// Fills a window with the bytes the owner serves and returns their checksums.
pub(crate) type Work = Box<dyn FnOnce(&mut [u8]) -> Result<Checksums> + Send>;

pub(crate) struct FillJob {
    pub owner: FillOwner,
    pub window: u32,
    /// The window's address and length inside a mapped region, which the engine
    /// keeps mapped until the job's completion is collected.
    pub address: usize,
    pub length: usize,
    pub work: Work,
}

pub(crate) struct FillDone {
    pub owner: FillOwner,
    pub window: u32,
    pub result: Result<Checksums>,
}

pub(crate) struct HostFiller {
    jobs: mpsc::Sender<FillJob>,
    done: mpsc::Receiver<FillDone>,
}

impl HostFiller {
    pub(crate) fn start(wake: Arc<Notify>) -> std::io::Result<Self> {
        let (jobs, rx) = mpsc::channel::<FillJob>();
        let (tx, done) = mpsc::channel();
        thread::Builder::new()
            .name("selene-host-fill".into())
            .spawn(move || {
                while let Ok(job) = rx.recv() {
                    // The window belongs to this job until it reports done.
                    let dst = unsafe {
                        std::slice::from_raw_parts_mut(job.address as *mut u8, job.length)
                    };
                    let result = (job.work)(dst);
                    let _ = tx.send(FillDone {
                        owner: job.owner,
                        window: job.window,
                        result,
                    });
                    wake.notify_one();
                }
            })?;
        Ok(Self { jobs, done })
    }

    pub(crate) fn submit(&self, job: FillJob) -> bool {
        self.jobs.send(job).is_ok()
    }

    pub(crate) fn collect(&self, out: &mut Vec<FillDone>) {
        while let Ok(done) = self.done.try_recv() {
            out.push(done);
        }
    }
}
