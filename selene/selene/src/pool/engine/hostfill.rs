//! The copy thread that fills windows from pinned host records.
//!
//! Started on the first host-tier export only, so an instance that never
//! serves one has no such thread.

use std::{
    sync::{Arc, Mutex, mpsc},
    thread,
};

use tokio::sync::Notify;

use crate::Result;

use super::super::exporter::{Checksums, HostPin, HostWindow};

pub(crate) struct FillJob {
    pub export: u64,
    pub window: u32,
    pub pin: Arc<Mutex<Box<dyn HostPin>>>,
    pub span: HostWindow,
    /// The window's address and length inside the mapped staging region, which
    /// the engine keeps mapped until the job's completion is collected.
    pub address: usize,
    pub length: usize,
}

pub(crate) struct FillDone {
    pub export: u64,
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
            .name("selene-host-export".into())
            .spawn(move || {
                while let Ok(job) = rx.recv() {
                    // The window is owned by this export until it reports done.
                    let dst = unsafe {
                        std::slice::from_raw_parts_mut(job.address as *mut u8, job.length)
                    };
                    let result = job.pin.lock().unwrap().read(&job.span, dst);
                    let _ = tx.send(FillDone {
                        export: job.export,
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
