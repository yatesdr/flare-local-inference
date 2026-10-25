//! Bind the pool's startup registration to the final CUDA cache geometry.
use super::{CacheGeometry, PoolOptions, PrefixStore, registrar, transfer};
use crate::{Result, startup::StartupProgress};
pub(crate) use flare_selene::pool::Budget;
use flare_selene::{
    HostStore, Layout, PageCopier,
    copier::{Job, Reply},
    pool::PoolBuilder,
};
use std::sync::{Arc, Mutex};

/// Registration precedes cache sizing; copying starts with the final cache geometry.
struct Copier {
    layout: Layout,
    transfers: Option<transfer::Transfers>,
}

impl PageCopier for Copier {
    fn layout(&self) -> &Layout {
        &self.layout
    }
    fn submit(&mut self, job: Job) -> flare_selene::Result<()> {
        self.transfers
            .as_mut()
            .ok_or("pool copies are not initialized")?
            .submit(job)
    }
    fn poll(&mut self, replies: &mut Vec<Reply>) {
        if let Some(transfers) = &mut self.transfers {
            transfers.poll(replies);
        }
    }
    fn in_flight(&self) -> usize {
        self.transfers.as_ref().map_or(0, PageCopier::in_flight)
    }
    fn close(&mut self) -> flare_selene::Result<()> {
        self.transfers.as_mut().map_or(Ok(()), PageCopier::close)
    }
}

pub(crate) struct PreparedPool {
    pool: flare_selene::pool::PreparedPool,
    copier: Arc<Mutex<Copier>>,
}
impl PreparedPool {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn start(
        options: &PoolOptions,
        identity: [u8; 32],
        geometry: &[CacheGeometry],
        gpu_uuids: Vec<[u8; 16]>,
        registration_context: usize,
        budget: Budget,
        progress: &StartupProgress,
        peers: Option<super::shard::PoolPeers>,
        disk: bool,
    ) -> Result<(Self, Vec<u64>)> {
        Self::start_with_registrar(
            options,
            identity,
            geometry,
            gpu_uuids,
            registration_context,
            budget,
            progress,
            Arc::new(registrar::CudaRegistrar),
            peers,
            disk,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn start_with_registrar(
        options: &PoolOptions,
        identity: [u8; 32],
        geometry: &[CacheGeometry],
        gpu_uuids: Vec<[u8; 16]>,
        registration_context: usize,
        budget: Budget,
        progress: &StartupProgress,
        registrar: Arc<dyn flare_selene::HostRegistrar + Send + Sync>,
        peers: Option<super::shard::PoolPeers>,
        disk: bool,
    ) -> Result<(Self, Vec<u64>)> {
        let phase = progress.phase("Wait for prefix cache pool", None);
        let copier = Arc::new(Mutex::new(Copier {
            layout: Layout::new(geometry)?,
            transfers: None,
        }));
        let (pool, charges) = PoolBuilder {
            options,
            identity,
            geometry,
            gpu_uuids,
            registration_context,
            budget,
            peers,
            disk,
        }
        .start(copier.clone(), registrar)?;
        phase.finish(pool.summary()?);
        Ok((Self { pool, copier }, charges))
    }
    pub(crate) fn take_listener(&mut self) -> Option<(std::net::TcpListener, [u8; 16])> {
        self.pool.take_listener()
    }
    pub(crate) fn approved(&self) -> bool {
        self.pool.approved()
    }
    pub(crate) fn close(&mut self) -> Result<()> {
        Ok(self.pool.close()?)
    }
    pub(crate) fn prepare_peer(&mut self, prepared: &super::Prepared) -> Result<()> {
        if self.pool.agent.is_some() {
            self.start_copies(&prepared.contexts, prepared.geometry.clone())?;
            if let Some(records) = &self.pool.records
                && prepared.directory_lock.is_some()
            {
                records.bind(
                    prepared
                        .resolved
                        .directory
                        .clone()
                        .ok_or("a rank's records need a disk directory")?,
                    prepared.identity,
                    prepared.fingerprint,
                    prepared.geometry.clone(),
                    prepared
                        .directory_lock
                        .clone()
                        .ok_or("a rank's records need a directory lock")?,
                )?;
            }
        }
        Ok(())
    }

    pub(super) fn start_copies(
        &self,
        contexts: &[(usize, i32)],
        geometry: Vec<CacheGeometry>,
    ) -> Result<()> {
        let mut copier = self.copier.lock().unwrap();
        if Layout::new(&geometry)? != copier.layout {
            return Err("pool registration and final cache layouts disagree".into());
        }
        copier.transfers = Some(transfer::Transfers::start_unmapped(contexts, geometry)?);
        Ok(())
    }

    pub(super) fn attach(self, local: Option<Box<dyn HostStore>>) -> Result<Box<dyn PrefixStore>> {
        Ok(self.pool.attach(local)?)
    }
}
#[cfg(test)]
mod tests;
