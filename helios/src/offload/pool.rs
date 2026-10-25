//! CUDA registration and progress display for Selene startup.
use super::{CacheGeometry, PoolOptions, registrar};
use crate::{Result, startup::StartupProgress};
pub(crate) use flare_selene::pool::{Budget, PreparedPool};
use flare_selene::pool::{PoolBuilder, PoolPeers};
use std::sync::Arc;

#[allow(clippy::too_many_arguments)]
pub(crate) fn start(
    options: &PoolOptions,
    identity: [u8; 32],
    geometry: &[CacheGeometry],
    gpu_uuids: Vec<[u8; 16]>,
    registration_context: usize,
    budget: Budget,
    progress: &StartupProgress,
    peers: Option<PoolPeers>,
    disk: bool,
) -> Result<(PreparedPool, Vec<u64>)> {
    let phase = progress.phase("Wait for prefix cache pool", None);
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
    .start(Arc::new(registrar::CudaRegistrar))?;
    phase.finish(pool.summary()?);
    Ok((pool, charges))
}

#[cfg(test)]
mod tests;
