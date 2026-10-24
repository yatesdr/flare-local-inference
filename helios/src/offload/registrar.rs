//! Registration of host memory with the CUDA driver for the selene host tiers.

use flare_selene::{HostRegistrar, Registration, Result, SplitMapping};

use crate::cuda::{self, DeviceHostParts, HostRegistration};

pub(super) struct CudaRegistrar;

/// Registers the pool's shared-memory regions. The pool's copier names their
/// host addresses directly in copies, so a registration whose device address
/// differs from its host address is refused.
pub(super) struct PoolRegistrar;

impl HostRegistrar for PoolRegistrar {
    fn register(
        &self,
        context: usize,
        address: usize,
        bytes: usize,
    ) -> Result<Box<dyn Registration>> {
        let registration = CudaRegistrar.register(context, address, bytes)?;
        if registration.device_address() != address as u64 {
            return Err(flare_selene::Error::new(
                "pool memory needs identical host and device addresses",
            ));
        }
        Ok(registration)
    }

    fn map_split(&self, _requests: &[(usize, i32, usize)]) -> Result<Box<dyn SplitMapping>> {
        Err(flare_selene::Error::new(
            "the pool does not use split mappings",
        ))
    }
}

impl HostRegistrar for CudaRegistrar {
    fn register(
        &self,
        context: usize,
        address: usize,
        bytes: usize,
    ) -> Result<Box<dyn Registration>> {
        // SAFETY: The slab pool that asks keeps the range mapped at this size
        // until it drops the returned handle, which unregisters it first.
        let registration = unsafe { cuda::register_host(context, address, bytes) }?;
        Ok(Box::new(registration))
    }

    fn map_split(&self, requests: &[(usize, i32, usize)]) -> Result<Box<dyn SplitMapping>> {
        Ok(Box::new(cuda::map_device_host_parts(requests)?))
    }
}

impl Registration for HostRegistration {
    fn device_address(&self) -> u64 {
        HostRegistration::device_address(self)
    }
}

impl SplitMapping for DeviceHostParts {
    fn address(&self) -> usize {
        DeviceHostParts::address(self)
    }

    fn nbytes(&self) -> usize {
        DeviceHostParts::nbytes(self)
    }

    fn part_address(&self, index: usize) -> usize {
        DeviceHostParts::part_address(self, index)
    }
}
