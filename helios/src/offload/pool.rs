//! Attach and budget pool regions before cache allocation and admission.

use std::sync::{Arc, Mutex};

use flare_selene::{
    HostStore, Layout, PageCopier, SharedCopier,
    copier::{Job, Reply},
    pool::{PlannedRegion, PoolClient, PoolConfig, PooledStore},
};

use super::{
    CacheGeometry, PoolOptions, PrefixStore, adapter, pool_address, pool_scope, registrar, transfer,
};
use crate::{Result, startup::StartupProgress};

/// Remaining startup supply, after local tiers, runtime headroom and minimum caches.
pub(crate) struct Budget {
    pub host: std::result::Result<u64, String>,
    pub devices: Vec<(u64, bool)>,
}

impl Budget {
    fn costs(plan: &[PlannedRegion]) -> std::result::Result<(u64, u64), String> {
        plan.iter()
            .try_fold((0u64, 0u64), |(host, device), region| {
                Ok((
                    host.checked_add(region.bytes)
                        .ok_or("pool host bytes overflow")?,
                    device
                        .checked_add(region.bytes.div_ceil(512))
                        .ok_or("pool device bytes overflow")?,
                ))
            })
    }

    pub(crate) fn charges(&self, host: u64, device: u64) -> Vec<u64> {
        let integrated = self.devices.iter().filter(|(_, shared)| *shared).count() as u64;
        self.devices
            .iter()
            .map(|(_, shared)| {
                device.saturating_add(if *shared {
                    host.div_ceil(integrated)
                } else {
                    0
                })
            })
            .collect()
    }

    fn approve(&self, plan: &[PlannedRegion]) -> std::result::Result<(), String> {
        let (host, device) = Self::costs(plan)?;
        let available_host = self
            .host
            .as_ref()
            .map_err(|error| format!("cannot budget pool host memory: {error}"))?;
        if host > *available_host {
            return Err(format!(
                "needs {host} host bytes and {device} device bytes per GPU; host budget has {} bytes left",
                available_host
            ));
        }
        for (rank, ((available, _), required)) in self
            .devices
            .iter()
            .zip(self.charges(host, device))
            .enumerate()
        {
            if required > *available {
                return Err(format!(
                    "needs {host} host bytes and {device} device bytes per GPU; GPU {rank} needs {required} bytes including shared host memory, but its budget has {available} bytes left"
                ));
            }
        }
        Ok(())
    }
}

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
    client: PoolClient,
    copier: Arc<Mutex<Copier>>,
    identity: flare_selene::plan::Identity,
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
    ) -> Result<(Self, Vec<u64>)> {
        Self::start_with_registrar(
            options,
            identity,
            geometry,
            gpu_uuids,
            registration_context,
            budget,
            progress,
            Arc::new(registrar::PoolRegistrar),
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
    ) -> Result<(Self, Vec<u64>)> {
        let phase = progress.phase("Wait for prefix cache pool", None);
        let copier = Arc::new(Mutex::new(Copier {
            layout: Layout::new(geometry)?,
            transfers: None,
        }));
        let devices = budget.devices.clone();
        let scope = pool_scope(&options.tenant, identity, geometry)?;
        let identity = flare_selene::plan::Identity {
            tenant: scope.tenant.clone(),
            model: scope.model_identity,
            layout: scope.layout_fingerprint,
        };
        let config = PoolConfig {
            address: pool_address(&options.address)?,
            tenant: options.tenant.clone(),
            ca: options.ca.clone(),
            scope,
            instance_name: format!("flare-{}", std::process::id()),
            gpu_uuids,
            export_slots: 2,
            claim_slots: 2,
            level_order: None,
            registration_context,
            approve: Some(Arc::new(move |plan| budget.approve(plan))),
        };
        let mut client = PoolClient::start(config, copier.clone(), registrar)?;
        if client.registered_regions().is_empty() {
            // Join an unfinished attachment before caches consume its startup allowance.
            client.close()?;
        }
        let plan: Vec<_> = client
            .registered_regions()
            .iter()
            .map(|region| PlannedRegion {
                name: region.name,
                bytes: region.bytes as u64,
            })
            .collect();
        let (host, device) = Budget::costs(&plan)?;
        let charges = Budget {
            host: Ok(0),
            devices,
        }
        .charges(host, device);
        phase.finish(if plan.is_empty() {
            "local tiers only".into()
        } else {
            format!("{host} host bytes · {device} device bytes per GPU")
        });
        Ok((
            Self {
                client,
                copier,
                identity,
            },
            charges,
        ))
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
        let layout = self.copier.lock().unwrap().layout.clone();
        let copier: SharedCopier = self.copier;
        let store = PooledStore::new(
            adapter::Tiers::new(local),
            self.client,
            self.identity,
            &layout,
        );
        Ok(Box::new(
            adapter::Host::new(Box::new(store), true).with_pool_copier(copier),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_selene::pool::PoolStatus;
    use std::{
        sync::atomic::{AtomicBool, Ordering},
        thread,
        time::{Duration, Instant},
    };

    fn plan(arena: u64, staging: u64) -> Vec<PlannedRegion> {
        vec![
            PlannedRegion {
                name: "arena",
                bytes: arena,
            },
            PlannedRegion {
                name: "staging",
                bytes: staging,
            },
        ]
    }

    #[test]
    fn approval_charges_whole_regions_and_each_gpu_before_registration() {
        let plan = plan(1 << 30, 1 << 29);
        let (host, device) = Budget::costs(&plan).unwrap();
        assert_eq!((host, device), (3 << 29, 3 << 20));
        let budget = Budget {
            host: Ok(host),
            devices: vec![(device, false); 2],
        };
        assert!(budget.approve(&plan).is_ok());
        assert_eq!(budget.charges(host, device), vec![device; 2]);
        assert!(
            Budget {
                host: Ok(host - 1),
                ..budget
            }
            .approve(&plan)
            .unwrap_err()
            .contains("host budget")
        );
        let budget = Budget {
            host: Ok(host),
            devices: vec![(device, false), (device - 1, false)],
        };
        assert!(budget.approve(&plan).unwrap_err().contains("GPU 1"));
        assert!(Budget::costs(&self::plan(u64::MAX, 1)).is_err());
    }

    #[test]
    fn remote_staging_and_shared_memory_use_the_same_budget() {
        let plan = vec![PlannedRegion {
            name: "data-staging",
            bytes: 4097,
        }];
        assert_eq!(Budget::costs(&plan).unwrap(), (4097, 9));
        let budget = Budget {
            host: Ok(4097),
            devices: vec![(2058, true), (2058, true), (9, false)],
        };
        assert_eq!(budget.charges(4097, 9), [2058, 2058, 9]);
        assert!(budget.approve(&plan).is_ok());
        assert!(
            Budget {
                host: Ok(4097),
                devices: vec![(2057, true), (2058, true)]
            }
            .approve(&plan)
            .is_err()
        );
        assert!(
            Budget {
                host: Err("no memory observation".into()),
                devices: Vec::new()
            }
            .approve(&plan)
            .unwrap_err()
            .contains("no memory observation")
        );
    }

    #[derive(Default)]
    struct Registrar {
        calls: Mutex<Vec<usize>>,
        drops: Arc<Mutex<usize>>,
    }
    struct Registration {
        address: usize,
        drops: Arc<Mutex<usize>>,
    }
    impl flare_selene::Registration for Registration {
        fn device_address(&self) -> u64 {
            self.address as u64
        }
    }
    impl Drop for Registration {
        fn drop(&mut self) {
            *self.drops.lock().unwrap() += 1;
        }
    }
    impl flare_selene::HostRegistrar for Registrar {
        fn register(
            &self,
            _: usize,
            address: usize,
            bytes: usize,
        ) -> flare_selene::Result<Box<dyn flare_selene::Registration>> {
            self.calls.lock().unwrap().push(bytes);
            Ok(Box::new(Registration {
                address,
                drops: self.drops.clone(),
            }))
        }
        fn map_split(
            &self,
            _: &[(usize, i32, usize)],
        ) -> flare_selene::Result<Box<dyn flare_selene::SplitMapping>> {
            Err("not used".into())
        }
    }

    struct Manager {
        config: selene_manager::store::Config,
        directory: std::path::PathBuf,
        stop: Arc<AtomicBool>,
        thread: Option<thread::JoinHandle<()>>,
    }
    impl Manager {
        fn new() -> Self {
            static SERIAL: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
            let name = format!(
                "flare-pool-startup-{}-{}",
                std::process::id(),
                SERIAL.fetch_add(1, Ordering::Relaxed)
            );
            let directory = std::env::temp_dir().join(&name);
            std::fs::create_dir_all(&directory).unwrap();
            let mut config =
                selene_manager::store::Config::dram_only(directory.join("manager.sock"), 8 << 20);
            config.name = name;
            config.staging = 8 << 20;
            Self {
                config,
                directory,
                stop: Arc::new(AtomicBool::new(false)),
                thread: None,
            }
        }
        fn run(&mut self) {
            self.stop.store(false, Ordering::Release);
            let (config, stop) = (self.config.clone(), self.stop.clone());
            self.thread = Some(thread::spawn(move || {
                selene_manager::control::run(config, stop).unwrap()
            }));
            wait(|| self.config.socket.exists());
        }
        fn stop(&mut self) {
            self.stop.store(true, Ordering::Release);
            if let Some(thread) = self.thread.take() {
                thread.join().unwrap();
            }
        }
        fn options(&self) -> PoolOptions {
            PoolOptions {
                address: format!("unix:{}", self.config.socket.display()),
                tenant: "default".into(),
                ca: None,
            }
        }
    }
    impl Drop for Manager {
        fn drop(&mut self) {
            self.stop();
            std::fs::remove_dir_all(&self.directory).unwrap();
            let regions = std::path::Path::new("/dev/shm/selene").join(&self.config.name);
            if regions.exists() {
                std::fs::remove_dir_all(regions).unwrap();
            }
        }
    }
    fn wait(mut condition: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(15);
        while !condition() {
            assert!(Instant::now() < deadline, "startup condition timed out");
            thread::sleep(Duration::from_millis(10));
        }
    }
    fn start(
        manager: &Manager,
        registrar: Arc<Registrar>,
        budget: Budget,
    ) -> (PreparedPool, Vec<u64>) {
        let geometry = vec![flare_selene::testing::geometry(64, &[("kv", 4096)], 4096)];
        PreparedPool::start_with_registrar(
            &manager.options(),
            [3; 32],
            &geometry,
            vec![[1; 16]],
            0,
            budget,
            &StartupProgress::silent(),
            registrar,
        )
        .unwrap()
    }
    fn unlimited() -> Budget {
        Budget {
            host: Ok(u64::MAX),
            devices: vec![(u64::MAX, false)],
        }
    }

    #[test]
    fn over_budget_startup_registers_nothing_and_serves_local_only() {
        let mut manager = Manager::new();
        manager.run();
        for budget in [
            Budget {
                host: Ok(1),
                ..unlimited()
            },
            Budget {
                devices: vec![(1, false)],
                ..unlimited()
            },
        ] {
            let registrar = Arc::new(Registrar::default());
            let (prepared, charges) = start(&manager, registrar.clone(), budget);
            wait(|| matches!(prepared.client.status(), PoolStatus::Disabled(_)));
            let PoolStatus::Disabled(reason) = prepared.client.status() else {
                unreachable!()
            };
            assert!(
                reason.contains("arena") && reason.contains("staging") && reason.contains("budget"),
                "{reason}"
            );
            assert_eq!(charges, [0]);
            assert!(prepared.client.registered_regions().is_empty());
            assert!(registrar.calls.lock().unwrap().is_empty());
            let mut store = prepared.attach(None).unwrap();
            let mut history = crate::cache::PrefixHistory::new([3; 32]);
            history.append(&[7; 128], &[]).unwrap();
            assert!(store.lookup(&history, 0, u64::MAX).is_none());
            let checkpoint = super::super::PublishedCheckpoint {
                id: crate::cache::CheckpointId::new(1),
                key: history.key_at(128).unwrap(),
                parent: None,
                bucket: [0; 16],
                chain_pages: vec![flare_selene::PageId(1), flare_selene::PageId(2)],
                tail_pages: 2,
                slot: flare_selene::StateSlotId(1),
            };
            assert!(matches!(
                store.store(&checkpoint).unwrap(),
                super::super::StoreOutcome::Noted
            ));
            store.close().unwrap();
        }
    }

    #[test]
    fn budget_rejection_emits_one_warning_per_instance() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "offload::pool::tests::over_budget_startup_registers_nothing_and_serves_local_only",
                "--nocapture",
            ])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stderr = String::from_utf8(output.stderr).unwrap();
        assert_eq!(
            stderr
                .lines()
                .filter(|line| line.starts_with("warning:"))
                .count(),
            2,
            "{stderr}"
        );
    }

    #[test]
    fn an_absent_manager_times_out_before_local_readiness_and_never_attaches_late() {
        let mut manager = Manager::new();
        let registrar = Arc::new(Registrar::default());
        let started = Instant::now();
        let (prepared, charges) = start(&manager, registrar.clone(), unlimited());
        assert!(started.elapsed() >= flare_selene::pool::STARTUP_WAIT);
        assert!(started.elapsed() < flare_selene::pool::STARTUP_WAIT + Duration::from_secs(5));
        assert_eq!(charges, [0]);
        assert!(matches!(prepared.client.status(), PoolStatus::Disabled(_)));
        manager.run();
        thread::sleep(Duration::from_millis(250));
        assert!(registrar.calls.lock().unwrap().is_empty());
        let mut store = prepared.attach(None).unwrap();
        store.poll(&mut Vec::new()).unwrap();
        store.close().unwrap();
    }

    #[test]
    fn startup_wait_precedes_readiness_and_restart_never_registers_again() {
        let mut manager = Manager::new();
        let registrar = Arc::new(Registrar::default());
        let options = manager.options();
        let registered = registrar.clone();
        let (ready, readiness) = std::sync::mpsc::channel();
        let startup = thread::spawn(move || {
            let geometry = vec![flare_selene::testing::geometry(64, &[("kv", 4096)], 4096)];
            let result = PreparedPool::start_with_registrar(
                &options,
                [3; 32],
                &geometry,
                vec![[1; 16]],
                0,
                unlimited(),
                &StartupProgress::silent(),
                registered,
            )
            .unwrap();
            ready.send(()).unwrap();
            result
        });
        assert!(readiness.recv_timeout(Duration::from_millis(100)).is_err());
        assert!(registrar.calls.lock().unwrap().is_empty());
        manager.run();
        readiness.recv_timeout(Duration::from_secs(5)).unwrap();
        let (mut prepared, charges) = startup.join().unwrap();
        assert_eq!(charges, [(16 << 20) / 512]);
        assert_eq!(registrar.calls.lock().unwrap().len(), 2);
        let changed = vec![flare_selene::testing::geometry(128, &[("kv", 4096)], 4096)];
        assert!(
            prepared
                .start_copies(&[], changed)
                .unwrap_err()
                .to_string()
                .contains("layouts disagree")
        );
        wait(|| prepared.client.status() == PoolStatus::Ready);
        let topology = prepared.client.topology();
        manager.stop();
        manager.run();
        wait(|| {
            prepared.client.topology() != topology && prepared.client.status() == PoolStatus::Ready
        });
        assert_eq!(registrar.calls.lock().unwrap().len(), 2);
        manager.stop();
        manager.config.staging *= 2;
        manager.run();
        wait(|| matches!(prepared.client.status(), PoolStatus::Disabled(_)));
        assert_eq!(registrar.calls.lock().unwrap().len(), 2);
        assert_eq!(*registrar.drops.lock().unwrap(), 0);
        prepared.client.close().unwrap();
        assert_eq!(*registrar.drops.lock().unwrap(), 2);
        let mut store = prepared.attach(None).unwrap();
        let mut history = crate::cache::PrefixHistory::new([3; 32]);
        history.append(&[7; 128], &[]).unwrap();
        assert!(store.lookup(&history, 0, u64::MAX).is_none());
        store.poll(&mut Vec::new()).unwrap();
        store.close().unwrap();
    }
}
