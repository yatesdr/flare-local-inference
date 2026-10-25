//! Attach and budget pool regions before cache allocation and admission.

use std::sync::{Arc, Mutex};

use flare_selene::{
    HostStore, Layout, PageCopier,
    copier::{Job, Reply},
    pool::{
        AgentConfig, PlannedRegion, PoolClient, PoolConfig, PooledStore, RankAgent, RankSet,
        RegisteredRegion, pool_address, pool_scope,
    },
};

use super::{CacheGeometry, PoolOptions, PrefixStore, registrar, transfer};
use crate::{Result, startup::StartupProgress};

pub(crate) use flare_selene::pool::Budget;

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
    client: Option<PoolClient>,
    agent: Option<RankAgent>,
    records: Option<super::shard::DeferredRecords>,
    listener: Option<(std::net::TcpListener, [u8; 16])>,
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
            Arc::new(registrar::PoolRegistrar),
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
        mut peers: Option<super::shard::PoolPeers>,
        disk: bool,
    ) -> Result<(Self, Vec<u64>)> {
        let listener = peers.as_mut().and_then(|peers| peers.listener.take());
        let phase = progress.phase("Wait for prefix cache pool", None);
        let copier = Arc::new(Mutex::new(Copier {
            layout: Layout::new(geometry)?,
            transfers: None,
        }));
        let devices = budget.devices.clone();
        let all_ranks = if let Some(peers) = &peers {
            vec![geometry[0].clone(); peers.gpu_uuids.len()]
        } else {
            geometry.to_vec()
        };
        let scope = pool_scope(&options.tenant, identity, &all_ranks)?;
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
            gpu_uuids: peers
                .as_ref()
                .map_or(gpu_uuids, |peers| peers.gpu_uuids.clone()),
            export_slots: 2,
            claim_slots: 2,
            level_order: None,
            registration_context,
            approve: Some(Arc::new(move |plan| budget.approve(plan))),
        };
        let (client, agent, records) = match peers {
            Some(mut peers) if peers.rank != 0 => {
                let records = disk.then(super::shard::DeferredRecords::default);
                let agent = RankAgent::start(AgentConfig {
                    address: config.address,
                    ca: config.ca,
                    scope: config.scope,
                    rank: peers.rank,
                    world: all_ranks.len(),
                    instance_name: config.instance_name,
                    link: peers.links.remove(0).into_rank_link()?,
                    copier: copier.clone(),
                    registrar,
                    registration_context,
                    gpu_uuid: config.gpu_uuids[peers.rank],
                    records: records.clone().map(|records| {
                        Box::new(records) as Box<dyn flare_selene::pool::RankRecords>
                    }),
                    approve: config.approve,
                })?;
                (None, Some(agent), records)
            }
            peers => {
                let client = match peers {
                    Some(peers) => PoolClient::start_ranked(
                        config,
                        RankSet {
                            links: peers
                                .links
                                .into_iter()
                                .map(super::shard::Link::into_rank_link)
                                .collect::<flare_selene::Result<_>>()?,
                        },
                        copier.clone(),
                        registrar,
                    )?,
                    None => PoolClient::start(config, copier.clone(), registrar)?,
                };
                (Some(client), None, None)
            }
        };
        let mut prepared = Self {
            client,
            agent,
            records,
            listener,
            copier,
            identity,
        };
        if prepared.registered_regions().is_empty() {
            // Finish attachment before caches consume its startup allowance.
            prepared.close()?;
        }
        let plan: Vec<_> = prepared
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
        Ok((prepared, charges))
    }

    fn registered_regions(&self) -> &[RegisteredRegion] {
        match (&self.client, &self.agent) {
            (Some(client), _) => client.registered_regions(),
            (_, Some(agent)) => agent.registered_regions(),
            _ => &[],
        }
    }

    pub(crate) fn take_listener(&mut self) -> Option<(std::net::TcpListener, [u8; 16])> {
        self.listener.take()
    }

    pub(crate) fn approved(&self) -> bool {
        !self.registered_regions().is_empty()
    }

    pub(crate) fn close(&mut self) -> Result<()> {
        if let Some(client) = &mut self.client {
            client.close()?;
        }
        if let Some(agent) = &mut self.agent {
            agent.close()?;
        }
        Ok(())
    }

    pub(crate) fn prepare_peer(&mut self, prepared: &super::Prepared) -> Result<()> {
        if self.agent.is_some() {
            self.start_copies(&prepared.contexts, prepared.geometry.clone())?;
            if let Some(records) = &self.records
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
        let layout = self.copier.lock().unwrap().layout.clone();
        let store = PooledStore::new(
            flare_selene::pool::Tiers::new(local),
            self.client
                .ok_or("a pool rank agent cannot serve rank zero's store")?,
            self.identity,
            &layout,
        );
        Ok(Box::new(store))
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
            None,
            false,
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
            wait(|| {
                matches!(
                    prepared.client.as_ref().unwrap().status(),
                    PoolStatus::Disabled(_)
                )
            });
            let PoolStatus::Disabled(reason) = prepared.client.as_ref().unwrap().status() else {
                unreachable!()
            };
            assert!(
                reason.contains("arena") && reason.contains("staging") && reason.contains("budget"),
                "{reason}"
            );
            assert_eq!(charges, [0]);
            assert!(
                prepared
                    .client
                    .as_ref()
                    .unwrap()
                    .registered_regions()
                    .is_empty()
            );
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
        assert!(matches!(
            prepared.client.as_ref().unwrap().status(),
            PoolStatus::Disabled(_)
        ));
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
                None,
                false,
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
        wait(|| prepared.client.as_ref().unwrap().status() == PoolStatus::Ready);
        let topology = prepared.client.as_ref().unwrap().topology();
        manager.stop();
        manager.run();
        wait(|| {
            prepared.client.as_ref().unwrap().topology() != topology
                && prepared.client.as_ref().unwrap().status() == PoolStatus::Ready
        });
        assert_eq!(registrar.calls.lock().unwrap().len(), 2);
        manager.stop();
        manager.config.staging *= 2;
        manager.run();
        wait(|| {
            matches!(
                prepared.client.as_ref().unwrap().status(),
                PoolStatus::Disabled(_)
            )
        });
        assert_eq!(registrar.calls.lock().unwrap().len(), 2);
        assert_eq!(*registrar.drops.lock().unwrap(), 0);
        prepared.client.as_mut().unwrap().close().unwrap();
        assert_eq!(*registrar.drops.lock().unwrap(), 2);
        let mut store = prepared.attach(None).unwrap();
        let mut history = crate::cache::PrefixHistory::new([3; 32]);
        history.append(&[7; 128], &[]).unwrap();
        assert!(store.lookup(&history, 0, u64::MAX).is_none());
        store.poll(&mut Vec::new()).unwrap();
        store.close().unwrap();
    }
    #[test]
    fn peer_registration_budget_disables_the_whole_ranked_instance() {
        use super::super::shard::{PoolOffer, pool_peer, pool_rank_zero};
        use crate::execution::RankStream;
        use std::net::{TcpListener, TcpStream};

        let mut manager = Manager::new();
        manager.run();
        for rejection in 0..3 {
            let reject = rejection != 2;
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let peer_stream = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
            let zero_stream = listener.accept().unwrap().0;
            let options = manager.options();
            let peer_registrar = Arc::new(Registrar::default());
            let registered = peer_registrar.clone();
            let offer = |rank| PoolOffer {
                tenant: Some("default".into()),
                identity: [3; 32],
                layout: [4; 16],
                rank,
                world: 2,
                gpu_uuid: [rank as u8 + 1; 16],
            };
            let peer = thread::spawn(move || {
                let peers = pool_peer(offer(1), &mut RankStream::Tcp(peer_stream)).unwrap();
                let budget = match rejection {
                    0 => Budget {
                        host: Ok(1),
                        ..unlimited()
                    },
                    1 => Budget {
                        devices: vec![(1, false)],
                        ..unlimited()
                    },
                    _ => unlimited(),
                };
                PreparedPool::start_with_registrar(
                    &options,
                    [3; 32],
                    &[flare_selene::testing::geometry(64, &[("kv", 4096)], 4096)],
                    vec![[2; 16]],
                    0,
                    budget,
                    &StartupProgress::silent(),
                    registered,
                    peers,
                    false,
                )
                .unwrap()
            });
            let peers = pool_rank_zero(offer(0), &mut [RankStream::Tcp(zero_stream)]).unwrap();
            let (mut zero, _) = PreparedPool::start_with_registrar(
                &manager.options(),
                [3; 32],
                &[flare_selene::testing::geometry(64, &[("kv", 4096)], 4096)],
                vec![[1; 16]],
                0,
                unlimited(),
                &StartupProgress::silent(),
                Arc::new(Registrar::default()),
                peers,
                false,
            )
            .unwrap();
            let (peer, charges) = peer.join().unwrap();
            assert!(peer.agent.is_some() && peer.client.is_none() && peer.records.is_none());
            if reject {
                assert_eq!(charges, [0]);
                assert!(!peer.approved());
                assert!(peer_registrar.calls.lock().unwrap().is_empty());
                wait(|| {
                    matches!(
                        zero.client.as_ref().unwrap().status(),
                        PoolStatus::Disabled(_)
                    )
                });
                let mut store = zero.attach(None).unwrap();
                let mut history = crate::cache::PrefixHistory::new([3; 32]);
                history.append(&[7; 128], &[]).unwrap();
                assert!(store.lookup(&history, 0, u64::MAX).is_none());
                store.close().unwrap();
            } else {
                assert!(peer.approved());
                wait(|| zero.client.as_ref().unwrap().status() == PoolStatus::Ready);
                wait(|| peer.agent.as_ref().unwrap().status() == PoolStatus::Ready);
                assert_eq!(
                    zero.identity.layout,
                    super::super::geometry::fingerprint(&vec![
                        flare_selene::testing::geometry(
                            64,
                            &[("kv", 4096)],
                            4096
                        );
                        2
                    ])
                );
                zero.close().unwrap();
            }
            let options = manager.options();
            let options = super::super::PrefixCacheOffload {
                pool: Some(options),
                ..Default::default()
            };
            let mut prepared = super::super::prepare_with_inputs(
                &options,
                std::path::Path::new("model.safetensors"),
                [3; 32],
                vec![flare_selene::testing::geometry(64, &[("kv", 4096)], 4096)],
                vec![(0, 0)],
                0,
                Some((1, 2)),
            )
            .unwrap();
            let super::super::Preparation::Store(resources) = &mut prepared else {
                panic!("no pool resources")
            };
            resources.pool = Some(peer);
            let shared = prepared.into_shared().unwrap();
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let stream = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
            let zero = listener.accept().unwrap().0;
            let service = thread::spawn(move || {
                super::super::shard::join_peer(Some(&shared), &mut RankStream::Tcp(stream), 1, 2)
                    .unwrap()
                    .unwrap()
            });
            assert!(
                super::super::shard::join_rank_zero(None, &mut [RankStream::Tcp(zero)])
                    .unwrap()
                    .is_none()
            );
            service.join().unwrap().join().unwrap();
        }
    }
}
