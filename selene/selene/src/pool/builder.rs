//! Attach a pool before the host allocates its final cache storage.
use super::{
    AgentConfig, Budget, DeferredRecords, PlannedRegion, PoolClient, PoolConfig, PoolOptions,
    PoolPeers, PooledStore, RankAgent, RankSet, RegisteredRegion, pool_address, pool_scope,
};
use crate::{
    CacheGeometry, HostRegistrar, HostStore, Layout, Registration, Result, SharedCopier,
    SplitMapping, plan::Identity,
};
use std::sync::Arc;

pub struct PoolBuilder<'a> {
    pub options: &'a PoolOptions,
    pub identity: [u8; 32],
    pub geometry: &'a [CacheGeometry],
    pub gpu_uuids: Vec<[u8; 16]>,
    pub registration_context: usize,
    pub budget: Budget,
    pub peers: Option<PoolPeers>,
    pub disk: bool,
}

pub struct PreparedPool {
    pub client: Option<PoolClient>,
    pub agent: Option<RankAgent>,
    pub records: Option<DeferredRecords>,
    pub identity: Identity,
    listener: Option<(std::net::TcpListener, [u8; 16])>,
    layout: Layout,
}

impl PoolBuilder<'_> {
    pub fn start(
        self,
        copier: SharedCopier,
        registrar: Arc<dyn HostRegistrar + Send + Sync>,
    ) -> Result<(PreparedPool, Vec<u64>)> {
        let Self {
            options,
            identity,
            geometry,
            gpu_uuids,
            registration_context,
            budget,
            mut peers,
            disk,
        } = self;
        let listener = peers.as_mut().and_then(|peers| peers.listener.take());
        let registrar: Arc<dyn HostRegistrar + Send + Sync> = Arc::new(PoolRegistrar(registrar));
        let devices = budget.devices.clone();
        let all_ranks = if let Some(peers) = &peers {
            vec![geometry[0].clone(); peers.gpu_uuids.len()]
        } else {
            geometry.to_vec()
        };
        let scope = pool_scope(&options.tenant, identity, &all_ranks)?;
        let identity = crate::plan::Identity {
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
                let records = disk.then(DeferredRecords::default);
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
                    records: records
                        .clone()
                        .map(|records| Box::new(records) as Box<dyn crate::pool::RankRecords>),
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
                                .map(crate::shard::Link::into_rank_link)
                                .collect::<crate::Result<_>>()?,
                        },
                        copier.clone(),
                        registrar,
                    )?,
                    None => PoolClient::start(config, copier.clone(), registrar)?,
                };
                (Some(client), None, None)
            }
        };
        let mut prepared = PreparedPool {
            client,
            agent,
            records,
            listener,
            layout: copier.lock().unwrap().layout().clone(),
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
        Ok((prepared, charges))
    }
}

impl PreparedPool {
    pub fn registered_regions(&self) -> &[RegisteredRegion] {
        match (&self.client, &self.agent) {
            (Some(client), _) => client.registered_regions(),
            (_, Some(agent)) => agent.registered_regions(),
            _ => &[],
        }
    }
    pub fn take_listener(&mut self) -> Option<(std::net::TcpListener, [u8; 16])> {
        self.listener.take()
    }
    pub fn approved(&self) -> bool {
        !self.registered_regions().is_empty()
    }
    pub fn close(&mut self) -> Result<()> {
        if let Some(client) = &mut self.client {
            client.close()?;
        }
        if let Some(agent) = &mut self.agent {
            agent.close()?;
        }
        Ok(())
    }
    pub fn attach(self, local: Option<Box<dyn HostStore>>) -> Result<Box<dyn HostStore>> {
        Ok(Box::new(PooledStore::new(
            super::local::Tiers::new(local),
            self.client
                .ok_or("a pool rank agent cannot serve rank zero's store")?,
            self.identity,
            &self.layout,
        )))
    }
}

/// Registers the pool's shared-memory regions. The pool's copier names their
/// host addresses directly in copies, so a registration whose device address
/// differs from its host address is refused.
struct PoolRegistrar(Arc<dyn HostRegistrar + Send + Sync>);

impl HostRegistrar for PoolRegistrar {
    fn register(
        &self,
        context: usize,
        address: usize,
        bytes: usize,
    ) -> Result<Box<dyn Registration>> {
        let registration = self.0.register(context, address, bytes)?;
        if registration.device_address() != address as u64 {
            return Err(crate::Error::new(
                "pool memory needs identical host and device addresses",
            ));
        }
        Ok(registration)
    }

    fn map_split(&self, _requests: &[(usize, i32, usize)]) -> Result<Box<dyn SplitMapping>> {
        Err(crate::Error::new("the pool does not use split mappings"))
    }
}
