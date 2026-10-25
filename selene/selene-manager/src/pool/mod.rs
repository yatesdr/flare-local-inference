//! The pool protocol (`selene.v2`), manager side.
//!
//! One actor ([`core::Core`]) owns every piece of protocol state: sessions, the
//! announcement index, claims, exports, windows and fences. Sessions reach it
//! through [`service::PoolService`]; it never blocks, and it answers each
//! session through that session's bounded outbound queue.

pub mod barrier;
mod claims;
pub mod core;
pub mod index;
pub mod limits;
mod lookup;
mod notices;
pub mod pass;
mod ranks;
pub mod service;
mod store_bridge;
pub mod windows;

use crate::{
    Key,
    proto::v2,
    record::{Descriptor, RankSizes},
};
use index::{Bucket, Entry, Tier};

pub use self::core::{Core, PoolConfig, Shared};
pub use self::service::PoolService;

/// What a grant names for a data-channel transfer: where to connect, the token, and the
/// carrier the manager prefers (the instance may still ask for the stream).
pub(crate) fn channel_ref(hub: &crate::data::channel::Hub, token: &[u8]) -> v2::DataChannelRef {
    v2::DataChannelRef {
        transport: if hub.offers_rdma() {
            v2::DataTransport::Rdma
        } else {
            v2::DataTransport::Tcp
        } as i32,
        address: hub.address().to_string(),
        token: token.to_vec(),
        rdma: Vec::new(),
    }
}

/// A violation of the protocol by the peer. The session is closed.
#[derive(Debug)]
pub struct Violation(pub String);

impl Violation {
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

pub type Checked<T> = std::result::Result<T, Violation>;

/// A parsed index notice.
#[derive(Clone, Debug)]
pub enum Notice {
    Saved(Entry),
    Spilled {
        to: Entry,
        from_tier: Tier,
        from_generation: u64,
    },
    Evicted {
        key: Key,
        tier: Tier,
        generation: u64,
        heir: Option<Entry>,
    },
}

pub fn key_from(key: Option<&v2::PrefixKey>) -> Checked<Key> {
    let key = key.ok_or_else(|| Violation::new("missing key"))?;
    let digest: [u8; 16] = key
        .digest
        .as_slice()
        .try_into()
        .map_err(|_| Violation::new("key digest must be 16 bytes"))?;
    Ok(Key {
        consumed_tokens: key.consumed_tokens,
        digest,
    })
}

pub fn key_to(key: Key) -> v2::PrefixKey {
    v2::PrefixKey {
        consumed_tokens: key.consumed_tokens,
        digest: key.digest.to_vec(),
    }
}

pub fn tier_from(value: i32) -> Checked<Tier> {
    match v2::Tier::try_from(value) {
        Ok(v2::Tier::Gpu) => Ok(Tier::Gpu),
        Ok(v2::Tier::Dram) => Ok(Tier::Dram),
        Ok(v2::Tier::Nvme) => Ok(Tier::Nvme),
        _ => Err(Violation::new("tier unspecified")),
    }
}

pub fn tier_to(tier: Tier) -> v2::Tier {
    match tier {
        Tier::Gpu => v2::Tier::Gpu,
        Tier::Dram => v2::Tier::Dram,
        Tier::Nvme => v2::Tier::Nvme,
    }
}

pub fn entry_from(entry: &v2::IndexEntry) -> Checked<Entry> {
    let key = key_from(entry.key.as_ref())?;
    let parent = if entry.has_parent {
        Some(key_from(entry.parent.as_ref())?)
    } else {
        None
    };
    let bucket: Bucket = entry
        .bucket
        .as_slice()
        .try_into()
        .map_err(|_| Violation::new("bucket must be 16 bytes"))?;
    if key.consumed_tokens == 0 {
        return Err(Violation::new("zero consumed_tokens"));
    }
    Ok(Entry {
        key,
        parent,
        bucket,
        tail_pages: entry.tail_pages,
        row: entry.row,
        tier: tier_from(entry.tier)?,
        generation: entry.generation,
        bytes: entry.bytes,
    })
}

pub fn entry_to(entry: &Entry) -> v2::IndexEntry {
    v2::IndexEntry {
        key: Some(key_to(entry.key)),
        has_parent: entry.parent.is_some(),
        parent: entry.parent.map(key_to),
        bucket: entry.bucket.to_vec(),
        tail_pages: entry.tail_pages,
        row: entry.row,
        tier: tier_to(entry.tier) as i32,
        generation: entry.generation,
        bytes: entry.bytes,
    }
}

pub fn notice_from(notice: &v2::Notice) -> Checked<Notice> {
    use v2::notice::Kind;
    match notice.kind.as_ref() {
        Some(Kind::Saved(saved)) => {
            let entry = entry_from(
                saved
                    .entry
                    .as_ref()
                    .ok_or_else(|| Violation::new("Saved without an entry"))?,
            )?;
            Ok(Notice::Saved(entry))
        }
        Some(Kind::Spilled(spilled)) => {
            let to = entry_from(
                spilled
                    .to
                    .as_ref()
                    .ok_or_else(|| Violation::new("Spilled without a destination"))?,
            )?;
            let from_tier = tier_from(spilled.from_tier)?;
            if to.tier <= from_tier {
                return Err(Violation::new("Spilled must move to a lower tier"));
            }
            Ok(Notice::Spilled {
                to,
                from_tier,
                from_generation: spilled.from_generation,
            })
        }
        Some(Kind::Evicted(evicted)) => {
            let key = key_from(evicted.key.as_ref())?;
            let tier = tier_from(evicted.tier)?;
            let heir = match v2::EvictKind::try_from(evicted.kind) {
                Ok(v2::EvictKind::Removed) => None,
                Ok(v2::EvictKind::Compacted) => {
                    Some(entry_from(evicted.heir.as_ref().ok_or_else(|| {
                        Violation::new("COMPACTED without an heir")
                    })?)?)
                }
                _ => return Err(Violation::new("eviction kind unspecified")),
            };
            Ok(Notice::Evicted {
                key,
                tier,
                generation: evicted.generation,
                heir,
            })
        }
        None => Err(Violation::new("empty notice")),
    }
}

/// The geometry a scope declared, as the manager uses it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScopeGeometry {
    pub page_tokens: u64,
    pub page_bytes: u64,
    pub row_bytes: u64,
}

/// A scope that passed validation: the descriptor the store keys records by,
/// and the id both ends derive.
#[derive(Clone, Debug)]
pub struct ValidScope {
    pub id: [u8; 32],
    pub descriptor: Descriptor,
    pub geometry: ScopeGeometry,
}

pub fn validate_scope(scope: Option<&v2::Scope>) -> Option<ValidScope> {
    let scope = scope?;
    if scope.tenant.is_empty() || scope.tenant.len() > 255 {
        return None;
    }
    let identity: [u8; 32] = scope.model_identity.as_slice().try_into().ok()?;
    let fingerprint: [u8; 16] = scope.layout_fingerprint.as_slice().try_into().ok()?;
    let geometry = scope.geometry.as_ref()?;
    if geometry.page_tokens == 0
        || geometry.ranks.is_empty()
        || geometry.page_record_bytes == 0
        || geometry.row_record_bytes == 0
        || geometry.page_record_bytes % 4096 != 0
        || geometry.row_record_bytes % 4096 != 0
    {
        return None;
    }
    let descriptor = Descriptor {
        identity,
        fingerprint,
        page_tokens: geometry.page_tokens,
        ranks: geometry
            .ranks
            .iter()
            .map(|rank| RankSizes {
                planes: rank.plane_bytes.clone(),
                row_bytes: rank.row_bytes,
            })
            .collect(),
        tenant: scope.tenant.clone(),
    };
    descriptor.validate().ok()?;
    if descriptor.page_bytes() != geometry.page_record_bytes
        || descriptor.row_bytes() != geometry.row_record_bytes
    {
        return None;
    }
    let canonical = flare_selene::pool::Geometry {
        page_tokens: geometry.page_tokens,
        ranks: geometry
            .ranks
            .iter()
            .map(|rank| flare_selene::pool::RankPlanes {
                plane_bytes: rank.plane_bytes.clone(),
                row_bytes: rank.row_bytes,
            })
            .collect(),
        page_record_bytes: geometry.page_record_bytes,
        row_record_bytes: geometry.row_record_bytes,
    };
    let id =
        flare_selene::pool::scope_id(&scope.tenant, &identity, &fingerprint, &canonical).ok()?;
    Some(ValidScope {
        id,
        geometry: ScopeGeometry {
            page_tokens: geometry.page_tokens,
            page_bytes: geometry.page_record_bytes,
            row_bytes: geometry.row_record_bytes,
        },
        descriptor,
    })
}
