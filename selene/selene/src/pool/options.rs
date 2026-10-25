use crate::{CacheGeometry, geometry};

/// The manager an instance shares prefix checkpoints with, and the tenant that
/// scopes what it may see.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PoolOptions {
    /// `unix:PATH`, TLS `HOST:PORT`, or plaintext `plain:HOST:PORT`.
    pub address: String,
    pub tenant: String,
    /// The CA certificate the manager's TLS certificate must chain to. Without
    /// one, TLS uses the host's trust store. Only a TLS `HOST:PORT` manager can use one.
    pub ca: Option<std::path::PathBuf>,
}

/// The tenant of an instance that names none: the protocol requires a nonempty one.
pub const DEFAULT_POOL_TENANT: &str = "default";

/// Accept a Unix socket, TLS address, or explicitly plaintext address.
pub fn parse_pool_address(text: &str) -> std::result::Result<String, String> {
    pool_address(text).map(|_| text.to_owned())
}

pub(super) fn pool_address(text: &str) -> std::result::Result<crate::pool::PoolAddress, String> {
    use crate::pool::PoolAddress;
    let invalid =
        || format!("invalid pool address '{text}': use unix:PATH, HOST:PORT or plain:HOST:PORT");
    if let Some(path) = text.strip_prefix("unix:") {
        return if path.is_empty() || path.chars().any(char::is_control) {
            Err(invalid())
        } else {
            Ok(PoolAddress::Unix(path.into()))
        };
    }
    let address = text.strip_prefix("plain:").unwrap_or(text);
    let host_valid = |host: &str| {
        if let Some(ip) = host.strip_prefix('[').and_then(|s| s.strip_suffix(']')) {
            return ip.parse::<std::net::Ipv6Addr>().is_ok();
        }
        !host.is_empty()
            && host
                .strip_suffix('.')
                .unwrap_or(host)
                .split('.')
                .all(|label| {
                    !label.is_empty()
                        && !label.starts_with('-')
                        && !label.ends_with('-')
                        && label
                            .bytes()
                            .all(|c| c.is_ascii_alphanumeric() || c == b'-')
                })
    };
    match address.rsplit_once(':') {
        Some((host, port))
            if host_valid(host)
                && port.bytes().all(|c| c.is_ascii_digit())
                && port.parse::<u16>().is_ok_and(|port| port != 0) =>
        {
            Ok(if text.starts_with("plain:") {
                PoolAddress::TcpPlaintext(address.to_owned())
            } else {
                PoolAddress::Tcp(address.to_owned())
            })
        }
        _ => Err(invalid()),
    }
}

/// A tenant names a partition the manager never crosses: nonempty, at most 255 bytes.
pub fn parse_pool_tenant(text: &str) -> std::result::Result<String, String> {
    if text.is_empty() || text.len() > 255 || text.chars().any(char::is_control) {
        Err("a pool tenant must be 1 to 255 bytes without control characters".into())
    } else {
        Ok(text.to_owned())
    }
}

/// What a pool shares: the tenant, the model and the record layout.
pub(super) fn pool_scope(
    tenant: &str,
    identity: [u8; 32],
    geometry: &[CacheGeometry],
) -> crate::Result<crate::pool::Scope> {
    let mut scope =
        crate::pool::Scope::new(tenant, identity, geometry::fingerprint(geometry), geometry)?;
    for rank in &mut scope.geometry.ranks {
        for bytes in rank.plane_bytes.iter_mut().chain([&mut rank.row_bytes]) {
            *bytes = bytes.div_ceil(geometry::BLOCK_BYTES) * geometry::BLOCK_BYTES;
        }
    }
    Ok(scope)
}

#[cfg(test)]
mod tests;
