//! Manager configuration: command line, optional TOML, and validation.
//!
//! This module is self-contained so it can be unit-tested without starting a manager.

use serde::Deserialize;
use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;

pub const DEFAULT_NAME: &str = "default";
pub const DEFAULT_MEMORY: u64 = 1 << 30;
pub const DEFAULT_STAGING: u64 = 256 << 20;
pub const DEFAULT_STREAM_CONNECTIONS: usize = 8;
pub const DEFAULT_REMOTE_CATALOG_ENTRIES: u64 = 1_000_000;
pub const MIN_MEMORY: u64 = 4096;
pub const MIN_STAGING: u64 = 8 << 20;
pub const MIN_NVME: u64 = 8192;
pub const MAX_NAME: usize = 64;
pub const MAX_SOCKET: usize = 107;
pub const MAX_STREAM_CONNECTIONS: usize = 64;
pub const MAX_UPSTREAM_WEIGHT: u64 = 1_000_000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Upstream {
    pub name: String,
    pub address: String,
    pub weight: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PeerTransport {
    Auto,
    Stream,
    Rdma,
}

impl fmt::Display for PeerTransport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Auto => "auto",
            Self::Stream => "stream",
            Self::Rdma => "rdma",
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Config {
    pub name: String,
    pub socket: PathBuf,
    pub socket_explicit: bool,
    pub root: Option<PathBuf>,
    pub memory: u64,
    pub nvme: u64,
    pub staging: u64,
    pub listen_peer: Option<SocketAddr>,
    pub listen_data: Option<SocketAddr>,
    pub upstreams: Vec<Upstream>,
    pub peer_transport: PeerTransport,
    pub stream_connections: usize,
    pub retain_remote: bool,
    pub remote_catalog_entries: u64,
    pub tls_cert: Option<PathBuf>,
    pub tls_key: Option<PathBuf>,
    pub tls_ca: Option<PathBuf>,
    pub listen_admin: Option<SocketAddr>,
    pub metrics: Option<SocketAddr>,
    pub write_budget: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self::defaults()
    }
}

impl Config {
    pub fn defaults() -> Self {
        Self {
            name: DEFAULT_NAME.into(),
            socket: PathBuf::from(format!("/run/selene/{DEFAULT_NAME}.sock")),
            socket_explicit: false,
            root: None,
            memory: DEFAULT_MEMORY,
            nvme: 0,
            staging: DEFAULT_STAGING,
            listen_peer: None,
            listen_data: None,
            upstreams: Vec::new(),
            peer_transport: PeerTransport::Auto,
            stream_connections: DEFAULT_STREAM_CONNECTIONS,
            retain_remote: true,
            remote_catalog_entries: DEFAULT_REMOTE_CATALOG_ENTRIES,
            tls_cert: None,
            tls_key: None,
            tls_ca: None,
            listen_admin: None,
            metrics: None,
            write_budget: 0,
        }
    }

    /// Default socket path for a manager name.
    pub fn default_socket(name: &str) -> PathBuf {
        PathBuf::from(format!("/run/selene/{name}.sock"))
    }

    /// Whether this build and configuration may select RDMA for peer transfers.
    pub fn rdma_enabled(&self) -> bool {
        cfg!(feature = "rdma") && !matches!(self.peer_transport, PeerTransport::Stream)
    }

    fn tls_configured(&self) -> bool {
        self.tls_cert.is_some() || self.tls_key.is_some() || self.tls_ca.is_some()
    }

    /// One line per opt-in that widens the manager's exposure, printed once at startup.
    pub fn startup_warnings(&self) -> Vec<String> {
        let mut warnings = Vec::new();
        if self.listen_peer.is_some() || self.listen_data.is_some() || self.listen_admin.is_some() {
            let mut listeners = Vec::new();
            for (label, address) in [
                ("peer", self.listen_peer),
                ("data", self.listen_data),
                ("admin", self.listen_admin),
            ] {
                if let Some(address) = address {
                    listeners.push(format!("{label} {address}"));
                }
            }
            warnings.push(format!(
                "selene: warning: network listening enabled ({})",
                listeners.join(", ")
            ));
        }
        if self.rdma_enabled() {
            warnings.push("selene: warning: RDMA enabled (experimental)".to_string());
        }
        warnings
    }

    /// Validate every configuration rule.
    pub fn validate(&self) -> io::Result<()> {
        if !valid_name(&self.name) {
            return Err(invalid(
                "--name must be 1-64 bytes of letters, digits, '.', '_' or '-'",
            ));
        }
        if self.socket.as_os_str().len() > MAX_SOCKET {
            return Err(invalid("--socket path must be at most 107 bytes"));
        }
        if self.memory < MIN_MEMORY || !self.memory.is_multiple_of(4096) {
            return Err(invalid(
                "--memory must be at least 4096 bytes and a multiple of 4096",
            ));
        }
        if self.staging < MIN_STAGING || !self.staging.is_multiple_of(4096) {
            return Err(invalid(
                "--staging must be at least 8 MiB and a multiple of 4096",
            ));
        }
        match &self.root {
            Some(_) if self.nvme < MIN_NVME => {
                return Err(invalid("--nvme must be at least 8192 when --root is set"));
            }
            None if self.nvme != 0 => {
                return Err(invalid("--nvme requires --root"));
            }
            _ => {}
        }
        if !(1..=MAX_STREAM_CONNECTIONS).contains(&self.stream_connections) {
            return Err(invalid("--stream-connections must be 1..=64"));
        }
        if self.remote_catalog_entries == 0 {
            return Err(invalid("--remote-catalog-entries must be at least 1"));
        }
        if self.listen_peer.is_some() {
            if self.tls_cert.is_none() || self.tls_key.is_none() || self.tls_ca.is_none() {
                return Err(invalid(
                    "--listen-peer requires --tls-cert, --tls-key and --tls-ca",
                ));
            }
            if self.listen_data.is_none() {
                return Err(invalid("--listen-peer requires --listen-data"));
            }
        }
        if self.listen_admin.is_some()
            && (self.tls_cert.is_none() || self.tls_key.is_none() || self.tls_ca.is_none())
        {
            return Err(invalid(
                "--listen-admin requires --tls-cert, --tls-key and --tls-ca",
            ));
        }
        if matches!(self.peer_transport, PeerTransport::Rdma) && !cfg!(feature = "rdma") {
            return Err(invalid(
                "--peer-transport rdma requires a build with the `rdma` feature",
            ));
        }
        if self.rdma_enabled() && self.tls_configured() {
            return Err(invalid(
                "RDMA transfers are not encrypted, so RDMA cannot be combined with --tls-*; \
                 pass --peer-transport stream to keep TLS, or remove the TLS options to use RDMA",
            ));
        }
        let mut names = std::collections::BTreeSet::new();
        for upstream in &self.upstreams {
            if !valid_name(&upstream.name) {
                return Err(invalid(
                    "--upstream NAME must be 1-64 bytes of letters, digits, '.', '_' or '-'",
                ));
            }
            if upstream.address.is_empty() {
                return Err(invalid("--upstream address must not be empty"));
            }
            if !(1..=MAX_UPSTREAM_WEIGHT).contains(&upstream.weight) {
                return Err(invalid("--upstream weight must be 1..=1000000"));
            }
            if !names.insert(upstream.name.clone()) {
                return Err(invalid(format!(
                    "duplicate upstream name {}",
                    upstream.name
                )));
            }
        }
        Ok(())
    }
}

/// The result of parsing the command line.
#[allow(clippy::large_enum_variant)]
pub enum Parsed {
    Run(Config),
    Help,
}

pub fn help_text() -> String {
    "selene [options]\n\
     \n\
     --name NAME                     default \"default\"\n\
     --socket PATH                   default /run/selene/NAME.sock\n\
     --root PATH                     NVMe root; omit for a DRAM-only near tier\n\
     --memory BYTES                  shared arena, default 1G\n\
     --nvme BYTES                    NVMe budget; required with --root\n\
     --staging BYTES                 shared staging, default 256M\n\
     --listen-peer ADDR:PORT         serve PeerCache; requires TLS and --listen-data\n\
     --listen-data ADDR:PORT         stream data listener\n\
     --upstream NAME=ADDR[,WEIGHT]   repeatable; weight default 1\n\
     --peer-transport auto|stream|rdma\n\
     --stream-connections N          default 8\n\
     --retain-remote BOOL            default true\n\
     --remote-catalog-entries N      default 1000000\n\
     --write-budget BYTES/s          per-connection write rate; 0 = unlimited\n\
     --tls-cert PATH --tls-key PATH --tls-ca PATH\n\
     --listen-admin ADDR:PORT        optional, mTLS\n\
     --metrics ADDR:PORT\n\
     --config PATH                   TOML defaults; flags override\n\
     \n\
     Sizes accept binary K, M, G and T suffixes."
        .to_string()
}

/// Parse a command line (without the program name).
pub fn parse_args<I>(args: I) -> io::Result<Parsed>
where
    I: IntoIterator<Item = String>,
{
    let args: Vec<String> = args.into_iter().collect();
    for argument in &args {
        if argument == "--help" || argument == "-h" {
            return Ok(Parsed::Help);
        }
    }

    let mut config_path: Option<PathBuf> = None;
    for pair in args.windows(2) {
        if pair[0] == "--config" {
            config_path = Some(pair[1].clone().into());
        }
    }
    let mut config = match &config_path {
        Some(path) => Config::from_toml(&std::fs::read_to_string(path).map_err(|error| {
            io::Error::new(error.kind(), format!("read {}: {error}", path.display()))
        })?)?,
        None => Config::defaults(),
    };
    let mut explicit_socket = false;
    let mut upstreams: Option<Vec<Upstream>> = None;

    let mut index = 0;
    while index < args.len() {
        let option = args[index].as_str();
        let value = |index: usize| -> io::Result<&str> {
            args.get(index + 1)
                .map(String::as_str)
                .ok_or_else(|| invalid(format!("missing value for {option}")))
        };
        match option {
            "--name" => {
                config.name = value(index)?.to_string();
                index += 2;
            }
            "--socket" => {
                config.socket = value(index)?.into();
                config.socket_explicit = true;
                explicit_socket = true;
                index += 2;
            }
            "--root" => {
                config.root = Some(value(index)?.into());
                index += 2;
            }
            "--memory" => {
                config.memory = parse_size(value(index)?)?;
                index += 2;
            }
            "--nvme" => {
                config.nvme = parse_size(value(index)?)?;
                index += 2;
            }
            "--staging" => {
                config.staging = parse_size(value(index)?)?;
                index += 2;
            }
            "--listen-peer" => {
                config.listen_peer = Some(parse_address(value(index)?)?);
                index += 2;
            }
            "--listen-data" => {
                config.listen_data = Some(parse_address(value(index)?)?);
                index += 2;
            }
            "--upstream" => {
                let upstream = parse_upstream(value(index)?)?;
                upstreams.get_or_insert_with(Vec::new).push(upstream);
                index += 2;
            }
            "--peer-transport" => {
                config.peer_transport = parse_transport(value(index)?)?;
                index += 2;
            }
            "--stream-connections" => {
                config.stream_connections = parse_usize(value(index)?, option)?;
                index += 2;
            }
            "--retain-remote" => {
                config.retain_remote = parse_bool(value(index)?)?;
                index += 2;
            }
            "--remote-catalog-entries" => {
                config.remote_catalog_entries = parse_u64(value(index)?, option)?;
                index += 2;
            }
            "--write-budget" => {
                config.write_budget = parse_size(value(index)?)?;
                index += 2;
            }
            "--tls-cert" => {
                config.tls_cert = Some(value(index)?.into());
                index += 2;
            }
            "--tls-key" => {
                config.tls_key = Some(value(index)?.into());
                index += 2;
            }
            "--tls-ca" => {
                config.tls_ca = Some(value(index)?.into());
                index += 2;
            }
            "--listen-admin" => {
                config.listen_admin = Some(parse_address(value(index)?)?);
                index += 2;
            }
            "--metrics" => {
                config.metrics = Some(parse_address(value(index)?)?);
                index += 2;
            }
            "--config" => {
                value(index)?;
                index += 2;
            }
            other => {
                return Err(invalid(format!("unknown option {other}")));
            }
        }
    }
    if let Some(upstreams) = upstreams {
        config.upstreams = upstreams;
    }
    if !explicit_socket && !config.socket_explicit {
        config.socket = Config::default_socket(&config.name);
    }
    config.validate()?;
    Ok(Parsed::Run(config))
}

impl Config {
    /// Parse a TOML document into a configuration with defaults applied.
    pub fn from_toml(text: &str) -> io::Result<Self> {
        let raw: RawConfig = toml::from_str(text)
            .map_err(|error| invalid(format!("invalid TOML configuration: {error}")))?;
        let mut config = Config::defaults();
        if let Some(name) = raw.name {
            config.name = name;
        }
        if let Some(path) = raw.socket {
            config.socket = path.into();
            config.socket_explicit = true;
        } else {
            config.socket = Config::default_socket(&config.name);
        }
        if let Some(path) = raw.root {
            config.root = Some(path.into());
        }
        if let Some(size) = raw.memory {
            config.memory = size;
        }
        if let Some(size) = raw.nvme {
            config.nvme = size;
        }
        if let Some(size) = raw.staging {
            config.staging = size;
        }
        if let Some(address) = raw.listen_peer {
            config.listen_peer = Some(parse_address(&address)?);
        }
        if let Some(address) = raw.listen_data {
            config.listen_data = Some(parse_address(&address)?);
        }
        if let Some(upstreams) = raw.upstream {
            config.upstreams = upstreams;
        }
        if let Some(transport) = raw.peer_transport {
            config.peer_transport = parse_transport(&transport)?;
        }
        if let Some(count) = raw.stream_connections {
            config.stream_connections = usize::try_from(count)
                .map_err(|_| invalid("stream_connections does not fit usize"))?;
        }
        if let Some(retain) = raw.retain_remote {
            config.retain_remote = retain;
        }
        if let Some(count) = raw.remote_catalog_entries {
            config.remote_catalog_entries = count;
        }
        if let Some(path) = raw.tls_cert {
            config.tls_cert = Some(path.into());
        }
        if let Some(path) = raw.tls_key {
            config.tls_key = Some(path.into());
        }
        if let Some(path) = raw.tls_ca {
            config.tls_ca = Some(path.into());
        }
        if let Some(address) = raw.listen_admin {
            config.listen_admin = Some(parse_address(&address)?);
        }
        if let Some(address) = raw.metrics {
            config.metrics = Some(parse_address(&address)?);
        }
        if let Some(size) = raw.write_budget {
            config.write_budget = size;
        }
        Ok(config)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
struct RawConfig {
    name: Option<String>,
    socket: Option<String>,
    root: Option<String>,
    #[serde(default, deserialize_with = "de_size")]
    memory: Option<u64>,
    #[serde(default, deserialize_with = "de_size")]
    nvme: Option<u64>,
    #[serde(default, deserialize_with = "de_size")]
    staging: Option<u64>,
    #[serde(alias = "listen_peer")]
    listen_peer: Option<String>,
    #[serde(alias = "listen_data")]
    listen_data: Option<String>,
    upstream: Option<Vec<Upstream>>,
    #[serde(alias = "peer_transport")]
    peer_transport: Option<String>,
    #[serde(alias = "stream_connections")]
    stream_connections: Option<u64>,
    #[serde(alias = "retain_remote")]
    retain_remote: Option<bool>,
    #[serde(alias = "remote_catalog_entries")]
    remote_catalog_entries: Option<u64>,
    #[serde(alias = "tls_cert")]
    tls_cert: Option<String>,
    #[serde(alias = "tls_key")]
    tls_key: Option<String>,
    #[serde(alias = "tls_ca")]
    tls_ca: Option<String>,
    #[serde(alias = "listen_admin")]
    listen_admin: Option<String>,
    metrics: Option<String>,
    #[serde(default, deserialize_with = "de_size")]
    #[serde(alias = "write_budget")]
    write_budget: Option<u64>,
}

impl<'de> Deserialize<'de> for Upstream {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct RawUpstream {
            name: String,
            address: String,
            #[serde(default = "default_weight")]
            weight: u64,
        }
        let raw = RawUpstream::deserialize(deserializer)?;
        Ok(Upstream {
            name: raw.name,
            address: raw.address,
            weight: raw.weight,
        })
    }
}

fn default_weight() -> u64 {
    1
}

#[derive(Deserialize)]
#[serde(untagged)]
enum SizeValue {
    Integer(u64),
    Text(String),
}

fn de_size<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    match Option::<SizeValue>::deserialize(deserializer)? {
        None => Ok(None),
        Some(SizeValue::Integer(value)) => Ok(Some(value)),
        Some(SizeValue::Text(text)) => parse_size(&text)
            .map(Some)
            .map_err(serde::de::Error::custom),
    }
}

/// Parse a byte size with an optional binary K, M, G or T suffix.
pub fn parse_size(text: &str) -> io::Result<u64> {
    let (number, multiplier) = match text.as_bytes().last() {
        Some(b'K' | b'k') => (&text[..text.len() - 1], 1u64 << 10),
        Some(b'M' | b'm') => (&text[..text.len() - 1], 1u64 << 20),
        Some(b'G' | b'g') => (&text[..text.len() - 1], 1u64 << 30),
        Some(b'T' | b't') => (&text[..text.len() - 1], 1u64 << 40),
        _ => (text, 1),
    };
    if number.is_empty() {
        return Err(invalid(format!("invalid byte size {text:?}")));
    }
    number
        .parse::<u64>()
        .ok()
        .and_then(|value| value.checked_mul(multiplier))
        .ok_or_else(|| invalid(format!("invalid byte size {text:?}")))
}

fn parse_address(text: &str) -> io::Result<SocketAddr> {
    text.parse()
        .map_err(|_| invalid(format!("invalid address {text:?}")))
}

fn parse_transport(text: &str) -> io::Result<PeerTransport> {
    match text {
        "auto" => Ok(PeerTransport::Auto),
        "stream" => Ok(PeerTransport::Stream),
        "rdma" => Ok(PeerTransport::Rdma),
        other => Err(invalid(format!("invalid --peer-transport {other:?}"))),
    }
}

fn parse_bool(text: &str) -> io::Result<bool> {
    match text {
        "true" | "1" | "yes" => Ok(true),
        "false" | "0" | "no" => Ok(false),
        other => Err(invalid(format!("invalid boolean {other:?}"))),
    }
}

fn parse_usize(text: &str, option: &str) -> io::Result<usize> {
    text.parse()
        .map_err(|_| invalid(format!("invalid value for {option}: {text:?}")))
}

fn parse_u64(text: &str, option: &str) -> io::Result<u64> {
    text.parse()
        .map_err(|_| invalid(format!("invalid value for {option}: {text:?}")))
}

fn parse_upstream(text: &str) -> io::Result<Upstream> {
    let (name, rest) = text
        .split_once('=')
        .ok_or_else(|| invalid("--upstream requires NAME=ADDR[,WEIGHT]"))?;
    let (address, weight) = match rest.rsplit_once(',') {
        Some((address, weight)) => (address, parse_u64(weight, "--upstream weight")?),
        None => (rest, 1),
    };
    if address.is_empty() {
        return Err(invalid("--upstream address must not be empty"));
    }
    Ok(Upstream {
        name: name.to_string(),
        address: address.to_string(),
        weight,
    })
}

/// A manager name is also a directory name and a rendezvous node name.
pub fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_NAME
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

pub fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    fn run(values: &[&str]) -> Config {
        match parse_args(args(values)).unwrap() {
            Parsed::Run(config) => config,
            Parsed::Help => panic!("unexpected help"),
        }
    }

    #[test]
    fn defaults_match_the_spec() {
        let config = Config::defaults();
        assert_eq!(config.name, "default");
        assert_eq!(config.socket, PathBuf::from("/run/selene/default.sock"));
        assert_eq!(config.memory, 1 << 30);
        assert_eq!(config.staging, 256 << 20);
        assert_eq!(config.nvme, 0);
        assert_eq!(config.stream_connections, 8);
        assert!(config.retain_remote);
        assert_eq!(config.remote_catalog_entries, 1_000_000);
        assert_eq!(config.peer_transport, PeerTransport::Auto);
        assert_eq!(config.write_budget, 0);
        config.validate().unwrap();
    }

    #[test]
    fn size_suffixes_are_binary() {
        assert_eq!(parse_size("1K").unwrap(), 1024);
        assert_eq!(parse_size("2M").unwrap(), 2 << 20);
        assert_eq!(parse_size("3G").unwrap(), 3 << 30);
        assert_eq!(parse_size("1T").unwrap(), 1 << 40);
        assert_eq!(parse_size("4096").unwrap(), 4096);
        assert!(parse_size("").is_err());
        assert!(parse_size("1X").is_err());
        assert!(parse_size("K").is_err());
    }

    #[test]
    fn socket_default_follows_name_unless_set() {
        let config = run(&["--name", "pool-a"]);
        assert_eq!(config.socket, PathBuf::from("/run/selene/pool-a.sock"));
        let config = run(&["--name", "pool-a", "--socket", "/tmp/x.sock"]);
        assert_eq!(config.socket, PathBuf::from("/tmp/x.sock"));
    }

    #[test]
    fn flags_parse_and_validate() {
        let config = run(&[
            "--name",
            "pool",
            "--root",
            "/var/lib/selene",
            "--nvme",
            "1G",
            "--memory",
            "2G",
            "--staging",
            "16M",
            "--stream-connections",
            "4",
            "--retain-remote",
            "false",
            "--remote-catalog-entries",
            "100",
            "--write-budget",
            "8M",
            "--upstream",
            "store1=127.0.0.1:9000,3",
            "--upstream",
            "store2=127.0.0.1:9001",
        ]);
        assert_eq!(config.nvme, 1 << 30);
        assert_eq!(config.memory, 2 << 30);
        assert_eq!(config.staging, 16 << 20);
        assert_eq!(config.stream_connections, 4);
        assert!(!config.retain_remote);
        assert_eq!(config.write_budget, 8 << 20);
        assert_eq!(config.upstreams.len(), 2);
        assert_eq!(config.upstreams[0].weight, 3);
        assert_eq!(config.upstreams[1].weight, 1);
        config.validate().unwrap();
    }

    #[test]
    fn toml_then_flags_override() {
        let text = r#"
            name = "from-toml"
            memory = "2G"
            staging = 16777216
            retain-remote = false
            write-budget = "4M"
            [[upstream]]
            name = "a"
            address = "box:9000"
            weight = 2
        "#;
        let config = Config::from_toml(text).unwrap();
        assert_eq!(config.name, "from-toml");
        assert_eq!(config.memory, 2 << 30);
        assert_eq!(config.staging, 16 << 20);
        assert!(!config.retain_remote);
        assert_eq!(config.write_budget, 4 << 20);
        assert_eq!(config.upstreams[0].name, "a");

        let path = std::env::temp_dir().join(format!("selene-config-{}", std::process::id()));
        std::fs::write(&path, text).unwrap();
        let config = run(&["--config", path.to_str().unwrap(), "--memory", "1G"]);
        std::fs::remove_file(&path).unwrap();
        assert_eq!(config.memory, 1 << 30);
        assert_eq!(config.name, "from-toml");
    }

    #[test]
    fn toml_accepts_kebab_case_and_underscore_aliases() {
        let kebab = "stream-connections = 2\nremote-catalog-entries = 5\nwrite-budget = \"1M\"\n";
        let config = Config::from_toml(kebab).unwrap();
        assert_eq!(config.stream_connections, 2);
        assert_eq!(config.remote_catalog_entries, 5);
        assert_eq!(config.write_budget, 1 << 20);
        let underscore = "stream_connections = 3\nretain_remote = false\n";
        let config = Config::from_toml(underscore).unwrap();
        assert_eq!(config.stream_connections, 3);
        assert!(!config.retain_remote);
    }

    #[test]
    fn invalid_configurations_are_rejected() {
        assert!(parse_args(args(&["--name", "bad name"])).is_err());
        assert!(parse_args(args(&["--name", "x".repeat(65).as_str()])).is_err());
        assert!(parse_args(args(&["--memory", "1000"])).is_err());
        assert!(parse_args(args(&["--staging", "1M"])).is_err());
        assert!(parse_args(args(&["--nvme", "1G"])).is_err());
        assert!(parse_args(args(&["--root", "/x", "--nvme", "1K"])).is_err());
        assert!(parse_args(args(&["--stream-connections", "0"])).is_err());
        assert!(parse_args(args(&["--stream-connections", "65"])).is_err());
        assert!(parse_args(args(&["--remote-catalog-entries", "0"])).is_err());
        assert!(
            parse_args(args(&[
                "--listen-peer",
                "127.0.0.1:9000",
                "--listen-data",
                "127.0.0.1:9001",
            ]))
            .is_err()
        );
        assert!(parse_args(args(&["--upstream", "a=x"])).is_ok());
        assert!(parse_args(args(&["--upstream", "a=x", "--upstream", "a=y"])).is_err());
        assert!(parse_args(args(&["--upstream", "a=x,0"])).is_err());
        assert!(parse_args(args(&["--upstream", "a=x,1000001"])).is_err());
        assert!(parse_args(args(&["--retain-remote", "maybe"])).is_err());
        assert!(parse_args(args(&["--peer-transport", "carrier-pigeon"])).is_err());
    }

    #[cfg(not(feature = "rdma"))]
    #[test]
    fn rdma_transport_requires_the_feature() {
        assert!(parse_args(args(&["--peer-transport", "rdma"])).is_err());
    }

    #[test]
    fn listen_peer_accepts_a_complete_tls_configuration() {
        let config = run(&[
            "--peer-transport",
            "stream",
            "--listen-peer",
            "127.0.0.1:9000",
            "--listen-data",
            "127.0.0.1:9001",
            "--tls-cert",
            "/c",
            "--tls-key",
            "/k",
            "--tls-ca",
            "/a",
        ]);
        assert!(config.listen_peer.is_some());
        config.validate().unwrap();
    }

    #[test]
    fn upstream_names_are_unique_and_valid() {
        assert!(parse_args(args(&["--upstream", "a b=x"])).is_err());
        assert!(parse_args(args(&["--upstream", "a="])).is_err());
        assert!(parse_args(args(&["--upstream", "a"])).is_err());
    }

    #[test]
    fn help_lists_the_new_options() {
        let text = help_text();
        assert!(text.contains("--write-budget"));
        assert!(text.contains("--remote-catalog-entries"));
    }

    #[test]
    fn write_budget_zero_is_unlimited() {
        assert_eq!(run(&[]).write_budget, 0);
        assert_eq!(run(&["--write-budget", "0"]).write_budget, 0);
        assert_eq!(run(&["--write-budget", "1G"]).write_budget, 1 << 30);
    }

    #[test]
    fn network_listeners_are_named_in_a_startup_warning() {
        let mut config = Config::defaults();
        config.peer_transport = PeerTransport::Stream;
        assert!(config.startup_warnings().is_empty());
        config.listen_peer = Some("127.0.0.1:9200".parse().unwrap());
        config.listen_data = Some("127.0.0.1:9201".parse().unwrap());
        let warnings = config.startup_warnings();
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("network listening enabled"));
        assert!(warnings[0].contains("peer 127.0.0.1:9200"));
        assert!(warnings[0].contains("data 127.0.0.1:9201"));
        assert!(!warnings[0].contains("admin"));
    }

    #[test]
    fn rdma_is_experimental_and_never_combined_with_tls() {
        let mut config = Config::defaults();
        config.tls_cert = Some("/c".into());
        config.tls_key = Some("/k".into());
        config.tls_ca = Some("/a".into());
        config.peer_transport = PeerTransport::Stream;
        assert!(!config.rdma_enabled());
        config.validate().unwrap();
        assert!(config.startup_warnings().is_empty());
        config.peer_transport = PeerTransport::Auto;
        if cfg!(feature = "rdma") {
            assert!(config.rdma_enabled());
            let error = config.validate().unwrap_err().to_string();
            assert!(error.contains("not encrypted"), "{error}");
            config.tls_cert = None;
            config.tls_key = None;
            config.tls_ca = None;
            config.validate().unwrap();
            let warnings = config.startup_warnings();
            assert_eq!(warnings, ["selene: warning: RDMA enabled (experimental)"]);
        } else {
            assert!(!config.rdma_enabled());
            config.validate().unwrap();
        }
    }
}
