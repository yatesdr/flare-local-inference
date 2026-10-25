use super::*;
use crate::pool::net::{self, Roots};

fn roots(manager: &Manager) -> Roots {
    let (_, ca) = remote(&manager).unwrap();
    Roots::Fixed(net::trusted(ca.as_deref(), &Roots::System).unwrap())
}

fn other_roots() -> Roots {
    let cert = selene_manager::testing::certificate();
    Roots::Fixed(vec![cert.cert.der().clone()])
}

fn connect(config: PoolConfig, roots: Roots) -> PoolClient {
    let (copier, _) = SimulatedGpu::shared(&geo()).unwrap();
    PoolClient::start_waiting(
        config,
        None,
        copier,
        Arc::new(CountingRegistrar::default()),
        roots,
        Duration::from_secs(2),
    )
    .unwrap()
}

fn ready(client: &PoolClient) {
    wait("the client to be ready", || {
        client.status() == PoolStatus::Ready
    });
}

#[test]
fn a_client_with_a_ca_connects_to_a_tls_manager() {
    let manager = network_manager(true);
    let mut client = connect(config(&manager, "ca", 1), Roots::System);
    ready(&client);
    assert!(!client.registered_regions().is_empty());
    client.close().unwrap();
}

#[test]
fn a_client_without_a_ca_file_trusts_the_hosts_store() {
    let manager = network_manager(true);
    let mut config = config(&manager, "roots", 1);
    config.ca = None;
    let mut client = connect(config, roots(&manager));
    ready(&client);
    client.close().unwrap();
}

#[test]
fn an_untrusted_manager_disables_the_pool_without_registration() {
    let manager = network_manager(true);
    let mut config = config(&manager, "untrusted", 1);
    config.ca = None;
    let mut client = connect(config, other_roots());
    let PoolStatus::Disabled(reason) = client.status() else {
        panic!("{:?}", client.status());
    };
    assert!(
        reason.contains("cannot verify the pool manager over TLS"),
        "{reason}"
    );
    assert!(client.registered_regions().is_empty());
    client.close().unwrap();
}

#[test]
fn a_ca_file_overrides_the_hosts_store() {
    let manager = network_manager(true);
    let mut client = connect(config(&manager, "override", 1), other_roots());
    ready(&client);
    client.close().unwrap();
}

#[test]
fn a_plaintext_address_is_explicit() {
    let manager = network_manager(false);
    let config = config(&manager, "plain", 1);
    assert!(matches!(config.address, PoolAddress::TcpPlaintext(_)));
    let mut client = connect(config, Roots::System);
    ready(&client);
    client.close().unwrap();
}

#[test]
fn a_client_with_a_ca_never_downgrades_to_plaintext() {
    let manager = network_manager(false);
    let mut config = config(&manager, "downgrade", 1);
    let PoolAddress::TcpPlaintext(address) = config.address else {
        panic!()
    };
    config.address = PoolAddress::Tcp(address);
    let cert = selene_manager::testing::certificate();
    let ca = manager.base.join("client-ca.pem");
    std::fs::write(&ca, cert.cert.pem()).unwrap();
    config.ca = Some(ca);
    let mut client = connect(config, Roots::System);
    assert!(matches!(client.status(), PoolStatus::Disabled(_)));
    assert!(client.registered_regions().is_empty());
    client.close().unwrap();
}
