# Serving instances on other hosts

Instances on the manager's host talk to it over its Unix socket and move bytes through
`/dev/shm/selene`. Instances on other hosts reach the same manager over TCP. There is one
manager per pool (see [operating.md](operating.md)); there is no manager-to-manager protocol, and
the manager is never put behind a load balancer.

## Turning it on

```sh
selene --name pool --root /var/lib/selene --memory 16G --nvme 1T --staging 1G \
  --listen 0.0.0.0:9200 --tls-cert server.pem --tls-key server-key.pem
```

- `--listen ADDR:PORT` serves the pool protocol on that address in addition to the Unix socket.
  The manager prints `selene: warning: network listening enabled on ADDR (TLS|plaintext)` at start.
- `--tls-cert` and `--tls-key` give the manager a TLS identity. A client that was given a CA
  verifies the manager against it and refuses a plaintext manager; a client without a CA speaks
  plaintext. `Hello.encryption` tells the client what the manager was started with, and a client
  whose setting differs disables the pool for that process rather than downgrading. There are no
  client certificates: an instance is identified by its session, not by a credential.
- Without TLS the listener is plaintext. Put it on a trusted network, or use TLS.
- Keep the key out of the image and mount it read-only (`deploy/compose-network.yaml`).

A session on this listener has no shared memory, so it gets no instance-sourced candidates, and the manager's own
records are offered to it as level 5 (remote NVMe).

## What works today, and what does not

Hello, sessions, notices, resync, lookup, and the readiness barrier work over the network
listener exactly as on the Unix socket. **The data channel is not served in this version.**
Moving record bytes to or from an instance on another host needs it, so a `Claim` or `PutBegin` in
mode `DATA_CHANNEL` is refused (`INVALID_CLAIM`; `PutRefused{INVALID}` with a detail saying the
data channel is not served). The stream transport and the RDMA transport that will carry it are
in `src/data` (`stream.rs`, `rdma.rs`, `ibv.rs`) and keep their unit tests; they move
bytes between regions the two ends have mapped, and the instance-to-manager framing is wired in a
later change.

## Certificates

Generate a CA and a server certificate whose subject alternative names contain every name or
address clients dial:

```sh
openssl req -x509 -newkey rsa:3072 -nodes -days 3650 \
  -keyout ca-key.pem -out ca.pem -subj '/CN=selene-ca'
openssl req -newkey rsa:3072 -nodes -keyout server-key.pem -out server.csr -subj '/CN=pool'
printf 'subjectAltName=DNS:pool.example\n' > server.ext
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca-key.pem -CAcreateserial \
  -days 3650 -extfile server.ext -out server.pem
```

Hand `ca.pem` to the instances (their pool CA setting), `server.pem` and `server-key.pem` to
the manager.

## RDMA (experimental)

The default build and image never link libibverbs. Build with `--features rdma` (or use
`Dockerfile.rdma`) and start with `--rdma`. At start the manager prints
`selene: warning: RDMA enabled (experimental)`. RDMA bytes are not encrypted, so the manager
**refuses to start** with `--rdma` and TLS together and says why; choose one. RDMA is never listed
in `Hello.data_transports` when the manager is TLS. The container needs
`--device /dev/infiniband` and `--ulimit memlock=-1:-1`. For a Soft-RoCE test host:

```sh
sudo modprobe rdma_rxe
sudo rdma link add rxe0 type rxe netdev eth0   # any active Ethernet device
ibv_devices                                    # rxe0 should appear
```

## Troubleshooting

| Symptom | Check |
|---|---|
| Remote instances run on local tiers only | The address and firewall; whether the instance has the CA for a TLS manager (or none for a plaintext one); `Hello.encryption`. |
| `DATA_CHANNEL` claims or puts refused | Expected until the data channel is wired; same-host instances use shared windows. |
| TLS handshake failures | The certificate's SAN against the name clients dial; that the CA the clients hold signed the server certificate. |
| `--rdma` refused | TLS options present, or a build without the `rdma` feature. |
