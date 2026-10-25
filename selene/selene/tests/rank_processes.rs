//! A multi-process instance as real processes on one host: a source, rank 0 and the agents of
//! the peer ranks are separate OS processes (the `rank_node` example), the manager runs in
//! this one. Ranks are killed in the middle of a restore.
//!
//! Run with `cargo test --features test-support --test rank_processes`.

use std::{
    io::{BufRead, BufReader, Read, Write},
    path::PathBuf,
    process::{Child, ChildStdin, Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, Ordering},
        mpsc::{Receiver, channel},
    },
    thread,
    time::{Duration, Instant},
};

use selene_manager::{control, store::Config};

static SERIAL: AtomicU32 = AtomicU32::new(0);

struct Manager {
    socket: PathBuf,
    base: PathBuf,
    metrics: std::net::SocketAddr,
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

fn free_address() -> std::net::SocketAddr {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    loop {
        let port = 30_000
            + (std::process::id() % 2_000) * 4
            + NEXT.fetch_add(1, Ordering::Relaxed) % 4_000;
        let address = std::net::SocketAddr::from(([127, 0, 0, 1], port as u16));
        if std::net::TcpListener::bind(address).is_ok() {
            return address;
        }
    }
}

impl Manager {
    fn start() -> Self {
        let id = SERIAL.fetch_add(1, Ordering::Relaxed);
        let base = std::env::temp_dir().join(format!("selene-ranks-{}-{id}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(base.join("run")).unwrap();
        let socket = base.join("run/m.sock");
        let metrics = free_address();
        let mut config = Config::dram_only(socket.clone(), 16 << 20);
        config.name = format!("selene-ranks-{}-{id}", std::process::id());
        config.metrics = Some(metrics.to_string());
        let stop = Arc::new(AtomicBool::new(false));
        let flag = stop.clone();
        let thread = thread::spawn(move || {
            if let Err(error) = control::run(config, flag) {
                eprintln!("manager exited: {error}");
            }
        });
        let deadline = Instant::now() + Duration::from_secs(15);
        while !socket.exists() {
            assert!(Instant::now() < deadline, "the manager did not start");
            thread::sleep(Duration::from_millis(5));
        }
        Self {
            socket,
            base,
            metrics,
            stop,
            thread: Some(thread),
        }
    }

    /// A gauge from the manager's metrics.
    fn gauge(&self, name: &str) -> u64 {
        let mut stream = std::net::TcpStream::connect(self.metrics).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .unwrap();
        let mut text = String::new();
        stream.read_to_string(&mut text).unwrap();
        text.lines()
            .find_map(|line| line.strip_prefix(&format!("{name} ")))
            .and_then(|value| value.trim().parse().ok())
            .unwrap_or(0)
    }
}

impl Drop for Manager {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        let _ = std::fs::remove_dir_all(&self.base);
    }
}

struct Node {
    child: Child,
    input: ChildStdin,
    lines: Receiver<String>,
    name: String,
}

fn example(name: &str) -> PathBuf {
    let mut path = std::env::current_exe().unwrap();
    path.pop();
    path.pop();
    path.join("examples").join(name)
}

impl Node {
    fn spawn(name: &str, args: &[&str]) -> Self {
        let mut child = Command::new(example("rank_node"))
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn rank_node");
        let input = child.stdin.take().unwrap();
        let output = child.stdout.take().unwrap();
        let (tx, lines) = channel();
        thread::spawn(move || {
            for line in BufReader::new(output).lines().map_while(|l| l.ok()) {
                if tx.send(line).is_err() {
                    return;
                }
            }
        });
        Self {
            child,
            input,
            lines,
            name: name.into(),
        }
    }

    fn send(&mut self, line: &str) {
        let _ = writeln!(self.input, "{line}");
        let _ = self.input.flush();
    }

    /// The next line that starts with `prefix`, within `seconds`.
    fn expect(&mut self, prefix: &str, seconds: u64) -> String {
        let deadline = Instant::now() + Duration::from_secs(seconds);
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            match self.lines.recv_timeout(left) {
                Ok(line) if line.starts_with(prefix) => return line,
                Ok(_) => {}
                Err(_) => panic!("{} said no `{prefix}` in {seconds}s", self.name),
            }
        }
    }

    /// Whether a line starting with `prefix` arrives within `seconds`.
    fn says(&mut self, prefix: &str, seconds: u64) -> bool {
        let deadline = Instant::now() + Duration::from_secs(seconds);
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            match self.lines.recv_timeout(left) {
                Ok(line) if line.starts_with(prefix) => return true,
                Ok(_) => {}
                Err(_) => return false,
            }
        }
    }

    fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.kill();
    }
}

/// A cluster: the source, rank 0 and `world - 1` agents, all running.
struct Cluster {
    manager: Manager,
    source: Node,
    rank0: Node,
    agents: Vec<Node>,
}

fn cluster(world: usize, stalls: &[u64]) -> Cluster {
    let manager = Manager::start();
    let socket = manager.socket.to_str().unwrap().to_string();
    let world_text = world.to_string();
    let mut source = Node::spawn(
        "source",
        &["source", "--socket", &socket, "--world", &world_text],
    );
    let mut rank0 = Node::spawn(
        "rank 0",
        &["rank0", "--socket", &socket, "--world", &world_text],
    );
    let port = rank0.expect("link-port ", 15)["link-port ".len()..].to_string();
    let link = format!("127.0.0.1:{port}");
    let mut agents = Vec::new();
    for rank in 1..world {
        let stall = stalls.get(rank - 1).copied().unwrap_or(0).to_string();
        agents.push(Node::spawn(
            &format!("agent {rank}"),
            &[
                "agent",
                "--socket",
                &socket,
                "--world",
                &world_text,
                "--rank",
                &rank.to_string(),
                "--link",
                &link,
                "--stall-ms",
                &stall,
            ],
        ));
    }
    source.expect("ready", 20);
    rank0.expect("ready", 20);
    for agent in &mut agents {
        agent.expect("ready", 20);
    }
    Cluster {
        manager,
        source,
        rank0,
        agents,
    }
}

fn wait_for(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(20));
    }
}

fn restore_and_verify(world: usize) {
    let mut cluster = cluster(world, &[]);
    cluster.rank0.send("restore");
    let line = cluster.rank0.expect("fetched ", 60);
    let (page, row) = (world * 8192, world * 4096);
    assert_eq!(line, format!("fetched ok {}", 4 * page + row));
    cluster.rank0.send("verify");
    assert_eq!(cluster.rank0.expect("verify ", 10), "verify ok");
    for agent in &mut cluster.agents {
        agent.send("verify");
        assert_eq!(agent.expect("verify ", 10), "verify ok");
    }
    cluster.rank0.send("quit");
    for agent in &mut cluster.agents {
        agent.send("quit");
        assert_eq!(agent.expect("closed ", 30), "closed ok");
    }
    cluster.source.send("quit");
    wait_for("the windows to be returned", || {
        cluster.manager.gauge("selene_pool_quarantined_bytes") == 0
    });
}

#[test]
fn two_ranks_in_separate_processes_restore_a_record_and_each_holds_its_slice() {
    restore_and_verify(2);
}

#[test]
fn four_ranks_in_separate_processes_restore_a_record() {
    restore_and_verify(4);
}

#[test]
fn a_killed_peer_stops_rank_zero_without_a_result_and_its_windows_come_back_when_it_is_gone() {
    // The peer's copies take three seconds, so it is killed with a window in its hands.
    let mut cluster = cluster(2, &[3_000]);
    cluster.rank0.send("restore");
    cluster.source.expect("served", 20);
    thread::sleep(Duration::from_millis(600));
    cluster.agents[0].kill();
    assert!(
        cluster.rank0.says("poll err", 30),
        "rank 0 did not stop when a rank died with a copy outstanding"
    );
    // No result was ever given for pages the dead rank may have been writing.
    assert!(!cluster.rank0.says("fetched ok", 1));
    // The windows the dead process held are released once its exit is confirmed.
    wait_for("the dead rank's windows to be released", || {
        cluster.manager.gauge("selene_pool_quarantined_bytes") == 0
    });
    cluster.source.send("quit");
}

#[test]
fn a_killed_rank_zero_lets_the_agents_drain_attest_and_exit() {
    let mut cluster = cluster(2, &[1_500]);
    cluster.rank0.send("restore");
    cluster.source.expect("served", 20);
    thread::sleep(Duration::from_millis(400));
    cluster.rank0.kill();
    // The agent sees its link close, finishes the copy it started, attests to the manager and
    // stops.
    assert!(
        cluster.agents[0].says("agent disabled", 30),
        "the agent did not stop when rank 0 died"
    );
    cluster.agents[0].expect("closed ", 30);
    wait_for("the windows to be returned", || {
        cluster.manager.gauge("selene_pool_quarantined_bytes") == 0
    });
    cluster.source.send("quit");
}
