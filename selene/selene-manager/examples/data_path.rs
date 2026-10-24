//! Local data-path throughput: UDS clients using the manager's shared-memory windows.
//!
//! Starts nothing: point it at an already running manager socket. Each client writes a
//! record through the staging window and reads it back through the arena/staging window,
//! which is the same path model clients use.
//!
//! Content is generated once per sample outside the timed region (its cost is reported
//! separately), the timed region only stores the prepared bytes and fetches them back, and
//! standalone checksum and memcpy baselines are reported so the numbers can be read against
//! memory and CPU limits. Each client reuses one Get destination across samples, so its
//! pages are touched once during the warm-up rather than faulted inside every timed Get.
//!
//! Environment:
//!   BENCH_SOCKET       manager Unix socket (required)
//!   BENCH_MODE         label for the report (default "mode")
//!   BENCH_MIB          record size in MiB (default 64)
//!   BENCH_CLIENTS      concurrent clients (default 1)
//!   BENCH_SAMPLES      samples including one warm-up (default 3)
//!   BENCH_PAGE_BYTES   record page size (default 65536)
//!   BENCH_MANAGER_PID  manager pid for its CPU share, if known

#[path = "../tests/support/mod.rs"]
mod support;

use selene_manager::{Key, record, store::Config};
use std::{env, fs, path::PathBuf, sync::Arc, time::Instant};
use support::{Client, descriptor, key};
use tokio::sync::{Barrier, Mutex};

fn process_cpu_seconds() -> f64 {
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe {
        libc::getrusage(libc::RUSAGE_SELF, &mut usage);
    }
    usage.ru_utime.tv_sec as f64
        + usage.ru_utime.tv_usec as f64 / 1e6
        + usage.ru_stime.tv_sec as f64
        + usage.ru_stime.tv_usec as f64 / 1e6
}

/// CPU seconds of another process from /proc/<pid>/stat (utime + stime).
fn other_cpu_seconds(pid: u32) -> f64 {
    let text = fs::read_to_string(format!("/proc/{pid}/stat")).unwrap_or_default();
    let Some(close) = text.rfind(')') else {
        return 0.0;
    };
    let fields: Vec<&str> = text[close + 1..].split_whitespace().collect();
    let ticks = |index: usize| -> f64 {
        fields
            .get(index)
            .and_then(|value| value.parse::<f64>().ok())
            .unwrap_or(0.0)
    };
    let ticks_per_second = unsafe { libc::sysconf(libc::_SC_CLK_TCK) } as f64;
    (ticks(11) + ticks(12)) / ticks_per_second
}

fn median(values: &mut [f64]) -> f64 {
    values.sort_by(f64::total_cmp);
    values[values.len() / 2]
}

fn variable(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_owned())
}

fn record_key(tokens: u64, sample: usize, client: usize) -> Key {
    let mut key = key(tokens);
    key.digest[0] = sample as u8;
    key.digest[1] = client as u8;
    key
}

/// Page and row checksums exactly as the Put path computes them.
fn checksum_seconds(header: &selene_manager::record::Header, bytes: &[u8], pages: u32) -> f64 {
    let started = Instant::now();
    let page_bytes = header.page_record_bytes() as usize;
    let mut folded = 0u64;
    for page in 0..pages as usize {
        let offset = header.page_segment as usize + page * page_bytes;
        folded ^= record::checksum(&bytes[offset..offset + page_bytes]);
    }
    folded ^= record::checksum(&bytes[header.row_segment as usize..]);
    std::hint::black_box(folded);
    started.elapsed().as_secs_f64()
}

/// Single-threaded memcpy between two buffers of the same size.
fn copy_seconds(size: usize) -> f64 {
    let source = vec![7u8; size];
    let mut destination = vec![0u8; size];
    destination.copy_from_slice(&source);
    let started = Instant::now();
    for _ in 0..3 {
        destination.copy_from_slice(&source);
        std::hint::black_box(&destination);
    }
    started.elapsed().as_secs_f64() / 3.0
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let socket = PathBuf::from(env::var("BENCH_SOCKET").expect("BENCH_SOCKET is required"));
    let mode = variable("BENCH_MODE", "mode");
    let mib: u64 = variable("BENCH_MIB", "64").parse().expect("BENCH_MIB");
    let clients: usize = variable("BENCH_CLIENTS", "1")
        .parse()
        .expect("BENCH_CLIENTS");
    let samples: usize = variable("BENCH_SAMPLES", "3")
        .parse()
        .expect("BENCH_SAMPLES");
    let page_bytes: u64 = variable("BENCH_PAGE_BYTES", "65536")
        .parse()
        .expect("BENCH_PAGE_BYTES");
    let manager_pid: Option<u32> = env::var("BENCH_MANAGER_PID")
        .ok()
        .and_then(|value| value.parse().ok());
    assert!(
        clients > 0 && samples > 1,
        "need clients and a warm-up sample"
    );

    let pages = (mib * 1024 * 1024) / page_bytes;
    let consumed = pages * 64;
    let mut descriptor = descriptor();
    descriptor.ranks[0].planes = vec![page_bytes];
    let config = Config::dram_only(socket, 1);

    let mut clients_ready = Vec::with_capacity(clients);
    for _ in 0..clients {
        let client = Client::connect(&config, descriptor.clone())
            .await
            .expect("client connection");
        clients_ready.push(Arc::new(Mutex::new(client)));
    }
    let (header, record_bytes) = {
        let client = clients_ready[0].lock().await;
        let (header, bytes, _) = client.content(key(consumed), None, 0);
        (header, bytes.len())
    };

    println!(
        "case mode={mode} mib={mib} clients={clients} page_bytes={page_bytes} \
         record_bytes={record_bytes}"
    );
    let mut checksums = Vec::new();
    for _ in 0..3 {
        checksums.push(checksum_seconds(
            &header,
            &vec![0u8; record_bytes],
            pages as u32,
        ));
    }
    let checksum = median(&mut checksums);
    println!(
        "checksum bytes={record_bytes} checksum_s={checksum:.6} \
         checksum_GB_s={:.3}",
        record_bytes as f64 / checksum / 1e9,
    );
    let copy = copy_seconds(record_bytes);
    println!(
        "copy bytes={record_bytes} copy_s={copy:.6} copy_GB_s={:.3}",
        record_bytes as f64 / copy / 1e9,
    );

    let get_only = env::var_os("BENCH_GET_ONLY").is_some();
    // One destination per client, first touched in the warm-up sample.
    let mut destinations: Vec<Option<Vec<Vec<u8>>>> = (0..clients).map(|_| None).collect();
    let mut puts = Vec::new();
    let mut gets = Vec::new();
    let mut generates = Vec::new();
    let mut cpu_shares = Vec::new();
    let mut manager_shares = Vec::new();
    for sample in 0..samples {
        // A get-only run stores once and then repeats the fetch, so a profiler sees one
        // long Get rather than alternating Put and Get.
        let storing = !get_only || sample == 0;
        let key_sample = if get_only { 0 } else { sample };
        let mut prepared: Vec<Option<(selene_manager::record::Header, Vec<u8>)>> =
            (0..clients).map(|_| None).collect();
        let mut prepare_s = 0.0;
        if storing {
            let started = Instant::now();
            for (index, client) in clients_ready.iter().enumerate() {
                let client = client.lock().await;
                let (header, bytes, _) =
                    client.content(record_key(consumed, key_sample, index), None, 0);
                prepared[index] = Some((header, bytes));
            }
            prepare_s = started.elapsed().as_secs_f64();
            generates.push(clients as f64 * record_bytes as f64 / prepare_s / 1e9);
        }

        let barrier = Arc::new(Barrier::new(clients));
        let cpu_before = process_cpu_seconds();
        let manager_before = manager_pid.map(other_cpu_seconds);
        let started = Instant::now();
        let mut tasks = Vec::with_capacity(clients);
        for (index, client) in clients_ready.iter().cloned().enumerate() {
            let barrier = barrier.clone();
            let record = prepared[index].take();
            let mut destination = destinations[index].take().unwrap_or_default();
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                let mut client = client.lock().await;
                let key = record_key(consumed, key_sample, index);
                let (put, stored) = match record {
                    Some(prepared) => {
                        let put_started = Instant::now();
                        let stored = client.store_prepared(key, None, prepared).await;
                        (put_started.elapsed().as_secs_f64(), Some(stored))
                    }
                    None => (0.0, None),
                };
                let get_started = Instant::now();
                assert!(
                    client
                        .fetch_into(&[key], 0, pages as u32, &mut destination)
                        .await,
                    "fetch failed"
                );
                let get = get_started.elapsed().as_secs_f64();
                if let Some(value) = stored {
                    assert_eq!(destination[0], value, "fetched bytes differ");
                }
                (index, put, get, destination)
            }));
        }
        let mut put = 0.0f64;
        let mut get = 0.0f64;
        for task in tasks {
            let (index, task_put, task_get, destination) = task.await.expect("client task");
            destinations[index] = Some(destination);
            put = put.max(task_put);
            get = get.max(task_get);
        }
        let wall = started.elapsed().as_secs_f64();
        let bytes = clients as f64 * record_bytes as f64;
        let cpu_share = 100.0 * (process_cpu_seconds() - cpu_before) / wall;
        let manager_share = manager_pid.map_or(0.0, |_| {
            100.0 * (other_cpu_seconds(manager_pid.unwrap()) - manager_before.unwrap()) / wall
        });
        println!(
            "sample mode={mode} mib={mib} clients={clients} sample={sample} \
             put_GB_s={:.3} get_GB_s={:.3} wall_s={wall:.4} prepare_s={prepare_s:.4} \
             gen_GB_s={:.3} client_cpu_pct={cpu_share:.1} manager_cpu_pct={manager_share:.1}",
            if put > 0.0 { bytes / put / 1e9 } else { 0.0 },
            bytes / get / 1e9,
            generates.last().copied().unwrap_or(0.0),
        );
        if sample == 0 {
            continue;
        }
        if put > 0.0 {
            puts.push(bytes / put / 1e9);
        }
        gets.push(bytes / get / 1e9);
        cpu_shares.push(cpu_share);
        manager_shares.push(manager_share);
    }
    let put_median = if puts.is_empty() {
        0.0
    } else {
        median(&mut puts)
    };
    let generated = if generates.len() > 1 {
        median(&mut generates[1..])
    } else {
        generates.first().copied().unwrap_or(0.0)
    };
    println!(
        "median mode={mode} mib={mib} clients={clients} \
         put_GB_s={put_median:.3} get_GB_s={:.3} gen_GB_s={generated:.3} \
         client_cpu_pct={:.1} manager_cpu_pct={:.1}",
        median(&mut gets),
        median(&mut cpu_shares),
        median(&mut manager_shares),
    );
}
