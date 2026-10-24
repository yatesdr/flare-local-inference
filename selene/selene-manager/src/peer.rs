//! Manager-to-manager peer state.
//!
//! This module holds the parts that do not depend on the store or the gRPC client: the bounded,
//! parent-first forwarding queue, generic over a key so it can be tested with small keys and
//! wired to the store's `Key` later. The merged catalog lives in the store.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::time::{Duration, Instant};

pub const FORWARD_QUEUE_LIMIT: usize = 1024;
pub const FORWARD_RETRIES: u32 = 5;
pub const FORWARD_BACKOFF_CAP: Duration = Duration::from_secs(30);

// ---------------------------------------------------------------------------------------------
// Forwarding
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action<K> {
    Pin(K),
    Unpin(K),
    Send { upstream: String, key: K },
    Abandoned { upstream: String, key: K },
    Skipped(K),
    MissingAncestry(K),
}

#[derive(Clone, Debug)]
struct Job<K> {
    upstream: String,
    ancestors: Vec<K>,
    parent: Option<K>,
    attempts: u32,
    due: Instant,
    sent: bool,
}

/// Bounded, parent-first write-through forwarding, one queue per upstream.
pub struct Forwarder<K> {
    limit: usize,
    jobs: BTreeMap<K, Job<K>>,
    queues: BTreeMap<String, VecDeque<K>>,
}

impl<K: Copy + Ord + std::fmt::Debug> Default for Forwarder<K> {
    fn default() -> Self {
        Self::new(FORWARD_QUEUE_LIMIT)
    }
}

impl<K: Copy + Ord + std::fmt::Debug> Forwarder<K> {
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            jobs: BTreeMap::new(),
            queues: BTreeMap::new(),
        }
    }

    pub fn pending(&self) -> usize {
        self.jobs.len()
    }

    pub fn queue_len(&self, upstream: &str) -> usize {
        self.queues.get(upstream).map_or(0, VecDeque::len)
    }

    /// Submit a local record for forwarding. `ancestors` are the keys to pin, including the
    /// record itself; parents must be submitted before children.
    pub fn submit(
        &mut self,
        now: Instant,
        upstream: &str,
        key: K,
        parent: Option<K>,
        ancestors: Vec<K>,
    ) -> Vec<Action<K>> {
        if self.jobs.contains_key(&key) {
            return Vec::new();
        }
        if self.queue_len(upstream) >= self.limit {
            return vec![Action::Skipped(key)];
        }
        let actions: Vec<Action<K>> = ancestors
            .iter()
            .map(|ancestor| Action::Pin(*ancestor))
            .collect();
        self.jobs.insert(
            key,
            Job {
                upstream: upstream.to_string(),
                ancestors,
                parent,
                attempts: 0,
                due: now,
                sent: false,
            },
        );
        let queue = self.queues.entry(upstream.to_string()).or_default();
        let position = queue.iter().position(|queued| {
            self.jobs
                .get(queued)
                .is_some_and(|job| job.ancestors.contains(&key))
        });
        match position {
            Some(position) => queue.insert(position, key),
            None => queue.push_back(key),
        }
        actions
    }

    /// A submit was accepted but the send failed synchronously.
    pub fn failed(&mut self, now: Instant, key: K) -> Vec<Action<K>> {
        let Some(job) = self.jobs.get_mut(&key) else {
            return Vec::new();
        };
        job.attempts += 1;
        job.sent = false;
        if job.attempts > FORWARD_RETRIES {
            let job = self.jobs.remove(&key).unwrap();
            self.remove_queued(&job.upstream, key);
            let mut actions: Vec<Action<K>> = job
                .ancestors
                .iter()
                .map(|ancestor| Action::Unpin(*ancestor))
                .collect();
            actions.push(Action::Abandoned {
                upstream: job.upstream,
                key,
            });
            return actions;
        }
        job.due = now + retry_delay(job.attempts);
        Vec::new()
    }

    /// The upstream refused the record because its parent is missing.
    pub fn missing_ancestry(&mut self, now: Instant, key: K) -> Vec<Action<K>> {
        let parent = self.jobs.get(&key).and_then(|job| job.parent);
        let mut actions = self.failed(now, key);
        if let Some(parent) = parent {
            actions.push(Action::MissingAncestry(parent));
        }
        actions
    }

    /// The record is stored (or already present) upstream.
    pub fn complete(&mut self, key: K) -> Vec<Action<K>> {
        let Some(job) = self.jobs.remove(&key) else {
            return Vec::new();
        };
        self.remove_queued(&job.upstream, key);
        job.ancestors
            .iter()
            .map(|ancestor| Action::Unpin(*ancestor))
            .collect()
    }

    /// Jobs whose retry time has arrived and which have not been sent since the last attempt.
    pub fn ready(&mut self, now: Instant) -> Vec<Action<K>> {
        let pending: BTreeSet<K> = self.jobs.keys().copied().collect();
        let mut due = Vec::new();
        for (upstream, queue) in &self.queues {
            for key in queue {
                if let Some(job) = self.jobs.get(key)
                    && !job.sent
                    && job.due <= now
                    && !job
                        .ancestors
                        .iter()
                        .any(|ancestor| ancestor != key && pending.contains(ancestor))
                {
                    due.push((upstream.clone(), *key));
                }
            }
        }
        let mut actions = Vec::with_capacity(due.len());
        for (upstream, key) in due {
            if let Some(job) = self.jobs.get_mut(&key) {
                job.sent = true;
            }
            actions.push(Action::Send { upstream, key });
        }
        actions
    }

    fn remove_queued(&mut self, upstream: &str, key: K) {
        if let Some(queue) = self.queues.get_mut(upstream) {
            queue.retain(|candidate| *candidate != key);
        }
    }
}

/// Retry delay after the given failure count: 1, 2, 4, 8, 16 seconds, capped at 30.
pub fn retry_delay(attempts: u32) -> Duration {
    let seconds = 1u64 << attempts.saturating_sub(1).min(5);
    Duration::from_secs(seconds).min(FORWARD_BACKOFF_CAP)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(value: u64) -> u64 {
        value
    }

    #[test]
    fn forwarding_pins_ancestors_and_unpins_on_completion() {
        let mut forwarder = Forwarder::new(8);
        let now = Instant::now();
        let actions = forwarder.submit(now, "a", key(2), Some(key(1)), vec![key(1), key(2)]);
        assert_eq!(actions, vec![Action::Pin(1), Action::Pin(2)]);
        assert_eq!(
            forwarder.ready(now),
            vec![Action::Send {
                upstream: "a".into(),
                key: 2
            }]
        );
        assert_eq!(
            forwarder.complete(key(2)),
            vec![Action::Unpin(1), Action::Unpin(2)]
        );
        assert_eq!(forwarder.pending(), 0);
    }

    #[test]
    fn forwarding_retries_then_abandons() {
        let mut forwarder = Forwarder::new(8);
        let now = Instant::now();
        forwarder.submit(now, "a", key(1), None, vec![key(1)]);
        for attempt in 1..=FORWARD_RETRIES {
            assert!(forwarder.failed(now, key(1)).is_empty());
            let due = now + retry_delay(attempt);
            assert_eq!(
                forwarder.ready(due),
                vec![Action::Send {
                    upstream: "a".into(),
                    key: 1
                }]
            );
        }
        let actions = forwarder.failed(now, key(1));
        assert!(matches!(
            actions.last(),
            Some(Action::Abandoned { upstream, key: 1 }) if upstream == "a"
        ));
        assert_eq!(forwarder.pending(), 0);
    }

    #[test]
    fn forwarding_is_bounded_per_upstream() {
        let mut forwarder = Forwarder::new(1);
        let now = Instant::now();
        forwarder.submit(now, "a", key(1), None, vec![key(1)]);
        let actions = forwarder.submit(now, "a", key(2), None, vec![key(2)]);
        assert_eq!(actions, vec![Action::Skipped(2)]);
        assert_eq!(forwarder.pending(), 1);
    }

    #[test]
    fn forwarding_reports_a_missing_parent() {
        let mut forwarder = Forwarder::new(8);
        let now = Instant::now();
        forwarder.submit(now, "a", key(2), Some(key(1)), vec![key(1), key(2)]);
        let actions = forwarder.missing_ancestry(now, key(2));
        assert_eq!(actions, vec![Action::MissingAncestry(1)]);
    }

    #[test]
    fn retry_delays_are_exponential_and_capped() {
        assert_eq!(retry_delay(1), Duration::from_secs(1));
        assert_eq!(retry_delay(2), Duration::from_secs(2));
        assert_eq!(retry_delay(3), Duration::from_secs(4));
        assert_eq!(retry_delay(4), Duration::from_secs(8));
        assert_eq!(retry_delay(5), Duration::from_secs(16));
        assert_eq!(retry_delay(9), FORWARD_BACKOFF_CAP);
        assert_eq!(retry_delay(100), FORWARD_BACKOFF_CAP);
    }

    #[test]
    fn a_queued_parent_is_sent_before_its_child() {
        let mut forwarder = Forwarder::new(8);
        let now = Instant::now();
        forwarder.submit(now, "a", key(1), None, vec![key(1)]);
        forwarder.submit(now, "a", key(2), Some(key(1)), vec![key(1), key(2)]);
        let sends: Vec<u64> = forwarder
            .ready(now)
            .into_iter()
            .filter_map(|action| match action {
                Action::Send { key, .. } => Some(key),
                _ => None,
            })
            .collect();
        assert_eq!(sends, vec![1]);
        forwarder.complete(key(1));
        let sends: Vec<u64> = forwarder
            .ready(now)
            .into_iter()
            .filter_map(|action| match action {
                Action::Send { key, .. } => Some(key),
                _ => None,
            })
            .collect();
        assert_eq!(sends, vec![2]);
    }
}
