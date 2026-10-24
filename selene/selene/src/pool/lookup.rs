//! The ticket a queued request holds for its asynchronous pool lookup.

use std::sync::{Arc, Mutex};

use crate::{History, PrefixKey};

use super::types::{Candidate, HeldRange};

/// What a lookup produced for one request.
#[derive(Debug)]
pub struct Answer {
    /// What the request held closer when the lookup started. A candidate is
    /// usable against this context, not against the history's final key.
    pub held: Vec<HeldRange>,
    pub candidates: Vec<Candidate>,
}

impl Answer {
    /// The candidates that agree with `history`: the endpoint and every chain
    /// node must be the key the history has at that length, and the chain must
    /// ascend to the endpoint. A candidate that fails is dropped, the others kept.
    pub fn verified(&self, history: &dyn History) -> Vec<Candidate> {
        self.candidates
            .iter()
            .filter(|candidate| candidate_matches(candidate, history))
            .cloned()
            .collect()
    }
}

fn candidate_matches(candidate: &Candidate, history: &dyn History) -> bool {
    let at = |key: &PrefixKey| history.key_at(key.consumed_tokens) == Some(*key);
    if !at(&candidate.key) || candidate.nodes.is_empty() {
        return false;
    }
    let mut previous = 0;
    for node in &candidate.nodes {
        if !at(&node.key) || node.key.consumed_tokens <= previous {
            return false;
        }
        previous = node.key.consumed_tokens;
    }
    candidate.nodes.last().map(|node| node.key) == Some(candidate.key)
}

#[derive(Clone, Debug)]
pub enum LookupState {
    /// No reply yet.
    Pending,
    /// The lookup finished with an answer, possibly with no candidates.
    Ready(Arc<Answer>),
    /// There is no answer and none will come: the pool is not ready, the lookup
    /// was refused or timed out, or the session was lost.
    None,
}

pub(crate) struct TicketInner {
    state: Mutex<LookupState>,
}

impl TicketInner {
    pub(crate) fn new(state: LookupState) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(state),
        })
    }

    pub(crate) fn get(&self) -> LookupState {
        self.state.lock().unwrap().clone()
    }
}

/// The handle one request holds for its lookup. Dropping it withdraws the
/// request: when it was the last holder of a lookup still on the wire, the
/// lookup is cancelled and a late reply is discarded.
pub struct LookupTicket {
    inner: Arc<TicketInner>,
    withdraw: Option<Box<dyn FnOnce() + Send>>,
}

impl LookupTicket {
    /// A ticket that will never have an answer and owns nothing.
    pub fn none() -> Self {
        Self {
            inner: TicketInner::new(LookupState::None),
            withdraw: None,
        }
    }

    /// The current state. Never waits for a reply.
    pub fn state(&self) -> LookupState {
        self.inner.get()
    }
}

impl Drop for LookupTicket {
    fn drop(&mut self) {
        if let Some(withdraw) = self.withdraw.take() {
            withdraw();
        }
    }
}

impl std::fmt::Debug for LookupTicket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LookupTicket")
            .field("state", &self.state())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pool::{Level, SourceRef, Tier};
    use crate::testing::TestHistory;

    fn history(tokens: usize) -> TestHistory {
        let mut history = TestHistory::new([7; 32]);
        history
            .append(&(0..tokens as u32).collect::<Vec<_>>(), &[])
            .unwrap();
        history
    }

    fn candidate(history: &TestHistory, ends: &[u64]) -> Candidate {
        let nodes: Vec<_> = ends
            .iter()
            .map(|end| ChainNode {
                key: history.key_at(*end).unwrap(),
                tail_pages: 1,
                generation: 1,
                row: false,
            })
            .collect();
        Candidate {
            level: Level::HostDram,
            tier: Tier::Dram,
            source: SourceRef {
                instance: vec![1],
                session_incarnation: 1,
            },
            key: nodes.last().unwrap().key,
            generation: 1,
            begin_tokens: 0,
            pages: ends.len() as u64,
            bytes: 0,
            row: true,
            nodes,
            skip_pages: 0,
            est_us_per_page: 0,
        }
    }

    use crate::pool::ChainNode;

    #[test]
    fn a_candidate_whose_node_disagrees_with_the_history_is_dropped_and_the_others_kept() {
        let history = history(512);
        let good = candidate(&history, &[64, 128]);
        let mut forged = candidate(&history, &[64, 128]);
        forged.nodes[0].key.digest[0] ^= 1;
        let mut wrong_endpoint = candidate(&history, &[64, 128]);
        wrong_endpoint.key = history.key_at(192).unwrap();
        let mut unordered = candidate(&history, &[128, 64]);
        unordered.key = history.key_at(64).unwrap();
        let answer = Answer {
            held: Vec::new(),
            candidates: vec![forged, good.clone(), wrong_endpoint, unordered],
        };
        assert_eq!(answer.verified(&history), vec![good]);
        let other = TestHistory::new([8; 32]);
        assert!(answer.verified(&other).is_empty());
    }

    #[test]
    fn a_ticket_without_a_lookup_has_no_answer() {
        let ticket = LookupTicket::none();
        assert!(matches!(ticket.state(), LookupState::None));
    }
}
