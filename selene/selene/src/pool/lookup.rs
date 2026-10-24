//! The ticket a queued request holds for its asynchronous pool lookup.

use std::sync::{Arc, Mutex};

use crate::plan;

use super::types::{HeldRange, PoolCandidate};

/// What a lookup produced for one request.
#[derive(Debug)]
pub struct Answer {
    /// What the request held closer when the lookup started. A candidate is
    /// usable against this context, not against the history's final key.
    pub held: Vec<HeldRange>,
    pub candidates: Vec<PoolCandidate>,
}

impl Answer {
    /// The candidates in the planner's terms. The planner checks each against
    /// the request's history and drops one that disagrees.
    pub fn plan_candidates(&self) -> Vec<plan::Candidate> {
        self.candidates
            .iter()
            .map(|candidate| candidate.plan.clone())
            .collect()
    }
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

#[cfg(any(test, feature = "test-support"))]
impl LookupTicket {
    /// A ticket in `state` that runs `withdraw` when it is dropped, for tests of
    /// the code that holds tickets.
    pub fn watched(state: LookupState, withdraw: impl FnOnce() + Send + 'static) -> Self {
        Self {
            inner: TicketInner::new(state),
            withdraw: Some(Box::new(withdraw)),
        }
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

    #[test]
    fn a_ticket_without_a_lookup_has_no_answer() {
        let ticket = LookupTicket::none();
        assert!(matches!(ticket.state(), LookupState::None));
    }
}
