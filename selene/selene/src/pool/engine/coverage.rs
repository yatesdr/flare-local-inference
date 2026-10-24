//! Exact coverage of a claim or export by its windows.

use super::{super::wire, requester::ExpectedRecord};

/// Tracks which pages and row the windows issued so far have covered. A window
/// is accepted only if it extends the coverage exactly: ordinal order, inside the
/// record being filled, the row only in the window that completes the last
/// record, and a length that matches its pages.
pub(crate) struct Coverage {
    pub(crate) expected: Vec<ExpectedRecord>,
    taken: Vec<u32>,
    cursor: usize,
    windows: u32,
}

impl Coverage {
    pub(crate) fn new(expected: Vec<ExpectedRecord>) -> Self {
        Self {
            taken: vec![0; expected.len()],
            expected,
            cursor: 0,
            windows: 0,
        }
    }

    /// Every record, page and the row has been covered once.
    pub(crate) fn complete(&self) -> bool {
        self.cursor == self.expected.len()
    }

    /// Accept `spec` as the next window and return the claim-order index of
    /// its first page.
    pub(crate) fn accept(
        &mut self,
        spec: &wire::WindowSpec,
        page_bytes: u64,
        row_bytes: u64,
    ) -> Result<u64, String> {
        if spec.index != self.windows {
            return Err(format!("window {} out of order or repeated", spec.index));
        }
        let record = spec.record as usize;
        if self.cursor >= self.expected.len() || record != self.cursor {
            return Err(format!(
                "window {} names record {record}, expected {}",
                spec.index, self.cursor
            ));
        }
        let expected = &self.expected[record];
        let taken = self.taken[record];
        let last = record + 1 == self.expected.len();
        let remaining = expected.pages - taken;
        if spec.first_page != expected.first_page + taken
            || spec.page_count > remaining
            || (spec.page_count == 0 && !(expected.pages == 0 && expected.row))
        {
            return Err(format!(
                "window {} does not extend the coverage",
                spec.index
            ));
        }
        let completes = spec.page_count == remaining;
        if spec.row != (last && completes && expected.row) {
            return Err(format!("window {} carries the row wrongly", spec.index));
        }
        let row = if spec.row { row_bytes } else { 0 };
        if spec.length != u64::from(spec.page_count) * page_bytes + row {
            return Err(format!("window {} has the wrong length", spec.index));
        }
        let before: u64 = self.expected[..record]
            .iter()
            .map(|r| u64::from(r.pages))
            .sum();
        self.taken[record] += spec.page_count;
        if completes {
            self.cursor += 1;
        }
        self.windows += 1;
        Ok(before + u64::from(taken))
    }
}
