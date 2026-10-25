//! Manager pool settings.

pub use flare_selene::pool::Limits;

/// The pool settings a manager is started with.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PoolSettings {
    pub limits: Limits,
}
