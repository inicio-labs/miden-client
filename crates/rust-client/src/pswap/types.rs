//! Shared internal types used by the PSWAP module.

use miden_protocol::Felt;

/// Wrapper over [`Felt`] providing `Ord`/`Hash`-compatible derives for use as
/// a `BTreeMap` key. `Felt` itself doesn't implement these because field
/// elements have multiple canonical representations.
#[derive(Clone, Copy)]
pub(crate) struct OrderIdKey(Felt);

impl From<Felt> for OrderIdKey {
    fn from(value: Felt) -> Self {
        Self(value)
    }
}

impl PartialEq for OrderIdKey {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_canonical_u64() == other.0.as_canonical_u64()
    }
}
impl Eq for OrderIdKey {}
impl PartialOrd for OrderIdKey {
    fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for OrderIdKey {
    fn cmp(&self, other: &Self) -> core::cmp::Ordering {
        self.0.as_canonical_u64().cmp(&other.0.as_canonical_u64())
    }
}
