use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};
use std::str::FromStr;
use thiserror::Error;

/// Events at or below this `main` height use the pre-extended (legacy) event
/// model; above it, extended events apply.
pub const LEGACY_EVENT_CUTOFF_HEIGHT: u64 = 6_422_526;

/// Lower height bound for `main` ingestion: the worker ingests `main` from above
/// this height.
pub const MAIN_ZERO_STATE_BOUNDARY_HEIGHT: u64 = 6_422_526;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum DomainError {
    #[error("{field} cannot be empty")]
    EmptyValue { field: &'static str },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NexusName(NonEmptyString);

impl NexusName {
    pub fn new(value: impl Into<String>) -> Result<Self, DomainError> {
        NonEmptyString::new("nexus", value).map(Self)
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl Display for NexusName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for NexusName {
    type Err = DomainError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ChainName(NonEmptyString);

impl ChainName {
    pub fn new(value: impl Into<String>) -> Result<Self, DomainError> {
        NonEmptyString::new("chain", value).map(Self)
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl Display for ChainName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for ChainName {
    type Err = DomainError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BlockHeight(u64);

impl BlockHeight {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn value(self) -> u64 {
        self.0
    }

    pub const fn is_legacy_event_height(self) -> bool {
        self.0 <= LEGACY_EVENT_CUTOFF_HEIGHT
    }
}

impl Display for BlockHeight {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockIdentity {
    pub chain: ChainName,
    pub height: BlockHeight,
    pub hash: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ServiceRole {
    Api,
    Worker,
    Migrate,
    Parity,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
struct NonEmptyString(String);

impl NonEmptyString {
    fn new(field: &'static str, value: impl Into<String>) -> Result<Self, DomainError> {
        let value = value.into();
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return Err(DomainError::EmptyValue { field });
        }

        Ok(Self(trimmed.to_owned()))
    }

    fn as_str(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_blank_chain_name() {
        // Domain names are part of table keys and RPC calls; accepting blanks
        // would make ingestion failures look like downstream RPC or SQL bugs.
        assert_eq!(
            ChainName::new("   "),
            Err(DomainError::EmptyValue { field: "chain" })
        );
    }

    #[test]
    fn classifies_legacy_event_cutoff_inclusively() {
        // The current C# Explorer treats the cutoff block itself as legacy, so
        // Rust parity must keep the boundary inclusive.
        assert!(BlockHeight::new(LEGACY_EVENT_CUTOFF_HEIGHT).is_legacy_event_height());
        assert!(!BlockHeight::new(LEGACY_EVENT_CUTOFF_HEIGHT + 1).is_legacy_event_height());
    }
}
