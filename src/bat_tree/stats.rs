//! Opt-in SMO counters used by the TPC-C tree-filling experiment.
//!
//! This whole module, and the corresponding field in `MVBTSt`, only exists
//! with `--features tpcc-tree-stats`. Normal release builds therefore pay no
//! memory, atomic-instruction, or code-size cost for the experiment.

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(usize)]
pub enum SmoKind {
    LeafVersionSplit,
    LeafKeySplit,
    InternalVersionSplit,
    InternalKeySplit,
    LeafMerge,
    InternalMerge,
    LeafMergeKeySplit,
    InternalMergeKeySplit,
    RootVersionSplit,
    RootKeySplit,
    RootMerge,
    OverflowAttempt,
    OverflowFailed,
    UnderflowAttempt,
    UnderflowFailed,
    RootSplitAttempt,
    RootSplitFailed,
    RootMergeAttempt,
    RootMergeFailed,
}

impl SmoKind {
    pub const ALL: [Self; 19] = [
        Self::LeafVersionSplit,
        Self::LeafKeySplit,
        Self::InternalVersionSplit,
        Self::InternalKeySplit,
        Self::LeafMerge,
        Self::InternalMerge,
        Self::LeafMergeKeySplit,
        Self::InternalMergeKeySplit,
        Self::RootVersionSplit,
        Self::RootKeySplit,
        Self::RootMerge,
        Self::OverflowAttempt,
        Self::OverflowFailed,
        Self::UnderflowAttempt,
        Self::UnderflowFailed,
        Self::RootSplitAttempt,
        Self::RootSplitFailed,
        Self::RootMergeAttempt,
        Self::RootMergeFailed,
    ];

    pub const COUNT: usize = Self::ALL.len();

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LeafVersionSplit => "leaf_version_split",
            Self::LeafKeySplit => "leaf_key_split",
            Self::InternalVersionSplit => "internal_version_split",
            Self::InternalKeySplit => "internal_key_split",
            Self::LeafMerge => "leaf_merge",
            Self::InternalMerge => "internal_merge",
            Self::LeafMergeKeySplit => "leaf_merge_key_split",
            Self::InternalMergeKeySplit => "internal_merge_key_split",
            Self::RootVersionSplit => "root_version_split",
            Self::RootKeySplit => "root_key_split",
            Self::RootMerge => "root_merge",
            Self::OverflowAttempt => "overflow_attempt",
            Self::OverflowFailed => "overflow_failed",
            Self::UnderflowAttempt => "underflow_attempt",
            Self::UnderflowFailed => "underflow_failed",
            Self::RootSplitAttempt => "root_split_attempt",
            Self::RootSplitFailed => "root_split_failed",
            Self::RootMergeAttempt => "root_merge_attempt",
            Self::RootMergeFailed => "root_merge_failed",
        }
    }

    pub const fn is_completed(self) -> bool {
        (self as usize) <= Self::RootMerge as usize
    }
}

pub struct SmoStats {
    values: [AtomicU64; SmoKind::COUNT],
}

impl SmoStats {
    pub const fn new() -> Self {
        Self {
            values: [const { AtomicU64::new(0) }; SmoKind::COUNT],
        }
    }

    #[inline(always)]
    pub fn record(&self, kind: SmoKind) {
        self.values[kind as usize].fetch_add(1, Relaxed);
    }

    pub fn snapshot(&self) -> SmoSnapshot {
        let mut values = [0; SmoKind::COUNT];
        for (out, value) in values.iter_mut().zip(&self.values) {
            *out = value.load(Relaxed);
        }
        SmoSnapshot { values }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SmoSnapshot {
    pub values: [u64; SmoKind::COUNT],
}

impl SmoSnapshot {
    pub fn get(self, kind: SmoKind) -> u64 {
        self.values[kind as usize]
    }

    pub fn saturating_sub(self, earlier: Self) -> Self {
        let mut values = [0; SmoKind::COUNT];
        for (i, out) in values.iter_mut().enumerate() {
            *out = self.values[i].saturating_sub(earlier.values[i]);
        }
        Self { values }
    }
}
