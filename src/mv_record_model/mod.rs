use std::sync::atomic::AtomicU64;

pub mod record_point;
pub mod version_info;
pub mod tx_stamp;

/// Declares the atomic version type.
pub type AtomicVersion = AtomicU64;