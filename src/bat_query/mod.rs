use crate::bat_record_model::version_info::Version;

pub mod dispatch;
pub mod interval;
pub mod iter_query;
pub mod olc_query;
pub mod query;
pub mod rand_query;
pub mod snapshot;

pub type SnapShot = Version;
