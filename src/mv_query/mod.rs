use crate::mv_record_model::version_info::Version;

pub mod dispatch;
pub mod query;
pub mod olc_query;
pub mod iter_query;
pub mod rand_query;
pub mod snapshot;
pub mod interval;

pub type SnapShot = Version;
