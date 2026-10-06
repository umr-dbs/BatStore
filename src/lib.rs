//! BatStore's embeddable storage-engine API.
//!
//! The [`bat_db`] module provides the general-purpose multi-table database
//! interface. The lower-level tree, transaction, query, and WAL modules are
//! public as well for applications that need direct access to the engine.

pub mod bat_bench;
pub mod bat_block;
pub mod bat_crud_model;
pub mod bat_db;
pub mod bat_gc;
pub mod bat_page_model;
pub mod bat_query;
pub mod bat_record_model;
pub mod bat_root;
pub mod bat_sync;
pub mod bat_test;
pub mod bat_tree;
#[cfg(feature = "tree-viz")]
pub mod bat_viz;
pub mod bat_wal;

// Keep the most commonly needed embedded-database types available from the
// crate root, while retaining the existing module paths for compatibility.
pub use bat_crud_model::crud_api::AtomicTxDispatcher;
pub use bat_crud_model::crud_operation::{CRUDOperation, TxAtomicOperation};
pub use bat_crud_model::crud_operation_result::{AtomicTxResult, CRUDOperationResult};
pub use bat_db::{Database, DbTransaction, IsolationLevel, TableId, TransactionState};
pub use bat_query::interval::Interval;
pub use bat_root::index_root::RootIndexType;
pub use bat_tree::mvbt::{Key, MVBT, MVBTSt, Payload};
