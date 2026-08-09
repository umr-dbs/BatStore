//! General-purpose Database/Table API: lets the MVBT engine be used as an
//! embedded, multi-table DB directly (create a [`Database`], `create_table`
//! several tables in it, transact across them via [`DbTransaction`]), rather
//! than only inside the TPC-C benchmark harness (`mv_bench::tpcc_schema`).
//!
//! The key structural difference from `mv_bench::tpcc_schema::TpccDatabase`
//! (the closest existing precedent — many tables sharing one `TxContext`
//! for atomic/snapshot-isolated cross-table transactions): a `Database` has
//! exactly **one** shared WAL (one file, one `WalWriter`, one group-commit
//! thread) for every table, instead of one WAL per table. Every table's own
//! `MVBTSt::wal` field holds a *clone of the same* `Arc<WalWriter>` (see
//! `MVBTSt::attach_wal`), and each WAL entry is tagged with a `TableId` (see
//! `mv_wal::record::TableId`) so `mv_wal::recovery::replay_database` can
//! demultiplex the one interleaved file back into the right table on
//! recovery. A `TableId` is simply a table's position in `Database`'s table
//! list, assigned once at `create_table` time and persisted, in that same
//! order, to a small catalog file colocated with the WAL — not a hash — so
//! every lookup (`Database::table`, every `DbTransaction` op) is a direct,
//! lock-free slice index, never a hash-map lookup or a lock. A side benefit
//! of the shared WAL: a cross-table `DbTransaction::commit` logs exactly
//! **one** Commit marker (any touched table's tree — they all share the
//! same writer), unlike `TpccTxn::commit`, which must log one marker per
//! touched table since each has its own file.
//!
//! Every table in one `Database` shares a single Rust `Payload` type — an
//! app-defined enum playing the same role as `TpccRow` — so the whole
//! `Database<FAN_OUT, NUM_RECORDS, Key, Payload>` is one monomorphized type;
//! there is no per-table type erasure.
pub mod database;
pub mod transaction;

pub use database::Database;
pub use transaction::{DbTransaction, TransactionState};
pub use crate::mv_wal::record::TableId;
