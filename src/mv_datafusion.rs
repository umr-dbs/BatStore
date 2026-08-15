//! Read-only DataFusion SQL integration for the general-purpose u64 MVBT database.

use crate::mv_db::Database;
use crate::mv_query::interval::Interval;
use crate::mv_query::iter_query::RangeQueryIter;
use datafusion::arrow::array::{ArrayRef, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use std::sync::Arc;

pub const DEFAULT_BATCH_SIZE: usize = 8_192;

/// A DataFusion session populated from a consistent MVBT snapshot.
pub struct SqlContext {
    context: SessionContext,
    batch_size: usize,
}

impl SqlContext {
    pub fn new() -> Self {
        Self::with_batch_size(DEFAULT_BATCH_SIZE)
    }

    pub fn with_batch_size(batch_size: usize) -> Self {
        assert!(batch_size > 0, "DataFusion batch size must be non-zero");
        Self {
            context: SessionContext::new(),
            batch_size,
        }
    }

    pub fn session(&self) -> &SessionContext {
        &self.context
    }

    /// Register or refresh every table under its database name. All tables
    /// are read at one database version, producing a cross-table SQL snapshot.
    pub fn refresh<const FAN_OUT: usize, const NUM_RECORDS: usize>(
        &self,
        db: &Database<FAN_OUT, NUM_RECORDS, u64, u64>,
    ) -> Result<()> {
        // One registration protects the shared snapshot from GC for the
        // entire cross-table conversion (including error paths).
        let version = db.begin_snapshot();
        let result = (|| {
            for (id, name) in db.table_names().into_iter().enumerate() {
                let tree = db.table(id as _).expect("table name/id catalog mismatch");
                let iter = RangeQueryIter::new(
                    tree.as_ref(),
                    version,
                    Interval::new(u64::MIN, u64::MAX),
                    false,
                    db.worker_id(),
                );
                let mut batches = Vec::new();
                let mut keys = Vec::with_capacity(self.batch_size);
                let mut payloads = Vec::with_capacity(self.batch_size);
                for row in iter {
                    keys.push(row.key);
                    payloads.push(*row.payload);
                    if keys.len() == self.batch_size {
                        batches.push(make_batch(&mut keys, &mut payloads)?);
                    }
                }
                if !keys.is_empty() {
                    batches.push(make_batch(&mut keys, &mut payloads)?);
                }
                self.context.deregister_table(&name)?;
                self.context
                    .register_table(&name, Arc::new(MemTable::try_new(schema(), vec![batches])?))?;
            }
            Ok(())
        })();
        db.end_snapshot(version);
        result
    }

    pub async fn sql(&self, query: &str) -> Result<Vec<RecordBatch>> {
        self.context.sql(query).await?.collect().await
    }
}

impl Default for SqlContext {
    fn default() -> Self {
        Self::new()
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::UInt64, false),
        Field::new("payload", DataType::UInt64, false),
    ]))
}

fn make_batch(keys: &mut Vec<u64>, payloads: &mut Vec<u64>) -> Result<RecordBatch> {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(std::mem::take(keys))),
        Arc::new(UInt64Array::from(std::mem::take(payloads))),
    ];
    RecordBatch::try_new(schema(), columns).map_err(Into::into)
}
