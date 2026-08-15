# DataFusion SQL integration

The optional `datafusion` feature exposes the general-purpose
`Database<..., u64, u64>` tables to Apache DataFusion. Each MVBT table appears
under its existing database name with this schema:

| column | Arrow / SQL type | nullable |
|---|---|---|
| `key` | `UInt64` | no |
| `payload` | `UInt64` | no |

## Design

`SqlContext::refresh` chooses one database version and scans every table at
that version using the native lazy `RangeQueryIter`. Rows are converted in
8,192-row Arrow batches and registered with DataFusion. SQL execution then
uses DataFusion's vectorized operators directly on those immutable batches.

This gives SQL a consistent, read-only snapshot and keeps it isolated from
concurrent OLTP writes. It necessarily performs one row-to-Arrow conversion
per refresh: MVBT records use their page-native layout while DataFusion
requires Arrow's columnar layout. There is no FFI, IPC, or serialization
boundary. A snapshot remains stable until explicitly refreshed.

The feature is optional because DataFusion and Arrow add substantial compile
time and binary size. Existing benchmark builds are unchanged.

## Use

Enable the feature:

```bash
cargo build --features datafusion
```

Inside a Tokio runtime, attach SQL to an existing database and query it:

```rust
use crate::mv_datafusion::SqlContext;

let sql = SqlContext::new();
sql.refresh(&db)?;

let batches = sql.sql(
    "SELECT key, payload FROM events WHERE key >= 100 ORDER BY key LIMIT 20"
).await?;

datafusion::arrow::util::pretty::print_batches(&batches)?;
```

Joins and aggregates work across registered tables:

```sql
SELECT a.key, a.payload + b.payload AS total
FROM accounts a
JOIN adjustments b USING (key)
WHERE a.payload > 0;
```

Writes made after `refresh` are deliberately not visible. Call it again to
replace the registered tables with a new consistent snapshot:

```rust
sql.refresh(&db)?;
```

For direct access to the DataFusion API (DataFrames, UDF registration,
configuration, or `EXPLAIN`), use `sql.session()`.

The batch size can be tuned before registration:

```rust
let sql = SqlContext::with_batch_size(32_768);
```

Larger batches slightly reduce per-batch overhead; smaller batches reduce
temporary allocation peaks. The default is a balanced starting point.

## Current scope

- Read-only SQL; writes continue through MVBT transactions.
- The adapter currently targets the core `u64` key / `u64` payload database.
- Refresh materializes a full snapshot. This is well suited to repeated
  analytical queries over a stable snapshot, but not to refreshing before
  every point lookup.
- DataFusion receives immutable Arrow batches, so SQL queries do not pin an
  MVBT reader snapshot after `refresh` returns and do not interfere with GC.

Run the end-to-end test with:

```bash
cargo test --features datafusion datafusion_sql_tests
```
