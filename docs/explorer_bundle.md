# Multi-table Explorer bundles

Build with `--features tree-viz` and export a `bat_db::Database` at a quiescent point:

```rust
use crate::bat_db::database::DumpColumn;

let schemas = vec![
    vec![
        DumpColumn { name: "key".into(), data_type: "integer".into() },
        DumpColumn { name: "balance".into(), data_type: "decimal".into() },
    ],
    vec![
        DumpColumn { name: "key".into(), data_type: "integer".into() },
        DumpColumn { name: "amount".into(), data_type: "decimal".into() },
    ],
];
db.dump_explorer_bundle("database.json", &schemas, |table_id, payload| {
    // Encode the columns for this table's application-defined payload variant.
    let mut row = serde_json::Map::new();
    let column = if table_id == accounts_id { "balance" } else { "amount" };
    row.insert(column.into(), serde_json::json!(payload.to_string()));
    row
})?;
```

Supply one column list per table, in creation order. The exporter adds each row's `key` automatically; a `key` column in the schema declares its display type. The row encoder should return the other named columns. BatStore's general-purpose `Database` has one application-defined payload type and no column catalog, so its caller provides that metadata.

The JSON bundle has `format: "batstore-explorer-bundle-v1"`, `snapshot_version`, `glc_next`, `max_worker_id`, and a `tables` array. Each table contains its ID, name, columns, rows visible to one shared snapshot, and its complete structural `tree` dump. The file contains actual row values, unlike a single-tree dump. Handle it according to the data's sensitivity.

Open the self-contained `tools/BatStore-Explorer.html`. **Table overview** shows a table card list, accepts a `ts_start` from zero through the last dump GLC timestamp, filters a column, and pages through matching records, initially 10 at a time. It displays creator and deletion worker IDs and `ts_start` values. Invalid inserts from aborted transactions appear as amber **PHANTOM** records when **Show phantoms** is enabled; they are excluded from transaction reads. Use **Add table dumps** to bring several single-tree JSON files into the same workspace; a database bundle already contains its named tables. Payload columns are available at the exported snapshot; earlier snapshots show keys and record stamps. **Tree view** inspects the selected table's index and marks invalid entries there too. **Transaction animation** starts with **New transaction**, then connects any number of compact read, scan, insert, update, and delete blocks between Begin and End. Select an existing worker or add a new worker as the previous maximum plus a positive offset. A read-only replay uses the chosen `ts_start`; adding a write locks it to the simulation GLC plus one. The simulation clock is shown separately from the dump clock and advances for the write start and successful commit. Each operation's result appears in sequence, including reads of earlier writes in the same transaction. This is an offline model: it does not perform engine conflict checks, update the database, or change the dump. Separate single-tree files may have independent clocks, so cross-file transaction timing is illustrative.

The dashboard's **Advanced** mode shows one GLC value and worker activity cards. Worker cards group IDs with recorded writes, read-only replays, and no observed activity. Complete shared commit history supplies per-worker write commit counts; retained entries of the selected index supply observed record writes and deletes. Read-only counts and average operations per transaction come from replays in the current Explorer session, because historical reads and operation counts are not stored in the file. Returning to the dashboard for the same loaded table reuses its rendered view and refreshes the GLC and replay activity without recalculating tree statistics.

The transaction builder shows the simulation GLC as a logical clock face. It starts in **Auto-commit** mode for one operation; adding a second operation selects **Explicit transaction** mode. A single operation can also be kept explicit by selecting that mode. Each operation block has its own color and can be changed between read, scan, insert, update, and delete in place. Read and scan blocks have a snapshot badge; selecting it opens a per-worker Progress Table with `LCB(worker, ts_start)`, the last commit strictly before that snapshot timestamp. Successful simulated writes add their commit timestamp to this table. Older files without complete commit history show unknown or partial entries rather than claiming exact visibility.

The structural exporter reads pages without concurrent modification protection. Stop writers while calling it. With `tree-viz` enabled, new dumps include the GLC position, full per-worker commit history, worker limit, and invalid-stamp flags. The feature keeps a debug-only commit archive even when the runtime commit log prunes entries. An older dump such as `tools/out.json` lacks that history and invalid-stamp flags, so Explorer labels its historical visibility and inferred clock bound as approximate and cannot identify phantom entries. It still supports key queries and transaction replay, but has no payload values.
