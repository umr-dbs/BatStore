use crate::mv_crud_model::crud_operation::TxAtomicOperation;
use crate::mv_datafusion::SqlContext;
use crate::mv_db::Database;
use crate::mv_root::index_root::RootIndexType;

#[test]
fn sql_queries_mvbt_snapshot_and_refreshes() {
    let db = Database::<16, 16, u64, u64>::new(
        RootIndexType::default(),
        |k| k.saturating_add(1),
        |k| k.saturating_sub(1),
        u64::MIN,
        u64::MAX,
    );
    let events = db.create_table("events");
    for (key, payload) in [(1, 10), (2, 20), (3, 30)] {
        events.dispatch_atomic_transaction(TxAtomicOperation::Insert(key, payload));
    }
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let sql = SqlContext::new();
        sql.refresh(&db).unwrap();
        let batches = sql
            .sql("SELECT SUM(payload) FROM events WHERE key >= 2")
            .await
            .unwrap();
        assert_eq!(
            datafusion::arrow::util::display::array_value_to_string(batches[0].column(0), 0)
                .unwrap(),
            "50"
        );
        events.dispatch_atomic_transaction(TxAtomicOperation::Insert(4, 40));
        let old = sql.sql("SELECT COUNT(*) FROM events").await.unwrap();
        assert_eq!(
            datafusion::arrow::util::display::array_value_to_string(old[0].column(0), 0).unwrap(),
            "3"
        );
        sql.refresh(&db).unwrap();
        let fresh = sql.sql("SELECT COUNT(*) FROM events").await.unwrap();
        assert_eq!(
            datafusion::arrow::util::display::array_value_to_string(fresh[0].column(0), 0).unwrap(),
            "4"
        );
    });
}
