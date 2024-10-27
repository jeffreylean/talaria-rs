use std::sync::Arc;

use arrow::{array::RecordBatch, datatypes::Schema};

struct Record {
    schema: Arc<Schema>,
    data: RecordBatch,
    timestamp: i64,
}
