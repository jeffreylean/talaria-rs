use std::sync::Arc;

use arrow::{
    array::RecordBatch,
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use async_trait::async_trait;
use datafusion::{catalog::TableProvider, physical_plan::ExecutionPlan};
use serde::Deserialize;
use serde_yaml::Value;

use crate::table::Appender;

#[derive(Debug)]
pub struct Table {
    name: Arc<str>,
    schema: Schema,
    storage: Vec<RecordBatch>,
}

#[derive(Deserialize)]
enum ColumnType {
    #[serde(alias = "string")]
    String,
    #[serde(alias = "int64")]
    Int64,
}

pub fn new(schema_src: String, name: String) -> anyhow::Result<Table> {
    let f = std::fs::File::open(schema_src)?;
    let val: Value = serde_yaml::from_reader(f)?;
    let mut fields: Vec<Field> = Vec::new();
    if let Some(map) = val.as_mapping() {
        for (k, v) in map {
            if let Value::String(type_str) = v {
                let column_type: ColumnType = serde_yaml::from_str(type_str)?;
                let data_type = match column_type {
                    ColumnType::String => DataType::Utf8,
                    ColumnType::Int64 => DataType::Int64,
                };
                if let Some(key) = k.as_str() {
                    fields.push(Field::new(key.to_string(), data_type, false));
                }
            }
        }
    }
    Ok(Table {
        name: name.into(),
        schema: Schema::new(fields),
        storage: Vec::new(),
    })
}

#[async_trait]
impl Appender for Table {
    async fn append(&mut self, batch: RecordBatch) {
        self.storage.push(batch);
    }
}

#[cfg(test)]
mod test {
    use arrow::datatypes::{DataType, Field};

    use super::new;

    #[test]
    fn test_new() {
        let table = new(
            "/Users/jeffreylean/Project/personal/talaria-rs/etc/test.yaml".to_string(),
            "test".to_string(),
        )
        .unwrap();
        assert_eq!(
            table.schema.flattened_fields(),
            vec![
                &Field::new("string1", DataType::Utf8, false),
                &Field::new("int1", DataType::Int64, false)
            ]
        );
    }
}
