use std::sync::{atomic::AtomicUsize, Arc};

use anyhow::Result;
use arrow::{
    array::RecordBatch,
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use async_trait::async_trait;
use crossbeam_skiplist::{SkipList, SkipMap};
use datafusion::{catalog::TableProvider, physical_plan::ExecutionPlan};
use serde::Deserialize;
use serde_yaml::Value;

use crate::table::Appender;
use bytes::Bytes;

#[derive(Debug)]
pub struct Table {
    id: usize,
    name: Arc<str>,
    schema: Schema,
    map: Arc<SkipMap<Bytes, Bytes>>,
    approximate_size: Arc<AtomicUsize>,
}

#[derive(Deserialize)]
enum ColumnType {
    #[serde(alias = "string")]
    String,
    #[serde(alias = "int64")]
    Int64,
}

pub fn new(id: usize, schema_src: String, name: String) -> anyhow::Result<Table> {
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
        map: Arc::new(SkipMap::new()),
        id,
        approximate_size: Arc::new(AtomicUsize::new(0)),
    })
}

impl Table {
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.map
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
        self.approximate_size.fetch_add(
            key.len() + value.len(),
            std::sync::atomic::Ordering::Relaxed,
        );
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use arrow::datatypes::{DataType, Field};

    use super::new;

    #[test]
    fn test_new() {
        let table = new(
            1,
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
