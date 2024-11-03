use std::{
    sync::{atomic::AtomicUsize, Arc},
    usize::{self, MAX},
};

use anyhow::Result;
use arrow::datatypes::{DataType, Field, Schema};
use bytes::Bytes;
use chrono::Utc;
use crossbeam_skiplist::SkipMap;
use serde::Deserialize;
use serde_yaml::Value;

#[derive(Debug)]
struct Record {
    data: Bytes,
}

#[derive(Deserialize)]
enum ColumnType {
    #[serde(alias = "string")]
    String,
    #[serde(alias = "int64")]
    Int64,
}

struct Memtable {
    id: usize,
    schema: Arc<Schema>,
    map: Arc<SkipMap<i64, Record>>,
    size: Arc<AtomicUsize>,
}

impl Memtable {
    pub fn new(id: usize, schema_src: String) -> Result<Self> {
        // Read the schema and construct Arrow Schema
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

        Ok(Self {
            id,
            map: Arc::new(SkipMap::new()),
            size: Arc::new(AtomicUsize::new(0)),
            schema: Arc::new(Schema::new(fields)),
        })
    }

    pub fn put(&self, data: &[u8]) -> Result<()> {
        let key = Utc::now().timestamp();
        let value = Record {
            data: Bytes::copy_from_slice(data),
        };

        self.map.insert(key, value);
        self.size.fetch_add(
            data.len() + usize::MAX,
            std::sync::atomic::Ordering::Relaxed,
        );
        Ok(())
    }

    pub fn get(&self, key: usize) -> Option<Record> {
        todo!();
    }
}
