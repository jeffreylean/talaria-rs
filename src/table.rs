use anyhow::Result;
use arrow::{array::RecordBatch, datatypes::DataType};
use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use std::{collections::BTreeMap, sync::Arc};

#[async_trait]
pub trait Table {
    async fn name(&self) -> Result<String>;
    async fn schema(&self) -> Result<Arc<BTreeMap<String, DataType>>>;
}

#[async_trait]
pub trait Appender {
    async fn append(&mut self, batch: RecordBatch);
}
