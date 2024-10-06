use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use datafusion::{
    catalog::{SchemaProvider, TableProvider},
    error::DataFusionError,
};

pub struct MemorySchemaProvider {
    tables: DashMap<Arc<str>, Arc<dyn TableProvider>>,
}

#[async_trait]
impl SchemaProvider for MemorySchemaProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.tables
            .iter()
            .map(|table| table.key().to_string())
            .collect()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        Ok(self.tables.get(name).map(|table| table.value().clone()))
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> datafusion::error::Result<Option<Arc<dyn TableProvider>>> {
        // Check if table exists
        if self.table_exist(name.as_str()) {
            return Err(DataFusionError::Execution(format!(
                "Table {name} already exist!"
            )));
        }
        Ok(self.tables.insert(name.into(), table))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}
