use crate::talaria_rs::{ingress_server::Ingress, IngestRequest, IngestResponse};
use arrow::array::AsArray;
use arrow::datatypes::{DataType, Field, Int64Type, Schema};
use arrow::json::ReaderBuilder;
use serde::Serialize;
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub struct Server;

#[derive(Serialize)]
pub struct Data {
    id: i64,
    event_name: String,
}

#[tonic::async_trait]
impl Ingress for Server {
    async fn ingest(
        &self,
        request: Request<IngestRequest>,
    ) -> Result<Response<IngestResponse>, Status> {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("event_name", DataType::Utf8, false)]
        );
        let rows = vec![Data { id: 1, event_name: "event1".to_string() }];
        let mut decoder = ReaderBuilder::new(Arc::new(schema)).build_decoder().unwrap();
        decoder.serialize(&rows).unwrap();

        if let Some(batch) = decoder.flush().unwrap() {
            println!("{:?}", batch.column(0).as_primitive::<Int64Type>().values())
        }

        Ok(Response::new(IngestResponse {}))
    }
}
