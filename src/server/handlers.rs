use crate::talaria_rs::{ingress_server::Ingress, IngestRequest, IngestResponse};
use tonic::{Request, Response, Status};

pub struct Server;

#[tonic::async_trait]
impl Ingress for Server {
    async fn ingest(
        &self,
        request: Request<IngestRequest>,
    ) -> Result<Response<IngestResponse>, Status> {
        println!("HELLO");
        Ok(Response::new(IngestResponse {}))
    }
}
