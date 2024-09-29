use talaria_rs::{ingress_server::Ingress, IngestRequest, IngestResponse};
use tonic::{Request, Response, Status};

pub mod talaria_rs {
    tonic::include_proto!("talaria_rs");
}

pub struct Server;

impl Ingress for Server {
    async fn ingest(
        &self,
        _request: Request<IngestRequest>,
    ) -> Result<Response<IngestResponse>, Status> {
        todo!()
    }
}

impl Server {
    async fn start_grpc(&self) -> anyhow::Result<()> {
        // Start a grpc server.

        Ok(())
    }
}
