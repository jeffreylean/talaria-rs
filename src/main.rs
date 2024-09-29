use server::handlers::Server as TalariaServer;
use talaria_proto::FILE_DESCRIPTOR_SET;
use talaria_rs::ingress_server::IngressServer;
use tonic::transport::Server;

mod server;
mod talaria_rs;

mod talaria_proto {
    include!("talaria_rs.rs");

    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("talaria_descriptor");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "127.0.0.1:9001".parse().expect("ERROR: error parsing addr");

    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
        .build_v1()
        .unwrap();

    let _ = Server::builder()
        .add_service(reflection_service)
        .add_service(IngressServer::new(TalariaServer {}))
        .serve(addr)
        .await;

    Ok(())
}
