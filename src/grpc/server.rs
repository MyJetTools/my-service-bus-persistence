use crate::app::AppContext;
use crate::persistence_grpc::my_service_bus_messages_persistence_grpc_service_server::MyServiceBusMessagesPersistenceGrpcServiceServer;
use crate::persistence_grpc::my_service_bus_queue_persistence_grpc_service_server::MyServiceBusQueuePersistenceGrpcServiceServer;
use anyhow::*;
use std::net::SocketAddr;
use std::sync::Arc;
use tonic::transport::Server;

#[derive(Clone)]
pub struct MyServicePersistenceGrpc {
    pub app: Arc<AppContext>,
}

impl MyServicePersistenceGrpc {
    pub fn new(app: Arc<AppContext>) -> Self {
        Self { app }
    }
}

pub async fn start(app: Arc<AppContext>, port: u16) -> Result<()> {
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let service = MyServicePersistenceGrpc::new(app);

    println!("Listening to {:?} as grpc endpoint", addr);
    Server::builder()
        .add_service(MyServiceBusQueuePersistenceGrpcServiceServer::new(
            service.clone(),
        ))
        .add_service(MyServiceBusMessagesPersistenceGrpcServiceServer::new(
            service,
        ))
        .serve(addr)
        .await
        .context("Server error")
}
