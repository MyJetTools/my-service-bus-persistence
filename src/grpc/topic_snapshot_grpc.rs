use super::contracts;
use super::server::MyServicePersistenceGrpc;
use crate::persistence_grpc::my_service_bus_queue_persistence_grpc_service_server::MyServiceBusQueuePersistenceGrpcService;
use crate::persistence_grpc::*;

use futures_core::Stream;
use std::pin::Pin;
use tokio::sync::mpsc;
use tonic::{Response, Status};

#[tonic::async_trait]
impl MyServiceBusQueuePersistenceGrpcService for MyServicePersistenceGrpc {
    type GetSnapshotStream = Pin<
        Box<
            dyn Stream<Item = Result<TopicAndQueuesSnapshotGrpcModel, Status>>
                + Send
                + Sync
                + 'static,
        >,
    >;

    async fn get_snapshot(
        &self,
        _: tonic::Request<()>,
    ) -> Result<tonic::Response<Self::GetSnapshotStream>, tonic::Status> {
        contracts::check_flags(self.app.as_ref())?;

        let app = self.app.clone();

        let (tx, rx) = mpsc::channel(4);

        tokio::spawn(async move {
            let result = app.topics_snapshot.get().await;

            for topic_snapshot in &result.snapshot.data {
                let grpc_contract =
                    super::topic_snapshot_mappers::to_grpc::to_topic_snapshot(topic_snapshot);
                tx.send(Ok(grpc_contract)).await.unwrap();
            }
        });

        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        )))
    }

    async fn save_snapshot(
        &self,
        request: tonic::Request<SaveQueueSnapshotGrpcRequest>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        contracts::check_flags(self.app.as_ref())?;

        let grpc_contract = request.into_inner();
        let snapshot = super::topic_snapshot_mappers::to_domain::to_topics_data(&grpc_contract);

        self.app.topics_snapshot.update(snapshot).await;

        Ok(tonic::Response::new(()))
    }
}
