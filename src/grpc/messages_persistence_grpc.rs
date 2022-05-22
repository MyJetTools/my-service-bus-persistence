use crate::operations::read_page::{MessagesReader, ReadCondition};
use crate::persistence_grpc::my_service_bus_messages_persistence_grpc_service_server::MyServiceBusMessagesPersistenceGrpcService;
use crate::persistence_grpc::*;
use crate::uncompressed_page::{UncompressedPageId, MESSAGES_PER_PAGE};

use futures_core::Stream;
use std::pin::Pin;
use tokio::sync::mpsc;
use tonic::Status;

use super::contracts;

use super::server::MyServicePersistenceGrpc;

#[tonic::async_trait]
impl MyServiceBusMessagesPersistenceGrpcService for MyServicePersistenceGrpc {
    type GetPageCompressedStream = Pin<
        Box<dyn Stream<Item = Result<CompressedMessageChunkModel, Status>> + Send + Sync + 'static>,
    >;

    type GetPageStream = Pin<
        Box<dyn Stream<Item = Result<MessageContentGrpcModel, Status>> + Send + Sync + 'static>,
    >;

    async fn get_message(
        &self,
        request: tonic::Request<GetMessageGrpcRequest>,
    ) -> Result<tonic::Response<MessageContentGrpcModel>, tonic::Status> {
        contracts::check_flags(self.app.as_ref())?;

        let req = request.into_inner();

        let message =
            crate::operations::get_message_by_id(&self.app, req.topic_id.as_ref(), req.message_id)
                .await
                .unwrap();

        if message.is_none() {
            let result = super::compressed_page_compiler::get_none_message();
            return Ok(tonic::Response::new(result));
        }

        let result = super::messages_mappers::to_grpc::to_message(message.as_ref().unwrap());
        return Ok(tonic::Response::new(result));
    }

    async fn get_page_compressed(
        &self,
        request: tonic::Request<crate::persistence_grpc::GetMessagesPageGrpcRequest>,
    ) -> Result<tonic::Response<Self::GetPageCompressedStream>, tonic::Status> {
        contracts::check_flags(self.app.as_ref())?;

        let req = request.into_inner();

        let app = self.app.clone();

        let (tx, rx) = mpsc::channel(4);

        tokio::spawn(async move {
            let data_by_topic = app.topics_list.get(&req.topic_id).await;
            if data_by_topic.is_none() {
                return;
            }

            let topic_data = data_by_topic.unwrap();

            let (from_id, to_id) = if req.from_message_id <= 0 && req.to_message_id <= 0 {
                let from_message_id = req.page_no * MESSAGES_PER_PAGE as i64;
                let to_message_id = from_message_id + MESSAGES_PER_PAGE as i64 - 1;
                (from_message_id, to_message_id)
            } else {
                (req.from_message_id, req.to_message_id)
            };

            let max_payload_size = app.get_max_payload_size();

            let zip_payload = if req.version == 1 {
                super::compressed_page_compiler::get_v1(
                    app,
                    topic_data,
                    max_payload_size,
                    from_id,
                    to_id,
                )
                .await
            } else {
                super::compressed_page_compiler::get_v0(
                    app,
                    topic_data,
                    max_payload_size,
                    from_id,
                    to_id,
                )
                .await
            };

            for chunk in zip_payload {
                let grpc_contract = CompressedMessageChunkModel { chunk };
                tx.send(Ok(grpc_contract)).await.unwrap();
            }
        });

        Ok(tonic::Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        )))
    }

    async fn get_page(
        &self,
        request: tonic::Request<crate::persistence_grpc::GetMessagesPageGrpcRequest>,
    ) -> Result<tonic::Response<Self::GetPageStream>, tonic::Status> {
        contracts::check_flags(self.app.as_ref())?;

        let req = request.into_inner();

        let app = self.app.clone();

        let (tx, rx) = mpsc::channel(4);

        tokio::spawn(async move {
            let data_by_topic = app.topics_list.get(&req.topic_id).await;
            if data_by_topic.is_none() {
                return;
            }

            let topic_data = data_by_topic.unwrap();

            let read_condition = if req.from_message_id < req.to_message_id {
                ReadCondition::as_from_to(req.from_message_id, req.to_message_id)
            } else {
                let from_id = req.page_no * MESSAGES_PER_PAGE;
                let to_id = from_id + MESSAGES_PER_PAGE; //TODO - check if we have to include
                ReadCondition::as_from_to(from_id, to_id)
            };

            let mut messages_reader = MessagesReader::new(app, topic_data, read_condition);

            while let Some(msgs) = messages_reader.get_next_chunk().await {
                for msg in msgs {
                    let grpc_contract = Ok(super::messages_mappers::to_grpc::to_message(&msg));
                    tx.send(grpc_contract).await.unwrap();
                }
            }
        });

        Ok(tonic::Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        )))
    }

    async fn save_messages(
        &self,
        request: tonic::Request<
            tonic::Streaming<crate::persistence_grpc::CompressedMessageChunkModel>,
        >,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        contracts::check_flags(self.app.as_ref())?;

        let grpc_contract =
            super::messages_mappers::unzip_and_deserialize(&mut request.into_inner()).await?;

        let topic_data = crate::operations::get_topic_data_to_publish_messages(
            self.app.as_ref(),
            grpc_contract.topic_id.as_str(),
        )
        .await;

        for (page_id, messages) in grpc_contract.messages_by_page {
            let uncompressed_page_id = UncompressedPageId::new(page_id);
            crate::operations::new_messages(
                self.app.as_ref(),
                topic_data.as_ref(),
                &uncompressed_page_id,
                messages,
            )
            .await;
        }

        return Ok(tonic::Response::new(()));
    }
}
