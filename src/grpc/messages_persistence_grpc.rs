use crate::grpc::compressed_page_compiler::MsgRange;
use crate::persistence_grpc::my_service_bus_messages_persistence_grpc_service_server::MyServiceBusMessagesPersistenceGrpcService;
use crate::persistence_grpc::*;

use futures_core::Stream;
use my_service_bus_abstractions::AsMessageId;
use my_service_bus_shared::page_id::AsPageId;
use rust_extensions::date_time::DateTimeAsMicroseconds;
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

    async fn get_version(
        &self,
        _request: tonic::Request<()>,
    ) -> Result<tonic::Response<MyServerBusPersistenceVersion>, tonic::Status> {
        let result = MyServerBusPersistenceVersion {
            version: crate::app::APP_VERSION.to_string(),
        };

        return Ok(tonic::Response::new(result));
    }

    async fn get_message(
        &self,
        request: tonic::Request<GetMessageGrpcRequest>,
    ) -> Result<tonic::Response<MessageContentGrpcModel>, tonic::Status> {
        contracts::check_flags(self.app.as_ref())?;

        let req = request.into_inner();

        let message = crate::operations::get_message_by_id(
            self.app.as_ref(),
            req.topic_id.as_ref(),
            req.message_id.into(),
        )
        .await
        .unwrap();

        let result = match message {
            Some(msg) => msg.as_ref().into(),
            None => MessageContentGrpcModel {
                created: DateTimeAsMicroseconds::now().unix_microseconds,
                data: Vec::new(),
                meta_data: Vec::new(),
                message_id: -1,
            },
        };

        return Ok(tonic::Response::new(result));
    }

    async fn get_page_compressed(
        &self,
        request: tonic::Request<crate::persistence_grpc::GetMessagesPageGrpcRequest>,
    ) -> Result<tonic::Response<Self::GetPageCompressedStream>, tonic::Status> {
        contracts::check_flags(self.app.as_ref())?;

        let req = request.into_inner();

        let app = self.app.clone();
        let grpc_timeout = self.app.grpc_timeout;
        let (tx, rx) = mpsc::channel(4);

        tokio::spawn(async move {
            let current_message_id = app
                .topics_snapshot
                .get_current_message_id(req.topic_id.as_ref())
                .await
                .unwrap();

            let data_by_topic = app.topics_list.get(&req.topic_id).await;
            if data_by_topic.is_none() {
                return;
            }

            let topic_data = data_by_topic.unwrap();

            let page = crate::operations::get_page_to_read(
                app.as_ref(),
                topic_data.as_ref(),
                req.page_no.as_page_id(),
            )
            .await;

            let range = if req.from_message_id <= 0 && req.to_message_id <= 0 {
                None
            } else {
                Some(MsgRange {
                    msg_from: req.from_message_id.as_message_id(),
                    msg_to: req.to_message_id.as_message_id(),
                })
            };

            let max_payload_size = app.get_max_payload_size();

            let zip_payload = if req.version == 1 {
                super::compressed_page_compiler::get_v1(
                    topic_data.topic_id.as_str(),
                    &page,
                    max_payload_size,
                    range,
                    current_message_id,
                )
                .await
                .unwrap()
            } else {
                super::compressed_page_compiler::get_v0(&page, max_payload_size, current_message_id)
                    .await
                    .unwrap()
            };

            for chunk in zip_payload {
                let grpc_contract = CompressedMessageChunkModel { chunk };

                let future = tx.send(Ok(grpc_contract));

                tokio::time::timeout(grpc_timeout, future)
                    .await
                    .unwrap()
                    .unwrap();
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
        let grpc_timeout = self.app.grpc_timeout;

        let (tx, rx) = mpsc::channel(4);

        tokio::spawn(async move {
            let current_message_id = app
                .topics_snapshot
                .get_current_message_id(req.topic_id.as_ref())
                .await
                .unwrap();

            let data_by_topic = app.topics_list.get(&req.topic_id).await;
            if data_by_topic.is_none() {
                return;
            }

            let topic_data = data_by_topic.unwrap();

            let page = crate::operations::get_page_to_read(
                app.as_ref(),
                topic_data.as_ref(),
                req.page_no.as_page_id(),
            )
            .await;

            if req.from_message_id >= 0 && req.to_message_id >= 0 {
                for msg in page
                    .get_range(
                        req.from_message_id.as_message_id(),
                        req.to_message_id.as_message_id(),
                    )
                    .await
                {
                    let grpc_contract = Ok(msg.as_ref().into());
                    let future = tx.send(grpc_contract);

                    tokio::time::timeout(grpc_timeout, future)
                        .await
                        .unwrap()
                        .unwrap();
                }
            } else {
                for msg in page.get_all(Some(current_message_id)).await {
                    let grpc_contract = Ok(msg.as_ref().into());
                    let future = tx.send(grpc_contract);

                    tokio::time::timeout(grpc_timeout, future)
                        .await
                        .unwrap()
                        .unwrap();
                }
            };
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

        let grpc_contract = super::messages_mappers::unzip_and_deserialize(
            &mut request.into_inner(),
            self.app.grpc_timeout,
        )
        .await?;

        let topic_data = crate::operations::get_topic_data_to_publish_messages(
            self.app.as_ref(),
            grpc_contract.topic_id.as_str(),
        )
        .await;

        for (page_id, messages) in grpc_contract.messages_by_page {
            crate::operations::new_messages(
                self.app.as_ref(),
                topic_data.as_ref(),
                page_id.as_page_id(),
                messages,
            )
            .await
        }

        return Ok(tonic::Response::new(()));
    }

    async fn save_messages_uncompressed(
        &self,
        request: tonic::Request<
            tonic::Streaming<crate::persistence_grpc::UnCompressedMessageChunkModel>,
        >,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        contracts::check_flags(self.app.as_ref())?;

        let grpc_contract = super::messages_mappers::deserialize_uncompressed(
            &mut request.into_inner(),
            self.app.grpc_timeout,
        )
        .await?;

        let topic_data = crate::operations::get_topic_data_to_publish_messages(
            self.app.as_ref(),
            grpc_contract.topic_id.as_str(),
        )
        .await;

        for (page_id, messages) in grpc_contract.messages_by_page {
            crate::operations::new_messages(
                self.app.as_ref(),
                topic_data.as_ref(),
                page_id.as_page_id(),
                messages,
            )
            .await
        }

        return Ok(tonic::Response::new(()));
    }

    async fn delete_topic(
        &self,
        request: tonic::Request<DeleteTopicGrpcRequest>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        let request = request.into_inner();

        crate::operations::delete_topic(
            self.app.as_ref(),
            &request.topic_id,
            request.delete_after.into(),
        )
        .await;
        return Ok(tonic::Response::new(()));
    }
}
