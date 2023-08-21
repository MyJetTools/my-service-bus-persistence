use crate::persistence_grpc::my_service_bus_messages_persistence_grpc_service_server::MyServiceBusMessagesPersistenceGrpcService;
use crate::persistence_grpc::*;

use futures_core::Stream;
use my_service_bus_abstractions::MessageId;
use my_service_bus_shared::page_id::PageId;
use my_service_bus_shared::sub_page::SubPageId;

use std::pin::Pin;
use std::time::Duration;
use tonic::Status;

use super::contracts;

use super::server::MyServicePersistenceGrpc;

const GRPC_TIMEOUT: Duration = Duration::from_secs(3);

const CHANNEL_SIZE: usize = 100;

const MAX_PAYLOAD_SIZE: usize = 1024 * 1024 * 4;

#[tonic::async_trait]
impl MyServiceBusMessagesPersistenceGrpcService for MyServicePersistenceGrpc {
    type GetPageCompressedStream = Pin<
        Box<dyn Stream<Item = Result<CompressedMessageChunkModel, Status>> + Send + Sync + 'static>,
    >;

    type GetPageStream = Pin<
        Box<dyn Stream<Item = Result<MessageContentGrpcModel, Status>> + Send + Sync + 'static>,
    >;

    type GetSubPageStream = Pin<
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
                created: 0,
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

        let page_id = PageId::new(req.page_no);

        let mut from_message_id = page_id.get_first_message_id();
        let mut to_message_id = page_id.get_last_message_id();

        if req.from_message_id > 0 && req.to_message_id > 0 {
            from_message_id = MessageId::new(req.from_message_id);
            to_message_id = MessageId::new(req.to_message_id);
        }

        let compressed = crate::operations::compressed_page_compiler::get_compressed_page(
            app,
            &req.topic_id,
            from_message_id,
            to_message_id,
            req.version == 0,
            MAX_PAYLOAD_SIZE,
        )
        .await;

        my_grpc_extensions::grpc_server::send_vec_to_stream(compressed, |chunk| {
            CompressedMessageChunkModel { chunk }
        })
        .await
    }

    async fn get_page(
        &self,
        request: tonic::Request<crate::persistence_grpc::GetMessagesPageGrpcRequest>,
    ) -> Result<tonic::Response<Self::GetPageStream>, tonic::Status> {
        contracts::check_flags(self.app.as_ref())?;

        let req = request.into_inner();

        let app = self.app.clone();

        let (tx, rx) = tokio::sync::mpsc::channel(CHANNEL_SIZE);

        let page_id = PageId::new(req.page_no);

        let mut from_message_id = page_id.get_first_message_id();
        let mut to_message_id = page_id.get_last_message_id();

        if req.from_message_id <= req.to_message_id {
            from_message_id = MessageId::new(req.from_message_id);
            to_message_id = MessageId::new(req.to_message_id);
        }

        let topic_id = req.topic_id;

        tokio::spawn(async move {
            crate::operations::send_messages_to_channel(
                app,
                topic_id,
                from_message_id,
                to_message_id,
                tx,
                GRPC_TIMEOUT,
            )
            .await;
        });

        Ok(tonic::Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        )))
    }

    async fn get_sub_page(
        &self,
        request: tonic::Request<crate::persistence_grpc::GetSubPageRequest>,
    ) -> Result<tonic::Response<Self::GetSubPageStream>, tonic::Status> {
        contracts::check_flags(self.app.as_ref())?;

        let req = request.into_inner();

        let app = self.app.clone();

        let sub_page_id = SubPageId::new(req.sub_page_no);

        let from_message_id = sub_page_id.get_first_message_id();
        let to_message_id = sub_page_id.get_last_message_id();

        let topic_id = req.topic_id;

        let (tx, rx) = tokio::sync::mpsc::channel(CHANNEL_SIZE);

        tokio::spawn(async move {
            crate::operations::send_messages_to_channel(
                app,
                topic_id,
                from_message_id,
                to_message_id,
                tx,
                GRPC_TIMEOUT,
            )
            .await;
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
            super::messages_mappers::unzip_and_deserialize(&mut request.into_inner(), GRPC_TIMEOUT)
                .await?;

        crate::operations::new_messages(
            &self.app,
            grpc_contract.topic_id,
            grpc_contract.messages_by_sub_page,
        )
        .await;

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
            GRPC_TIMEOUT,
        )
        .await?;

        crate::operations::new_messages(
            &self.app,
            grpc_contract.topic_id,
            grpc_contract.messages_by_sub_page,
        )
        .await;
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

    async fn restore_topic(
        &self,
        request: tonic::Request<RestoreTopicRequest>,
    ) -> Result<tonic::Response<RestoreTopicResponse>, tonic::Status> {
        let request = request.into_inner();

        let response = if let Some(restored) =
            crate::operations::restore_topic(self.app.as_ref(), &request.topic_id).await
        {
            RestoreTopicResponse {
                result: true,
                message_id: restored.message_id,
            }
        } else {
            RestoreTopicResponse {
                result: false,
                message_id: 0,
            }
        };

        return Ok(tonic::Response::new(response));
    }
}
