use crate::persistence_grpc::my_service_bus_messages_persistence_grpc_service_server::MyServiceBusMessagesPersistenceGrpcService;
use crate::persistence_grpc::*;
use crate::topics_snapshot::TopicSnapshotProtobufModel;

use my_grpc_extensions::{server::*, StreamedRequestReader, StreamedResponseWriter};
use my_service_bus::abstractions::MessageId;
use my_service_bus::shared::page_id::PageId;
use my_service_bus::shared::sub_page::SubPageId;

use super::contracts;

use super::server::MyServicePersistenceGrpc;

const MAX_PAYLOAD_SIZE: usize = 1024 * 1024 * 4;

#[tonic::async_trait]
impl MyServiceBusMessagesPersistenceGrpcService for MyServicePersistenceGrpc {
    generate_server_stream!(stream_name:"GetQueueSnapshotStream", item_name:"TopicAndQueuesSnapshotGrpcModel");
    async fn get_queue_snapshot(
        &self,
        _: tonic::Request<()>,
    ) -> Result<tonic::Response<Self::GetQueueSnapshotStream>, tonic::Status> {
        contracts::check_flags(self.app.as_ref())?;

        let result = self.app.topics_snapshot.get().await;

        my_grpc_extensions::grpc_server_streams::send_from_iterator(
            result.snapshot.data.into_iter(),
        )
        .await
    }

    async fn save_queue_snapshot(
        &self,
        request: tonic::Request<tonic::Streaming<TopicAndQueuesSnapshotGrpcModel>>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        contracts::check_flags(self.app.as_ref())?;

        let stream = request.into_inner();

        let values = StreamedRequestReader::new(stream);

        let snapshot: Vec<TopicSnapshotProtobufModel> = values.into_vec().await.unwrap();

        self.app.topics_snapshot.update(snapshot).await;

        Ok(tonic::Response::new(()))
    }

    async fn get_version(
        &self,
        _request: tonic::Request<()>,
    ) -> Result<tonic::Response<GetVersionGrpcResponse>, tonic::Status> {
        let result = GetVersionGrpcResponse {
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

    generate_server_stream!(stream_name:"GetPageCompressedStream", item_name:"CompressedMessageChunkModel");
    async fn get_page_compressed(
        &self,
        request: tonic::Request<crate::persistence_grpc::GetPageCompressedGrpcRequest>,
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

        my_grpc_extensions::grpc_server_streams::send_from_iterator(compressed.into_iter()).await
    }

    generate_server_stream!(stream_name:"GetPageStream", item_name:"MessageContentGrpcModel");
    async fn get_page(
        &self,
        request: tonic::Request<crate::persistence_grpc::GetPageGrpcRequest>,
    ) -> Result<tonic::Response<Self::GetPageStream>, tonic::Status> {
        contracts::check_flags(self.app.as_ref())?;

        let req = request.into_inner();

        let app = self.app.clone();

        let page_id = PageId::new(req.page_no);

        let mut from_message_id = page_id.get_first_message_id();
        let mut to_message_id = page_id.get_last_message_id();

        if req.from_message_id <= req.to_message_id {
            from_message_id = MessageId::new(req.from_message_id);
            to_message_id = MessageId::new(req.to_message_id);
        }

        let topic_id = req.topic_id;

        let mut streamed_response = StreamedResponseWriter::new(1024);

        let producer = streamed_response.get_stream_producer();

        tokio::spawn(crate::operations::send_messages_to_channel(
            app,
            topic_id,
            from_message_id,
            to_message_id,
            producer,
        ));

        streamed_response.get_result()
    }

    generate_server_stream!(stream_name:"GetSubPageStream", item_name:"MessageContentGrpcModel");
    async fn get_sub_page(
        &self,
        request: tonic::Request<GetSubPageGrpcRequest>,
    ) -> Result<tonic::Response<Self::GetSubPageStream>, tonic::Status> {
        contracts::check_flags(self.app.as_ref())?;

        let req = request.into_inner();

        let sub_page_id = SubPageId::new(req.sub_page_no);

        let from_message_id = sub_page_id.get_first_message_id();
        let to_message_id = sub_page_id.get_last_message_id();

        let mut streamed_response = StreamedResponseWriter::new(1024);

        let producer = streamed_response.get_stream_producer();

        tokio::spawn(crate::operations::send_messages_to_channel(
            self.app.clone(),
            req.topic_id,
            from_message_id,
            to_message_id,
            producer,
        ));

        streamed_response.get_result()
    }

    async fn save_messages(
        &self,
        request: tonic::Request<tonic::Streaming<SaveMessagesGrpcRequest>>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        contracts::check_flags(self.app.as_ref())?;

        let request = request.into_inner();

        let mut stream_reader = StreamedRequestReader::new(request);

        while let Some(message) = stream_reader.get_next().await {
            let message = message.unwrap();

            crate::operations::new_messages(
                &self.app,
                message.topic_id,
                message.messages.into_iter().map(|itm| itm.into()),
            )
            .await;
        }

        Ok(tonic::Response::new(()))
    }

    async fn delete_topic(
        &self,
        request: tonic::Request<DeleteTopicGrpcRequest>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        let request = request.into_inner();

        match crate::operations::delete_topic(
            self.app.as_ref(),
            &request.topic_id,
            request.delete_after.into(),
        )
        .await
        {
            Ok(_) => Ok(tonic::Response::new(())),
            Err(crate::operations::OperationError::TopicNotFound(_)) => {
                Err(tonic::Status::not_found("Topic not found"))
            }
            Err(err) => Err(tonic::Status::internal(format!("{:?}", err))),
        }
    }

    async fn restore_topic(
        &self,
        request: tonic::Request<RestoreTopicGrpcRequest>,
    ) -> Result<tonic::Response<RestoreTopicGrpcResponse>, tonic::Status> {
        let request = request.into_inner();

        let response = if let Some(restored) =
            crate::operations::restore_topic(self.app.as_ref(), &request.topic_id).await
        {
            RestoreTopicGrpcResponse {
                result: true,
                message_id: restored.message_id,
            }
        } else {
            RestoreTopicGrpcResponse {
                result: false,
                message_id: 0,
            }
        };

        return Ok(tonic::Response::new(response));
    }

    generate_server_stream!(stream_name:"GetHistoryByDateStream", item_name:"MessageContentGrpcModel");
    async fn get_history_by_date(
        &self,
        _request: tonic::Request<GetHistoryByDateGrpcRequest>,
    ) -> Result<tonic::Response<Self::GetHistoryByDateStream>, tonic::Status> {
        contracts::check_flags(self.app.as_ref())?;

        my_grpc_extensions::grpc_server_streams::send_from_iterator([].into_iter()).await
    }

    async fn ping(&self, _: tonic::Request<()>) -> Result<tonic::Response<()>, tonic::Status> {
        Ok(tonic::Response::new(()))
    }
}
