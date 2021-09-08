use crate::compression;
use crate::message_pages::MessagePageId;
use crate::messages_protobuf::MessageProtobufModel;
use crate::persistence_grpc::my_service_bus_messages_persistence_grpc_service_server::MyServiceBusMessagesPersistenceGrpcService;
use crate::persistence_grpc::*;
use anyhow::*;
use futures_core::Stream;
use std::collections::HashMap;
use std::pin::Pin;
use tokio::sync::mpsc;
use tonic::Status;

use super::contracts;
use super::contracts::NewMessagesProtobufContract;
use super::server::MyServicePersistenceGrpc;

#[tonic::async_trait]
impl MyServiceBusMessagesPersistenceGrpcService for MyServicePersistenceGrpc {
    type GetPageCompressedStream = Pin<
        Box<dyn Stream<Item = Result<CompressedMessageChunkModel, Status>> + Send + Sync + 'static>,
    >;

    async fn get_message(
        &self,
        request: tonic::Request<GetMessageGrpcRequest>,
    ) -> Result<tonic::Response<MessageContentGrpcModel>, tonic::Status> {
        contracts::check_flags(self.app.as_ref())?;

        let req = request.into_inner();

        let pages_cache = self.app.get_data_by_topic(&req.topic_id).await;

        if pages_cache.is_none() {
            let result = super::messages_persistence_mappers::get_none_message();
            return Ok(tonic::Response::new(result));
        }

        let pages_cache = pages_cache.unwrap();

        let page_id = MessagePageId::from_message_id(req.message_id);

        let data_by_topic = pages_cache.as_ref().get(page_id).await;

        let read_access = data_by_topic.data.read().await;

        let read_access = read_access.get(0).unwrap();

        let message = read_access.messages.get(&req.message_id);

        if message.is_none() {
            let result = super::messages_persistence_mappers::get_none_message();
            return Ok(tonic::Response::new(result));
        }

        let message = message.unwrap();

        let result = super::messages_persistence_mappers::to_message_grpc_contract(message);
        return Ok(tonic::Response::new(result));
    }

    async fn get_page_compressed(
        &self,
        request: tonic::Request<crate::persistence_grpc::GetMessagesPageGrpcRequest>,
    ) -> Result<tonic::Response<Self::GetPageCompressedStream>, tonic::Status> {
        contracts::check_flags(self.app.as_ref())?;

        let req = request.into_inner();
        println!("{:?}", req);

        let app = self.app.clone();

        let (tx, rx) = mpsc::channel(4);

        tokio::spawn(async move {
            let data_by_topic = app.get_data_by_topic(&req.topic_id).await;
            if data_by_topic.is_none() {
                return;
            }

            let data_by_topic = data_by_topic.unwrap();

            let page_no = MessagePageId::new(req.page_no);

            let data_by_topic = data_by_topic.get(page_no).await;

            let mut compressed_data = super::messages_persistence_mappers::get_compressed_page(
                &data_by_topic,
                app.get_max_payload_size(),
                req.from_message_id,
                req.to_message_id,
            )
            .await
            .unwrap();

            for chunk in compressed_data.drain(..) {
                let grpc_contract = CompressedMessageChunkModel { chunk };
                tx.send(Ok(grpc_contract)).await.unwrap();
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

        let mut payload: Vec<u8> = Vec::new();

        let mut req = request.into_inner();

        loop {
            let next = req.message().await.unwrap();

            if next.is_none() {
                break;
            }

            let next = next.unwrap();
            payload.extend(next.chunk);
        }

        let unzipped = compression::zip::decompress_payload(payload.as_slice()).unwrap();

        let mut contract = NewMessagesProtobufContract::parse(unzipped.as_slice());

        let mut messages_by_page: HashMap<i64, Vec<MessageProtobufModel>> = HashMap::new();

        for msg in contract.messages.drain(..) {
            let page_id = MessagePageId::from_message_id(msg.message_id);

            if !messages_by_page.contains_key(&page_id.value) {
                messages_by_page.insert(page_id.value, Vec::new());
            }

            let messages = messages_by_page.get_mut(&page_id.value).unwrap();

            messages.push(msg);
        }

        let app = self.app.clone();

        let data_by_topic = self
            .app
            .get_or_create_data_by_topic(&contract.topic_id, app)
            .await;

        for (page_id, messages) in messages_by_page {
            let page_id = MessagePageId::new(page_id);

            self.app
                .index_by_minute
                .new_messages(&contract.topic_id, messages.as_slice())
                .await;

            let data_by_topic = data_by_topic
                .get_restore_or_create_uncompressed_only(page_id)
                .await
                .unwrap();

            data_by_topic.new_messages(messages).await;
        }

        return Ok(tonic::Response::new(()));
    }

    async fn delete_topic(
        &self,
        request: tonic::Request<DeleteTopicRequest>
    ) -> Result<tonic::Response<()>, tonic::Status> {

        let req = request.into_inner();

        if req.topic_id != self.app.settings.delete_topic_secret_key {
            
            return Ok(tonic::Response::new(()));
        }

        let topic = self.app.delete_topic(req.topic_id).await;

        self.app.logs
        .add_info_string(
            None,
            "TopicDelete",
            format!("Topic deleted. Id: {}, Message Id: {}, Not used: {}", topic.topic_id, topic.message_id, topic.not_used),
        )
        .await;

        return Ok(tonic::Response::new(()));
    }
}
