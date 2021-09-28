use crate::grpc::messages_persistence_mappers::MsgRange;
use crate::message_pages::MessagePageId;
use crate::persistence_grpc::my_service_bus_messages_persistence_grpc_service_server::MyServiceBusMessagesPersistenceGrpcService;
use crate::persistence_grpc::*;
use anyhow::*;
use futures_core::Stream;
use my_service_bus_shared::{page_compressor, protobuf_models::MessageProtobufModel};
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

        let topic_data = self.app.topics_data_list.get(&req.topic_id).await;

        if topic_data.is_none() {
            let result = super::messages_persistence_mappers::get_none_message();
            return Ok(tonic::Response::new(result));
        }

        let topic_data = topic_data.unwrap();

        let page_id = MessagePageId::from_message_id(req.message_id);

        let messages_page = topic_data.as_ref().get(page_id).await;

        if messages_page.is_none() {
            let result = super::messages_persistence_mappers::get_none_message();
            return Ok(tonic::Response::new(result));
        }

        let messages_page = messages_page.unwrap();

        let read_access = messages_page.data.lock().await;

        let message = read_access.get_message(req.message_id).unwrap();

        if message.is_none() {
            let result = super::messages_persistence_mappers::get_none_message();
            return Ok(tonic::Response::new(result));
        }

        let result = super::messages_persistence_mappers::to_message_grpc_contract(
            message.as_ref().unwrap(),
        );
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
            //TODO - Restore page before
            let data_by_topic = app.topics_data_list.get(&req.topic_id).await;
            if data_by_topic.is_none() {
                return;
            }

            let topic_data = data_by_topic.unwrap();

            let page_id = MessagePageId::new(req.page_no);

            let current_page_id = app
                .get_current_page_id(topic_data.topic_id.as_str())
                .await
                .unwrap();

            let page = crate::operations::pages::get_or_restore(
                app.clone(),
                topic_data.clone(),
                page_id,
                page_id.value >= current_page_id.value,
            )
            .await;

            let range = if req.from_message_id <= 0 && req.to_message_id <= 0 {
                Some(MsgRange {
                    msg_from: req.from_message_id,
                    msg_to: req.to_message_id,
                })
            } else {
                None
            };

            let max_payload_size = app.get_max_payload_size();

            let mut compressed_data = super::messages_persistence_mappers::get_compressed_page(
                page.as_ref(),
                max_payload_size,
                range,
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

        let unzipped = page_compressor::zip::decompress_payload(payload.as_slice()).unwrap();

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

        let topic_data = self
            .app
            .topics_data_list
            .get_or_create_data_by_topic(contract.topic_id.as_str(), app.clone())
            .await;

        for (page_id, messages) in messages_by_page {
            let page_id = MessagePageId::new(page_id);

            self.app
                .index_by_minute
                .new_messages(&contract.topic_id, messages.as_slice())
                .await;

            let page = crate::operations::pages::get_or_restore(
                app.clone(),
                topic_data.clone(),
                page_id,
                true,
            )
            .await;

            page.new_messages(messages).await;
        }

        return Ok(tonic::Response::new(()));
    }

    async fn delete_topic(
        &self,
        _: tonic::Request<DeleteTopicRequest>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        todo!("Not Implemented");

        //   return Ok(tonic::Response::new(()));
    }
}
