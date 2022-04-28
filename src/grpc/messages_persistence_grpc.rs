use crate::grpc::compressed_page_compiler::MsgRange;
use crate::message_pages::MessagePageId;
use crate::persistence_grpc::my_service_bus_messages_persistence_grpc_service_server::MyServiceBusMessagesPersistenceGrpcService;
use crate::persistence_grpc::*;

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

    async fn get_message(
        &self,
        request: tonic::Request<GetMessageGrpcRequest>,
    ) -> Result<tonic::Response<MessageContentGrpcModel>, tonic::Status> {
        todo!("Implement");
        /*
        contracts::check_flags(self.app.as_ref())?;


        let req = request.into_inner();

        let topic_data = self.app.topics_list.get(&req.topic_id).await;

        if topic_data.is_none() {
            let result = super::compressed_page_compiler::get_none_message();
            return Ok(tonic::Response::new(result));
        }

        let topic_data = topic_data.unwrap();

        let page_id = MessagePageId::from_message_id(req.message_id);

        let messages_page = topic_data.as_ref().get(page_id).await;

        if messages_page.is_none() {
            let result = super::compressed_page_compiler::get_none_message();
            return Ok(tonic::Response::new(result));
        }

        let messages_page = messages_page.unwrap();

        let read_access = messages_page.data.read().await;

        let message = read_access.get_message(req.message_id).unwrap();

        if message.is_none() {
            let result = super::compressed_page_compiler::get_none_message();
            return Ok(tonic::Response::new(result));
        }

        let result = super::messages_mappers::to_grpc::to_message(message.as_ref().unwrap());
        return Ok(tonic::Response::new(result));
         */
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
            //TODO - Restore page before
            let data_by_topic = app.topics_list.get(&req.topic_id).await;
            if data_by_topic.is_none() {
                return;
            }

            let topic_data = data_by_topic.unwrap();

            let page_id = MessagePageId::new(req.page_no);

            let current_page_id = app
                .get_current_page_id(topic_data.topic_id.as_str())
                .await
                .unwrap();

            let page = crate::operations::get_or_restore_page(
                app.as_ref(),
                topic_data.as_ref(),
                page_id.value,
            )
            .await;

            let range = if req.from_message_id <= 0 && req.to_message_id <= 0 {
                None
            } else {
                Some(MsgRange {
                    msg_from: req.from_message_id,
                    msg_to: req.to_message_id,
                })
            };

            let max_payload_size = app.get_max_payload_size();

            let zip_payload = if req.version == 1 {
                super::compressed_page_compiler::get_v1(
                    topic_data.topic_id.as_str(),
                    page.as_ref(),
                    max_payload_size,
                    range,
                )
                .await
                .unwrap()
            } else {
                super::compressed_page_compiler::get_v0(page.as_ref(), max_payload_size)
                    .await
                    .unwrap()
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

    async fn save_messages(
        &self,
        request: tonic::Request<
            tonic::Streaming<crate::persistence_grpc::CompressedMessageChunkModel>,
        >,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        contracts::check_flags(self.app.as_ref())?;

        let grpc_contract =
            super::messages_mappers::unzip_and_deserialize(&mut request.into_inner()).await?;

        let topic_data = crate::operations::get_or_create_topic_data(
            self.app.as_ref(),
            grpc_contract.topic_id.as_str(),
        )
        .await;

        for (page_id, messages) in grpc_contract.messages_by_page {
            crate::operations::new_messages(
                self.app.as_ref(),
                topic_data.as_ref(),
                page_id,
                messages,
            )
            .await
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
