use std::sync::Arc;

use my_service_bus::abstractions::MessageId;
use my_service_bus::shared::sub_page::SubPageId;

use crate::{app::AppContext, persistence_grpc::MessageContentGrpcModel};

pub async fn send_messages_to_channel(
    app: Arc<AppContext>,
    topic_id: String,
    from_message_id: MessageId,
    to_message_id: MessageId,
    tx: tokio::sync::mpsc::Sender<Result<MessageContentGrpcModel, tonic::Status>>,
    send_timeout: std::time::Duration,
) {
    let mut sub_page_read_copy = None;

    for message_id in from_message_id.get_value()..to_message_id.get_value() + 1 {
        let message_id: MessageId = message_id.into();

        let sub_page_id: SubPageId = message_id.into();

        if sub_page_read_copy.is_none() {
            let sub_page =
                crate::operations::get_sub_page_to_read(&app, &topic_id, sub_page_id).await;

            sub_page_read_copy = Some(sub_page.get_all_messages().await);
        }

        if sub_page_read_copy.as_ref().unwrap().sub_page_id.get_value() != sub_page_id.get_value() {
            let sub_page =
                crate::operations::get_sub_page_to_read(&app, &topic_id, sub_page_id).await;

            sub_page_read_copy = Some(sub_page.get_all_messages().await);
        }

        let message = sub_page_read_copy.as_ref().unwrap().get(message_id);

        if let Some(message) = message {
            let future = tx.send(Ok(message.into()));

            match tokio::time::timeout(send_timeout, future).await {
                Ok(_) => {}
                Err(_) => {
                    panic!("Timeout while sending message to channel at send_messages_to_channel");
                }
            }
        }
    }
}
