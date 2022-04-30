use my_service_bus_shared::{page_id::PageId, protobuf_models::MessageProtobufModel};

use crate::{app::AppContext, topic_data::TopicData};

pub async fn new_messages(
    app: &AppContext,
    topic_data: &TopicData,
    page_id: PageId,
    messages: Vec<MessageProtobufModel>,
) {
    let page = crate::operations::get_page_to_publish_messages(app, topic_data, page_id).await;

    crate::operations::index_by_minute::new_messages(app, topic_data, messages.as_slice()).await;

    page.new_messages(messages).await;
}
