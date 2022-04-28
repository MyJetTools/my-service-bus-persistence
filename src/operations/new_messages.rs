use my_service_bus_shared::{page_id::PageId, protobuf_models::MessageProtobufModel};

use crate::{app::AppContext, topic_data::TopicData};

pub async fn new_messages(
    app: &AppContext,
    topic_data: &TopicData,
    page_id: PageId,
    messages: Vec<MessageProtobufModel>,
) {
    let page = topic_data
        .pages_list
        .get_or_create_uncompressed(page_id)
        .await;

    crate::operations::index_by_minute::new_messages(
        app,
        topic_data.topic_id.as_str(),
        messages.as_slice(),
    );

    page.new_messages(messages).await;
}
