use std::collections::BTreeMap;

use my_service_bus::shared::{protobuf_models::MessageProtobufModel, sub_page::SubPageId};

use crate::app::AppContext;

pub async fn new_messages(
    app: &AppContext,
    topic_id: String,
    messages_by_sub_page: BTreeMap<i64, Vec<MessageProtobufModel>>,
) {
    let topic_data = crate::operations::get_topic_data_to_write(app, topic_id.as_str()).await;
    for (sub_page_id, messages) in messages_by_sub_page {
        let sub_page_id = SubPageId::new(sub_page_id);
        let page = topic_data
            .get_sub_page_to_publish_messages(sub_page_id)
            .await;

        crate::operations::index_by_minute::new_messages(app, &topic_data, messages.as_slice())
            .await;

        page.new_messages(messages).await;
    }
}
