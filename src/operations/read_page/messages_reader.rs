use std::sync::Arc;

use my_service_bus_shared::sub_page::{SubPage, SubPageId};
use my_service_bus_shared::{protobuf_models::MessageProtobufModel, MessageId};

use crate::app::AppContext;

use crate::{topic_data::TopicData, uncompressed_page::UncompressedPageId};

use super::{ReadCondition, ReadFromUncompressedPage};

pub struct MessagesReader {
    app: Arc<AppContext>,
    topic_data: Arc<TopicData>,
    current_uncompressed_page: Option<ReadFromUncompressedPage>,
    read_condition: ReadCondition,
    current_message_id: MessageId,
    read_messages_amount: usize,
}

impl MessagesReader {
    pub fn new(
        app: Arc<AppContext>,
        topic_data: Arc<TopicData>,
        read_condition: ReadCondition,
    ) -> Self {
        Self {
            app,
            current_message_id: read_condition.get_from_message_id(),
            read_condition,
            current_uncompressed_page: None,

            topic_data,
            read_messages_amount: 0,
        }
    }

    pub async fn get_message(&mut self) -> Option<MessageProtobufModel> {
        let uncompressed_page_id = UncompressedPageId::from_message_id(self.current_message_id);

        self.read_subpage_if_needed(uncompressed_page_id).await;

        let (sub_page, message_id, _) = self.get_next_sub_page_with_messages().await?;

        sub_page.get_message(message_id).await
    }

    async fn read_subpage_if_needed(&mut self, uncompressed_page_id: UncompressedPageId) {
        let update_uncompressed_page =
            if let Some(current_uncompressed_page) = &self.current_uncompressed_page {
                current_uncompressed_page.page_id.value != uncompressed_page_id.value
            } else {
                true
            };

        if update_uncompressed_page {
            let uncompressed_page = if let Some(page) = self
                .topic_data
                .uncompressed_pages_list
                .get(&uncompressed_page_id)
                .await
            {
                Some(page)
            } else {
                crate::operations::restore_uncompressed_page::open_if_exists(
                    self.app.as_ref(),
                    self.topic_data.as_ref(),
                    &uncompressed_page_id,
                )
                .await
            };

            self.current_uncompressed_page = Some(ReadFromUncompressedPage::new(
                uncompressed_page,
                uncompressed_page_id,
            ));
        }
    }

    pub async fn get_next_sub_page_with_messages(
        &mut self,
    ) -> Option<(Arc<SubPage>, MessageId, MessageId)> {
        loop {
            if self
                .read_condition
                .we_reached_the_end(self.current_message_id, self.read_messages_amount)
            {
                return None;
            }

            let uncompressed_page_id = UncompressedPageId::from_message_id(self.current_message_id);

            self.read_subpage_if_needed(uncompressed_page_id).await;

            let sub_page_id = SubPageId::from_message_id(self.current_message_id);

            if let Some(current_uncompressed_page) = &self.current_uncompressed_page {
                if let Some(sub_page) = current_uncompressed_page
                    .get_sub_page(self.topic_data.as_ref(), &sub_page_id)
                    .await
                {
                    let from_message_id = self.current_message_id;
                    let to_message_id = sub_page_id.get_first_message_id_of_next_page() - 1;
                    self.current_message_id = to_message_id + 1;
                    return Some((sub_page, from_message_id, to_message_id));
                }
            }

            self.current_message_id = sub_page_id.get_first_message_id_of_next_page();
        }
    }

    //TODO - Unit Test It
    pub async fn get_next_chunk(&mut self) -> Option<Vec<MessageProtobufModel>> {
        loop {
            let (sub_page, from_message_id, to_message_id) =
                self.get_next_sub_page_with_messages().await?;

            let result = sub_page.get_messages(from_message_id, to_message_id).await;

            if result.is_some() {
                return result;
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use my_service_bus_shared::protobuf_models::*;
    use rust_extensions::date_time::DateTimeAsMicroseconds;

    use crate::{
        app::AppContext, operations::read_page::ReadCondition, settings::SettingsModel,
        uncompressed_page::UncompressedPageId,
    };

    use super::MessagesReader;

    #[tokio::test]
    async fn test_empty_blobs() {
        const TOPIC_ID: &str = "test";

        let settings_model = SettingsModel::create_for_test_environment();

        let app = Arc::new(AppContext::new(settings_model).await);

        app.topics_list
            .update_message_id_or_create(TOPIC_ID, 1)
            .await;

        let topic_data =
            crate::operations::get_topic_data_to_publish_messages(app.as_ref(), TOPIC_ID).await;

        let read_condition = ReadCondition::SingleMessage(1);
        let mut topic_data = MessagesReader::new(app, topic_data, read_condition);

        let result = topic_data.get_message().await;

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_we_have_exact_message_in_uncompressed_page_but_not_in_storage() {
        const TOPIC_ID: &str = "test";
        let settings_model = SettingsModel::create_for_test_environment();
        let app = Arc::new(AppContext::new(settings_model).await);

        app.topics_list
            .update_message_id_or_create(TOPIC_ID, 1)
            .await;

        let topic_data =
            crate::operations::get_topic_data_to_publish_messages(app.as_ref(), TOPIC_ID).await;

        let new_messages = vec![MessageProtobufModel {
            message_id: 1,
            created: DateTimeAsMicroseconds::now().unix_microseconds,
            data: vec![0u8, 1u8, 2u8],
            headers: vec![],
        }];

        let page_id = UncompressedPageId::new(0);

        crate::operations::new_messages(app.as_ref(), topic_data.as_ref(), &page_id, new_messages)
            .await;

        let read_condition = ReadCondition::SingleMessage(1);
        let mut messages_reader = MessagesReader::new(app, topic_data, read_condition);

        let result = messages_reader.get_message().await;

        assert!(result.is_some());
    }

    #[tokio::test]
    async fn test_we_have_exact_message_in_storage_only() {
        const TOPIC_ID: &str = "test";

        let settings_model = SettingsModel::create_for_test_environment();
        let app = Arc::new(AppContext::new(settings_model).await);

        app.topics_list
            .update_message_id_or_create(TOPIC_ID, 1)
            .await;

        let topic_data =
            crate::operations::get_topic_data_to_publish_messages(app.as_ref(), TOPIC_ID).await;

        let new_messages = vec![MessageProtobufModel {
            message_id: 1,
            created: DateTimeAsMicroseconds::now().unix_microseconds,
            data: vec![0u8, 1u8, 2u8],
            headers: vec![],
        }];

        let page_id = UncompressedPageId::new(0);

        let page = crate::operations::new_messages(
            app.as_ref(),
            topic_data.as_ref(),
            &page_id,
            new_messages,
        )
        .await;

        page.flush_to_storage(1024 * 1024).await;

        //Let's Assume we GC our page
        topic_data.uncompressed_pages_list.remove_page(0).await;

        let read_condition = ReadCondition::SingleMessage(1);
        let mut messages_reader = MessagesReader::new(app, topic_data, read_condition);

        let result = messages_reader.get_message().await;

        assert!(result.is_some());
    }
}
