use my_service_bus_shared::{protobuf_models::MessageProtobufModel, MessageId};
use tokio::sync::Mutex;

use super::{BlankPage, MessagePageId, MessagesPageData, PageMetrics, PageOperationError};

pub struct MessagesPage {
    pub data: Mutex<MessagesPageData>,
    pub page_id: MessagePageId,
    pub metrics: PageMetrics,
}

impl MessagesPage {
    pub fn brand_new(page_id: MessagePageId) -> Self {
        Self {
            data: Mutex::new(MessagesPageData::NotInitialized(BlankPage::new())),
            page_id,
            metrics: PageMetrics::new(),
        }
    }

    pub fn has_messages_to_save(&self) -> bool {
        self.metrics.get_messages_amount_to_save() > 0
    }

    pub async fn get_message(
        &self,
        message_id: MessageId,
    ) -> Result<Option<MessageProtobufModel>, PageOperationError> {
        self.metrics.update_last_access_to_now();
        let read_access = self.data.lock().await;
        return read_access.get_message(message_id);
    }

    pub async fn new_messages(&self, messages: Vec<MessageProtobufModel>) {
        {
            let mut write_access = self.data.lock().await;

            if let MessagesPageData::Uncompressed(page) = &mut *write_access {
                page.add(messages);
            } else {
                panic!(
                    "Can not add {} message[s] to the page {}. The type of the page is {}",
                    messages.len(),
                    self.page_id.value,
                    write_access.get_page_type()
                );
            }

            self.metrics
                .update_messages_amount_to_save(write_access.get_messages_to_save_amount())
        }

        self.metrics.update_last_access_to_now();
    }
}
