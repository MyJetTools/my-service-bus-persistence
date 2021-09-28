use std::collections::BTreeMap;

use my_service_bus_shared::{
    date_time::DateTimeAsMicroseconds, protobuf_models::MessageProtobufModel, MessageId,
};
use tokio::sync::Mutex;

use crate::{
    azure_storage::messages_page_blob::MessagesPageBlob, uncompressed_pages::UncompressedPage,
};

use super::{BlankPage, CompressedPage, MessagePageId, PageOperationError, MESSAGES_PER_PAGE};

pub struct MessagesPage {
    pub data: Mutex<MessagesPageData>,
    pub page_id: MessagePageId,
}

impl MessagesPage {
    pub fn restore(page_id: MessagePageId, data: MessagesPageData) -> Self {
        Self {
            data: Mutex::new(data),
            page_id,
        }
    }
    pub fn brand_new(page_id: MessagePageId) -> Self {
        Self {
            data: Mutex::new(MessagesPageData::NotInitialized(BlankPage::new())),
            page_id,
        }
    }

    pub async fn get_last_access(&self) -> DateTimeAsMicroseconds {
        let read_access = self.data.lock().await;
        return read_access.get_last_access();
    }

    pub async fn has_messages_to_save(&self) -> bool {
        let read_access = self.data.lock().await;
        return read_access.has_messages_to_save();
    }

    pub async fn get_messages_to_save_amount(&self) -> usize {
        let read_access = self.data.lock().await;
        return read_access.get_messages_to_save_amount();
    }

    pub async fn get_message(
        &self,
        message_id: MessageId,
    ) -> Result<Option<MessageProtobufModel>, PageOperationError> {
        let read_access = self.data.lock().await;
        return read_access.get_message(message_id);
    }

    pub async fn new_messages(&self, messages: Vec<MessageProtobufModel>) {
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
    }
}

pub enum MessagesPageData {
    NotInitialized(BlankPage),
    Uncompressed(UncompressedPage),
    Compressed(CompressedPage),
    Blank(BlankPage),
}

impl MessagesPageData {
    pub fn new_blank() -> Self {
        Self::Blank(BlankPage::new())
    }
    pub fn is_initialized(&self) -> bool {
        match self {
            MessagesPageData::NotInitialized(_) => false,
            _ => true,
        }
    }

    pub fn is_uncompressed(&self) -> bool {
        match self {
            MessagesPageData::Uncompressed(_) => true,
            _ => false,
        }
    }

    pub fn restored_uncompressed(
        page_id: MessagePageId,
        messages: BTreeMap<MessageId, MessageProtobufModel>,
        blob: MessagesPageBlob,
    ) -> Self {
        Self::Uncompressed(UncompressedPage::new(page_id, messages, blob))
    }

    pub fn resored_compressed(page_id: MessagePageId, zip_archive: Vec<u8>) -> Option<Self> {
        let result = CompressedPage::new(page_id, zip_archive)?;
        Some(Self::Compressed(result))
    }

    pub fn get_message(
        &self,
        message_id: MessageId,
    ) -> Result<Option<MessageProtobufModel>, PageOperationError> {
        match self {
            MessagesPageData::NotInitialized(_) => Err(PageOperationError::NotInitialized),
            MessagesPageData::Uncompressed(page) => {
                return Ok(page.get_and_clone_message(message_id));
            }
            MessagesPageData::Compressed(page) => Ok(page.get(message_id)?),
            MessagesPageData::Blank(_) => Ok(None),
        }
    }

    pub fn get_last_access(&self) -> DateTimeAsMicroseconds {
        match self {
            MessagesPageData::NotInitialized(page) => page.last_access,
            MessagesPageData::Uncompressed(page) => page.last_access,
            MessagesPageData::Compressed(page) => page.last_access,
            MessagesPageData::Blank(page) => page.last_access,
        }
    }

    pub fn has_messages_to_save(&self) -> bool {
        match self {
            MessagesPageData::NotInitialized(_) => false,
            MessagesPageData::Uncompressed(page) => page.queue_to_save.len() > 0,
            MessagesPageData::Compressed(_) => false,
            MessagesPageData::Blank(_) => false,
        }
    }

    pub fn get_messages_to_save_amount(&self) -> usize {
        match self {
            MessagesPageData::NotInitialized(_) => 0,
            MessagesPageData::Uncompressed(page) => page.queue_to_save.len(),
            MessagesPageData::Compressed(_) => 0,
            MessagesPageData::Blank(_) => 0,
        }
    }

    pub fn get_page_type(&self) -> &str {
        match self {
            MessagesPageData::NotInitialized(_) => "NotInitialized",
            MessagesPageData::Uncompressed(_) => "Uncompressed",
            MessagesPageData::Compressed(_) => "Compressed",
            MessagesPageData::Blank(_) => "Blank",
        }
    }

    pub async fn get_write_position(&self) -> usize {
        match self {
            MessagesPageData::NotInitialized(_) => 0,
            MessagesPageData::Uncompressed(page) => page.blob.get_wirte_position().await,
            MessagesPageData::Compressed(page) => page.zip_data.len(),
            MessagesPageData::Blank(_) => 0,
        }
    }

    pub fn percent(&self) -> usize {
        match self {
            MessagesPageData::NotInitialized(_) => 0,
            MessagesPageData::Uncompressed(page) => calc_page_precent(page.messages.len()),
            MessagesPageData::Compressed(page) => calc_page_precent(page.len),
            MessagesPageData::Blank(_) => 0,
        }
    }

    pub fn message_count(&self) -> usize {
        match self {
            MessagesPageData::NotInitialized(_) => 0,
            MessagesPageData::Uncompressed(page) => page.messages.len(),
            MessagesPageData::Compressed(page) => page.len,
            MessagesPageData::Blank(_) => 0,
        }
    }

    pub fn has_skipped_messages(&self) -> bool {
        match self {
            MessagesPageData::NotInitialized(_) => true,
            MessagesPageData::Uncompressed(page) => {
                has_skipped_messages(page.page_id, page.messages.len(), page.max_message_id)
            }
            MessagesPageData::Compressed(page) => {
                has_skipped_messages(page.page_id, page.len, page.max_message_id)
            }
            MessagesPageData::Blank(_) => true,
        }
    }
}

pub fn calc_page_precent(amount: usize) -> usize {
    amount / 1000
}

pub fn has_skipped_messages(
    page_id: MessagePageId,
    amount: usize,
    max_message_id: Option<MessageId>,
) -> bool {
    amount != should_have_amount(page_id, max_message_id)
}

fn should_have_amount(page_id: MessagePageId, max_message_id: Option<MessageId>) -> usize {
    if max_message_id.is_none() {
        return 0;
    }
    let max_message_id = max_message_id.unwrap();

    let first_message_id = page_id.value * MESSAGES_PER_PAGE;
    let result = max_message_id - first_message_id + 1;

    result as usize
}
