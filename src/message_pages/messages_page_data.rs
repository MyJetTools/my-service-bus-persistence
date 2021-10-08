use std::collections::BTreeMap;

use my_service_bus_shared::{protobuf_models::MessageProtobufModel, MessageId};

use crate::azure_storage::messages_page_blob::MessagesPageBlob;

use super::{
    BlankPage, CompressedPage, MessagePageId, PageMetrics, PageOperationError, UncompressedPage,
    MESSAGES_PER_PAGE,
};

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

    pub fn restored_from_corrupted(page_id: MessagePageId, blob: MessagesPageBlob) -> Self {
        Self::Uncompressed(UncompressedPage::new(page_id, BTreeMap::new(), blob))
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

    pub fn get_write_position(&self) -> usize {
        match self {
            MessagesPageData::NotInitialized(_) => 0,
            MessagesPageData::Uncompressed(page) => page.blob.get_write_position(),
            MessagesPageData::Compressed(page) => page.zip_data.len(),
            MessagesPageData::Blank(_) => 0,
        }
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

    pub fn percent(&self) -> usize {
        match self {
            MessagesPageData::NotInitialized(_) => 0,
            MessagesPageData::Uncompressed(page) => calc_page_precent(page.messages.len()),
            MessagesPageData::Compressed(page) => calc_page_precent(page.len),
            MessagesPageData::Blank(_) => 0,
        }
    }

    fn message_count(&self) -> usize {
        match self {
            MessagesPageData::NotInitialized(_) => 0,
            MessagesPageData::Uncompressed(page) => page.messages.len(),
            MessagesPageData::Compressed(page) => page.len,
            MessagesPageData::Blank(_) => 0,
        }
    }

    fn has_skipped_messages(&self) -> bool {
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

    pub fn update_metrics(&self, metrics: &PageMetrics) {
        metrics.update_blob_position(self.get_write_position());
        metrics.update_has_skipped_messages(self.has_skipped_messages());
        metrics.update_precent(self.percent());
        metrics.update_messages_count(self.message_count());
        metrics.update_messages_amount_to_save(self.get_messages_to_save_amount())
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
