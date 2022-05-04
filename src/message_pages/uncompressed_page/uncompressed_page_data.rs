use std::{collections::BTreeMap, sync::Arc};

use my_service_bus_shared::{page_id::PageId, protobuf_models::MessageProtobufModel, MessageId};

use crate::{
    message_pages::MESSAGES_PER_PAGE,
    page_blob_random_access::{PageBlobPageId, PageBlobRandomAccess},
    uncompressed_page_storage::toc::{MessageContentOffset, UncompressedFileToc},
};

use super::{read_intervals_compiler::ReadIntervalsCompiler, PayloadsToUploadContainer};

pub struct MinMax {
    pub min: MessageId,
    pub max: MessageId,
}

impl MinMax {
    pub fn new(message_id: MessageId) -> Self {
        MinMax {
            min: message_id,
            max: message_id,
        }
    }
    pub fn update(&mut self, message_id: MessageId) {
        if self.min > message_id {
            self.min = message_id;
        }

        if self.max < message_id {
            self.max = message_id;
        }
    }
}

pub enum MessageStatus {
    Loaded(Arc<MessageProtobufModel>),
    Missing,
}

impl MessageStatus {
    pub fn get_message_content(&self) -> Option<Arc<MessageProtobufModel>> {
        match self {
            MessageStatus::Loaded(message) => Some(message.clone()),
            MessageStatus::Missing => None,
        }
    }
}

pub struct UncompressedPageData {
    pub page_id: PageId,
    pub toc: UncompressedFileToc,
    pub messages: BTreeMap<i64, MessageStatus>,
    pub min_max: Option<MinMax>,
    pub page_blob: PageBlobRandomAccess,
    pub max_message_size_protection: usize,
}

impl UncompressedPageData {
    pub async fn new(
        page_id: PageId,
        mut page_blob: PageBlobRandomAccess,
        max_message_size_protection: usize,
    ) -> Self {
        let toc = crate::uncompressed_page_storage::read_toc(&mut page_blob).await;

        let min_max = get_min_max_from_toc(page_id, &toc);

        Self {
            page_id,
            messages: BTreeMap::new(),
            min_max,
            toc,
            page_blob,
            max_message_size_protection,
        }
    }

    pub fn get_message_offset(&self, message_id: MessageId) -> MessageContentOffset {
        let file_no = message_id - self.page_id * MESSAGES_PER_PAGE;
        self.toc.get_position(file_no as usize)
    }

    fn update_min_max(&mut self, message_id: MessageId) {
        if let Some(ref mut min_max) = self.min_max {
            min_max.update(message_id);
        } else {
            self.min_max = Some(MinMax::new(message_id));
        }
    }

    pub fn add_message(&mut self, msg: Arc<MessageProtobufModel>) {
        self.update_min_max(msg.message_id);
        self.messages
            .insert(msg.message_id, MessageStatus::Loaded(msg));
    }

    async fn load_message(&mut self, message_id: MessageId) {
        let toc_offset = self.get_message_offset(message_id);

        if !toc_offset.has_data(self.max_message_size_protection) {
            return;
        }

        let result = self
            .page_blob
            .read_from_position(toc_offset.offset, toc_offset.size)
            .await;

        self.restore_message(message_id, result.as_slice());
    }

    fn restore_message(
        &mut self,
        message_id: MessageId,
        payload: &[u8],
    ) -> Option<Arc<MessageProtobufModel>> {
        //TODO - сделать проверку на мусор
        match prost::Message::decode(payload) {
            Ok(msg) => {
                let msg: Arc<MessageProtobufModel> = Arc::new(msg);
                self.add_message(msg.clone());
                return Some(msg);
            }
            Err(err) => {
                println!("Can not decode message{}", err);
                self.messages.insert(message_id, MessageStatus::Missing);
            }
        }

        None
    }

    pub async fn get(&mut self, message_id: MessageId) -> Option<Arc<MessageProtobufModel>> {
        if !self.messages.contains_key(&message_id) {
            self.load_message(message_id).await;
        }

        let result = self.messages.get(&message_id)?;
        result.get_message_content()
    }

    pub async fn persist_messages(
        &mut self,
        messages_to_persist: &[Arc<MessageProtobufModel>],
    ) -> (usize, MessageId) {
        let write_position = self.toc.get_write_position();

        let mut upload_container =
            PayloadsToUploadContainer::new(self.page_id, write_position, &mut self.toc);

        let mut last_saved_message_id = 0;

        for msg_to_persist in messages_to_persist {
            if msg_to_persist.message_id > last_saved_message_id {
                last_saved_message_id = msg_to_persist.message_id;
            }

            upload_container.append(msg_to_persist);
        }

        let payload_size = upload_container.payload.len();

        self.page_blob
            .write_at_position(write_position, upload_container.payload.as_slice(), 8092)
            .await;

        for interval in upload_container.toc_pages.intervals {
            let start_page = interval.from_id as usize;
            let pages_amount = (interval.to_id - interval.from_id + 1) as usize;

            let toc_pages_content = self.toc.get_toc_pages(start_page, pages_amount);

            self.page_blob
                .save_pages(&PageBlobPageId::new(start_page), toc_pages_content)
                .await;
        }

        (payload_size, last_saved_message_id)
    }

    pub async fn get_all(
        &mut self,
        current_message_id: Option<MessageId>,
    ) -> Vec<Arc<MessageProtobufModel>> {
        let start_message_id = self.page_id * MESSAGES_PER_PAGE;

        let mut to_message_id = start_message_id + MESSAGES_PER_PAGE - 1;

        if let Some(current_message_id) = current_message_id {
            if to_message_id > current_message_id {
                to_message_id = current_message_id;
            }
        }

        return self.get_range(start_message_id, to_message_id, None).await;
    }

    pub async fn get_range(
        &mut self,
        from_id: MessageId,
        to_id: MessageId,
        limit: Option<usize>,
    ) -> Vec<Arc<MessageProtobufModel>> {
        let mut read_intervals = ReadIntervalsCompiler::new();
        let mut result = Vec::new();

        let mut collected_messages_count = 0;

        for message_id in from_id..=to_id {
            if let Some(msg) = self.messages.get(&message_id) {
                match msg {
                    MessageStatus::Loaded(msg) => {
                        result.push(msg.clone());
                        collected_messages_count += 1;
                    }
                    MessageStatus::Missing => {}
                }
            } else {
                let message_offset = self.get_message_offset(message_id);

                if message_offset.has_data(self.max_message_size_protection) {
                    read_intervals.add_new_interval(message_id, message_offset);
                    collected_messages_count += 1;
                } else {
                    self.messages.insert(message_id, MessageStatus::Missing);
                }
            }

            if let Some(limit) = limit {
                if collected_messages_count >= limit {
                    break;
                }
            }
        }

        for interval in &read_intervals.intervals {
            let full_payload = self
                .page_blob
                .read_from_position(interval.start_pos, interval.len)
                .await;

            for msg_id in interval.messages.keys() {
                let message_payload = read_intervals.read_payload(*msg_id, full_payload.as_slice());

                if let Some(msg) = self.restore_message(*msg_id, message_payload) {
                    result.push(msg);
                }
            }
        }

        result
    }

    pub async fn read_from_message_id(
        &mut self,
        from_message_id: MessageId,
        max_amount: usize,
    ) -> Vec<Arc<MessageProtobufModel>> {
        let start_message_id = self.page_id * MESSAGES_PER_PAGE;

        let to_message_id = start_message_id + MESSAGES_PER_PAGE - 1;

        return self
            .get_range(from_message_id, to_message_id, Some(max_amount))
            .await;
    }
}

pub fn get_min_max_from_toc(page_id: PageId, toc: &UncompressedFileToc) -> Option<MinMax> {
    let mut result: Option<MinMax> = None;

    for file_no in 0..100_000 {
        if toc.has_content(file_no) {
            let message_id: MessageId = file_no as MessageId + page_id * MESSAGES_PER_PAGE;

            if let Some(ref mut min_max) = result {
                min_max.update(message_id);
            } else {
                result = Some(MinMax::new(message_id));
            }
        }
    }

    result
}

#[cfg(test)]
mod test {
    use my_azure_storage_sdk::{page_blob::AzurePageBlobStorage, AzureStorageConnection};
    use my_service_bus_shared::bcl::BclDateTime;
    use rust_extensions::date_time::DateTimeAsMicroseconds;

    use crate::{
        page_blob_random_access::PageBlobRandomAccess,
        uncompressed_page_storage::toc::{TOC_SIZE, TOC_SIZE_IN_PAGES},
    };

    use super::*;

    #[tokio::test]
    async fn test_init_new_blob_we_have_toc_initialized() {
        let connection = AzureStorageConnection::new_in_memory();

        let page_blob =
            AzurePageBlobStorage::new(Arc::new(connection), "test".to_string(), "test".to_string())
                .await;

        let page_blob_random_access = PageBlobRandomAccess::open_or_create(page_blob).await;

        let mut uncompressed_data =
            UncompressedPageData::new(1, page_blob_random_access, 1024 * 1024).await;

        let result = uncompressed_data.page_blob.download(None).await;

        assert_eq!(result.len(), TOC_SIZE);
    }

    #[tokio::test]
    async fn test_we_are_reading_saved_messages() {
        let connection = AzureStorageConnection::new_in_memory();

        let page_blob =
            AzurePageBlobStorage::new(Arc::new(connection), "test".to_string(), "test".to_string())
                .await;

        let page_blob_random_access = PageBlobRandomAccess::open_or_create(page_blob).await;

        let mut uncompressed_page_data =
            UncompressedPageData::new(1, page_blob_random_access, 1024 * 1024).await;

        let msg_to_upload0 = MessageProtobufModel {
            message_id: 100_001,
            created: Some(BclDateTime::from(DateTimeAsMicroseconds::now())),
            data: vec![1, 2, 3],
            headers: vec![],
        };

        let msg_to_upload1 = MessageProtobufModel {
            message_id: 100_002,
            created: Some(BclDateTime::from(DateTimeAsMicroseconds::now())),
            data: vec![1, 2, 3],
            headers: vec![],
        };

        let items_to_upload = vec![Arc::new(msg_to_upload0), Arc::new(msg_to_upload1)];

        uncompressed_page_data
            .persist_messages(&items_to_upload)
            .await;

        let result = uncompressed_page_data
            .page_blob
            .load_pages(&PageBlobPageId::new(TOC_SIZE_IN_PAGES), 1, None)
            .await;

        let mut uncompressed_page_data =
            UncompressedPageData::new(1, uncompressed_page_data.page_blob, 1024 * 1024).await;

        let result2 = uncompressed_page_data
            .page_blob
            .load_pages(&PageBlobPageId::new(TOC_SIZE_IN_PAGES), 1, None)
            .await;

        assert_eq!(result, result2);

        let result_message0 = uncompressed_page_data.get(100_001).await.unwrap();
        let result_message1 = uncompressed_page_data.get(100_002).await.unwrap();

        assert_eq!(result_message0.data, items_to_upload[0].data);
        assert_eq!(result_message1.data, items_to_upload[1].data);
    }
}
