use std::{collections::BTreeMap, sync::Arc};

use my_service_bus_shared::{page_id::PageId, protobuf_models::MessageProtobufModel, MessageId};

use crate::{
    page_blob_random_access::{PageBlobPageId, PageBlobRandomAccess},
    sub_page::{SubPage, SubPageId},
    toc::*,
};

use super::{toc::*, utils::MESSAGES_PER_PAGE, PayloadsToUploadContainer};

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

pub struct UncompressedPageData {
    pub page_id: PageId,
    pub toc: FileToc,
    pub sub_pages: BTreeMap<usize, Arc<SubPage>>,
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
        let toc = FileToc::read_toc(
            &mut page_blob,
            TOC_SIZE_IN_PAGES,
            TOC_SIZE,
            MESSAGES_PER_PAGE as usize,
        )
        .await;

        let min_max = get_min_max_from_toc(page_id, &toc);

        Self {
            page_id,
            sub_pages: BTreeMap::new(),
            min_max,
            toc,
            page_blob,
            max_message_size_protection,
        }
    }

    pub fn get_message_offset(&self, message_id: MessageId) -> ContentOffset {
        let payload_no = PayloadNo::from_uncompressed_message_id(message_id, self.page_id);
        self.toc.get_position(&payload_no)
    }

    fn update_min_max(&mut self, message_id: MessageId) {
        if let Some(ref mut min_max) = self.min_max {
            min_max.update(message_id);
        } else {
            self.min_max = Some(MinMax::new(message_id));
        }
    }

    pub async fn add_message(&mut self, msg: MessageProtobufModel) {
        self.update_min_max(msg.message_id);

        let sub_page_id = SubPageId::from_message_id(msg.message_id);

        if !self.sub_pages.contains_key(&sub_page_id.value) {
            self.sub_pages
                .insert(sub_page_id.value, Arc::new(SubPage::new(sub_page_id)));
        }

        self.sub_pages
            .get(&sub_page_id.value)
            .unwrap()
            .add_message(msg)
            .await;
    }

    async fn load_message(&mut self, message_id: MessageId) {
        let toc_offset = self.get_message_offset(message_id);

        if !toc_offset.has_data(self.max_message_size_protection) {
            return;
        }

        let sub_page_id = SubPageId::from_message_id(message_id);

        if toc_offset.last_position() > self.page_blob.get_blob_size(None).await {
            if !self.sub_pages.contains_key(&sub_page_id.value) {
                self.sub_pages
                    .insert(sub_page_id.value, Arc::new(SubPage::new(sub_page_id)));
            }
            self.sub_pages
                .get(&sub_page_id.value)
                .unwrap()
                .add_missing(message_id)
                .await;
            return;
        }

        let result = self
            .page_blob
            .read_from_position(toc_offset.offset, toc_offset.size)
            .await;

        self.restore_message(message_id, result.as_slice()).await;
    }

    async fn restore_message(
        &mut self,
        message_id: MessageId,
        payload: &[u8],
    ) -> Option<Arc<SubPage>> {
        let sub_page_id = SubPageId::from_message_id(message_id);

        if !self.sub_pages.contains_key(&sub_page_id.value) {
            self.sub_pages
                .insert(sub_page_id.value, Arc::new(SubPage::new(sub_page_id)));
        }

        let sub_page = self.sub_pages.get(&sub_page_id.value).unwrap();
        match prost::Message::decode(payload) {
            Ok(msg) => {
                sub_page.add_message(msg).await;
            }
            Err(err) => {
                println!("Can not decode message{}", err);
                sub_page.add_missing(message_id).await;
            }
        }

        return Some(sub_page.clone());
    }

    pub async fn persist_messages(
        &mut self,
        messages_to_persist: &[Arc<MessageProtobufModel>],
    ) -> (usize, MessageId) {
        let (payload_size, last_saved_message_id) = {
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

            println!("Payload Size: {}", payload_size);

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
        };

        self.toc.increase_write_position(payload_size);

        (payload_size, last_saved_message_id)
    }
}

pub fn get_min_max_from_toc(page_id: PageId, toc: &FileToc) -> Option<MinMax> {
    let mut result: Option<MinMax> = None;

    for file_no in 0..100_000 {
        if toc.has_content(&PayloadNo::new(file_no)) {
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
        uncompressed_page::toc::{TOC_SIZE, TOC_SIZE_IN_PAGES},
    };

    use super::*;

    #[tokio::test]
    async fn test_init_new_blob_we_have_toc_initialized() {
        let connection = AzureStorageConnection::new_in_memory();

        let page_blob =
            AzurePageBlobStorage::new(Arc::new(connection), "test".to_string(), "test".to_string())
                .await;

        let page_blob_random_access =
            PageBlobRandomAccess::open_or_create(page_blob, 1024 * 1024).await;

        let mut uncompressed_data =
            UncompressedPageData::new(1, page_blob_random_access, 1024 * 1024).await;

        let result = uncompressed_data.page_blob.download(None).await;

        assert_eq!(result.len(), TOC_SIZE);
    }

    #[tokio::test]
    async fn test_we_are_reading_saved_messages() {
        let message_id1 = 100_001;
        let message_id2 = 100_002;

        let connection = AzureStorageConnection::new_in_memory();

        let page_blob =
            AzurePageBlobStorage::new(Arc::new(connection), "test".to_string(), "test".to_string())
                .await;

        let page_blob_random_access =
            PageBlobRandomAccess::open_or_create(page_blob, 1024 * 1024).await;

        let mut uncompressed_page_data =
            UncompressedPageData::new(1, page_blob_random_access, 1024 * 1024).await;

        let msg_to_upload0 = MessageProtobufModel {
            message_id: message_id1,
            created: Some(BclDateTime::from(DateTimeAsMicroseconds::now())),
            data: vec![1, 2, 3],
            headers: vec![],
        };

        let msg_to_upload1 = MessageProtobufModel {
            message_id: message_id2,
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

        let sub_page_id = SubPageId::from_message_id(message_id1);
        let sub_page = uncompressed_page_data
            .sub_pages
            .get(&sub_page_id.value)
            .unwrap();

        let result_message = sub_page.get_message(message_id1).await.unwrap();

        assert_eq!(result_message.data, items_to_upload[0].data);

        let sub_page_id = SubPageId::from_message_id(message_id2);
        let sub_page = uncompressed_page_data
            .sub_pages
            .get(&sub_page_id.value)
            .unwrap();

        let result_message = sub_page.get_message(message_id2).await.unwrap();

        assert_eq!(result_message.data, items_to_upload[1].data);
    }

    fn get_value(src: &[u8]) -> i32 {
        let mut result = [0u8; 4];

        result.copy_from_slice(src);

        i32::from_le_bytes(result)
    }

    #[tokio::test]
    async fn test_persist() {
        let connection = AzureStorageConnection::new_in_memory();

        let page_blob =
            AzurePageBlobStorage::new(Arc::new(connection), "test".to_string(), "test".to_string())
                .await;

        let page_blob_random_access = PageBlobRandomAccess::open_or_create(page_blob, 1024).await;

        let mut uncompressed_page_data =
            UncompressedPageData::new(0, page_blob_random_access, 1024 * 1024).await;

        let mut contents = Vec::new();

        let mut message_id = 0;

        for _ in 0..10 {
            let mut items_to_upload = Vec::new();

            for _ in 0..10000 {
                let msg_to_upload = MessageProtobufModel {
                    message_id,
                    created: Some(BclDateTime::from(DateTimeAsMicroseconds::now())),
                    data: format!("CONTENT: {:20}", message_id).into_bytes(),
                    headers: vec![],
                };

                message_id += 1;

                let mut content = Vec::new();

                prost::Message::encode(&msg_to_upload, &mut content).unwrap();

                contents.push(content);

                items_to_upload.push(Arc::new(msg_to_upload));
            }

            uncompressed_page_data
                .persist_messages(&items_to_upload)
                .await;
        }

        let result = uncompressed_page_data.page_blob.download(None).await;

        let mut pos: usize = 0;

        let mut result_offset = TOC_SIZE;

        for message_id in 0..100_000 {
            let offset = get_value(&result[pos..pos + 4]) as usize;
            pos += 4;
            assert_eq!(offset, result_offset);
            let size = get_value(&result[pos..pos + 4]) as usize;
            assert_eq!(size, contents[message_id].len());
            pos += 4;
            result_offset += size;

            assert_eq!(
                contents[message_id].as_slice(),
                &result[offset..offset + size]
            );
        }

        println!("len:{}", result.len());
    }
}
