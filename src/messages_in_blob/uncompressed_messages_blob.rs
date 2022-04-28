use my_azure_page_blob::MyAzurePageBlob;
use my_azure_page_blob_append::{AppendPageBlobSettings, PageBlobAppend, PageBlobAppendError};
use my_service_bus_shared::{page_id::PageId, protobuf_models::MessageProtobufModel};
use tokio::sync::Mutex;

const BLOB_AUTO_RESSIZE_IN_PAGES: usize = 16384;

pub enum LoadUncompressedPageError {
    PageBlobAppendError(PageBlobAppendError),
    FileIsCorrupted,
}

pub struct UncompressedMessagesBlob {
    pub topic_id: String,
    pub page_id: PageId,
    pub page_blob_append: Mutex<PageBlobAppend<MyAzurePageBlob>>,
}

impl UncompressedMessagesBlob {
    pub fn new(topic_id: String, page_id: PageId, blob: MyAzurePageBlob) -> Self {
        let append_page_blob_settings = AppendPageBlobSettings {
            max_payload_size_protection: 1024 * 1024 * 5,
            blob_auto_resize_in_pages: BLOB_AUTO_RESSIZE_IN_PAGES,
            cache_capacity_in_pages: 8000,
            max_pages_to_write_single_round_trip: 4000,
        };

        Self {
            topic_id,
            page_id,
            page_blob_append: Mutex::new(PageBlobAppend::new(blob, append_page_blob_settings)),
        }
    }

    pub async fn init(&self, auto_create_if_not_exists: bool) {
        let mut page_blob_append = self.page_blob_append.lock().await;
        let result = page_blob_append
            .initialize_to_read_mode(auto_create_if_not_exists)
            .await;

        match result {
            Ok(_) => {
                return;
            }
            Err(err) => {
                panic!(
                    "Init page blob. Autocreate is {}",
                    auto_create_if_not_exists
                );
            }
        }
    }

    pub async fn load(&self) -> Result<Vec<MessageProtobufModel>, LoadUncompressedPageError> {
        todo!("Implement");
        /*
        let mut result = Vec::new();

        let mut page_blob_append = self.page_blob_append.lock().await;

        while let Some(payload) = page_blob_append.get_next_payload().await? {
            let protobuf_result: Result<MessageProtobufModel, prost::DecodeError> =
                prost::Message::decode(payload.as_slice());

            match protobuf_result {
                Ok(protobuf_model) => {
                    result.push(protobuf_model);
                }
                Err(err) => {
                    println!("Can not decode payload. Stopping decoding and starting writing from that position. Reason {}", err);
                    page_blob_append.force_to_write_mode().await.unwrap();
                }
            }
        }

        Ok(result)
         */
    }

    pub async fn save_to_blob(&self, messages: &[MessageProtobufModel]) {
        todo!("Implement");
    }
}
