use my_azure_page_blob::*;
use my_azure_page_blob_append::{PageBlobAppendCache, PageBlobAppendCacheError};
use my_service_bus_shared::protobuf_models::MessageProtobufModel;

pub struct MessagesStream {
    pub append_cache: PageBlobAppendCache,
}

impl MessagesStream {
    pub fn new(
        capacity_in_pages: usize,
        blob_auto_resize_in_pages: usize,
        max_page_size_protection: usize,
    ) -> Self {
        let append_cache = PageBlobAppendCache::new(
            capacity_in_pages,
            blob_auto_resize_in_pages,
            max_page_size_protection,
            true,
        );

        Self { append_cache }
    }

    pub async fn get_next_message<T: MyPageBlob>(
        &mut self,
        page_blob: &mut T,
    ) -> Result<Option<MessageProtobufModel>, PageBlobAppendCacheError> {
        let payload_result = self.append_cache.get_next_payload(page_blob).await?;

        let result = match payload_result {
            Some(payload) => {
                let result: MessageProtobufModel =
                    prost::Message::decode(payload.as_slice()).unwrap();
                Some(result)
            }

            None => None,
        };

        return Ok(result);
    }

    pub async fn append<T: MyPageBlob>(
        &mut self,
        page_blob: &mut T,
        messages: &[MessageProtobufModel],
    ) -> Result<(), PageBlobAppendCacheError> {
        let mut pages_to_append = Vec::new();

        for message in messages {
            let mut payload: Vec<u8> = Vec::new();
            prost::Message::encode(message, &mut payload).unwrap();
            pages_to_append.push(payload);
        }

        self.append_cache
            .append_and_write(page_blob, &pages_to_append)
            .await
    }

    pub async fn get_write_position(&self) -> usize {
        self.append_cache.blob_position
    }
}
