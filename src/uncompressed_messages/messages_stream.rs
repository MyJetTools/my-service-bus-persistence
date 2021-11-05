use my_azure_page_blob::*;
use my_azure_page_blob_append::{PageBlobAppend, PageBlobAppendError};
use my_service_bus_shared::protobuf_models::MessageProtobufModel;

use super::ReadingUncompressedMessagesError;

pub struct MessagesStream<TMyPageBlob: MyPageBlob> {
    pub page_blob_append: PageBlobAppend<TMyPageBlob>,
}

impl<TMyPageBlob: MyPageBlob> MessagesStream<TMyPageBlob> {
    pub fn new(
        page_blob: TMyPageBlob,
        cache_capacity_in_pages: usize,
        blob_auto_resize_in_pages: usize,
        max_payload_size_protection: u32,
    ) -> Self {
        let settings = my_azure_page_blob_append::AppendPageBlobSettings {
            blob_auto_resize_in_pages,
            cache_capacity_in_pages,
            max_pages_to_write_single_round_trip: 8000,
            max_payload_size_protection,
        };

        let page_blob_append = PageBlobAppend::new(page_blob, settings);

        Self { page_blob_append }
    }

    pub async fn get_next_message(
        &mut self,
    ) -> Result<Option<MessageProtobufModel>, ReadingUncompressedMessagesError> {
        let pos = self.page_blob_append.get_blob_position();
        let getting_payload_result = self.page_blob_append.get_next_payload().await?;

        match getting_payload_result {
            Some(payload) => {
                let payload_size = payload.len();

                let result: Result<MessageProtobufModel, prost::DecodeError> =
                    prost::Message::decode(payload.as_slice());

                match result {
                    Ok(model) => {
                        return Ok(Some(model));
                    }
                    Err(err) => {
                        let page_blob = self.page_blob_append.get_page_blob();

                        return Err(ReadingUncompressedMessagesError::CorruptedContent{
                                pos,
                                reason:format!(
                                    "[{}/{}]Can not decode message at position: {} with size {}. Skipping it Err: {:?}",
                                    page_blob.get_container_name(),
                                    page_blob.get_blob_name(),
                                    pos, payload_size, err)
                             },
                            );
                    }
                };
            }

            None => return Ok(None),
        };
    }

    pub async fn append(
        &mut self,

        messages: &[MessageProtobufModel],
    ) -> Result<(), PageBlobAppendError> {
        let mut pages_to_append = Vec::new();

        for message in messages {
            let mut payload: Vec<u8> = Vec::new();
            prost::Message::encode(message, &mut payload).unwrap();
            pages_to_append.push(payload);
        }

        self.page_blob_append
            .append_and_write(&pages_to_append)
            .await
    }

    pub fn get_write_position(&self) -> usize {
        self.page_blob_append.get_blob_position()
    }

    pub async fn init_new_blob(&mut self) -> Result<(), PageBlobAppendError> {
        self.page_blob_append.init_blob(None).await
    }

    pub async fn init(&mut self, backup_blob: &mut TMyPageBlob) -> Result<(), PageBlobAppendError> {
        self.page_blob_append.init_blob(Some(backup_blob)).await
    }

    pub fn get_blob_formal_name(&self) -> String {
        let blob = self.page_blob_append.get_page_blob();
        format!("{}/{}", blob.get_container_name(), blob.get_blob_name())
    }
}
