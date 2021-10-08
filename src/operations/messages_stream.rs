use my_azure_page_blob::*;
use my_azure_page_blob_append::{PageBlobAppend, PageBlobAppendError};
use my_service_bus_shared::protobuf_models::MessageProtobufModel;

pub struct MessagesStream<TMyPageBlob: MyPageBlob> {
    pub page_blob_append: PageBlobAppend<TMyPageBlob>,
}

impl<TMyPageBlob: MyPageBlob> MessagesStream<TMyPageBlob> {
    pub fn new(
        page_blob: TMyPageBlob,
        cache_capacity_in_pages: usize,
        blob_auto_resize_in_pages: usize,
        max_payload_size_protection: i32,
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
    ) -> Result<Option<MessageProtobufModel>, PageBlobAppendError> {
        let payload_result = self.page_blob_append.get_next_payload().await?;

        match payload_result {
            Some(payload) => {
                let result: Result<MessageProtobufModel, prost::DecodeError> =
                    prost::Message::decode(payload.as_slice());

                return match result {
                    Ok(model) => Ok(Some(model)),
                    Err(err) => Err(PageBlobAppendError::Corrupted(format!("{:?}", err))),
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
}
