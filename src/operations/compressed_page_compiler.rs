use std::sync::Arc;

use my_service_bus_abstractions::MessageId;
use my_service_bus_shared::{page_compressor::CompressedPageBuilder, sub_page::SubPageId};

use crate::app::AppContext;

pub async fn get_compressed_page(
    app: Arc<AppContext>,
    topic_id: &str,
    from_message_id: MessageId,
    to_message_id: MessageId,
    v0: bool,
    max_payload_size: usize,
) -> Vec<Vec<u8>> {
    let mut compressed_writer = if v0 {
        CompressedPageBuilder::new_as_single_file()
    } else {
        CompressedPageBuilder::new_by_files()
    };

    let mut sub_page_read_copy = None;

    for message_id in from_message_id.get_value()..to_message_id.get_value() + 1 {
        let message_id: MessageId = message_id.into();

        let sub_page_id: SubPageId = message_id.into();

        if sub_page_read_copy.is_none() {
            let sub_page =
                crate::operations::get_sub_page_to_read(&app, &topic_id, sub_page_id).await;

            sub_page_read_copy = Some(sub_page.get_all_messages().await);
        }

        if sub_page_read_copy.as_ref().unwrap().sub_page_id.get_value() != sub_page_id.get_value() {
            let sub_page =
                crate::operations::get_sub_page_to_read(&app, &topic_id, sub_page_id).await;

            sub_page_read_copy = Some(sub_page.get_all_messages().await);
        }

        let message = sub_page_read_copy.as_ref().unwrap().get(message_id);

        if let Some(message) = message {
            compressed_writer.add_message(message).unwrap();
        }
    }

    let result = compressed_writer.get_payload().unwrap();

    split(result.as_slice(), max_payload_size)
}

fn split(src: &[u8], max_payload_size: usize) -> Vec<Vec<u8>> {
    let mut result: Vec<Vec<u8>> = Vec::new();

    let mut pos: usize = 0;

    while pos < src.len() {
        if src.len() - pos < max_payload_size {
            let payload = &src[pos..];
            result.push(payload.to_vec());
            break;
        }
        let payload = &src[pos..pos + max_payload_size];
        result.push(payload.to_vec());
        pos += max_payload_size;
    }

    result
}
