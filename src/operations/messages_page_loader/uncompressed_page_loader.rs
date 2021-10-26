use my_azure_page_blob_append::PageBlobAppendError;

use crate::{
    message_pages::{MessagePageId, MessagesPageData},
    uncompressed_messages::messages_page_blob::MessagesPageBlob,
};

pub async fn load(
    page_id: MessagePageId,
    page_is_current: bool,
    mut messages_page_blob: MessagesPageBlob,
) -> Result<MessagesPageData, PageBlobAppendError> {
    let messages = messages_page_blob.load(page_is_current).await?;

    let as_tree_map = crate::message_pages::utils::vec_of_messages_to_tree_map(messages);

    let page_data =
        MessagesPageData::restored_uncompressed(page_id.clone(), as_tree_map, messages_page_blob);

    return Ok(page_data);
}
