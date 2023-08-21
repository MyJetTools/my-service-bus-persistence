use std::time::Duration;

use my_azure_page_blob_ext::MyAzurePageBlobStorageWithRetries;
use my_azure_page_blob_random_access::PageBlobRandomAccess;
use my_azure_storage_sdk::{page_blob::AzurePageBlobStorage, AzureStorageError};
use my_service_bus_abstractions::MessageId;

use super::{
    utils::{MINUTE_INDEX_FILE_SIZE, MINUTE_INDEX_PAGES_AMOUNT},
    MinuteWithinYear,
};

pub struct IndexByMinutePageBlob {
    page_blob: PageBlobRandomAccess<MyAzurePageBlobStorageWithRetries>,
}

impl IndexByMinutePageBlob {
    pub fn new(page_blob: AzurePageBlobStorage) -> Self {
        let page_blob =
            MyAzurePageBlobStorageWithRetries::new(page_blob, 3, Duration::from_secs(3));
        Self {
            page_blob: PageBlobRandomAccess::new(page_blob, true, MINUTE_INDEX_PAGES_AMOUNT),
        }
    }

    #[cfg(test)]
    pub fn get_page_blob(&self) -> &PageBlobRandomAccess<MyAzurePageBlobStorageWithRetries> {
        &self.page_blob
    }

    pub async fn check_index_by_minute_blob(&self) -> Option<()> {
        let result = self.page_blob.get_blob_properties().await;

        match result {
            Ok(props) => {
                let file_size = props.get_blob_size();
                if file_size < MINUTE_INDEX_FILE_SIZE {
                    return None;
                }

                return Some(());
            }

            Err(err) => {
                if let AzureStorageError::BlobNotFound = &err {
                    return None;
                }

                if let AzureStorageError::ContainerNotFound = &err {
                    return None;
                }

                panic!("Error: {:?}", err);
            }
        }
    }

    pub async fn init_index_by_minute(&self) {
        let page_blob_props = self
            .page_blob
            .create_blob_if_not_exists(MINUTE_INDEX_PAGES_AMOUNT, true)
            .await
            .unwrap();

        if page_blob_props.get_pages_amount() < MINUTE_INDEX_PAGES_AMOUNT {
            self.page_blob
                .resize(MINUTE_INDEX_PAGES_AMOUNT)
                .await
                .unwrap();
        }
    }
    pub async fn write_message_id_to_minute_index(
        &self,
        minute: MinuteWithinYear,
        message_id: MessageId,
    ) {
        let message_id = message_id.get_value() as i64;

        let payload = message_id.to_le_bytes();
        let position_in_file = minute.get_position_in_file();
        self.page_blob
            .write(position_in_file, payload.as_slice())
            .await
            .unwrap();
    }

    pub async fn read_message_id_from_minute_index(
        &self,
        minute: MinuteWithinYear,
    ) -> Option<MessageId> {
        let position_in_file = minute.get_position_in_file();

        let mut payload = self.page_blob.read(position_in_file, 8).await.unwrap();

        let result = payload.read_i64();

        if result == 0 {
            return None;
        }

        Some(MessageId::new(result))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::index_by_minute::MinuteWithinYear;

    use super::IndexByMinutePageBlob;
    use my_azure_storage_sdk::{page_blob::AzurePageBlobStorage, AzureStorageConnection};
    use my_service_bus_abstractions::MessageId;

    #[tokio::test]
    async fn test_create_new_blob_then_write_and_read() {
        let connection = AzureStorageConnection::new_in_memory();
        let page_blob =
            AzurePageBlobStorage::new(Arc::new(connection), "test".to_string(), "test".to_string())
                .await;

        page_blob.create_container_if_not_exists().await.unwrap();

        let page_blob = IndexByMinutePageBlob::new(page_blob);

        page_blob.init_index_by_minute().await;

        let minute = MinuteWithinYear::new(15);

        let message_id = MessageId::new(123);
        page_blob
            .write_message_id_to_minute_index(minute, message_id)
            .await;

        let result = page_blob.read_message_id_from_minute_index(minute).await;

        assert_eq!(result.unwrap(), message_id);
    }

    #[tokio::test]
    async fn test_create_new_blob_then_read() {
        let connection = AzureStorageConnection::new_in_memory();
        let page_blob =
            AzurePageBlobStorage::new(Arc::new(connection), "test".to_string(), "test".to_string())
                .await;

        page_blob.create_container_if_not_exists().await.unwrap();

        let page_blob = IndexByMinutePageBlob::new(page_blob);

        page_blob.init_index_by_minute().await;

        let minute = MinuteWithinYear::new(15);

        let result = page_blob.read_message_id_from_minute_index(minute).await;

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_position_in_blob() {
        let connection = AzureStorageConnection::new_in_memory();
        let page_blob =
            AzurePageBlobStorage::new(Arc::new(connection), "test".to_string(), "test".to_string())
                .await;

        page_blob.create_container_if_not_exists().await.unwrap();

        let page_blob = IndexByMinutePageBlob::new(page_blob);

        page_blob.init_index_by_minute().await;

        let minute = MinuteWithinYear::new(15);

        let message_id = MessageId::new(123);
        page_blob
            .write_message_id_to_minute_index(minute, message_id)
            .await;

        let blob_payload = page_blob.get_page_blob().download().await.unwrap();

        assert_eq!(
            &blob_payload[minute.get_value() as usize * 8..minute.get_value() as usize * 8 + 8],
            [123u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8]
        )
    }
}
