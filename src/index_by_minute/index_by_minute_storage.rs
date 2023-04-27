use my_azure_page_blob_random_access::PageBlobRandomAccess;
use my_azure_storage_sdk::{page_blob::consts::BLOB_PAGE_SIZE, AzureStorageError};
use my_service_bus_abstractions::MessageId;

use crate::azure_storage_with_retries::AzurePageBlobStorageWithRetries;

use super::{utils::MINUTE_INDEX_FILE_SIZE, MinuteWithinYear};
pub static INIT_PAGES_SIZE: usize = MINUTE_INDEX_FILE_SIZE / BLOB_PAGE_SIZE;

#[async_trait::async_trait]
pub trait IndexByMinutePageBlobExt {
    async fn check_index_by_minute_blob(&self) -> Option<()>;

    async fn init_index_by_minute(&self);
    async fn write_message_id_to_minute_index(
        &self,
        minute: MinuteWithinYear,
        message_id: MessageId,
    );

    async fn read_message_id_from_minute_index(
        &self,
        minute: MinuteWithinYear,
    ) -> Option<MessageId>;
}

#[async_trait::async_trait]
impl IndexByMinutePageBlobExt for PageBlobRandomAccess {
    async fn check_index_by_minute_blob(&self) -> Option<()> {
        let result = self.get_blob_size_with_retires().await;

        match result {
            Ok(file_size) => {
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

    async fn init_index_by_minute(&self) {
        let file_size = self
            .get_blob_size_or_create_page_blob(MINUTE_INDEX_FILE_SIZE)
            .await
            .unwrap();

        if file_size < MINUTE_INDEX_FILE_SIZE {
            self.resize(INIT_PAGES_SIZE).await.unwrap();
        }
    }
    async fn write_message_id_to_minute_index(
        &self,
        minute: MinuteWithinYear,
        message_id: MessageId,
    ) {
        let message_id = message_id.get_value() as i64;

        let payload = message_id.to_le_bytes();
        let position_in_file = minute.get_position_in_file();
        self.write(position_in_file, payload.as_slice())
            .await
            .unwrap();
    }

    async fn read_message_id_from_minute_index(
        &self,
        minute: MinuteWithinYear,
    ) -> Option<MessageId> {
        let position_in_file = minute.get_position_in_file();

        let mut payload = self.read(position_in_file, 8).await.unwrap();

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

    use super::IndexByMinutePageBlobExt;
    use my_azure_page_blob_random_access::PageBlobRandomAccess;
    use my_azure_storage_sdk::{page_blob::AzurePageBlobStorage, AzureStorageConnection};
    use my_service_bus_abstractions::MessageId;

    #[tokio::test]
    async fn test_create_new_blob_then_write_and_read() {
        let connection = AzureStorageConnection::new_in_memory();
        let page_blob =
            AzurePageBlobStorage::new(Arc::new(connection), "test".to_string(), "test".to_string())
                .await;

        let page_blob = PageBlobRandomAccess::new(page_blob, true, 512);

        page_blob.create_container_if_not_exists().await.unwrap();

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

        let page_blob = PageBlobRandomAccess::new(page_blob, true, 512);

        page_blob.create_container_if_not_exists().await.unwrap();

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

        let page_blob = PageBlobRandomAccess::new(page_blob, true, 512);

        page_blob.create_container_if_not_exists().await.unwrap();

        page_blob.init_index_by_minute().await;

        let minute = MinuteWithinYear::new(15);

        let message_id = MessageId::new(123);
        page_blob
            .write_message_id_to_minute_index(minute, message_id)
            .await;

        let blob_payload = page_blob.download().await.unwrap();

        assert_eq!(
            &blob_payload[minute.get_value() as usize * 8..minute.get_value() as usize * 8 + 8],
            [123u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8]
        )
    }
}
