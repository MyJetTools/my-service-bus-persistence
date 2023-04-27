use my_azure_page_blob_random_access::{
    PageBlobRandomAccess, PageBlobRandomAccessError, ReadChunk,
};
use my_service_bus_shared::sub_page::SubPageId;

use crate::azure_storage_with_retries::AzurePageBlobStorageWithRetries;

use super::{
    consts::{CALCULATED_TOC_PAGES_AMOUNT, TOC_SIZE_IN_BITES, TOC_STRUCTURE_SIZE},
    toc::SubPagePosition,
    ArchiveFileNo,
};

pub struct ArchiveStorage {
    pub archive_file_no: ArchiveFileNo,
    pub page_blob: PageBlobRandomAccess,
}

impl ArchiveStorage {
    pub async fn open_if_exists(
        archive_file_no: ArchiveFileNo,
        page_blob: PageBlobRandomAccess,
    ) -> Option<Self> {
        let result = page_blob.get_blob_size_with_retires().await;

        match result {
            Ok(blob_size) => {
                if blob_size >= TOC_SIZE_IN_BITES {
                    return Self {
                        archive_file_no,
                        page_blob,
                    }
                    .into();
                } else {
                    None
                }
            }
            Err(err) => match err {
                my_azure_storage_sdk::AzureStorageError::ContainerNotFound => None,
                my_azure_storage_sdk::AzureStorageError::BlobNotFound => None,
                _ => {
                    panic!("Can not open Archive Storage. {:?}", err);
                }
            },
        }
    }

    pub async fn open_or_create(
        archive_file_no: ArchiveFileNo,
        page_blob: PageBlobRandomAccess,
    ) -> Self {
        let file_size = page_blob
            .get_blob_size_or_create_page_blob(TOC_STRUCTURE_SIZE)
            .await
            .unwrap();

        if file_size < TOC_SIZE_IN_BITES {
            page_blob
                .resize_with_retries(CALCULATED_TOC_PAGES_AMOUNT)
                .await
                .unwrap();
        }

        Self {
            archive_file_no,
            page_blob,
        }
    }

    pub async fn read_sub_page_payload(
        &self,
        sub_page_id: SubPageId,
    ) -> Result<Option<ReadChunk>, PageBlobRandomAccessError> {
        let pos =
            super::toc::read_file_position(&self.page_blob, self.archive_file_no, sub_page_id)
                .await;

        if pos.length == 0 {
            return Ok(None);
        }

        let result = self
            .page_blob
            .read(pos.offset as usize, pos.length as usize)
            .await?;

        Ok(Some(result))
    }

    pub async fn write_payload(&self, sub_page_id: SubPageId, payload: &[u8]) {
        let pos =
            super::toc::read_file_position(&self.page_blob, self.archive_file_no, sub_page_id)
                .await;

        if pos.length > 0 {
            println!(
                "Overwrite payload for sub_page_id: {}",
                sub_page_id.get_value()
            );
            return;
        }

        let blob_size = self.page_blob.get_blob_size().await.unwrap();

        let pos = SubPagePosition {
            offset: blob_size as u64,
            length: payload.len() as u32,
        };

        self.page_blob
            .write(pos.offset as usize, payload)
            .await
            .unwrap();

        super::toc::write_file_position(&self.page_blob, self.archive_file_no, sub_page_id, pos)
            .await;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use my_azure_page_blob_random_access::PageBlobRandomAccess;
    use my_azure_storage_sdk::{page_blob::AzurePageBlobStorage, AzureStorageConnection};
    use my_service_bus_shared::sub_page::SubPageId;

    use crate::archive_storage::ArchiveFileNo;

    #[tokio::test]
    async fn test_try_open_if_no_file_exists() {
        let azure_connection = Arc::new(AzureStorageConnection::new_in_memory());

        let page_blob = AzurePageBlobStorage::new(azure_connection, "test", "test").await;
        let page_blob = PageBlobRandomAccess::new(page_blob, true, 512);

        let archive_storage =
            super::ArchiveStorage::open_if_exists(ArchiveFileNo::new(0), page_blob).await;

        assert!(archive_storage.is_none());
    }

    #[tokio::test]
    async fn test_try_open_or_create_and_read_non_existing_message() {
        let azure_connection = Arc::new(AzureStorageConnection::new_in_memory());

        let page_blob = AzurePageBlobStorage::new(azure_connection, "test", "test").await;
        let page_blob = PageBlobRandomAccess::new(page_blob, true, 512);

        let archive_storage =
            super::ArchiveStorage::open_or_create(ArchiveFileNo::new(0), page_blob).await;

        let sub_page_id = SubPageId::new(0);
        let result = archive_storage
            .read_sub_page_payload(sub_page_id)
            .await
            .unwrap();

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_try_open_or_create_and_write_and_read_payload() {
        let azure_connection = Arc::new(AzureStorageConnection::new_in_memory());

        let page_blob = AzurePageBlobStorage::new(azure_connection, "test", "test").await;
        let page_blob = PageBlobRandomAccess::new(page_blob, true, 512);

        let archive_storage =
            super::ArchiveStorage::open_or_create(ArchiveFileNo::new(0), page_blob).await;

        let src_payload = "Hello".as_bytes();

        archive_storage
            .write_payload(SubPageId::new(0), src_payload)
            .await;

        let sub_page_id = SubPageId::new(0);
        let result = archive_storage
            .read_sub_page_payload(sub_page_id)
            .await
            .unwrap();

        let result = result.unwrap();
        assert_eq!(result.as_slice(), src_payload);
    }
}
