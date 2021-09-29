use my_azure_page_blob::*;
use my_azure_page_blob_append::page_blob_utils::get_pages_amount_by_size;
use my_azure_storage_sdk::{page_blob::consts::BLOB_PAGE_SIZE, AzureStorageError};

use crate::{
    compressed_pages::{ClusterPageId, ReadCompressedPageError},
    message_pages::MessagePageId,
};

use super::{
    toc::{PageAllocationIndex, TocIndex, TOC_PAGES_AMOUNT},
    ReadPageResult,
};

pub struct PagesClusterAzureBlob<T: MyPageBlob> {
    toc: TocIndex,
    page_blob: T,
}

impl<T: MyPageBlob> PagesClusterAzureBlob<T> {
    pub fn new(page_blob: T, cluster_page_id: &ClusterPageId) -> Self {
        Self {
            page_blob,
            toc: TocIndex::new(cluster_page_id.clone()),
        }
    }
    pub async fn read(
        &mut self,
        page_id: &MessagePageId,
    ) -> Result<ReadPageResult, ReadCompressedPageError> {
        let toc_index = self
            .toc
            .get_page_alocation_index(&mut self.page_blob, page_id)
            .await?;

        let pages_amount = get_pages_amount_by_size(toc_index.data_len, BLOB_PAGE_SIZE);

        let data = self
            .page_blob
            .get(toc_index.start_page, pages_amount)
            .await?;

        let result = ReadPageResult {
            data,
            size: toc_index.data_len,
        };
        Ok(result)
    }

    async fn get_pages_amount(&mut self) -> Result<usize, AzureStorageError> {
        let result = self.page_blob.get_available_pages_amount().await;

        if let Ok(page_size) = result {
            return Ok(page_size);
        }

        let err = result.err().unwrap();

        if let AzureStorageError::ContainerNotFound = err {
            self.page_blob.create_container_if_not_exist().await?;
            self.page_blob
                .create_if_not_exists(TOC_PAGES_AMOUNT)
                .await?;
        }

        if let AzureStorageError::BlobNotFound = err {
            self.page_blob
                .create_if_not_exists(TOC_PAGES_AMOUNT)
                .await?;
        }

        self.page_blob.get_available_pages_amount().await
    }

    pub async fn write(
        &mut self,
        page_id: MessagePageId,
        zip: &[u8],
    ) -> Result<(), AzureStorageError> {
        let page_size = self.get_pages_amount().await?;

        let toc_index = PageAllocationIndex {
            start_page: page_size,
            data_len: zip.len(),
        };

        self.toc
            .write_page_allocation_index(
                &mut self.page_blob,
                &page_id,
                &toc_index,
                page_size == TOC_PAGES_AMOUNT,
            )
            .await?;

        self.page_blob
            .auto_ressize_and_save_pages(page_size, 8000, zip.to_vec(), 1)
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use my_azure_page_blob::MyPageBlobMock;

    use super::*;

    #[tokio::test]
    async fn test_cluster_azure_blob() {
        let page_blob = MyPageBlobMock::new();

        let page_id = MessagePageId::new(1_000_005);

        let cluster_page_id = ClusterPageId::from_page_id(&page_id);

        let mut page_cluster = PagesClusterAzureBlob::new(page_blob, &cluster_page_id);

        let content = vec![0u8, 1u8, 2u8];

        page_cluster.write(page_id, &content).await.unwrap();

        let result = page_cluster.read(&page_id).await.unwrap();

        assert_eq!(3, result.size);

        assert_eq!(content[0], result.data[0]);
        assert_eq!(content[1], result.data[1]);
        assert_eq!(content[2], result.data[2]);

        let content = vec![5u8, 6u8, 7u8, 8u8];

        page_cluster.write(page_id, &content).await.unwrap();

        let result = page_cluster.read(&page_id).await.unwrap();

        assert_eq!(4, result.size);

        assert_eq!(content[0], result.data[0]);
        assert_eq!(content[1], result.data[1]);
        assert_eq!(content[2], result.data[2]);
        assert_eq!(content[3], result.data[3]);
    }
}
