use std::sync::Arc;

use my_azure_page_blob::*;
use my_azure_page_blob_append::page_blob_utils::get_pages_amount_by_size;
use my_azure_storage_sdk::{page_blob::consts::BLOB_PAGE_SIZE, AzureConnection, AzureStorageError};
use my_service_bus_shared::MessageProtobufModel;
use tokio::sync::Mutex;

use crate::{
    app::{AppError, Logs},
    message_pages::MessagePageId,
};

use super::{
    toc::{PageAllocationIndex, TocIndex, TOC_PAGES_AMOUNT},
    utils::{decompress_cluster, ClusterPageId},
};

pub struct ReadPageResult {
    data: Vec<u8>,
    size: usize,
}

pub struct PagesClusterBlobData<T: MyPageBlob> {
    toc: TocIndex,
    page_blob: T,
}

impl<T: MyPageBlob> PagesClusterBlobData<T> {
    pub fn new(page_blob: T, cluster_page_id: &ClusterPageId) -> Self {
        Self {
            page_blob,
            toc: TocIndex::new(cluster_page_id.clone()),
        }
    }
    pub async fn read(&mut self, page_id: &MessagePageId) -> Result<ReadPageResult, AppError> {
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

    pub async fn write(&mut self, page_id: MessagePageId, zip: &[u8]) -> Result<(), AppError> {
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
            .auto_ressize_and_save_pages(page_size, zip.to_vec(), 1)
            .await?;

        Ok(())
    }
}

pub struct PagesClusterAzureBlob {
    pub topic_id: String,
    pub cluster_page_id: ClusterPageId,
    blob_data: Mutex<PagesClusterBlobData<MyAzurePageBlob>>,
    logs: Arc<Logs>,
}

impl PagesClusterAzureBlob {
    pub fn new(
        connection: AzureConnection,
        topic_id: &str,
        cluster_page_id: ClusterPageId,
        logs: Arc<Logs>,
    ) -> Self {
        let blob_name = get_cluster_blob_name(&cluster_page_id);

        let my_page_blob = MyAzurePageBlob::new(connection, topic_id.to_string(), blob_name);

        let blob_data = PagesClusterBlobData::new(my_page_blob, &cluster_page_id);

        Self {
            cluster_page_id,
            blob_data: Mutex::new(blob_data),
            logs,
            topic_id: topic_id.to_string(),
        }
    }

    pub async fn read(
        &mut self,
        page_id: &MessagePageId,
    ) -> Result<Option<Vec<MessageProtobufModel>>, AppError> {
        let mut blob_data = self.blob_data.lock().await;
        let zip = blob_data.read(page_id).await?;

        let result = decompress_cluster(
            &zip.data[..zip.size],
            self.topic_id.as_str(),
            self.logs.as_ref(),
        )
        .await;

        result
    }

    pub async fn write(&self, page_id: MessagePageId, zip: &[u8]) -> Result<(), AppError> {
        let mut blob_data = self.blob_data.lock().await;
        blob_data.write(page_id, zip).await
    }
}

fn get_cluster_blob_name(cluster: &ClusterPageId) -> String {
    format!("cluster-{:019}.zip", cluster.value)
}

#[cfg(test)]
mod tests {

    use super::*;

    #[tokio::test]
    async fn test_cluster_azure_blob() {
        let page_blob = MyPageBlobMock::new();

        let page_id = MessagePageId::new(1_000_005);

        let cluster_page_id = ClusterPageId::from_page_id(&page_id);

        let mut page_cluster = PagesClusterBlobData::new(page_blob, &cluster_page_id);

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
