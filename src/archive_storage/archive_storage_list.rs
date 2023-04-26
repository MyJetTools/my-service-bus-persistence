use std::{collections::BTreeMap, sync::Arc};

use crate::settings::*;
use my_azure_page_blob_random_access::PageBlobRandomAccess;

use tokio::sync::Mutex;

use crate::azure_storage_with_retries::AzurePageBlobStorageWithRetries;

use super::{consts::TOC_SIZE, ArchiveFileNo, ArchiveStorage};

#[async_trait::async_trait]
pub trait ArchivePageBlobCreator {
    async fn create(&self, topic_id: &str, archive_file_no: ArchiveFileNo) -> PageBlobRandomAccess;
}

pub struct ArchiveStorageList {
    items: Mutex<BTreeMap<String, BTreeMap<i64, Arc<ArchiveStorage>>>>,
}

impl ArchiveStorageList {
    pub fn new() -> Self {
        Self {
            items: Mutex::new(BTreeMap::new()),
        }
    }

    async fn get_existing(
        &self,
        topic_id: &str,
        archive_file_no: ArchiveFileNo,
    ) -> Option<Arc<ArchiveStorage>> {
        let read_access = self.items.lock().await;

        if let Some(archive_storages) = read_access.get(topic_id) {
            if let Some(archive_storage) = archive_storages.get(&archive_file_no.get_value()) {
                return Some(archive_storage.clone());
            }
        }

        None
    }

    async fn insert(
        &self,
        topic_id: &str,
        archive_file_no: ArchiveFileNo,
        storage: Arc<ArchiveStorage>,
    ) {
        let mut write_access = self.items.lock().await;

        if !write_access.contains_key(topic_id) {
            write_access.insert(topic_id.to_string(), BTreeMap::new());
        }
        write_access
            .get_mut(topic_id)
            .unwrap()
            .insert(archive_file_no.get_value(), storage);
    }

    pub async fn get_or_create(
        &self,
        archive_file_no: ArchiveFileNo,
        topic_id: &str,
        page_blob_creator: &impl ArchivePageBlobCreator,
    ) -> Arc<ArchiveStorage> {
        if let Some(page_blob) = self.get_existing(topic_id, archive_file_no).await {
            return page_blob;
        }

        let page_blob = page_blob_creator.create(topic_id, archive_file_no).await;

        let result = page_blob
            .get_blob_size_or_create_page_blob(TOC_SIZE, IO_RETRIES, DELAY_BETWEEN_IO_RETRIES)
            .await
            .unwrap();

        if result < TOC_SIZE {
            page_blob.resize(TOC_SIZE).await.unwrap();
        }

        let archive_storage = ArchiveStorage::new(archive_file_no, page_blob);

        let archive_storage = Arc::new(archive_storage);

        self.insert(topic_id, archive_file_no, archive_storage.clone())
            .await;

        archive_storage
    }

    pub async fn try_get_or_open(
        &self,
        archive_file_no: ArchiveFileNo,
        topic_id: &str,
        page_blob_creator: &impl ArchivePageBlobCreator,
    ) -> Option<Arc<ArchiveStorage>> {
        if let Some(page_blob) = self.get_existing(topic_id, archive_file_no).await {
            return Some(page_blob);
        }

        let page_blob = page_blob_creator.create(topic_id, archive_file_no).await;

        let blob_size = page_blob
            .get_blob_size_with_retires(IO_RETRIES, DELAY_BETWEEN_IO_RETRIES)
            .await;

        if blob_size.is_err() {
            return None;
        }

        let blob_size = blob_size.unwrap();

        if blob_size < TOC_SIZE {
            return None;
        }

        let archive_storage = ArchiveStorage::new(archive_file_no, page_blob);

        let archive_storage = Arc::new(archive_storage);

        self.insert(topic_id, archive_file_no, archive_storage.clone())
            .await;

        Some(archive_storage)
    }
}
