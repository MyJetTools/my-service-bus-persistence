use std::{collections::BTreeMap, sync::Arc, time::Duration};

use my_azure_page_blob_ext::MyAzurePageBlobStorageWithRetries;
use my_azure_storage_sdk::page_blob::MyAzurePageBlobStorage;
use tokio::sync::Mutex;

use super::{
    consts::{CALCULATED_TOC_PAGES_AMOUNT, TOC_SIZE},
    ArchiveFileNo, ArchiveStorage,
};

#[async_trait::async_trait]
pub trait ArchivePageBlobCreator {
    async fn create(
        &self,
        topic_id: &str,
        archive_file_no: ArchiveFileNo,
    ) -> my_azure_storage_sdk::page_blob::AzurePageBlobStorage;
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

        let page_blob =
            MyAzurePageBlobStorageWithRetries::new(page_blob, 3, Duration::from_secs(1));

        let page_blob_props = page_blob
            .create_if_not_exists(TOC_SIZE, true)
            .await
            .unwrap();

        if page_blob_props.get_pages_amount() < CALCULATED_TOC_PAGES_AMOUNT {
            page_blob.resize(CALCULATED_TOC_PAGES_AMOUNT).await.unwrap();
        }

        let archive_storage = ArchiveStorage::open_or_create(archive_file_no, page_blob).await;

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

        let page_blob =
            MyAzurePageBlobStorageWithRetries::new(page_blob, 3, Duration::from_secs(3));

        let blob_props = page_blob.get_blob_properties().await;

        if blob_props.is_err() {
            return None;
        }

        let blob_props = blob_props.unwrap();

        if blob_props.get_pages_amount() < CALCULATED_TOC_PAGES_AMOUNT {
            return None;
        }

        let archive_storage = ArchiveStorage::open_if_exists(archive_file_no, page_blob).await?;

        let archive_storage = Arc::new(archive_storage);

        self.insert(topic_id, archive_file_no, archive_storage.clone())
            .await;

        Some(archive_storage)
    }
}
