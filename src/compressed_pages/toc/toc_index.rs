use my_azure_storage_sdk::{page_blob::consts::BLOB_PAGE_SIZE, AzureStorageError};

use crate::message_pages::MessagePageId;
use my_azure_page_blob::*;

use super::{super::ClusterPageId, PageAllocationIndex};

pub const TOC_PAGES_AMOUNT: usize = 2;

const TOC_INDEX_SIZE: usize = 4 + 4;

const PAGES_IN_CLUSTER: usize = 100;

const TOC_SIZE: usize = TOC_PAGES_AMOUNT * BLOB_PAGE_SIZE;

pub struct TocIndex {
    pub toc: Vec<u8>,
    cluster_page_id: ClusterPageId,
}

impl TocIndex {
    pub fn new(cluster_page_id: ClusterPageId) -> Self {
        Self {
            toc: Vec::with_capacity(TOC_SIZE),
            cluster_page_id,
        }
    }

    fn init_toc(&mut self) {
        let init_toc = [0u8; TOC_SIZE];
        self.toc.extend(&init_toc);
    }

    async fn init<T: MyPageBlob>(&mut self, page_blob: &mut T) -> Result<(), AzureStorageError> {
        if self.toc.len() > 0 {
            return Ok(());
        }

        let available_pages_amount = page_blob.get_available_pages_amount().await?;

        if available_pages_amount >= TOC_PAGES_AMOUNT {
            let toc_from_blob = page_blob.get(0, TOC_PAGES_AMOUNT).await?;
            self.toc.extend(toc_from_blob);
            return Ok(());
        }

        if available_pages_amount == 0 {
            self.init_toc();
            page_blob.resize(TOC_PAGES_AMOUNT).await?;

            return Ok(());
        }

        Err(AzureStorageError::UnknownError {
            msg: "Blob file is corrupted".to_string(),
        })
    }

    fn get_toc_offset(&self, page_id: &MessagePageId) -> Result<usize, AzureStorageError> {
        let first_page_on_non_compressed_page =
            self.cluster_page_id.get_first_page_id_on_compressed_page();

        let result = page_id.value - first_page_on_non_compressed_page.value;

        let result = result as usize;

        check_range(result)?;

        return Ok(result * TOC_INDEX_SIZE);
    }

    pub async fn get_page_alocation_index<T: MyPageBlob>(
        &mut self,
        page_blob: &mut T,
        page_id: &MessagePageId,
    ) -> Result<PageAllocationIndex, AzureStorageError> {
        self.init(page_blob).await?;

        let offset = self.get_toc_offset(page_id)?;

        let result = PageAllocationIndex::parse(&self.toc[offset..]);

        return Ok(result);
    }

    pub async fn write_page_allocation_index<T: MyPageBlob>(
        &mut self,
        page_blob: &mut T,
        page_id: &MessagePageId,
        index: &PageAllocationIndex,
        just_initialized: bool,
    ) -> Result<(), AzureStorageError> {
        if just_initialized {
            self.init_toc();
        } else {
            self.init(page_blob).await?;
        }

        let offset = self.get_toc_offset(page_id)?;

        {
            let toc = &mut self.toc[offset..offset + 8];

            index.copy_to_slice(toc);
        }

        let start_page_no = offset / BLOB_PAGE_SIZE;

        let page_offset = start_page_no * BLOB_PAGE_SIZE;

        let page_to_write = &self.toc[page_offset..page_offset + BLOB_PAGE_SIZE];

        page_blob
            .save_pages(start_page_no, page_to_write.to_vec())
            .await?;

        Ok(())
    }
}

fn check_range(toc_index: usize) -> Result<(), AzureStorageError> {
    if toc_index >= PAGES_IN_CLUSTER {
        return Err(AzureStorageError::UnknownError {
            msg: format!("toc index is over 99. Value: {}", toc_index),
        });
    }

    Ok(())
}
