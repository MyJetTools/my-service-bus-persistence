use my_azure_storage_sdk::{
    blob::BlobApi, page_blob::consts::BLOB_PAGE_SIZE, page_blob::PageBlobApi, AzureConnection,
    AzureStorageError,
};

use super::pages_cache::{PageCacheItem, PagesCache};

pub struct PageBlobRandomAccess {
    pages_cache: PagesCache,
    pub connection: AzureConnection,
    blob_size: Option<usize>,
    container_name: String,
    blob_name: String,
}

impl PageBlobRandomAccess {
    pub fn new(connection: &AzureConnection, container_name: &str, blob_name: &str) -> Self {
        Self {
            pages_cache: PagesCache::new(4),

            connection: connection.clone(),
            blob_size: None,
            container_name: container_name.to_string(),
            blob_name: blob_name.to_string(),
        }
    }

    pub async fn get_blob_size(&mut self) -> Result<usize, AzureStorageError> {
        if self.blob_size.is_some() {
            return Ok(self.blob_size.unwrap());
        }

        let props = self
            .connection
            .get_blob_properties(self.container_name.as_str(), self.blob_name.as_str())
            .await?;

        self.blob_size = Some(props.blob_size);

        return Ok(self.blob_size.unwrap());
    }

    async fn read_page(&mut self, page_no: usize) -> Result<&PageCacheItem, AzureStorageError> {
        if self.pages_cache.has_page(page_no) {
            return Ok(self.pages_cache.get_page(page_no).unwrap());
        }

        let data = self
            .connection
            .get_pages(&self.container_name, &self.blob_name, page_no, 1)
            .await?;

        self.pages_cache.add_page(page_no, data);
        let page = self.pages_cache.get_page(page_no);
        return Ok(page.unwrap());
    }

    pub async fn read(
        &mut self,
        start_pos: usize,
        copy_to: &mut [u8],
    ) -> Result<(), AzureStorageError> {
        let blob_size = self.get_blob_size().await?;

        let max_len = start_pos + copy_to.len();

        if max_len > blob_size {
            return Err(AzureStorageError::UnknownError {
                msg: "insufficient size".to_string(),
            });
        }

        let page_no =
            super::page_blob_utils::get_page_no_from_page_blob_position(start_pos, BLOB_PAGE_SIZE);

        let page = self.read_page(page_no).await?;

        let pos_in_page =
            super::page_blob_utils::get_position_within_page(start_pos, BLOB_PAGE_SIZE);

        copy_to.copy_from_slice(&page.data[pos_in_page..pos_in_page + &copy_to.len()]);

        Ok(())
    }

    pub async fn make_sure_page_is_in_cache(
        &mut self,
        page_no: usize,
    ) -> Result<(), AzureStorageError> {
        let has_page = self.pages_cache.has_page(page_no);

        if has_page {
            return Ok(());
        }

        let page_data = self
            .connection
            .get_pages(
                self.container_name.as_str(),
                self.blob_name.as_str(),
                page_no,
                1,
            )
            .await?;

        self.pages_cache.add_page(page_no, page_data);

        Ok(())
    }

    pub async fn write(
        &mut self,
        start_pos: usize,
        payload: &[u8],
    ) -> Result<(), AzureStorageError> {
        let page_no =
            super::page_blob_utils::get_page_no_from_page_blob_position(start_pos, BLOB_PAGE_SIZE);

        {
            self.make_sure_page_is_in_cache(page_no).await?;

            let pos_in_page =
                super::page_blob_utils::get_position_within_page(start_pos, BLOB_PAGE_SIZE);

            let buf = self.pages_cache.get_page_mut(page_no).unwrap();

            &buf[pos_in_page..pos_in_page + payload.len()].copy_from_slice(payload);
        }

        self.connection
            .save_pages(
                self.container_name.as_str(),
                self.blob_name.as_str(),
                page_no,
                self.pages_cache.clone_page(page_no).unwrap(),
            )
            .await?;

        Ok(())
    }

    pub async fn create_new(&mut self, pages: usize) -> Result<(), AzureStorageError> {
        self.connection
            .create_page_blob_if_not_exists(
                self.container_name.as_str(),
                self.blob_name.as_str(),
                pages,
            )
            .await?;

        Ok(())
    }
}
