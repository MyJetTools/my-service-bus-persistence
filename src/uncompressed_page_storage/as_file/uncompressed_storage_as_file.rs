use std::collections::BTreeMap;

use crate::{
    file_random_access::FileRandomAccess,
    uncompressed_page_storage::{
        load_trait::LoadFromStorage, toc::UncompressedFileToc, PayloadsToUploadContainer,
    },
};

pub struct UncompressedPageStorageAsFile {
    toc: UncompressedFileToc,
    file: FileRandomAccess,
}

impl UncompressedPageStorageAsFile {
    pub async fn opend_or_append(file_name: &str) -> std::io::Result<Self> {
        let file = FileRandomAccess::opend_and_append_as_file(file_name).await?;

        let result = Self {
            file,
            toc: UncompressedFileToc::new(),
        };

        Ok(result)
    }

    pub fn get_write_position(&self) -> usize {
        self.toc.get_write_position()
    }

    pub async fn create_new(file_name: &str) -> std::io::Result<Self> {
        let file = FileRandomAccess::create_new_file(file_name).await?;

        let mut result = Self {
            file,
            toc: UncompressedFileToc::new(),
        };

        result.reset_as_new().await?;

        Ok(result)
    }

    pub async fn reset_as_new(&mut self) -> std::io::Result<()> {
        self.file.reduce_size(0).await?;

        let toc_content = self.toc.reset_as_new();
        self.file.write_to_file(toc_content).await?;

        Ok(())
    }

    pub async fn read_payload(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        self.file.read_from_file(buf).await?;
        Ok(())
    }

    pub async fn upload_payload(
        &mut self,
        payload_to_upload: PayloadsToUploadContainer,
    ) -> std::io::Result<()> {
        self.file
            .set_position(payload_to_upload.write_position)
            .await?;

        self.file
            .write_to_file(payload_to_upload.payload.as_slice())
            .await?;

        let mut pages_to_update = BTreeMap::new();

        for (no_in_page, offset) in payload_to_upload.tocs {
            let page_no = self.toc.update_file_position(no_in_page, &offset);
            pages_to_update.insert(page_no, ());
        }

        /////
        Ok(())
    }
}

#[async_trait::async_trait]
impl LoadFromStorage for UncompressedPageStorageAsFile {
    async fn reset_file(&mut self, position: u64) -> std::io::Result<()> {
        self.reset_file(position).await
    }

    async fn read(&mut self, buf: &mut [u8]) -> std::io::Result<bool> {
        self.read(buf).await
    }
}
