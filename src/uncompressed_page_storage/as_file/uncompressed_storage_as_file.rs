use std::{collections::BTreeMap, io::SeekFrom};

use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

use crate::uncompressed_page_storage::{
    load_trait::LoadFromStorage, toc::UncompressedFileToc, PayloadsToUploadContainer,
};

pub struct UncompressedPageStorageAsFile {
    toc: UncompressedFileToc,
    file: File,
}

impl UncompressedPageStorageAsFile {
    pub async fn opend_or_append(file_name: &str) -> std::io::Result<Self> {
        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .append(true)
            .open(file_name)
            .await?;

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
        let file = tokio::fs::OpenOptions::new()
            .create_new(true)
            .open(file_name)
            .await?;

        let mut result = Self {
            file,
            toc: UncompressedFileToc::new(),
        };

        result.reset_as_new().await?;

        Ok(result)
    }

    pub async fn reset_as_new(&mut self) -> std::io::Result<()> {
        self.reset_file(0).await?;

        let toc_content = self.toc.reset_as_new();
        self.file.write_all(toc_content).await?;

        Ok(())
    }

    pub async fn read_payload(&mut self, buf: &mut [u8]) -> std::io::Result<bool> {
        let read_size = self.file.read(buf).await?;
        Ok(read_size == buf.len())
    }

    pub async fn append_payload(&mut self, file_no: usize, payload: &[u8]) -> std::io::Result<()> {
        self.file.write_all(payload).await?;

        Ok(())
    }

    pub async fn get_next_payload(&mut self) -> std::io::Result<Vec<u8>> {
        let mut len_as_array = [0u8; 4];
        self.file.read(len_as_array.as_mut()).await?;

        let len = u32::from_be_bytes(len_as_array) as usize;

        let mut result = Vec::with_capacity(len);

        unsafe {
            result.set_len(len);
        }

        self.file.read(&mut result).await?;
        Ok(result)
    }

    pub async fn upload_payload(
        &mut self,
        payload_to_upload: PayloadsToUploadContainer,
    ) -> std::io::Result<()> {
        self.file
            .seek(SeekFrom::Start(payload_to_upload.write_position as u64))
            .await?;

        self.file
            .write_all(payload_to_upload.payload.as_slice())
            .await?;

        let mut pages_to_update = BTreeMap::new();

        for (no_in_page, offset) in payload_to_upload.tocs {
            let page_no = self.toc.update_file_position(no_in_page, &offset);
            pages_to_update.insert(page_no, ());
        }

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
