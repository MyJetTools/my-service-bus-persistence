use my_azure_page_blob_random_access::{
    PageBlobRandomAccess, PageBlobRandomAccessError, ReadChunk,
};
use my_service_bus_shared::sub_page::SubPageId;

use super::{toc::SubPagePosition, ArchiveFileNo};

pub struct ArchiveStorage {
    pub archive_file_no: ArchiveFileNo,
    pub page_blob: PageBlobRandomAccess,
}

impl ArchiveStorage {
    pub fn new(archive_file_no: ArchiveFileNo, page_blob: PageBlobRandomAccess) -> Self {
        Self {
            archive_file_no,
            page_blob,
        }
    }

    pub async fn read_sub_page_payload(
        &self,
        sub_page_id: SubPageId,
    ) -> Result<ReadChunk, PageBlobRandomAccessError> {
        let pos =
            super::toc::read_file_position(&self.page_blob, self.archive_file_no, sub_page_id)
                .await;

        self.page_blob
            .read(pos.offset as usize, pos.length as usize)
            .await
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
