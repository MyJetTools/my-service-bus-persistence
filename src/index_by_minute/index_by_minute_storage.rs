use crate::{
    file_random_access::FileRandomAccess,
    page_blob_random_access::{PageBlobPageId, PageBlobRandomAccess},
};

pub enum IndexByMinuteStorage {
    AsFile(FileRandomAccess),
    AsPageBlob(PageBlobRandomAccess),
}

impl IndexByMinuteStorage {
    pub fn as_file(file: FileRandomAccess) -> Self {
        Self::AsFile(file)
    }

    pub async fn load(&mut self) -> Vec<u8> {
        match self {
            IndexByMinuteStorage::AsFile(as_file) => {
                super::as_file::load_and_init_content(as_file).await
            }
            IndexByMinuteStorage::AsPageBlob(page_blob) => {
                super::as_page_blob::load_and_init_content(page_blob).await
            }
        }
    }

    pub async fn save(&mut self, content: &[u8], page_no: &PageBlobPageId) {
        match self {
            IndexByMinuteStorage::AsFile(as_file) => {
                super::as_file::save_page(as_file, page_no, content).await;
            }
            IndexByMinuteStorage::AsPageBlob(page_blob) => {
                page_blob.save_pages(page_no, content).await;
            }
        }
    }

    #[cfg(test)]
    pub fn unwrap_as_file(&self) -> &FileRandomAccess {
        match self {
            IndexByMinuteStorage::AsFile(as_file) => as_file,
            _ => panic!("Not a file storage"),
        }
    }
}
