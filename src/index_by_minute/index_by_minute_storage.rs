use crate::{file_random_access::FileRandomAccess, page_blob_utils::PageBlobPageId};

pub enum IndexByMinuteStorage {
    AsFile(FileRandomAccess),
}

impl IndexByMinuteStorage {
    pub fn as_file(file: FileRandomAccess) -> Self {
        Self::AsFile(file)
    }

    pub async fn load(&mut self) -> Vec<u8> {
        match self {
            IndexByMinuteStorage::AsFile(as_file) => {
                super::as_file::load_and_init_content_as_file(as_file).await
            }
        }
    }

    pub async fn save(&mut self, content: &[u8], page_no: &PageBlobPageId) {
        match self {
            IndexByMinuteStorage::AsFile(as_file) => {
                super::as_file::save_page(as_file, page_no, content).await
            }
        }
    }

    #[cfg(test)]
    pub fn unwrap_as_file(&self) -> &FileRandomAccess {
        match self {
            IndexByMinuteStorage::AsFile(as_file) => as_file,
        }
    }
}
