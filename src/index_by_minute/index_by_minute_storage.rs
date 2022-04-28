use crate::page_blob_utils::PageBlobPageId;

pub enum IndexByMinuteStorage {
    AsFile,
}

impl IndexByMinuteStorage {
    pub async fn load(&self) -> Vec<u8> {
        todo!("Implement");
    }

    pub async fn save(&self, content: &[u8], page_no: &PageBlobPageId) {
        todo!("Implement");
    }
}
