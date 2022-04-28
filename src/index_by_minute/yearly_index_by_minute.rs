use std::collections::HashMap;

use my_service_bus_shared::{page_id::PageId, MessageId};

use crate::page_blob_utils::PageBlobPageId;

use super::{IndexByMinuteStorage, MinuteWithinYear};

pub struct YearlyIndexByMinute {
    pub index_data: Vec<u8>,
    pub year: u32,
    pub pages_to_save: HashMap<usize, ()>,
    storage: IndexByMinuteStorage,
}

impl YearlyIndexByMinute {
    pub async fn new(year: u32, storage: IndexByMinuteStorage) -> Self {
        Self {
            year,
            index_data: storage.load().await,
            pages_to_save: HashMap::new(),
            storage,
        }
    }

    pub fn update_minute_index_if_new(
        &mut self,
        minute_witin_year: MinuteWithinYear,
        message_id: MessageId,
    ) {
        let position_in_file = minute_witin_year.get_position_in_file();

        if self.get_page_id_from_position(position_in_file) != 0 {
            return;
        }

        let dest = &mut self.index_data[position_in_file..8];

        dest.copy_from_slice(message_id.to_le_bytes().as_slice());

        let page_id = PageBlobPageId::from_blob_position(position_in_file);

        self.pages_to_save.insert(page_id.value, ());
    }

    fn get_page_id_from_position(&self, position_in_file: usize) -> PageId {
        let mut page_id_in_file = [0u8; 8];
        page_id_in_file.copy_from_slice(&self.index_data[position_in_file..8]);

        PageId::from_be_bytes(page_id_in_file)
    }

    fn get_page_content_to_sync_if_needed(&self) -> Option<(PageBlobPageId, &[u8])> {
        for page_to_save in self.pages_to_save.keys() {
            let content =
                crate::page_blob_utils::get_page_content(self.index_data.as_slice(), *page_to_save);
            return Some((PageBlobPageId::new(*page_to_save), content));
        }

        None
    }

    pub async fn flush_to_storage(&mut self) {
        while let Some((page_id, content)) = self.get_page_content_to_sync_if_needed() {
            self.storage.save(content, &page_id).await;
            self.pages_to_save.remove(&page_id.value);
        }
    }
}
