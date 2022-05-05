use std::collections::HashMap;

use my_service_bus_shared::MessageId;

use crate::{page_blob_random_access::*, typing::*};

use super::{IndexByMinuteStorage, MinuteWithinYear};

pub struct YearlyIndexByMinute {
    pub index_data: Vec<u8>,
    pub year: Year,
    pub pages_to_save: HashMap<usize, ()>,
    storage: IndexByMinuteStorage,
}

impl YearlyIndexByMinute {
    pub async fn new(year: Year, mut storage: IndexByMinuteStorage) -> Self {
        Self {
            year,
            index_data: storage.load().await,
            pages_to_save: HashMap::new(),
            storage,
        }
    }

    pub fn update_minute_index_if_new(
        &mut self,
        minute_witin_year: &MinuteWithinYear,
        message_id: MessageId,
    ) {
        let position_in_file = minute_witin_year.get_position_in_file();

        if self.get_message_id_from_position(position_in_file) != 0 {
            return;
        }

        let dest = &mut self.index_data[position_in_file..position_in_file + 8];

        dest.copy_from_slice(message_id.to_le_bytes().as_slice());

        let page_id = PageBlobPageId::from_blob_position(position_in_file);

        self.pages_to_save.insert(page_id.value, ());
    }

    fn get_message_id_from_position(&self, position_in_file: usize) -> MessageId {
        let mut page_id_in_file = [0u8; 8];
        page_id_in_file.copy_from_slice(&self.index_data[position_in_file..position_in_file + 8]);

        MessageId::from_le_bytes(page_id_in_file)
    }

    fn get_page_content_to_sync_if_needed(&self) -> Option<(PageBlobPageId, Vec<u8>)> {
        for page_to_save in self.pages_to_save.keys() {
            let content = crate::page_blob_random_access::utils::get_page_content(
                self.index_data.as_slice(),
                *page_to_save,
            );
            return Some((PageBlobPageId::new(*page_to_save), content.to_vec()));
        }

        None
    }

    pub fn get_message_id(&self, minute_witin_year: &MinuteWithinYear) -> Option<MessageId> {
        let position_in_file = minute_witin_year.get_position_in_file();
        let result = self.get_message_id_from_position(position_in_file);

        if result == 0 {
            None
        } else {
            Some(result)
        }
    }

    pub async fn flush_to_storage(&mut self) {
        while let Some((page_id, content)) = self.get_page_content_to_sync_if_needed() {
            self.storage.save(content.as_slice(), &page_id).await;
            self.pages_to_save.remove(&page_id.value);
        }
    }
}

#[cfg(test)]
mod test {

    use std::sync::Arc;

    use my_azure_storage_sdk::{page_blob::AzurePageBlobStorage, AzureStorageConnection};

    use super::*;

    #[tokio::test]
    async fn test_1() {
        let azure_conn_string = AzureStorageConnection::new_in_memory();
        let page_blob = AzurePageBlobStorage::new(
            Arc::new(azure_conn_string),
            "test".to_string(),
            "test".to_string(),
        )
        .await;

        let random_file_access = PageBlobRandomAccess::open_or_create(page_blob, 1024).await;

        let storage = IndexByMinuteStorage::new(random_file_access);
        let mut index_by_year = YearlyIndexByMinute::new(2020, storage).await;

        let minute = MinuteWithinYear::new(5);
        index_by_year.update_minute_index_if_new(&minute, 15);
        index_by_year.update_minute_index_if_new(&minute, 16);

        let message_id = index_by_year.get_message_id(&minute);

        assert_eq!(message_id.unwrap(), 15);

        //Check that page to persist has changes
        assert_eq!(index_by_year.pages_to_save.contains_key(&0), true);
    }

    #[tokio::test]
    async fn test_2() {
        let azure_conn_string = AzureStorageConnection::new_in_memory();
        let page_blob = AzurePageBlobStorage::new(
            Arc::new(azure_conn_string),
            "test".to_string(),
            "test".to_string(),
        )
        .await;

        let random_file_access = PageBlobRandomAccess::open_or_create(page_blob, 1024).await;

        let storage = IndexByMinuteStorage::new(random_file_access);
        let mut index_by_year = YearlyIndexByMinute::new(2020, storage).await;

        let minute = MinuteWithinYear::new(5);
        index_by_year.update_minute_index_if_new(&minute, 15);

        //Like we did it from timer
        index_by_year.flush_to_storage().await;

        let result = index_by_year.storage.load().await;

        assert_eq!(result[40], 15);
        assert_eq!(result[41], 0);
        assert_eq!(result[42], 0);
        assert_eq!(result[43], 0);
    }
}
