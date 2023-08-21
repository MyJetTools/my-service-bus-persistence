use my_azure_storage_sdk::page_blob::AzurePageBlobStorage;
use my_service_bus_abstractions::MessageId;
use rust_extensions::date_time::AtomicDateTimeAsMicroseconds;

use crate::typing::*;

use super::{IndexByMinutePageBlob, MinuteWithinYear, UpdateQueue};

pub struct YearlyIndexByMinute {
    pub year: Year,
    page_blob: IndexByMinutePageBlob,
    update_queue: UpdateQueue,
    pub last_access: AtomicDateTimeAsMicroseconds,
}

impl YearlyIndexByMinute {
    pub async fn open_or_create(year: Year, page_blob: AzurePageBlobStorage) -> Self {
        let page_blob = IndexByMinutePageBlob::new(page_blob);

        page_blob.init_index_by_minute().await;

        Self {
            year,
            page_blob,
            update_queue: UpdateQueue::new(),
            last_access: AtomicDateTimeAsMicroseconds::now(),
        }
    }
    #[cfg(test)]
    pub fn get_page_blob(
        &self,
    ) -> &my_azure_page_blob_random_access::PageBlobRandomAccess<
        my_azure_page_blob_ext::MyAzurePageBlobStorageWithRetries,
    > {
        self.page_blob.get_page_blob()
    }

    pub async fn load_if_exists(year: Year, page_blob: AzurePageBlobStorage) -> Option<Self> {
        let page_blob = IndexByMinutePageBlob::new(page_blob);
        page_blob.check_index_by_minute_blob().await?;

        Self {
            year,
            page_blob,
            update_queue: UpdateQueue::new(),
            last_access: AtomicDateTimeAsMicroseconds::now(),
        }
        .into()
    }

    pub async fn update_minute_index_if_new(
        &self,
        minute_within_year: MinuteWithinYear,
        message_id: MessageId,
    ) {
        self.update_queue
            .update(minute_within_year, message_id)
            .await;
    }

    pub async fn get_message_id(&self, minute_within_year: MinuteWithinYear) -> Option<MessageId> {
        if let Some(result) = self.update_queue.get(minute_within_year).await {
            return Some(result);
        }

        self.page_blob
            .read_message_id_from_minute_index(minute_within_year)
            .await
    }

    pub async fn flush_to_storage(&self) {
        let items_to_write = self.update_queue.get_items_ready_to_be_gc().await;

        if items_to_write.is_none() {
            return;
        }

        for minute in items_to_write.unwrap() {
            let minute = (minute).into();

            if let Some(message_id) = self.update_queue.remove(minute).await {
                self.write_to_blob(minute, message_id).await;
            }
        }
    }

    pub async fn write_everything_before_gc(&self) {
        while let Some((minute, message_id)) = self.update_queue.remove_first_element().await {
            self.write_to_blob(minute, message_id).await;
        }
    }

    async fn write_to_blob(&self, minute: MinuteWithinYear, message_id: MessageId) {
        let value = self
            .page_blob
            .read_message_id_from_minute_index(minute)
            .await;

        if value.is_some() {
            return;
        }

        self.page_blob
            .write_message_id_to_minute_index(minute, message_id)
            .await;
    }
}

#[cfg(test)]
mod tests {

    use my_azure_storage_sdk::{page_blob::AzurePageBlobStorage, AzureStorageConnection};
    use my_service_bus_abstractions::MessageId;

    use crate::index_by_minute::{MinuteWithinYear, YearlyIndexByMinute};

    #[tokio::test]
    async fn test_open_not_existing() {
        let connection = AzureStorageConnection::new_in_memory();
        let page_blob = AzurePageBlobStorage::new(connection.into(), "test", "test").await;

        // let page_blob = PageBlobRandomAccess::new(page_blob, true, 512);

        let index = YearlyIndexByMinute::load_if_exists(2021, page_blob).await;

        assert_eq!(index.is_none(), true);
    }

    #[tokio::test]
    async fn test_we_already_written() {
        let connection = AzureStorageConnection::new_in_memory();
        let page_blob = AzurePageBlobStorage::new(connection.into(), "test", "test").await;

        let index = YearlyIndexByMinute::open_or_create(2021, page_blob).await;

        //Writing First element
        let minute = MinuteWithinYear::new(0);

        let message_id = MessageId::new(15);

        index.update_minute_index_if_new(minute, message_id).await;

        let minute = MinuteWithinYear::new(1);

        //Writing Second element

        let message_id = MessageId::new(16);

        index.update_minute_index_if_new(minute, message_id).await;

        index.flush_to_storage().await;

        let mut result = index.get_page_blob().read(0, 4).await.unwrap();

        let result = result.read_i64();

        assert_eq!(result, 15);
    }
}
