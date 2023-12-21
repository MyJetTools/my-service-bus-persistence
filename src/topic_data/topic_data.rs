use std::sync::Arc;

use my_service_bus::shared::sub_page::SubPageId;

use crate::{
    archive_storage::ArchiveStorageList,
    index_by_minute::IndexByMinuteList,
    message_pages::{PagesList, SubPage, SubPageInner},
};

use super::topic_data_metrics::TopicDataMetrics;

pub struct TopicData {
    pub topic_id: String,
    pub pages_list: PagesList,
    pub metrics: TopicDataMetrics,
    pub yearly_index_by_minute: IndexByMinuteList,
    pub archive_pages_list: ArchiveStorageList,
}

impl TopicData {
    pub fn new(topic_id: &str) -> Self {
        Self {
            topic_id: topic_id.to_string(),
            pages_list: PagesList::new(),
            metrics: TopicDataMetrics::new(),
            yearly_index_by_minute: IndexByMinuteList::new(),
            archive_pages_list: ArchiveStorageList::new(),
        }
    }

    pub async fn get_sub_page_to_publish_messages(&self, sub_page_id: SubPageId) -> Arc<SubPage> {
        loop {
            let sub_page = self.pages_list.get(sub_page_id).await;

            if let Some(sub_page) = sub_page {
                return sub_page;
            };

            let sub_page_inner = SubPageInner::new(sub_page_id);
            self.pages_list.add_new(sub_page_inner).await;
        }
    }
}
