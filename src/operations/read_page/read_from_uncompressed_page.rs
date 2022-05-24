use std::sync::Arc;

use my_service_bus_shared::sub_page::*;

use crate::{topic_data::TopicData, uncompressed_page::*};

pub struct ReadFromUncompressedPage {
    page: Option<Arc<UncompressedPage>>,
    pub page_id: UncompressedPageId,
}

impl ReadFromUncompressedPage {
    pub fn new(page: Option<Arc<UncompressedPage>>, page_id: UncompressedPageId) -> Self {
        Self { page, page_id }
    }

    pub async fn get_sub_page(
        &self,
        topic_data: &TopicData,
        sub_page_id: &SubPageId,
    ) -> Option<Arc<SubPage>> {
        let page = self.page.as_ref()?;

        let message_id = topic_data.get_current_message_id();

        let current_sub_page_id = SubPageId::from_message_id(message_id);

        let result = page.get_or_restore_sub_page(sub_page_id).await?;

        if current_sub_page_id.value != sub_page_id.value {
            page.gc_sub_pages(&current_sub_page_id, Some(sub_page_id))
                .await;
        }

        return Some(result);
    }
}
