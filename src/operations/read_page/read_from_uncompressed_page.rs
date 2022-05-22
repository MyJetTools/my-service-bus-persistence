use std::sync::Arc;

use my_service_bus_shared::sub_page::*;

use crate::uncompressed_page::*;

pub struct ReadFromUncompressedPage {
    page: Option<Arc<UncompressedPage>>,
    pub page_id: UncompressedPageId,
}

impl ReadFromUncompressedPage {
    pub fn new(page: Option<Arc<UncompressedPage>>, page_id: UncompressedPageId) -> Self {
        Self { page, page_id }
    }

    pub async fn get_sub_page(&self, sub_page_id: &SubPageId) -> Option<Arc<SubPage>> {
        let page = self.page.as_ref()?;
        page.get_or_restore_sub_page(sub_page_id).await
    }
}
