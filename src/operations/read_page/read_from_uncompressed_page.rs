use std::sync::Arc;

use crate::{
    sub_page::{SubPage, SubPageId},
    uncompressed_page::*,
};

pub struct ReadFromUncompressedPage {
    page: Option<Arc<UncompressedPage>>,
    pub page_id: UncompressedPageId,
}

impl ReadFromUncompressedPage {
    pub fn new(page: Option<Arc<UncompressedPage>>, page_id: UncompressedPageId) -> Self {
        Self { page, page_id }
    }

    pub fn get_sub_page(&self, sub_page_id: &SubPageId) -> Option<Arc<SubPage>> {
        let page = self.page.as_ref()?;

        todo!("Implement")
    }
}
