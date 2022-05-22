use std::{collections::HashMap, sync::Arc};

use my_service_bus_shared::page_id::PageId;
use tokio::sync::Mutex;

use super::{UncompressedPage, UncompressedPageId};

pub struct UncompressedPagesList {
    pub pages: Mutex<HashMap<PageId, Arc<UncompressedPage>>>,
}

impl UncompressedPagesList {
    pub fn new() -> Self {
        Self {
            pages: Mutex::new(HashMap::new()),
        }
    }

    pub async fn add(&self, page_id: PageId, page: Arc<UncompressedPage>) {
        self.pages.lock().await.insert(page_id, page);
    }

    pub async fn get(&self, page_id: &UncompressedPageId) -> Option<Arc<UncompressedPage>> {
        let pages_access = self.pages.lock().await;
        let result = pages_access.get(&page_id.value)?;
        Some(result.clone())
    }

    pub async fn remove_page(&self, page_id: PageId) -> Option<Arc<UncompressedPage>> {
        let mut pages_access = self.pages.lock().await;
        pages_access.remove(&page_id)
    }

    pub async fn get_all(&self) -> Vec<Arc<UncompressedPage>> {
        let mut result = Vec::new();
        let read_access = self.pages.lock().await;

        for page in read_access.values() {
            let itm = page.clone();
            result.push(itm);
        }

        result
    }

    pub async fn get_pages_with_data_to_save(&self) -> Vec<Arc<UncompressedPage>> {
        let mut result = Vec::new();

        let pages_access = self.pages.lock().await;

        for page in pages_access.values() {
            if page.new_messages.get_count().await > 0 {
                result.push(page.clone());
            }
        }

        return result;
    }

    pub async fn has_messages_to_save(&self) -> bool {
        let pages_access = self.get_all().await;

        for page in pages_access {
            if page.new_messages.get_count().await > 0 {
                return true;
            }
        }

        false
    }
}
