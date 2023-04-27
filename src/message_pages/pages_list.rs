use std::{collections::BTreeMap, sync::Arc};

use my_service_bus_shared::sub_page::{SizeAndAmount, SubPageId};
use tokio::sync::Mutex;

use super::{SubPage, SubPageInner};

pub struct PagesList {
    pub sub_pages: Mutex<BTreeMap<i64, Arc<SubPage>>>,
}

impl PagesList {
    pub fn new() -> Self {
        Self {
            sub_pages: Mutex::new(BTreeMap::new()),
        }
    }

    pub async fn add_new(&self, sub_page_inner: SubPageInner) {
        let sub_page_id = sub_page_inner.sub_page_id.get_value();

        self.sub_pages
            .lock()
            .await
            .insert(sub_page_id, Arc::new(SubPage::create_new(sub_page_inner)));
    }

    pub async fn restore_from_archive(&self, sub_page: SubPage) {
        self.sub_pages
            .lock()
            .await
            .insert(sub_page.get_id().get_value(), sub_page.into());
    }

    pub async fn add_missing(&self, sub_page_id: SubPageId) {
        self.sub_pages.lock().await.insert(
            sub_page_id.get_value(),
            SubPage::create_missing(sub_page_id).into(),
        );
    }

    pub async fn get(&self, sub_page_id: SubPageId) -> Option<Arc<SubPage>> {
        let pages_access = self.sub_pages.lock().await;
        let result = pages_access.get(sub_page_id.as_ref())?;
        Some(result.clone())
    }

    pub async fn get_all(&self) -> Vec<Arc<SubPage>> {
        let mut result = Vec::new();
        let read_access = self.sub_pages.lock().await;

        for page in read_access.values() {
            let itm = page.clone();
            result.push(itm);
        }

        result
    }

    pub async fn gc(&self) -> Option<Arc<SubPage>> {
        let mut pages_access = self.sub_pages.lock().await;

        if pages_access.len() <= 1 {
            return None;
        }

        let first_key = pages_access.keys().next().unwrap().clone();
        pages_access.remove(&first_key)
    }

    pub async fn get_active_sub_page(&self) -> Option<Arc<SubPage>> {
        let read_access = self.sub_pages.lock().await;
        let last_key = read_access.keys().last().unwrap().clone();
        let result = read_access.get(&last_key).unwrap().clone();
        Some(result)
    }

    pub async fn get_messages_amount_to_save(&self) -> SizeAndAmount {
        let mut result = SizeAndAmount::new();
        let read_access = self.sub_pages.lock().await;
        for sub_page in read_access.values() {
            if sub_page.is_active() {
                let size_and_amount = sub_page.get_size_and_amount().await;
                result.size += size_and_amount.size;
                result.amount += size_and_amount.amount;
            }
        }

        result
    }
}
