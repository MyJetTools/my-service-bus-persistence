use my_azure_storage_sdk::page_blob::consts::BLOB_PAGE_SIZE;

use super::PageBlobPageId;

pub struct PageData {
    content: Vec<u8>,
    page_no: usize,
}

pub struct LastKnownPageCache {
    data: Option<PageData>,
}

impl LastKnownPageCache {
    pub fn new() -> Self {
        Self { data: None }
    }

    pub fn update(&mut self, page_no: usize, content: &[u8]) {
        let pages_in_content = content.len() / BLOB_PAGE_SIZE;

        let last_page_id = page_no + pages_in_content - 1;

        if let Some(ref mut data) = self.data {
            if last_page_id >= data.page_no {
                data.page_no = last_page_id;
                data.content = content[content.len() - BLOB_PAGE_SIZE..content.len()].to_vec();
            }
        } else {
            self.data = Some(PageData {
                content: content[content.len() - BLOB_PAGE_SIZE..content.len()].to_vec(),
                page_no: last_page_id,
            });
        }
    }

    pub fn get_page_cache_content(&self, page_no: usize) -> Option<&[u8]> {
        if let Some(page_data) = self.data.as_ref() {
            if page_data.page_no == page_no {
                return Some(&page_data.content);
            }
        }

        None
    }

    pub fn get_page_no(&self) -> Option<PageBlobPageId> {
        let data = self.data.as_ref()?;
        Some(PageBlobPageId::new(data.page_no))
    }

    pub fn clear(&mut self) {
        self.data = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_writing_to_cache() {
        let mut last_known_page = LastKnownPageCache::new();

        let mut to_write = Vec::new();

        to_write.extend_from_slice([1u8; 512].as_slice());
        to_write.extend_from_slice([2u8; 512].as_slice());

        last_known_page.update(0, to_write.as_slice());

        assert_eq!(last_known_page.get_page_no().unwrap().value, 1);
        assert_eq!(
            last_known_page.get_page_cache_content(1).unwrap(),
            &[2u8; 512]
        );
    }
}
