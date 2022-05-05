use my_azure_storage_sdk::page_blob::consts::BLOB_PAGE_SIZE;

use super::PageBlobPageId;

pub struct RandomAccessData {
    data: Option<Vec<u8>>,
    pos_start: usize,
    len: usize,
}
impl RandomAccessData {
    pub fn new(pos_start: usize, len: usize) -> Self {
        Self {
            data: None,
            pos_start,
            len,
        }
    }

    pub fn new_empty() -> Self {
        return Self {
            data: Some(vec![]),
            pos_start: 0,
            len: 0,
        };
    }

    pub fn as_slice(&self) -> &[u8] {
        if let Some(content) = self.data.as_ref() {
            let payload_offset = self.get_payload_offset();
            return &content[payload_offset..payload_offset + self.len];
        }

        panic!("Payload is not initialized");
    }

    pub fn get_start_page_id(&self) -> PageBlobPageId {
        PageBlobPageId::from_blob_position(self.pos_start)
    }

    pub fn get_payload_offset(&self) -> usize {
        self.pos_start - self.get_start_page_id().value * BLOB_PAGE_SIZE
    }

    pub fn get_pages_amout(&self) -> usize {
        let start_paget_id = self.get_start_page_id();
        let page_start_position = start_paget_id.value * BLOB_PAGE_SIZE;

        let offset = self.pos_start - page_start_position;

        let len = self.len + offset;

        (len - 1) / BLOB_PAGE_SIZE + 1
    }

    pub fn assign_content(&mut self, content: Vec<u8>) {
        self.data = Some(content);
    }

    pub fn assign_write_content(&mut self, first_page: Option<Vec<u8>>, content: &[u8]) {
        let len = self.get_pages_amout() * BLOB_PAGE_SIZE;
        let mut result = Vec::with_capacity(len);

        let payload_offset = self.get_payload_offset();
        if let Some(first_page) = first_page.as_ref() {
            result.extend_from_slice(&first_page[..payload_offset]);
        } else {
            while result.len() < payload_offset {
                result.push(0)
            }
        }

        result.extend_from_slice(content);

        self.data = Some(result);
    }

    pub fn make_payload_size_complient(&mut self) {
        let data = self.data.as_mut().unwrap();

        let pages_amount = super::utils::get_pages_amount(data.len(), BLOB_PAGE_SIZE);

        let complient_size = pages_amount * BLOB_PAGE_SIZE;

        while data.len() < complient_size {
            data.push(0);
        }
    }

    pub fn get_whole_payload(&self) -> &[u8] {
        if let Some(payload) = self.data.as_ref() {
            return payload.as_slice();
        }

        panic!("Payload is not initialized");
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    pub fn test_fiest_page_everything_in_the_same_page() {
        let mut pos_start = 2;
        for page_no in 0..5 {
            let rad = RandomAccessData::new(pos_start, 2);

            assert_eq!(rad.get_start_page_id().value, page_no);
            assert_eq!(rad.get_pages_amout(), 1);
            assert_eq!(rad.get_payload_offset(), 2);

            pos_start += 512;
        }
    }
    #[test]
    pub fn test_we_cover_whole_page() {
        let mut pos_start = 0;
        for page_no in 0..5 {
            let rad = RandomAccessData::new(pos_start, 512);

            assert_eq!(rad.get_start_page_id().value, page_no);
            assert_eq!(rad.get_pages_amout(), 1);
            assert_eq!(rad.get_payload_offset(), 0);

            pos_start += 512;
        }
    }

    #[test]
    pub fn test_we_cover_not_whole_page_but_last_byte() {
        let mut pos_start = 2;
        for page_no in 0..5 {
            let rad = RandomAccessData::new(pos_start, 510);

            assert_eq!(rad.get_start_page_id().value, page_no);
            assert_eq!(rad.get_pages_amout(), 1);
            assert_eq!(rad.get_payload_offset(), 2);

            pos_start += 512;
        }
    }

    #[test]
    pub fn test_we_cover_two_whole_pages() {
        let mut pos_start = 0;
        for page_no in 0..5 {
            let rad = RandomAccessData::new(pos_start, 512 * 2);

            assert_eq!(rad.get_start_page_id().value, page_no);
            assert_eq!(rad.get_pages_amout(), 2);
            assert_eq!(rad.get_payload_offset(), 0);

            pos_start += 512;
        }
    }

    #[test]
    pub fn test_we_cover_not_two_whole_pages_but_last_byte_at_last_page() {
        let mut pos_start = 2;
        for page_no in 0..5 {
            let rad = RandomAccessData::new(pos_start, 512 * 2 - 2);

            assert_eq!(rad.get_start_page_id().value, page_no);
            assert_eq!(rad.get_pages_amout(), 2);
            assert_eq!(rad.get_payload_offset(), 2);

            pos_start += 512;
        }
    }

    #[test]
    pub fn test_we_cover_not_two_whole_pages_but_last_byte_at_not_the_last_at_last_page() {
        let mut pos_start = 2;
        for page_no in 0..5 {
            let rad = RandomAccessData::new(pos_start, 512 * 2 - 5);

            assert_eq!(rad.get_start_page_id().value, page_no);
            assert_eq!(rad.get_pages_amout(), 2);
            assert_eq!(rad.get_payload_offset(), 2);

            pos_start += 512;
        }
    }

    #[test]
    pub fn test_we_cover_two_pages_at_the_end_of_first_page() {
        let mut pos_start = 511;
        for page_no in 0..5 {
            let rad = RandomAccessData::new(pos_start, 5);

            assert_eq!(rad.get_start_page_id().value, page_no);
            assert_eq!(rad.get_pages_amout(), 2);
            assert_eq!(rad.get_payload_offset(), 511);

            pos_start += 512;
        }
    }
}
