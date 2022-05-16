use my_azure_storage_sdk::page_blob::consts::BLOB_PAGE_SIZE;

use crate::page_blob_random_access::{PageBlobPageId, PageBlobRandomAccess};

use super::{ContentOffset, PayloadNo};

pub struct FileToc {
    toc_data: Vec<u8>,
    write_position: usize,
    messages_count: usize,
    max_toc_elements: usize,
}

impl FileToc {
    pub async fn read_toc(
        page_blob: &mut PageBlobRandomAccess,
        toc_size_in_pages: usize,
        toc_size: usize,
        max_toc_elements: usize,
    ) -> FileToc {
        let size = page_blob.get_blob_size(Some(toc_size_in_pages)).await;

        if size < toc_size {
            page_blob.resize_blob(toc_size_in_pages).await;
        }

        let content = page_blob
            .load_pages(
                &PageBlobPageId::new(0),
                toc_size_in_pages,
                Some(toc_size_in_pages),
            )
            .await;

        FileToc::new(content, toc_size, max_toc_elements)
    }

    fn new(toc_data: Vec<u8>, toc_size: usize, max_toc_elements: usize) -> Self {
        if toc_data.len() != toc_size {
            panic!(
                "TOC size is not correct. It must be {} but it is {}",
                toc_size,
                toc_data.len()
            );
        }

        let mut result = Self {
            toc_data,
            write_position: toc_size,
            messages_count: 0,
            max_toc_elements,
        };

        result.init_write_position();

        result
    }

    fn init_write_position(&mut self) {
        for file_no in 0..self.max_toc_elements {
            let pos = self.get_position(&PayloadNo::new(file_no));
            let last_position = pos.last_position();

            if last_position > self.write_position {
                self.write_position = last_position;
            }

            if pos.offset > 0 {
                self.messages_count += 1;
            }
        }
    }

    pub fn get_write_position(&self) -> usize {
        self.write_position
    }

    pub fn increase_write_position(&mut self, delta: usize) {
        self.write_position += delta;
    }

    pub fn update_file_position(
        &mut self,
        payload_no: &PayloadNo,
        offset: &ContentOffset,
    ) -> Option<usize> {
        if self.has_content(payload_no) {
            return None;
        }

        let toc_offset = payload_no.get_toc_offset();
        self.messages_count += 1;

        offset.serialize(&mut self.toc_data[toc_offset..toc_offset + 8]);

        return Some(toc_offset / 512);
    }

    pub fn get_position(&self, payload_no: &PayloadNo) -> ContentOffset {
        let toc_pos = payload_no.get_toc_offset();
        ContentOffset::deserialize(&self.toc_data[toc_pos..toc_pos + 8])
    }

    pub fn has_content(&self, payload_no: &PayloadNo) -> bool {
        let toc_offset = payload_no.get_toc_offset();

        get_value(&self.toc_data[toc_offset..toc_offset + 4]) != 0
    }

    pub fn get_toc_pages(&self, page_from: usize, pages_amount: usize) -> &[u8] {
        let start_pos = page_from * BLOB_PAGE_SIZE;
        let end_pos = start_pos + pages_amount * BLOB_PAGE_SIZE;
        &self.toc_data[start_pos..end_pos]
    }

    pub fn get_messages_count(&self) -> usize {
        self.messages_count
    }
}

fn get_value(src: &[u8]) -> i32 {
    let mut result = [0u8; 4];

    result.copy_from_slice(src);

    i32::from_le_bytes(result)
}

#[cfg(test)]
mod test {
    use super::*;

    const MAX_MESSAGES_AMOUNT: usize = 100_000;

    const TOC_SIZE: usize = MAX_MESSAGES_AMOUNT * 8;

    #[test]
    fn test_we_save_toc_data_and_get() {
        let content = vec![0u8; TOC_SIZE];
        let mut toc = FileToc::new(content, TOC_SIZE, MAX_MESSAGES_AMOUNT);

        for file_no in 0..100_000 {
            let src_offset = ContentOffset {
                offset: file_no + 1,
                size: file_no + 1,
            };

            let res_page_no = toc.update_file_position(&PayloadNo::new(file_no), &src_offset);

            assert_eq!(res_page_no.unwrap(), file_no * 8 / 512);

            let result = toc.get_position(&PayloadNo::new(file_no));

            assert_eq!(src_offset.offset, result.offset);
            assert_eq!(src_offset.size, result.size);
        }
    }

    #[test]
    fn test_message_count_is_calculated() {
        let content = vec![0u8; TOC_SIZE];
        let mut toc = FileToc::new(content, TOC_SIZE, MAX_MESSAGES_AMOUNT);

        assert_eq!(0, toc.get_messages_count());

        toc.update_file_position(&PayloadNo::new(1), &ContentOffset { offset: 1, size: 1 });
        assert_eq!(1, toc.get_messages_count());

        toc.update_file_position(&PayloadNo::new(1), &ContentOffset { offset: 1, size: 1 });
        assert_eq!(1, toc.get_messages_count());
    }
}
