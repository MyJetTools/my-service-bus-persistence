use my_azure_storage_sdk::page_blob::consts::BLOB_PAGE_SIZE;

use crate::uncompressed_page_storage::FileNo;

use super::MessageContentOffset;

pub const TOC_SIZE_IN_PAGES: usize = 1563;
pub const TOC_SIZE: usize = TOC_SIZE_IN_PAGES * 512;

pub struct UncompressedFileToc {
    toc_data: Vec<u8>,
    write_position: usize,
    messages_count: usize,
}

impl UncompressedFileToc {
    pub fn new(toc_data: Vec<u8>) -> Self {
        if toc_data.len() != TOC_SIZE {
            panic!(
                "TOC size is not correct. It must be {} but it is {}",
                TOC_SIZE,
                toc_data.len()
            );
        }

        let mut result = Self {
            toc_data,
            write_position: TOC_SIZE,
            messages_count: 0,
        };

        result.init_write_position();

        result
    }

    fn init_write_position(&mut self) {
        for file_no in 0..100_000 {
            let pos = self.get_position(FileNo::new(file_no));
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
        file_no: usize,
        offset: &MessageContentOffset,
    ) -> Option<usize> {
        let toc_pos = file_no * 8;
        if self.has_content(file_no) {
            return None;
        }

        self.messages_count += 1;

        offset.serialize(&mut self.toc_data[toc_pos..toc_pos + 8]);

        return Some(toc_pos / 512);
    }

    pub fn get_position(&self, file_no: FileNo) -> MessageContentOffset {
        MessageContentOffset::deserialize(file_no.get_toc_range(&self.toc_data))
    }

    pub fn has_content(&self, file_no: usize) -> bool {
        let toc_pos = file_no * 8;

        get_value(&self.toc_data[toc_pos..toc_pos + 4]) != 0
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

    #[test]
    fn test_we_save_toc_data_and_get() {
        let content = vec![0u8; TOC_SIZE];
        let mut toc = UncompressedFileToc::new(content);

        for file_no in 0..100_000 {
            let src_offset = MessageContentOffset {
                offset: file_no + 1,
                size: file_no + 1,
            };

            let res_page_no = toc.update_file_position(file_no, &src_offset);

            assert_eq!(res_page_no.unwrap(), file_no * 8 / 512);

            let result = toc.get_position(FileNo::new(file_no));

            assert_eq!(src_offset.offset, result.offset);
            assert_eq!(src_offset.size, result.size);
        }
    }

    #[test]
    fn test_message_count_is_calculated() {
        let content = vec![0u8; TOC_SIZE];
        let mut toc = UncompressedFileToc::new(content);

        assert_eq!(0, toc.get_messages_count());

        toc.update_file_position(1, &MessageContentOffset { offset: 1, size: 1 });
        assert_eq!(1, toc.get_messages_count());

        toc.update_file_position(1, &MessageContentOffset { offset: 1, size: 1 });
        assert_eq!(1, toc.get_messages_count());
    }
}
