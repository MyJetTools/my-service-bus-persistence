use super::MessageContentOffset;

pub const TOC_SIZE_IN_PAGES: usize = 1563;
pub const TOC_SIZE: usize = TOC_SIZE_IN_PAGES * 512;

pub struct UncompressedFileToc {
    toc_data: Vec<u8>,
    write_position: usize,
    messages_count: usize,
}

impl MessageContentOffset {
    pub fn last_position(&self) -> usize {
        self.offset + self.size
    }
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
            write_position: 0,
            messages_count: 0,
        };

        result.init_write_position();

        result
    }

    fn init_write_position(&mut self) {
        for file_no in 0..100_000 {
            let pos = self.get_position(file_no);
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

    pub fn reset_as_new(&mut self) -> &[u8] {
        self.toc_data.clear();
        self.toc_data.extend_from_slice([0u8; TOC_SIZE].as_slice());

        self.toc_data.as_slice()
    }

    pub fn update_file_position(
        &mut self,
        no_in_page: usize,
        offset: &MessageContentOffset,
    ) -> usize {
        let toc_pos = no_in_page * 8;

        offset.serialize(&mut self.toc_data[toc_pos..toc_pos + 8]);

        toc_pos / 512
    }

    fn get_position(&self, file_no: usize) -> MessageContentOffset {
        let toc_pos = file_no * 8;
        MessageContentOffset::deserialize(&self.toc_data[toc_pos..toc_pos + 8])
    }

    pub fn has_content(&self, file_no: usize) -> bool {
        let toc_pos = file_no * 8;

        get_value(&self.toc_data[toc_pos..toc_pos + 4]) != 0
    }
}

fn get_value(src: &[u8]) -> i32 {
    let mut result = [0u8; 4];

    result.copy_from_slice(src);

    i32::from_le_bytes(result)
}
