use super::MessageContentOffset;

pub const TOC_SIZE: usize = 1563 * 512;

pub struct UncompressedFileToc {
    content: Vec<u8>,
    write_position: usize,
}

impl MessageContentOffset {
    pub fn last_position(&self) -> usize {
        self.offset + self.size
    }
}

impl UncompressedFileToc {
    pub fn new() -> Self {
        Self {
            content: Vec::with_capacity(TOC_SIZE),
            write_position: 0,
        }
    }

    pub fn init(&mut self, toc_data: &[u8]) {
        if toc_data.len() != TOC_SIZE {
            panic!(
                "TOC size is not correct. It must be {} but it is {}",
                TOC_SIZE,
                toc_data.len()
            );
        }

        self.content.extend_from_slice(toc_data);

        self.init_write_position();
    }

    fn init_write_position(&mut self) {
        for file_no in 0..100_000 {
            let pos = self.get_position(file_no);
            let last_position = pos.last_position();

            if last_position > self.write_position {
                self.write_position = last_position;
            }
        }
    }

    pub fn get_write_position(&self) -> usize {
        self.write_position
    }

    pub fn reset_as_new(&mut self) -> &[u8] {
        self.content.clear();
        self.content.extend_from_slice([0u8; TOC_SIZE].as_slice());

        self.content.as_slice()
    }

    pub fn update_file_position(
        &mut self,
        no_in_page: usize,
        offset: &MessageContentOffset,
    ) -> usize {
        let toc_pos = no_in_page * 8;

        offset.serialize(&mut self.content[toc_pos..toc_pos + 8]);

        toc_pos / 512
    }

    fn get_position(&self, file_no: usize) -> MessageContentOffset {
        let toc_pos = file_no * 8;
        MessageContentOffset::deserialize(&self.content[toc_pos..toc_pos + 8])
    }
}
