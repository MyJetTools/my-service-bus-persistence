pub struct FileRandomAccessInMem {
    content: Vec<u8>,
    pos: usize,
}

impl FileRandomAccessInMem {
    pub fn new() -> Self {
        Self {
            content: Vec::new(),
            pos: 0,
        }
    }

    pub fn write_to_file(&mut self, content: &[u8]) -> std::io::Result<()> {
        while self.content.len() - self.pos >= content.len() {
            let dest = &mut self.content[self.pos..self.pos + content.len()];
            dest.copy_from_slice(content);
            return Ok(());
        }

        while self.content.len() > self.pos {
            self.content.remove(self.content.len() - 1);
        }

        if self.content.len() == self.pos {
            self.content.extend_from_slice(content);
        }

        self.pos += content.len();

        Ok(())
    }

    pub fn read_from_file(&mut self, dest: &mut [u8]) -> std::io::Result<()> {
        let src = &self.content[self.pos..self.pos + dest.len()];
        dest.copy_from_slice(src);

        self.pos += dest.len();
        Ok(())
    }

    pub fn set_position(&mut self, position: usize) -> std::io::Result<()> {
        if position >= self.content.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "position out of range",
            ));
        }

        self.pos = position;
        Ok(())
    }

    pub fn reduce_size(&mut self, len: usize) -> Result<(), std::io::Error> {
        if self.content.len() > len {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "reduce_len error. Size can not be increased".to_string(),
            ));
        }

        while self.content.len() > len {
            self.content.remove(self.content.len() - 1);
        }

        if self.pos > len {
            self.pos = len;
        }

        Ok(())
    }
}
