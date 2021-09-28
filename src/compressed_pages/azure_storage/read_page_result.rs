pub struct ReadPageResult {
    pub data: Vec<u8>,
    pub size: usize,
}

impl ReadPageResult {
    pub fn get_result(&self) -> &[u8] {
        return &self.data[..self.size];
    }
}
