pub struct PageAllocationIndex {
    pub start_page: usize,
    pub data_len: usize,
}

impl PageAllocationIndex {
    pub fn parse(src: &[u8]) -> Self {
        let mut position_bytes = [0u8; 4];

        position_bytes.copy_from_slice(&src[..4]);

        let position = u32::from_le_bytes(position_bytes);

        let mut len_bytes = [0u8; 4];

        len_bytes.copy_from_slice(&src[4..8]);

        let len = u32::from_le_bytes(len_bytes);

        Self {
            start_page: position as usize,
            data_len: len as usize,
        }
    }

    pub fn copy_to_slice(&self, dest: &mut [u8]) {
        let start_page = self.start_page as u32;
        &dest[..4].copy_from_slice(&start_page.to_le_bytes());

        let data_len = self.data_len as u32;
        &dest[4..8].copy_from_slice(&data_len.to_le_bytes());
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_serializer_deserializer() {
        let src = PageAllocationIndex {
            start_page: 15,
            data_len: 16,
        };

        let mut buffer = [0u8; 8];

        src.copy_to_slice(&mut buffer);

        let dest = PageAllocationIndex::parse(&buffer);

        assert_eq!(src.start_page, dest.start_page);
        assert_eq!(src.data_len, dest.data_len);
    }
}
