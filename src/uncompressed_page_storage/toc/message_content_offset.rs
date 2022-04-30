pub struct MessageContentOffset {
    pub offset: usize,
    pub size: usize,
}

impl MessageContentOffset {
    pub fn serialize(&self, dest: &mut [u8]) {
        serialize_value(self.offset as u32, &mut dest[0..4]);
        serialize_value(self.size as u32, &mut dest[4..8]);
    }

    pub fn deserialize(src: &[u8]) -> Self {
        let offset = deserialize_value(&src[0..4]) as usize;
        let size = deserialize_value(&src[4..8]) as usize;

        Self { offset, size }
    }

    pub fn has_data(&self) -> bool {
        self.offset > 0
    }
}

fn serialize_value(value: u32, dest: &mut [u8]) {
    dest.copy_from_slice(value.to_le_bytes().as_slice());
}

fn deserialize_value(src: &[u8]) -> u32 {
    let mut result = [0u8; 4];

    result.copy_from_slice(src);

    u32::from_le_bytes(result)
}
