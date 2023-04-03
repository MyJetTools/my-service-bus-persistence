#[derive(Clone, Copy)]
pub struct MinuteWithinYear(usize);

impl MinuteWithinYear {
    pub fn new(minute: u32) -> Self {
        Self(minute as usize)
    }

    pub fn get_position_in_file(&self) -> usize {
        self.0 * 8
    }

    #[cfg(test)]
    pub fn get_value(&self) -> usize {
        self.0
    }
}
