#[derive(Clone, Copy)]
pub struct MinuteWithinYear {
    pub minute: usize,
}

impl MinuteWithinYear {
    pub fn new(minute: u32) -> Self {
        Self {
            minute: minute as usize,
        }
    }

    pub fn get_position_in_file(&self) -> usize {
        self.minute * 8
    }
}
