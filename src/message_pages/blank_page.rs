use rust_extensions::date_time::DateTimeAsMicroseconds;

pub struct BlankPage {
    pub last_access: DateTimeAsMicroseconds,
}

impl BlankPage {
    pub fn new() -> BlankPage {
        Self {
            last_access: DateTimeAsMicroseconds::now(),
        }
    }
}
