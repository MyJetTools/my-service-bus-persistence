use my_service_bus_shared::date_time::DateTimeAsMicroseconds;

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
