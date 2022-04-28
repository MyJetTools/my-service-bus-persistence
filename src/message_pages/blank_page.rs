use my_service_bus_shared::page_id::PageId;
use rust_extensions::date_time::DateTimeAsMicroseconds;

pub struct BlankPage {
    pub last_access: DateTimeAsMicroseconds,
    pub page_id: PageId,
}

impl BlankPage {
    pub fn new(page_id: PageId) -> BlankPage {
        Self {
            last_access: DateTimeAsMicroseconds::now(),
            page_id,
        }
    }
}
