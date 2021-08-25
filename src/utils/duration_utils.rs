use std::time::Duration;

pub fn duration_to_string(d: Duration) -> String {
    let secs = d.as_secs();
    if secs < 60 {
        return format!("{:?}", d);
    }

    return format_duration(secs);
}

fn format_duration(mut secs: u64) -> String {
    let days = secs / 3600 * 24;

    secs = secs - days * 3600 * 24;

    let hours = secs / 3600;

    secs = secs - hours * 3600;

    let mins = secs / 60;

    secs = secs - mins * 60;

    if days > 0 {
        return format!("{}:{:02}:{:02}:{:02}", days, hours, mins, secs);
    } else {
        return format!("{:02}:{:02}:{:02}", hours, mins, secs);
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_format_duration() {
        assert_eq!("00:01:00", format_duration(60));
        assert_eq!("00:01:01", format_duration(61));
    }
}
