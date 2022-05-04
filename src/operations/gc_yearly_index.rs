use std::collections::HashMap;

use rust_extensions::date_time::DateTimeAsMicroseconds;

use crate::{
    app::AppContext, index_by_minute::YearlyIndexByMinute, topic_data::TopicData, typing::Year,
};

pub async fn gc_yearly_index(app: &AppContext, topic_data: &TopicData) {
    let now = DateTimeAsMicroseconds::now();

    let (_, year) = app.index_by_minute_utils.get_minute_within_the_year(now);

    let mut yearly_index = topic_data.yearly_index_by_minute.lock().await;

    while let Some(year_to_gc) = get_years_to_gc(&yearly_index, year) {
        yearly_index.remove(&year_to_gc);
        app.logs.add_info(
            Some(topic_data.topic_id.as_str()),
            "GC Yearly index",
            format!("Year index for year {} is GCed", year),
        )
    }
}

fn get_years_to_gc(years: &HashMap<u32, YearlyIndexByMinute>, now_year: Year) -> Option<Year> {
    for year in years.keys() {
        if *year < now_year {
            return Some(*year);
        }
    }

    None
}
