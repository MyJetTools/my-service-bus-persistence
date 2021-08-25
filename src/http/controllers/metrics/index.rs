use crate::{
    app::AppContext,
    http::{HttpFailResult, HttpOkResult},
};

pub async fn get(app: &AppContext) -> Result<HttpOkResult, HttpFailResult> {
    let data = app.metrics_keeper.build_prometheus_content();
    Ok(data.into())
}
