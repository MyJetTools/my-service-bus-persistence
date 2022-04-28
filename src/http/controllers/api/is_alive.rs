use std::{sync::Arc, time::SystemTime};

use my_http_server::{HttpFailResult, HttpOkResult, HttpOutput};
use serde::{Deserialize, Serialize};

use crate::app::AppContext;

pub struct IsAliveAction {
    app: Arc<AppContext>,
}

impl IsAliveAction {
    pub fn new(app: Arc<AppContext>) -> Self {
        Self { app }
    }
}

pub fn handle_request(app: &AppContext) -> Result<HttpOkResult, HttpFailResult> {
    let version = env!("CARGO_PKG_VERSION");

    let time = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let model = ApiModel {
        name: "MyServiceBus.Persistence".to_string(),
        time: time,
        version: version.to_string(),
        env_info: app.get_env_info(),
    };

    let result = HttpOutput::as_json(model).into_ok_result(true);

    return Ok(result);
}

#[derive(Serialize, Deserialize, Debug)]
struct ApiModel {
    name: String,
    time: u64,
    version: String,
    env_info: String,
}
