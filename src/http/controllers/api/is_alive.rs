use std::time::SystemTime;

use crate::{
    app::AppContext,
    http::{HttpFailResult, HttpOkResult},
};
use serde::{Deserialize, Serialize};

pub fn get(app: &AppContext) -> Result<HttpOkResult, HttpFailResult> {
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

    return Ok(HttpOkResult::create_json_response(model));
}

#[derive(Serialize, Deserialize, Debug)]
struct ApiModel {
    name: String,
    time: u64,
    version: String,
    env_info: String,
}
