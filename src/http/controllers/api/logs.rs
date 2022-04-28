use my_http_server::{HttpFailResult, HttpOkResult};

use crate::{
    app::AppContext,
    utils::{StopWatch, StringBuilder},
};

pub async fn get(app: &AppContext) -> Result<HttpOkResult, HttpFailResult> {
    let mut sw = StopWatch::new();
    sw.start();
    let logs = app.logs.get().await;

    let mut sb = StringBuilder::new();

    for log_item in logs {
        let line = format!("{} {:?}", log_item.date.to_rfc3339(), log_item.level);
        sb.append_line(&line);

        if let Some(topic_id) = log_item.topic_id {
            sb.append_line(format!("Topic: {}", topic_id).as_str());
        }

        let line = format!("Process: {}", log_item.process);
        sb.append_line(line.as_str());

        sb.append_line(log_item.message.as_str());

        if let Some(err_ctx) = log_item.err_ctx {
            let line = format!("ErrCTX: {}", err_ctx);
            sb.append_line(line.as_str());
        }

        sb.append_line("-----------------------------");
    }

    sw.pause();

    let line = format!("Rendered in {:?}", sw.duration());
    sb.append_line(line.as_str());

    Ok(sb.to_string_utf8().unwrap().into())
}

pub async fn get_by_topic(path: &str, app: &AppContext) -> Result<HttpOkResult, HttpFailResult> {
    let topic_id = path.split('/').last().unwrap();

    let mut sw = StopWatch::new();
    sw.start();
    let logs = app.logs.get_by_topic(topic_id).await;

    if logs.is_none() {
        return Ok("---".to_string().into());
    }

    let logs = logs.unwrap();

    let mut sb = StringBuilder::new();

    for log_item in logs {
        let line = format!("{} {:?}", log_item.date.to_rfc3339(), log_item.level);
        sb.append_line(&line);

        let line = format!("Process: {}", log_item.process);
        sb.append_line(line.as_str());

        sb.append_line(log_item.message.as_str());

        if let Some(err_ctx) = log_item.err_ctx {
            let line = format!("ErrCTX: {}", err_ctx);
            sb.append_line(line.as_str());
        }

        sb.append_line("-----------------------------");
    }

    sw.pause();

    let line = format!("Rendered in {:?}", sw.duration());
    sb.append_line(line.as_str());

    Ok(sb.to_string_utf8().unwrap().into())
}
