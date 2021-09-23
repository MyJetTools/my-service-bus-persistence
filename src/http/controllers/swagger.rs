use std::collections::HashMap;

use crate::http::{HttpContext, HttpFailResult, HttpOkResult};

pub async fn handle(ctx: HttpContext) -> Result<HttpOkResult, HttpFailResult> {
    let path = ctx.get_path();
    let scheme = ctx.get_scheme();

    let host = ctx.get_host();

    if path == "/swagger" {
        let new_url = format!("{}://{}/swagger/Index.html", scheme, host);
        return Ok(HttpOkResult::Redirect { url: new_url });
    }

    if path == "/swagger/v1/swagger.json" {
        let mut placehloders = HashMap::new();

        placehloders.insert("SCHEME", ctx.get_scheme());

        placehloders.insert("HOST", host.to_string());

        placehloders.insert("VERSION", crate::app::APP_VERSION.to_string());

        return super::files::serve_file_with_placeholders(path, None, &placehloders).await;
    }

    if path.starts_with("/swagger") {
        return super::files::get_content_from_file(path, None).await;
    }

    let new_url = format!("{}://{}/swagger/Index.html", scheme, host);
    return Ok(HttpOkResult::Redirect { url: new_url });
}
