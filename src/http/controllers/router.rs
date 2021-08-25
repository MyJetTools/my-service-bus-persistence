use hyper::{Body, Method, Request};

use crate::{
    app::AppContext,
    http::{web_content_type::WebContentType, HttpContext, HttpFailResult, HttpOkResult},
};
use std::sync::Arc;

pub async fn route_requests(
    req: Request<Body>,
    app: Arc<AppContext>,
) -> Result<HttpOkResult, HttpFailResult> {
    let ctx = HttpContext::new(req);

    let path = ctx.get_path();

    match (ctx.get_method(), path) {
        (&Method::GET, "/") => {
            return super::static_content::get_index_page_content();
        }

        (&Method::GET, "/metrics") => {
            return super::metrics::index::get(app.as_ref()).await;
        }

        (&Method::GET, "/api/status") => {
            return super::api::status::get(app.as_ref()).await;
        }

        (&Method::GET, "/debug/page") => {
            return super::debug::page::get(ctx, app.as_ref()).await;
        }

        _ => {}
    }

    if path.starts_with("/swagger") {
        return super::swagger::handle(ctx).await;
    }

    if path.starts_with("/logs") {
        return super::api::logs::get(app.as_ref()).await;
    }

    if path.starts_with("/logs/topic") {
        return super::api::logs::get_by_topic(path, app.as_ref()).await;
    }

    if path.starts_with("/css") {
        return super::files::get_content_from_file(path, Some(WebContentType::Css)).await;
    }

    if path.starts_with("/js") || path.starts_with("/lib") {
        return super::files::get_content_from_file(path, Some(WebContentType::JavaScript)).await;
    }

    if !app.is_initialized() {
        return Err(HttpFailResult::not_initialized());
    }

    match (ctx.get_method(), path) {
        (&Method::GET, "/api/isalive") => {
            return super::api::is_alive::get(app.as_ref());
        }

        (&Method::GET, "/read/byid") => {
            return super::read::by_id::get(ctx, app.as_ref()).await;
        }

        (&Method::GET, "/read/listfromdate") => {
            return super::read::list_from_date::get(ctx, app.as_ref()).await;
        }

        _ => {}
    };

    return Err(HttpFailResult::not_found("Page not found".to_string()));
}
