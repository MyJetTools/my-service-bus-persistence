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


        _ => {}
    }


    if !app.is_initialized() {
        return Err(HttpFailResult::not_initialized());
    }

    match (ctx.get_method(), path) {
        (&Method::GET, "/api/isalive") => {
            return super::api::is_alive::get(app.as_ref());
        }

        (&Method::GET, "/read/listfromdate") => {
            return super::read_controller::list_from_date_action::get(ctx, app.as_ref()).await;
        }

        _ => {}
    };

    return Err(HttpFailResult::not_found("Page not found".to_string()));
}
