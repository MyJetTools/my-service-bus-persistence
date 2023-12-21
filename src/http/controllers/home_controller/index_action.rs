use my_http_server::{HttpFailResult, HttpOkResult, HttpOutput, WebContentType};
use rand::Rng;

#[my_http_server::macros::http_route(method: "GET",route: "/",)]
pub struct IndexAction {
    rnd: u64,
}

impl IndexAction {
    pub fn new() -> Self {
        let mut rng = rand::thread_rng();
        IndexAction { rnd: rng.gen() }
    }
}

async fn handle_request(
    action: &IndexAction,
    _ctx: &my_http_server::HttpContext,
) -> Result<HttpOkResult, HttpFailResult> {
    let content = format!(
        r###"<html><head><title>{ver} MyServiceBusPersistence</title>
        <link href="/css/bootstrap.css" type="text/css" rel="stylesheet" /><link href="/css/site.css?ver={rnd}" type="text/css" rel="stylesheet" /> <script src="/lib/jquery.js"></script><script src="/js/app.js?ver={rnd}"></script>
        </head><body></body></html>"###,
        ver = crate::app::APP_VERSION,
        rnd = action.rnd
    );

    HttpOutput::Content {
        headers: None,
        content_type: Some(WebContentType::Html),
        content: content.into_bytes(),
    }
    .into_ok_result(true)
    .into()
}
