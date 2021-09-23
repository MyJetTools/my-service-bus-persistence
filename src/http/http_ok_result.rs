use super::web_content_type::WebContentType;
use hyper::{Body, Response, StatusCode};
use serde::Serialize;

pub enum HttpOkResult {
    Content {
        content_type: Option<WebContentType>,
        content: Vec<u8>,
    },
    Html {
        head: String,
        body: String,
    },
    Redirect {
        url: String,
    },
}

impl HttpOkResult {
    pub fn create_json_response<T: Serialize>(model: T) -> HttpOkResult {
        let json = serde_json::to_vec(&model).unwrap();
        HttpOkResult::Content {
            content_type: Some(WebContentType::Json),
            content: json,
        }
    }
}

impl Into<HttpOkResult> for String {
    fn into(self) -> HttpOkResult {
        HttpOkResult::Content {
            content_type: Some(WebContentType::Text),
            content: self.into_bytes(),
        }
    }
}

impl Into<Response<Body>> for HttpOkResult {
    fn into(self) -> Response<Body> {
        match self {
            HttpOkResult::Content {
                content_type,
                content,
            } => {
                let mut builder = Response::builder();

                if let Some(content_type) = content_type {
                    builder = builder.header("Content-Type", content_type.to_string());
                }

                builder
                    .status(StatusCode::OK)
                    .body(Body::from(content))
                    .unwrap()
            }
            HttpOkResult::Html { head, body } => {
                let content = build_html(head, body);

                Response::builder()
                    .header("Content-Type", WebContentType::Html.to_string())
                    .status(StatusCode::OK)
                    .body(Body::from(content))
                    .unwrap()
            }
            HttpOkResult::Redirect { url } => Response::builder()
                .status(StatusCode::PERMANENT_REDIRECT)
                .header("Location", url)
                .body(Body::empty())
                .unwrap(),
        }
    }
}

fn build_html(head: String, body: String) -> String {
    format!(
        r###"<!DOCTYPE html><html lang="en"><head><title>{ver} MyServiceBus-Persistence</title>{head}
        </head><body>{body}</body></html>"###,
        ver = crate::app::APP_VERSION,
        head = head,
        body = body
    )
}
