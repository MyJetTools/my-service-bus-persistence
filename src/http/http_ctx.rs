use hyper::{Body, Method, Request};

use super::query_string::QueryString;

pub struct HttpContext {
    req: Request<Body>,
    path: String,
}

impl HttpContext {
    pub fn new(req: Request<Body>) -> Self {
        let path = req.uri().path().to_lowercase();
        Self { req, path }
    }

    pub fn get_method(&self) -> &Method {
        self.req.method()
    }

    pub fn get_path(&self) -> &str {
        self.path.as_str()
    }

    pub fn get_query_string(&self) -> QueryString {
        let query = self.req.uri().query();
        return QueryString::new(query);
    }

    pub fn get_host(&self) -> &str {
        std::str::from_utf8(&self.req.headers().get("host").unwrap().as_bytes()).unwrap()
    }

    pub fn get_scheme(&self) -> String {
        let headers = self.req.headers();
        let proto_header = headers.get("X-Forwarded-Proto");

        if let Some(scheme) = proto_header {
            let bytes = scheme.as_bytes();
            return String::from_utf8(bytes.to_vec()).unwrap();
        }

        let scheme = self.req.uri().scheme();

        match scheme {
            Some(scheme) => {
                return scheme.to_string();
            }
            None => "http".to_string(),
        }
    }
}
