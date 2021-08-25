use hyper::{
    service::{make_service_fn, service_fn},
    Server,
};
use hyper::{Body, Request, Response, Result};

use std::net::SocketAddr;

use crate::app::AppContext;
use std::sync::Arc;

pub async fn start(app: Arc<AppContext>, port: u16) {
    app.logs
        .add_info_string(
            None,
            "HTTP servser start",
            format!("Starting http server 0.0.0.0:{}", port),
        )
        .await;

    let make_service = make_service_fn(move |_| {
        let app = app.clone();

        async move { Ok::<_, hyper::Error>(service_fn(move |_req| handle_requests(_req, app.clone()))) }
    });

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    println!("Listening to {:?} as http endpoint", addr);
    Server::bind(&addr).serve(make_service).await.unwrap();
}

async fn handle_requests(req: Request<Body>, app: Arc<AppContext>) -> Result<Response<Body>> {
    let response = super::controllers::router::route_requests(req, app).await;

    let response = match response {
        Ok(ok_result) => ok_result.into(),
        Err(fail_result) => fail_result.into(),
    };

    return Ok(response);
}
