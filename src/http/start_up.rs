use std::{net::SocketAddr, sync::Arc};

use my_http_server::{middlewares::StaticFilesMiddleware, MyHttpServer};

use my_http_server_controllers::swagger::SwaggerMiddleware;

use crate::app::AppContext;

pub fn setup_server(app: &Arc<AppContext>, port: u16) {
    let mut http_server = MyHttpServer::new(SocketAddr::from(([0, 0, 0, 0], port)));

    let controllers = Arc::new(super::builder::build(app));

    let swagger_middleware = SwaggerMiddleware::new(
        controllers.clone(),
        "MyServiceBusPersistence".to_string(),
        crate::app::APP_VERSION.to_string(),
    );

    http_server.add_middleware(Arc::new(swagger_middleware));
    http_server.add_middleware(controllers);

    http_server.add_middleware(Arc::new(StaticFilesMiddleware::new(None)));
    http_server.start(app.clone());
}
