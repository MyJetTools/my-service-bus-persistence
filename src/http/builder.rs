use std::sync::Arc;

use my_http_server_controllers::controllers::ControllersMiddleware;

use crate::app::AppContext;

pub fn build(app: &Arc<AppContext>) -> ControllersMiddleware {
    let mut result = ControllersMiddleware::new();

    result.register_get_action(Arc::new(
        super::controllers::api_controller::IsAliveAction::new(app.clone()),
    ));
    result.register_get_action(Arc::new(
        super::controllers::api_controller::GetStatusAction::new(app.clone()),
    ));

    result.register_get_action(Arc::new(
        super::controllers::logs_controller::ActionLogs::new(app.clone()),
    ));

    result.register_get_action(Arc::new(
        super::controllers::logs_controller::ActionLogsByTopic::new(app.clone()),
    ));

    result.register_delete_action(Arc::new(
        super::controllers::topic_controller::DeleteTopicAction::new(app.clone()),
    ));

    result.register_get_action(Arc::new(
        super::controllers::prometheus_controller::MetricsAction::new(app.clone()),
    ));

    result
}
