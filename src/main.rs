use std::{sync::Arc, time::Duration};
mod app;

mod archive_storage;

//mod azure_storage_with_retries;
mod grpc;
mod http;
mod index_by_minute;
mod message_pages;
mod operations;

mod settings;
mod timers;
mod topic_data;
mod topics_snapshot;
mod utils;

use rust_extensions::MyTimer;
mod typing;

use crate::{
    app::AppContext,
    settings::SettingsModel,
    timers::{
        metrics_updater::MetricsUpdater, pages_gc::PagesGcTimer, save_min_index::SaveMinIndexTimer,
        topics_snapshot_saver::TopicsSnapshotSaverTimer,
    },
};
#[allow(non_snake_case)]
pub mod persistence_grpc {
    tonic::include_proto!("persistence");
}

#[tokio::main]
async fn main() {
    let settings = SettingsModel::read().await;

    let app = AppContext::new(settings).await;

    let app = Arc::new(app);

    let mut timer_1s = MyTimer::new(Duration::from_secs(1));
    timer_1s.register_timer("MetricsUpdater", Arc::new(MetricsUpdater::new(app.clone())));

    let mut timer_3s = MyTimer::new(Duration::from_secs(3));

    timer_3s.register_timer(
        "TopicsSnapshotSaver",
        Arc::new(TopicsSnapshotSaverTimer::new(app.clone())),
    );

    timer_3s.register_timer("PagesGc", Arc::new(PagesGcTimer::new(app.clone())));

    timer_3s.register_timer(
        "SaveMinIndexTimer",
        Arc::new(SaveMinIndexTimer::new(app.clone())),
    );

    timer_3s.start(app.app_states.clone(), app.logs.clone());
    timer_1s.start(app.app_states.clone(), app.logs.clone());

    crate::http::start_up::setup_server(&app, 7123);

    tokio::spawn(operations::data_initializer::init(app.clone()));

    tokio::spawn(grpc::server::start(app.clone(), 7124));

    app.app_states.wait_until_shutdown().await;

    crate::operations::before_shut_down::execute_before_shutdown(app).await;
}
