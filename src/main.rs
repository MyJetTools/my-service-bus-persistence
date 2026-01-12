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
        deleted_topics_gc::DeletedTopicsGcTimer, metrics_updater::MetricsUpdater,
        pages_gc::PagesGcTimer, save_min_index::SaveMinIndexTimer,
        topics_snapshot_saver::TopicsSnapshotSaverTimer,
    },
};
#[allow(non_snake_case)]
pub mod persistence_grpc {
    tonic::include_proto!("persistence");
}

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[tokio::main]
async fn main() {
    let settings = SettingsModel::read().await;

    let app = AppContext::new(settings).await;

    let app = Arc::new(app);

    let mut timer_3s = MyTimer::new(Duration::from_secs(3));

    timer_3s.register_timer(
        "TopicsSnapshotSaver",
        Arc::new(TopicsSnapshotSaverTimer::new(app.clone())),
    );

    timer_3s.register_timer(
        "SaveMinIndexTimer",
        Arc::new(SaveMinIndexTimer::new(app.clone())),
    );

    timer_3s.start(app.app_states.clone(), my_logger::LOGGER.clone());

    let mut timer_persist_queues = MyTimer::new(Duration::from_secs(1));

    timer_persist_queues.register_timer("PagesGc", Arc::new(PagesGcTimer::new(app.clone())));

    timer_persist_queues.start(app.app_states.clone(), my_logger::LOGGER.clone());

    let mut timer_30s = MyTimer::new(Duration::from_secs(30));
    timer_30s.register_timer(
        "DeletedTopicsGc",
        Arc::new(DeletedTopicsGcTimer::new(app.clone())),
    );
    timer_30s.start(app.app_states.clone(), my_logger::LOGGER.clone());

    let http_connections_counter = crate::http::start_up::setup_server(&app, 7123);

    let mut timer_1s = MyTimer::new(Duration::from_secs(1));
    timer_1s.register_timer(
        "MetricsUpdater",
        Arc::new(MetricsUpdater::new(app.clone(), http_connections_counter)),
    );
    timer_1s.start(app.app_states.clone(), my_logger::LOGGER.clone());

    tokio::spawn(operations::data_initializer::init(app.clone()));

    tokio::spawn(grpc::server::start(app.clone(), 7124));

    if let Some(unix_socket_addr) = app.settings.listen_unix_socket.clone() {
        tokio::spawn(grpc::server::start_unix_socket(
            app.clone(),
            unix_socket_addr,
        ));
    }

    app.app_states.wait_until_shutdown().await;

    crate::operations::before_shut_down::execute_before_shutdown(app).await;
}
