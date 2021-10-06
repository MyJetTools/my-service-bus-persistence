use std::{sync::Arc, time::Duration};

mod app;
mod azure_storage;
mod compressed_pages;
mod grpc;
mod http;
mod index_by_minute;
mod message_pages;
mod operations;
mod settings;
mod timers;
mod toipics_snapshot;
mod utils;
use toipics_snapshot::current_snapshot::TopicsSnapshotData;

use crate::{app::AppContext, settings::SettingsModel};

use tokio::signal;

pub mod persistence_grpc {
    tonic::include_proto!("persistence");
}

#[tokio::main]
async fn main() {
    let settings = SettingsModel::read().await;

    let mut topics_snapshot_page_blob = settings.get_topics_snapshot_page_blob();

    let topics_data =
        toipics_snapshot::blob_repository::read_from_blob(&mut topics_snapshot_page_blob)
            .await
            .unwrap();

    let app = AppContext::new(topics_data, settings);

    let app = Arc::new(app);

    signal_hook::flag::register(signal_hook::consts::SIGTERM, app.shutting_down.clone()).unwrap();

    tokio::spawn(run_app(app.clone()));

    let shut_down_task = tokio::spawn(shut_down(app.clone()));

    signal::ctrl_c().await.unwrap();

    println!("Detected ctrl+c");

    app.as_ref()
        .shutting_down
        .store(true, std::sync::atomic::Ordering::Release);

    shut_down_task.await.unwrap();
}

async fn run_app(app: Arc<AppContext>) {
    let init_handler = tokio::spawn(azure_storage::data_initializer::init(app.clone()));

    timers::timer_3s::start(app.clone());

    let http_server_task = tokio::spawn(http::http_server::start(app.clone(), 7123));

    let grpc_server_task = tokio::spawn(grpc::server::start(app.clone(), 7124));

    http_server_task.await.unwrap();
    grpc_server_task.await.unwrap().unwrap();
    init_handler.await.unwrap();
}

async fn shut_down(app: Arc<AppContext>) {
    let duration = Duration::from_secs(1);

    while !app.as_ref().is_shutting_down() {
        tokio::time::sleep(duration).await;
    }

    println!("Shutting down application");

    println!("Waiting until we flush all the queues and messages");
    tokio::time::sleep(duration).await;

    let mut snapshot = app.topics_snapshot.get().await;

    while snapshot.snapshot_id != snapshot.last_saved_snapshot_id {
        println!("Topic Snapshot is not synchronized yet. SnapshotID is {}. Last saved snapshot Id is: {}", snapshot.snapshot_id, snapshot.last_saved_snapshot_id);
        tokio::time::sleep(duration).await;
        snapshot = app.topics_snapshot.get().await;
    }

    println!("Topic snapshot is flushed");

    check_queues_are_empty(app.as_ref(), &snapshot).await;

    println!("Application can be closed now safely");
}

async fn check_queues_are_empty(app: &AppContext, snapshot: &TopicsSnapshotData) {
    let duration = Duration::from_secs(1);

    loop {
        let mut has_data_to_sync = None;
        for topic in &snapshot.snapshot.data {
            let topic_data = app.topics_data_list.get(&topic.topic_id).await;

            if topic_data.is_none() {
                continue;
            }

            let topic_data = topic_data.unwrap();

            if topic_data.has_messages_to_save().await {
                has_data_to_sync = Some(topic_data.as_ref().topic_id.to_string());
            }
        }

        if let Some(topic) = has_data_to_sync {
            println!(
                "Topic {} has still data to save. Waiting one second...",
                topic
            );
            tokio::time::sleep(duration).await;
        } else {
            break;
        }
    }
}
