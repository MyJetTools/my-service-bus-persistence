use std::{sync::Arc, time::Duration};

mod app;
mod azure_page_blob_writer;
mod azure_storage;
mod bcl_proto;
mod compressed_pages;
mod compression;
mod date_time;
mod grpc;
mod http;
mod index_by_minute;
mod message_pages;
mod message_pages_loader;
mod messages_protobuf;
mod settings;
mod timers;
mod toipics_snapshot;
mod utils;
use toipics_snapshot::CurrentTopicsSnapshot;

use my_azure_page_blob::{MyAzurePageBlob, MyPageBlob};

use crate::app::AppContext;

use my_azure_storage_sdk::AzureConnection;

use tokio::signal;

pub mod persistence_grpc {
    tonic::include_proto!("persistence");
}

#[tokio::main]
async fn main() {
    let settings = settings::read().await;

    let connection = AzureConnection::from_conn_string(settings.queues_connection_string.as_str());

    let mut topics_snapshot_blob =
        MyAzurePageBlob::new(connection, "topics".to_string(), "topicsdata".to_string());

    topics_snapshot_blob.create_container_if_not_exist().await.unwrap();

    let topics_data = toipics_snapshot::blob_repository::read_from_blob(&mut topics_snapshot_blob)
        .await
        .unwrap();

    let app = AppContext::new(topics_data, settings);

    let app = Arc::new(app);

    signal_hook::flag::register(signal_hook::consts::SIGTERM, app.shutting_down.clone()).unwrap();

    tokio::spawn(run_app(app.clone(), topics_snapshot_blob));

    let shut_down_task = tokio::spawn(shut_down(app.clone()));

    signal::ctrl_c().await.unwrap();

    println!("Detected ctrl+c");

    app.as_ref()
        .shutting_down
        .store(true, std::sync::atomic::Ordering::Release);

    shut_down_task.await.unwrap();
}

async fn run_app(app: Arc<AppContext>, topics_snapshot_blob: MyAzurePageBlob) {
    let init_handler = tokio::spawn(azure_storage::data_initializer::init(app.clone()));

    timers::timer_3s::start(app.clone(), topics_snapshot_blob);

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

    let mut snapshot = app.get_topics_snapshot().await;
    while snapshot.snapshot_id != snapshot.last_saved_snapshot_id {
        println!("Topic Snapshot is not synchronized yet. SnapshotID is {}. Last saved snapshot Id is: {}", snapshot.snapshot_id, snapshot.last_saved_snapshot_id);
        tokio::time::sleep(duration).await;
        snapshot = app.get_topics_snapshot().await;
    }

    println!("Topic snapshot is flushed");

    check_queues_are_empty(app.as_ref(), &snapshot).await;

    println!("Application can be closed now safely");
}

async fn check_queues_are_empty(app: &AppContext, snapshot: &CurrentTopicsSnapshot) {
    let duration = Duration::from_secs(1);

    loop {
        let mut has_data_to_sync = None;
        for topic in &snapshot.snapshot.data {
            let topic_data = app.get_data_by_topic(&topic.topic_id).await;

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
