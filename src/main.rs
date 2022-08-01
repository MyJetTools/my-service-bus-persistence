use std::{sync::Arc, time::Duration};
mod app;

mod grpc;
mod http;
mod index_by_minute;
mod message_pages;
mod operations;
mod page_blob_random_access;
mod settings;
mod timers;
mod toipics_snapshot;
mod topic_data;
mod utils;
use rust_extensions::MyTimer;
use toipics_snapshot::current_snapshot::TopicsSnapshotData;
mod typing;
mod uncompressed_page_storage;

use crate::{
    app::AppContext,
    settings::SettingsModel,
    timers::{
        metrics_updater::MetricsUpdater, pages_gc::PagesGcTimer, save_min_index::SaveMinIndexTimer,
        topics_snapshot_saver::TopicsSnapshotSaverTimer, SaveMessagesTimer,
    },
};

pub mod persistence_grpc {
    tonic::include_proto!("persistence");
}

#[tokio::main]
async fn main() {
    let settings = SettingsModel::read().await;

    let app = AppContext::new(settings).await;

    let app = Arc::new(app);

    let mut timer_1s = MyTimer::new(Duration::from_secs(1));
    timer_1s.register_timer(
        "SaveMessagesTimer",
        Arc::new(SaveMessagesTimer::new(app.clone())),
    );
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

    shut_down(app).await;
}

async fn shut_down(app: Arc<AppContext>) {
    let duration = Duration::from_secs(1);
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
            let topic_data = app.topics_list.get(&topic.topic_id).await;

            if topic_data.is_none() {
                continue;
            }

            let topic_data = topic_data.unwrap();

            if topic_data.pages_list.has_messages_to_save().await {
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
