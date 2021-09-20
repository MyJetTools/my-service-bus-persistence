use tokio::{sync::RwLock, task::JoinError};

use crate::date_time::DateTimeAsMicroseconds;

use super::LogsCluster;

#[derive(Debug, Clone)]
pub enum LogLevel {
    Info,
    Error,
    FatalError,
}
#[derive(Debug, Clone)]
pub struct LogItem {
    pub date: DateTimeAsMicroseconds,

    pub topic_id: Option<String>,

    pub level: LogLevel,

    pub process: String,

    pub message: String,

    pub err_ctx: Option<String>,
}

pub struct Logs {
    items: RwLock<LogsCluster>,
}

impl Logs {
    pub fn new() -> Self {
        Self {
            items: RwLock::new(LogsCluster::new()),
        }
    }

    pub async fn add_info(&self, topic_id: Option<&str>, process: &str, message: &str) {
        let date = DateTimeAsMicroseconds::now();
        let item = LogItem {
            topic_id: topic_id_to_string(topic_id),
            date,
            level: LogLevel::Info,
            process: process.to_string(),
            message: message.to_string(),
            err_ctx: None,
        };

        let mut wirte_access = self.items.write().await;
        if let Some(topic_id) = topic_id {
            println!(
                "{}: INFO Topic: {}. Message: {}",
                date.to_rfc3339(),
                topic_id,
                item.message
            );
        } else {
            println!("{}: INFO {}", date.to_rfc3339(), item.message);
        }

        wirte_access.push(item);
    }

    pub async fn add_info_string(&self, topic_id: Option<&str>, process: &str, message: String) {
        let date = DateTimeAsMicroseconds::now();
        let item = LogItem {
            topic_id: topic_id_to_string(topic_id),
            date,
            level: LogLevel::Info,
            process: process.to_string(),
            message: message,
            err_ctx: None,
        };

        let mut wirte_access = self.items.write().await;

        if let Some(topic_id) = topic_id {
            println!(
                "{}: INFO Topic: {}. Message: {}",
                date.to_rfc3339(),
                topic_id,
                item.message
            );
        } else {
            println!("{}: INFO {}", date.to_rfc3339(), item.message);
        }

        wirte_access.push(item);
    }

    pub async fn add_error(
        &self,
        topic_id: Option<&str>,
        process: &str,
        message: &str,
        err_ctx: String,
    ) {
        let date = DateTimeAsMicroseconds::now();

        let item = LogItem {
            topic_id: topic_id_to_string(topic_id),
            date,
            level: LogLevel::Error,
            process: process.to_string(),
            message: message.to_string(),
            err_ctx: Some(err_ctx),
        };

        let mut wirte_access = self.items.write().await;

        if let Some(topic_id) = topic_id {
            println!(
                "{}: ERR Topic: {}. Message: {}",
                date.to_rfc3339(),
                topic_id,
                item.message
            );
        } else {
            println!("{}: ERR {}", date.to_rfc3339(), item.message);
        }

        wirte_access.push(item);
    }

    pub async fn add_fatal_error(&self, process: &str, err: JoinError) {
        let date = DateTimeAsMicroseconds::now();

        let item = LogItem {
            topic_id: None,
            date,
            level: LogLevel::FatalError,
            process: process.to_string(),
            message: format!("{:?}", err),
            err_ctx: None,
        };

        let mut wirte_access = self.items.write().await;

        println!("{}: FATAL_ERR {}", date.to_rfc3339(), item.message);

        wirte_access.push(item);
    }

    pub async fn add_error_str(
        &self,
        topic_id: Option<&str>,
        process: &str,
        message: String,
        err_ctx: String,
    ) {
        let date = DateTimeAsMicroseconds::now();

        let item = LogItem {
            topic_id: topic_id_to_string(topic_id),
            date,
            level: LogLevel::Error,
            process: process.to_string(),
            message: message,
            err_ctx: Some(err_ctx),
        };

        let mut wirte_access = self.items.write().await;

        if let Some(topic_id) = topic_id {
            println!(
                "{}: ERR Topic: {}. Message: {}",
                date.to_rfc3339(),
                topic_id,
                item.message
            );
        } else {
            println!("{}: ERR {}", date.to_rfc3339(), item.message);
        }

        wirte_access.push(item);
    }

    pub async fn get(&self) -> Vec<LogItem> {
        let read_access = self.items.read().await;
        read_access.all.to_vec()
    }

    pub async fn get_by_topic(&self, topic_id: &str) -> Option<Vec<LogItem>> {
        let read_access = self.items.read().await;
        let result = read_access.by_topic.get(topic_id)?;
        return Some(result.to_vec());
    }
}

fn topic_id_to_string(topic_id: Option<&str>) -> Option<String> {
    let result = topic_id?;
    return Some(result.to_string());
}
