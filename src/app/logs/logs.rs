use std::sync::Arc;

use rust_extensions::date_time::DateTimeAsMicroseconds;
use tokio::sync::RwLock;

use super::LogsCluster;

#[derive(Debug, Clone)]
pub enum LogLevel {
    Info,
    Error,
    FatalError,
}

impl LogLevel {
    fn as_str(&self) -> &str {
        match self {
            LogLevel::Info => "Info",
            LogLevel::Error => "Error",
            LogLevel::FatalError => "FalalError",
        }
    }
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
    items: Arc<RwLock<LogsCluster>>,
}

impl Logs {
    pub fn new() -> Self {
        Self {
            items: Arc::new(RwLock::new(LogsCluster::new())),
        }
    }

    pub fn add_info(&self, topic_id: Option<&str>, process: &str, message: String) {
        let date = DateTimeAsMicroseconds::now();
        let item = LogItem {
            topic_id: topic_id_to_string(topic_id),
            date,
            level: LogLevel::Info,
            process: process.to_string(),
            message: message,
            err_ctx: None,
        };

        tokio::spawn(write(self.items.clone(), item));
    }

    pub fn add_info_string(&self, topic_id: Option<&str>, process: &str, message: String) {
        let date = DateTimeAsMicroseconds::now();
        let item = LogItem {
            topic_id: topic_id_to_string(topic_id),
            date,
            level: LogLevel::Info,
            process: process.to_string(),
            message: message,
            err_ctx: None,
        };

        tokio::spawn(write(self.items.clone(), item));
    }

    /*
       pub fn add_error(
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
               message,
               err_ctx: Some(err_ctx),
           };

           tokio::spawn(write(self.items.clone(), item));
       }
    */
    pub fn add_fatal_error(&self, process: &str, err: String) {
        let date = DateTimeAsMicroseconds::now();

        let item = LogItem {
            topic_id: None,
            date,
            level: LogLevel::FatalError,
            process: process.to_string(),
            message: format!("{:?}", err),
            err_ctx: None,
        };

        tokio::spawn(write(self.items.clone(), item));
    }

    pub fn add_error_str(
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
        tokio::spawn(write(self.items.clone(), item));
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

async fn write(logs: Arc<RwLock<LogsCluster>>, item: LogItem) {
    let mut wirte_access = logs.write().await;

    if let Some(topic_id) = &item.topic_id {
        println!(
            "{dt}: {level} Topic: {topic_id}. Message: {msg}",
            dt = item.date.to_rfc3339(),
            level = item.level.as_str(),
            msg = item.message
        );
    } else {
        println!(
            "{dt}: {level} Message: {msg}",
            dt = item.date.to_rfc3339(),
            level = item.level.as_str(),
            msg = item.message
        );
    }

    wirte_access.push(item);
}
