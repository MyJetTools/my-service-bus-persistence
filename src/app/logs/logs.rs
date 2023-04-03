use std::{collections::HashMap, sync::Arc};

use rust_extensions::{date_time::DateTimeAsMicroseconds, Logger, StrOrString};
use tokio::sync::RwLock;

use super::LogsCluster;

#[derive(Debug, Clone)]
pub enum LogLevel {
    Info,
    Warning,
    Error,
    FatalError,
    Debug,
}

impl LogLevel {
    pub fn as_str(&self) -> &str {
        match self {
            LogLevel::Info => "Info",
            LogLevel::Warning => "Warning",
            LogLevel::Error => "Error",
            LogLevel::FatalError => "FatalError",
            LogLevel::Debug => "Debug",
        }
    }
}
#[derive(Debug, Clone)]
pub struct LogItem {
    pub date: DateTimeAsMicroseconds,
    pub level: LogLevel,
    pub process: String,
    pub message: String,
    pub ctx: Option<HashMap<String, String>>,
}

impl LogItem {
    pub fn get_topic_id(&self) -> Option<&str> {
        if let Some(ctx) = &self.ctx {
            if let Some(topic_id) = ctx.get("topicId") {
                return Some(topic_id);
            }
        }
        return None;
    }
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

    pub fn write_by_topic(
        &self,
        level: LogLevel,
        topic_id: impl Into<StrOrString<'static>>,
        process: impl Into<StrOrString<'static>>,
        message: impl Into<StrOrString<'static>>,
    ) {
        let mut ctx = HashMap::new();

        ctx.insert("topicId".to_string(), topic_id.into().to_string());

        let date = DateTimeAsMicroseconds::now();
        let item = LogItem {
            date,
            level,
            process: process.into().to_string(),
            message: message.into().to_string(),
            ctx: Some(ctx),
        };

        tokio::spawn(write(self.items.clone(), item));
    }

    pub fn write(
        &self,
        level: LogLevel,
        process: impl Into<StrOrString<'static>>,
        message: impl Into<StrOrString<'static>>,
        ctx: Option<HashMap<String, String>>,
    ) {
        let date = DateTimeAsMicroseconds::now();
        let item = LogItem {
            date,
            level,
            process: process.into().to_string(),
            message: message.into().to_string(),
            ctx,
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

async fn write(items: Arc<RwLock<LogsCluster>>, item: LogItem) {
    let mut write_access = items.write().await;
    write_access.push(item);
}

impl Logger for Logs {
    fn write_info(&self, process: String, message: String, ctx: Option<HashMap<String, String>>) {
        self.write(LogLevel::Info, process, message, ctx);
    }

    fn write_warning(
        &self,
        process: String,
        message: String,
        ctx: Option<HashMap<String, String>>,
    ) {
        self.write(LogLevel::Warning, process, message, ctx);
    }

    fn write_error(&self, process: String, message: String, ctx: Option<HashMap<String, String>>) {
        self.write(LogLevel::Error, process, message, ctx);
    }

    fn write_fatal_error(
        &self,
        process: String,
        message: String,
        ctx: Option<HashMap<String, String>>,
    ) {
        self.write(LogLevel::FatalError, process, message, ctx);
    }

    fn write_debug_info(
        &self,
        process: String,
        message: String,
        ctx: Option<HashMap<String, String>>,
    ) {
        self.write(LogLevel::Debug, process, message, ctx);
    }
}
