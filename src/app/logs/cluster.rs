use std::collections::HashMap;

use super::LogItem;

pub struct LogsCluster {
    pub all: Vec<LogItem>,
    pub by_topic: HashMap<String, Vec<LogItem>>,
}

impl LogsCluster {
    pub fn new() -> Self {
        Self {
            all: Vec::new(),
            by_topic: HashMap::new(),
        }
    }

    pub fn push(&mut self, itm: LogItem) {
        if let Some(ctx) = &itm.ctx {
            if let Some(topic_id) = ctx.get("topicId") {
                if !self.by_topic.contains_key(topic_id) {
                    self.by_topic.insert(topic_id.to_string(), Vec::new());
                }
            }
        }

        self.all.push(itm);
        gc(&mut self.all);
    }
}

fn gc(write_access: &mut Vec<LogItem>) {
    while write_access.len() > 100 {
        write_access.remove(0);
    }
}
