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
        if let Some(topic_id) = &itm.topic_id {
            if !self.by_topic.contains_key(topic_id) {
                self.by_topic.insert(topic_id.to_string(), Vec::new());
            }

            let mut by_topic = self.by_topic.get_mut(topic_id).unwrap();
            by_topic.push(itm.clone());

            gc(&mut by_topic);
        }

        self.all.push(itm);
        gc(&mut self.all);
    }
}

fn gc(wirte_access: &mut Vec<LogItem>) {
    while wirte_access.len() > 100 {
        wirte_access.remove(0);
    }
}
