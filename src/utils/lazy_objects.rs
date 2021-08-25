use std::{collections::HashMap, hash::Hash, sync::Arc};

use tokio::sync::Mutex;

pub struct LazyObjectsHashMap<TKey, TItem>
where
    TKey: std::cmp::Eq + Hash + Clone,
{
    items: Mutex<HashMap<TKey, Arc<TItem>>>,
}

impl<TKey, TItem> LazyObjectsHashMap<TKey, TItem>
where
    TKey: std::cmp::Eq + Hash + Clone,
{
    pub fn new() -> Self {
        Self {
            items: Mutex::new(HashMap::new()),
        }
    }

    pub async fn get<F: Fn() -> TItem>(&self, key: &TKey, create: F) -> Arc<TItem> {
        let mut hash_map_access = self.items.lock().await;

        let result = hash_map_access.get(key);

        match result {
            Some(result) => {
                return result.clone();
            }
            None => {
                let item = create();

                let item = Arc::new(item);

                hash_map_access.insert(key.clone(), item.clone());

                return item;
            }
        }
    }
}
