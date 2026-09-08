use crate::models::QueuedItem;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone, Default)]
pub struct ProxyState {
    pub active_queue: Arc<RwLock<HashMap<String, Vec<QueuedItem>>>>,
    pub queue_mode_enabled: Arc<RwLock<bool>>,
    pub hardware_delay_ms: Arc<RwLock<u64>>,
    pub self_user_id: Arc<RwLock<String>>,
    pub self_username: Arc<RwLock<String>>,
}

impl ProxyState {
    pub fn new() -> Self {
        Self {
            active_queue: Arc::new(RwLock::new(HashMap::new())),
            queue_mode_enabled: Arc::new(RwLock::new(true)),
            hardware_delay_ms: Arc::new(RwLock::new(45u64)),
            self_user_id: Arc::new(RwLock::new(String::new())),
            self_username: Arc::new(RwLock::new(String::new())),
        }
    }
}
