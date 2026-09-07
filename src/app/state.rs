// src/app/state.rs
use crate::models::{DiscordMessage, QueuedItem};
use ratatui::widgets::ListState;
use std::collections::HashMap;
use std::time::Instant;
use tokio::sync::watch;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActiveModal {
    None,
    LogoutPrompt,
    SwitchChannelPrompt,
}

pub struct AppState {
    pub token: String,
    pub target_channel_id: String,
    pub channel_id_tx: watch::Sender<String>,
    pub channel_id_rx: watch::Receiver<String>,
    pub self_username: String,
    pub messages: Vec<DiscordMessage>,
    pub input_text: String,
    pub modal_input: String,
    pub active_modal: ActiveModal,
    pub list_state: ListState,
    pub failed_nonces: Vec<String>,
    pub last_typing_sent: Option<Instant>,
    pub outbound_timers: HashMap<String, Instant>,
    pub gateway_rtt_ms: Option<u64>,
    pub clock_offset_ms: Option<i64>,
    pub show_timestamp: bool,
    pub show_latency: bool,
    pub scroll_offset: usize,
    pub queue_mode: bool,
    pub queue: Vec<QueuedItem>,
    pub last_keystroke_time: Option<Instant>,
    pub hardware_delay_ms: u64,
}

impl AppState {
    pub fn new(token: String) -> Self {
        let mut list_state = ListState::default();
        list_state.select(Some(0));

        let (channel_id_tx, channel_id_rx) = watch::channel(String::new());

        Self {
            token,
            target_channel_id: String::new(),
            channel_id_tx,
            channel_id_rx,
            self_username: "You".to_string(),
            messages: Vec::new(),
            input_text: String::new(),
            modal_input: String::new(),
            active_modal: ActiveModal::None,
            list_state,
            failed_nonces: Vec::new(),
            last_typing_sent: None,
            outbound_timers: HashMap::new(),
            gateway_rtt_ms: None,
            clock_offset_ms: None,
            show_timestamp: true,
            show_latency: true,
            scroll_offset: 0,
            queue_mode: true, // Queue Mode Enabled by default
            queue: Vec::new(),
            last_keystroke_time: None,
            hardware_delay_ms: 45,
        }
    }

    pub fn set_target_channel_id(&mut self, channel_id: String) {
        self.target_channel_id = channel_id.clone();
        let _ = self.channel_id_tx.send(channel_id);
    }
}
