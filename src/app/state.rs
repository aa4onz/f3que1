// src/app/state.rs
use crate::models::{DiscordMessage, QueuedItem, ReactionDelayMode};
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
    pub reaction_delay_mode: ReactionDelayMode,
    pub preview_typed_text: String,
    pub simulated_target_text: String,
    pub last_char_tick: Option<Instant>,
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
            reaction_delay_mode: ReactionDelayMode::Normal,
            preview_typed_text: String::new(),
            simulated_target_text: String::new(),
            last_char_tick: None,
        }
    }

    pub fn set_target_channel_id(&mut self, channel_id: String) {
        self.target_channel_id = channel_id.clone();
        let _ = self.channel_id_tx.send(channel_id);
    }

    pub fn update_preview_typed_text(&mut self) {
        if !self.queue_mode || self.queue.is_empty() {
            self.preview_typed_text.clear();
            self.simulated_target_text.clear();
            self.last_char_tick = None;
            return;
        }

        let target = self.queue[0].content.clone();
        if self.simulated_target_text != target {
            self.simulated_target_text = target.clone();
            let chars: Vec<char> = target.chars().collect();
            let len = chars.len();

            if len > 2 {
                // Instantly paste everything except the last 2 digits
                let paste_part: String = chars[..len - 2].iter().collect();
                self.preview_typed_text = paste_part;
            } else {
                self.preview_typed_text.clear();
            }
            self.last_char_tick = Some(Instant::now());
        }
    }

    pub fn step_simulated_typing(&mut self) -> bool {
        if !self.queue_mode || self.queue.is_empty() || self.simulated_target_text.is_empty() {
            return false;
        }

        let target_chars: Vec<char> = self.simulated_target_text.chars().collect();
        let current_chars: Vec<char> = self.preview_typed_text.chars().collect();

        if current_chars.len() < target_chars.len() {
            let delay = self.hardware_delay_ms.max(45);
            let should_advance = match self.last_char_tick {
                Some(last) => last.elapsed().as_millis() as u64 >= delay,
                None => true,
            };

            if should_advance {
                let next_char = target_chars[current_chars.len()];
                self.preview_typed_text.push(next_char);
                self.last_char_tick = Some(Instant::now());
                return true;
            }
        }
        false
    }
}
