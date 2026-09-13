// src/app/state.rs
use crate::models::{DiscordMessage, QueuedItem, ReactionDelayMode};
use rand::Rng;
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
    pub current_char_delay_ms: u64,
}

impl AppState {
    pub fn new(token: String) -> Self {
        let mut list_state = ListState::default();
        list_state.select(Some(0));

        let (channel_id_tx, channel_id_rx) = watch::channel(String::new());
        let is_proxy = token.starts_with("ws://") || token.starts_with("wss://");

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
            queue_mode: is_proxy, // Queue Mode Enabled only in Proxy Mode by default
            queue: Vec::new(),
            last_keystroke_time: None,
            hardware_delay_ms: 45,
            reaction_delay_mode: ReactionDelayMode::Normal,
            preview_typed_text: String::new(),
            simulated_target_text: String::new(),
            last_char_tick: None,
            current_char_delay_ms: 75,
        }
    }

    pub fn set_target_channel_id(&mut self, channel_id: String) {
        self.target_channel_id = channel_id.clone();
        let _ = self.channel_id_tx.send(channel_id);
    }

    pub fn update_preview_typed_text(&mut self) {
        let is_proxy = self.token.starts_with("ws://") || self.token.starts_with("wss://");
        if !is_proxy || !self.queue_mode || self.queue.is_empty() {
            self.preview_typed_text.clear();
            self.simulated_target_text.clear();
            self.last_char_tick = None;
            return;
        }

        let target = self.queue[0].content.clone();
        if self.simulated_target_text != target {
            self.simulated_target_text = target.clone();
            self.preview_typed_text.clear();
            self.last_char_tick = Some(Instant::now());
            self.current_char_delay_ms = rand::thread_rng().gen_range(70..=80);
        }
    }

    pub fn step_simulated_typing(&mut self) -> bool {
        let is_proxy = self.token.starts_with("ws://") || self.token.starts_with("wss://");
        if !is_proxy || !self.queue_mode || self.queue.is_empty() || self.simulated_target_text.is_empty() {
            return false;
        }

        let target_chars: Vec<char> = self.simulated_target_text.chars().collect();
        let current_chars: Vec<char> = self.preview_typed_text.chars().collect();

        if current_chars.len() < target_chars.len() {
            let should_advance = match self.last_char_tick {
                Some(last) => last.elapsed().as_millis() as u64 >= self.current_char_delay_ms,
                None => true,
            };

            if should_advance {
                if current_chars.is_empty() {
                    // Find leading digits portion (allowing leading spaces/underscores)
                    let mut digit_indices = Vec::new();
                    for (i, &c) in target_chars.iter().enumerate() {
                        if c.is_ascii_digit() {
                            digit_indices.push(i);
                        } else if digit_indices.is_empty() && (c == ' ' || c == '_') {
                            continue;
                        } else {
                            break;
                        }
                    }

                    if digit_indices.len() > 2 {
                        // Paste up to digits.len() - 2
                        let paste_until_idx = digit_indices[digit_indices.len() - 2];
                        let paste_part: String = target_chars[..paste_until_idx].iter().collect();
                        self.preview_typed_text = paste_part;
                    } else {
                        // If 2 or fewer digits, type character by character starting from index 0
                        let next_char = target_chars[0];
                        self.preview_typed_text.push(next_char);
                    }
                } else {
                    // Type the next remaining character individually
                    let next_char = target_chars[current_chars.len()];
                    self.preview_typed_text.push(next_char);
                }

                self.last_char_tick = Some(Instant::now());
                self.current_char_delay_ms = rand::thread_rng().gen_range(70..=80);
                return true;
            }
        }
        false
    }
}
