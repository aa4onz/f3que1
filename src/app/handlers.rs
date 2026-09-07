// src/app/handlers.rs
use crate::app::state::ActiveModal;
use crate::models::{AppEvent, DiscordMessage, MessageStatus};
use chrono::Local;
use crossterm::event::{Event, KeyCode, KeyModifiers, MouseEventKind};
use std::time::Instant;
use tokio::sync::mpsc::Sender;

fn parse_text_number(text: &str) -> Option<i64> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }

    // Rule 1: Starts with a digit or preceded by space/underscore followed by digit
    let first_char = text.chars().next().unwrap();
    let is_num_start = first_char.is_ascii_digit()
        || (text.starts_with(' ') && text.trim_start().chars().next().map_or(false, |c| c.is_ascii_digit()))
        || (text.starts_with('_') && text.trim_start_matches('_').chars().next().map_or(false, |c| c.is_ascii_digit()));

    if is_num_start {
        // Isolate numeric portion
        let num_str: String = trimmed.chars().take_while(|c| c.is_ascii_digit()).collect();
        if !num_str.is_empty() {
            return num_str.parse::<i64>().ok();
        }
    }

    None
}

impl crate::app::state::AppState {
    pub async fn handle_event(
        &mut self,
        event: AppEvent,
        tx: &Sender<AppEvent>,
    ) -> bool {
        match event {
            AppEvent::ToggleQueueMode => {
                self.queue_mode = !self.queue_mode;
            }
            AppEvent::ClearQueue => {
                self.queue.clear();
            }
            AppEvent::UpdateQueueState(q) => {
                self.queue = q;
            }
            AppEvent::ToggleTimestamp => {
                self.show_timestamp = !self.show_timestamp;
            }
            AppEvent::ToggleLatency => {
                self.show_latency = !self.show_latency;
            }
            AppEvent::ScrollChat(delta) => {
                if delta < 0 {
                    let amount = (-delta) as usize;
                    let current = self.list_state.selected().unwrap_or(0);
                    let new_idx = current.saturating_sub(amount);
                    self.list_state.select(Some(new_idx));
                } else if delta > 0 {
                    let amount = delta as usize;
                    let current = self.list_state.selected().unwrap_or(0);
                    let max_idx = self.messages.len().saturating_sub(1);
                    let new_idx = (current + amount).min(max_idx);
                    self.list_state.select(Some(new_idx));
                }
            }
            AppEvent::UpdateClockOffset(offset_ms) => {
                self.clock_offset_ms = Some(offset_ms);
            }
            AppEvent::UpdateGatewayRtt { rtt_ms, offset_ms } => {
                self.gateway_rtt_ms = Some(rtt_ms);
                if self.clock_offset_ms.is_none() {
                    self.clock_offset_ms = Some(offset_ms);
                }
            }
            AppEvent::SetSelfUsername(username) => {
                if !username.is_empty() {
                    self.self_username = username;
                }
            }
            AppEvent::LoadChannelHistory(msgs) => {
                self.messages = msgs;
                if !self.messages.is_empty() {
                    self.list_state.select(Some(self.messages.len() - 1));
                }
            }
            AppEvent::SwitchChannel(new_channel_id) => {
                if !new_channel_id.is_empty() {
                    self.set_target_channel_id(new_channel_id.clone());
                    let _ = std::fs::write(".channel_cache", &new_channel_id);
                    self.messages.clear();
                    self.queue.clear();
                    let _ = tx.send(AppEvent::FetchChannelHistory(new_channel_id)).await;
                }
            }
            AppEvent::IncomingMessage(m) => {
                if m.nonce.starts_with("err-") {
                    self.messages.push(m);
                } else if !self
                    .messages
                    .iter()
                    .any(|x| x.nonce == m.nonce && !m.nonce.is_empty())
                {
                    self.messages.push(m);
                }
                if !self.messages.is_empty() {
                    self.list_state.select(Some(self.messages.len() - 1));
                }
            }
            AppEvent::MessageSent { nonce, timestamp } => {
                let elapsed_ms = self.outbound_timers.remove(&nonce).map(|start| start.elapsed().as_millis());

                if let Some(m) = self.messages.iter_mut().find(|x| x.nonce == nonce) {
                    m.status = MessageStatus::Delivered;
                    
                    if let Some(rtt) = elapsed_ms {
                        m.timestamp = format!(
                            "{} | {}ms",
                            Local::now().format("%H:%M:%S%.3f"),
                            rtt
                        );
                    } else if !timestamp.is_empty() {
                        m.timestamp = timestamp;
                    }
                    
                    self.failed_nonces.retain(|x| x != &nonce);
                }
            }
            AppEvent::MessageFailed { nonce } => {
                self.outbound_timers.remove(&nonce);
                if let Some(m) = self.messages.iter_mut().find(|x| x.nonce == nonce) {
                    m.status = MessageStatus::Failed;
                }
                if !self.failed_nonces.contains(&nonce) {
                    self.failed_nonces.push(nonce);
                }
            }
            AppEvent::GatewayClosed => {
                self.messages.push(DiscordMessage {
                    nonce: "err-close".into(),
                    author: "System".into(),
                    content: "⚠️ WebSocket closed. Reconnecting...".into(),
                    timestamp: Local::now().format("%H:%M:%S%.3f").to_string(),
                    status: MessageStatus::Failed,
                });
            }
            AppEvent::Terminal(Event::Mouse(m)) => match m.kind {
                MouseEventKind::ScrollUp => {
                    let current = self.list_state.selected().unwrap_or(0);
                    let new_idx = current.saturating_sub(3);
                    self.list_state.select(Some(new_idx));
                }
                MouseEventKind::ScrollDown => {
                    let current = self.list_state.selected().unwrap_or(0);
                    let max_idx = self.messages.len().saturating_sub(1);
                    let new_idx = (current + 3).min(max_idx);
                    self.list_state.select(Some(new_idx));
                }
                _ => {}
            },
            AppEvent::Terminal(Event::Key(k))
                if k.kind == crossterm::event::KeyEventKind::Press =>
            {
                if k.code == KeyCode::Char('c') && k.modifiers.contains(KeyModifiers::CONTROL) && !self.input_text.is_empty() {
                    self.input_text.clear();
                    return false;
                }

                // Handle active modals first
                if self.active_modal != ActiveModal::None {
                    match self.active_modal {
                        ActiveModal::LogoutPrompt => match k.code {
                            KeyCode::Char('y') | KeyCode::Char('Y') => {
                                let _ = std::fs::remove_file(".token_cache");
                                let _ = std::fs::remove_file(".channel_cache");
                                return true;
                            }
                            KeyCode::Char('n') | KeyCode::Char('N') | KeyCode::Esc => {
                                self.active_modal = ActiveModal::None;
                            }
                            _ => {}
                        },
                        ActiveModal::SwitchChannelPrompt => match k.code {
                            KeyCode::Esc => {
                                self.active_modal = ActiveModal::None;
                                self.modal_input.clear();
                            }
                            KeyCode::Backspace => {
                                self.modal_input.pop();
                            }
                            KeyCode::Enter => {
                                let input = self.modal_input.trim().to_string();
                                let clean_input = input.trim_end_matches('/');
                                let target_id = clean_input.split('/').last().unwrap_or("").split('?').next().unwrap_or("").to_string();
                                if !target_id.is_empty() && target_id.chars().all(|c| c.is_numeric()) {
                                    let _ = tx.send(AppEvent::SwitchChannel(target_id)).await;
                                }
                                self.modal_input.clear();
                                self.active_modal = ActiveModal::None;
                            }
                            KeyCode::Char(c) => {
                                self.modal_input.push(c);
                            }
                            _ => {}
                        },
                        ActiveModal::None => {}
                    }
                    return false;
                }

                // Global shortcut keys
                if k.code == KeyCode::Char('q') && k.modifiers.contains(KeyModifiers::CONTROL) {
                    self.queue_mode = !self.queue_mode;
                    let _ = tx.send(AppEvent::ToggleQueueMode).await;
                    return false;
                }
                if k.code == KeyCode::Char('c') && k.modifiers.contains(KeyModifiers::CONTROL) && self.input_text.is_empty() {
                    self.queue.clear();
                    let _ = tx.send(AppEvent::ClearQueue).await;
                    return false;
                }
                if k.code == KeyCode::Char('x') && k.modifiers.contains(KeyModifiers::CONTROL) {
                    self.active_modal = ActiveModal::LogoutPrompt;
                    return false;
                }
                if k.code == KeyCode::Char('g') && k.modifiers.contains(KeyModifiers::CONTROL) {
                    self.active_modal = ActiveModal::SwitchChannelPrompt;
                    self.modal_input.clear();
                    return false;
                }
                if (k.code == KeyCode::Char('p') || k.code == KeyCode::Char('P')) && self.input_text.is_empty() {
                    self.queue_mode = !self.queue_mode;
                    let _ = tx.send(AppEvent::ToggleQueueMode).await;
                    return false;
                }

                // Keystroke Latency Measurement: calculate time delta between key actions
                let now = Instant::now();
                if let Some(prev) = self.last_keystroke_time {
                    let diff_ms = now.duration_since(prev).as_millis() as u64;
                    if diff_ms > 10 && diff_ms < 1500 {
                        self.hardware_delay_ms = diff_ms;
                        let _ = tx.send(AppEvent::UpdateHardwareDelay(self.hardware_delay_ms)).await;
                    }
                }
                self.last_keystroke_time = Some(now);

                match k.code {
                    KeyCode::F(6) => {
                        self.queue_mode = !self.queue_mode;
                        let _ = tx.send(AppEvent::ToggleQueueMode).await;
                    }
                    KeyCode::F(7) => {
                        self.queue.clear();
                        let _ = tx.send(AppEvent::ClearQueue).await;
                    }
                    KeyCode::F(2) => {
                        self.show_timestamp = !self.show_timestamp;
                    }
                    KeyCode::F(3) => {
                        self.show_latency = !self.show_latency;
                    }
                    KeyCode::F(4) => {
                        self.active_modal = ActiveModal::LogoutPrompt;
                    }
                    KeyCode::F(5) => {
                        self.active_modal = ActiveModal::SwitchChannelPrompt;
                        self.modal_input.clear();
                    }
                    KeyCode::PageUp => {
                        let current = self.list_state.selected().unwrap_or(0);
                        let new_idx = current.saturating_sub(10);
                        self.list_state.select(Some(new_idx));
                    }
                    KeyCode::PageDown => {
                        let current = self.list_state.selected().unwrap_or(0);
                        let max_idx = self.messages.len().saturating_sub(1);
                        let new_idx = (current + 10).min(max_idx);
                        self.list_state.select(Some(new_idx));
                    }
                    KeyCode::Home => {
                        if !self.messages.is_empty() {
                            self.list_state.select(Some(0));
                        }
                    }
                    KeyCode::End => {
                        if !self.messages.is_empty() {
                            self.list_state.select(Some(self.messages.len() - 1));
                        }
                    }
                    KeyCode::Up if self.input_text.is_empty() || k.modifiers.contains(KeyModifiers::SHIFT) || k.modifiers.contains(KeyModifiers::ALT) => {
                        let current = self.list_state.selected().unwrap_or(0);
                        let new_idx = current.saturating_sub(1);
                        self.list_state.select(Some(new_idx));
                    }
                    KeyCode::Down if self.input_text.is_empty() || k.modifiers.contains(KeyModifiers::SHIFT) || k.modifiers.contains(KeyModifiers::ALT) => {
                        let current = self.list_state.selected().unwrap_or(0);
                        let max_idx = self.messages.len().saturating_sub(1);
                        let new_idx = (current + 1).min(max_idx);
                        self.list_state.select(Some(new_idx));
                    }
                    KeyCode::Tab => {
                        if let Some(last_failed_nonce) = self.failed_nonces.last().cloned() {
                            let (text, found) = if let Some(m) = self.messages.iter_mut().find(|x| x.nonce == last_failed_nonce) {
                                m.status = MessageStatus::Sending;
                                m.timestamp = format!("{} | ...", Local::now().format("%H:%M:%S%.3f"));
                                (m.content.clone(), true)
                            } else {
                                (String::new(), false)
                            };

                            if found {
                                self.outbound_timers.insert(last_failed_nonce.clone(), Instant::now());
                                let _ = tx.send(AppEvent::HttpSendChat {
                                    nonce: last_failed_nonce,
                                    text,
                                }).await;
                            }
                        }
                    }
                    KeyCode::Char(c) => {
                        self.input_text.push(c);
                        
                        let trigger = match self.last_typing_sent {
                            Some(last_sent) => last_sent.elapsed() >= std::time::Duration::from_secs(7),
                            None => true,
                        };

                        if trigger && !self.target_channel_id.is_empty() {
                            self.last_typing_sent = Some(std::time::Instant::now());
                            let _ = tx.send(AppEvent::HttpTriggerTyping).await;
                        }
                    }
                    KeyCode::Backspace => {
                        self.input_text.pop();
                    }
                    KeyCode::Enter if !self.input_text.is_empty() => {
                        self.last_typing_sent = None;
                        let text = std::mem::take(&mut self.input_text);
                        let parsed_number = parse_text_number(&text);

                        let is_number = parsed_number.is_some();

                        if !self.queue_mode || !is_number {
                            // Standard Text or Queue Mode Disabled: Bypass Queue completely
                            let now_instant = Instant::now();
                            let nonce = format!("n-{}", chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0));
                            let current_time_str = Local::now().format("%H:%M:%S%.3f").to_string();

                            self.outbound_timers.insert(nonce.clone(), now_instant);

                            self.messages.push(DiscordMessage {
                                nonce: nonce.clone(),
                                author: self.self_username.clone(),
                                content: text.clone(),
                                timestamp: format!("{} | ...", current_time_str),
                                status: MessageStatus::Sending,
                            });

                            if !self.messages.is_empty() {
                                self.list_state.select(Some(self.messages.len() - 1));
                            }

                            let _ = tx.send(AppEvent::HttpSendChat { nonce, text }).await;
                        } else if let Some(num) = parsed_number {
                            // Queue Mode Enabled & Number Detected (+2 generator logic)
                            if self.queue.is_empty() {
                                // First number X is sent immediately to Discord
                                let now_instant = Instant::now();
                                let nonce = format!("n-{}", chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0));
                                let current_time_str = Local::now().format("%H:%M:%S%.3f").to_string();

                                self.outbound_timers.insert(nonce.clone(), now_instant);

                                self.messages.push(DiscordMessage {
                                    nonce: nonce.clone(),
                                    author: self.self_username.clone(),
                                    content: num.to_string(),
                                    timestamp: format!("{} | ...", current_time_str),
                                    status: MessageStatus::Sending,
                                });

                                if !self.messages.is_empty() {
                                    self.list_state.select(Some(self.messages.len() - 1));
                                }

                                let _ = tx.send(AppEvent::HttpSendChat { nonce, text: num.to_string() }).await;

                                // X+2 stored in queue array
                                self.queue.push(num + 2);
                                let _ = tx.send(AppEvent::EnqueueNumberItem(num + 2)).await;
                            } else {
                                // Queue non-empty: Y+2 pushed to tail
                                self.queue.push(num + 2);
                                let _ = tx.send(AppEvent::EnqueueNumberItem(num + 2)).await;
                            }
                        }
                    }
                    _ => {}
                }
            }
            _ => {}
        }
        false
    }
}
