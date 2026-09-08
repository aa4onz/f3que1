use crate::models::{QueuedItem, DiscordMessage, MessageStatus, AppEvent};
use crate::app::state::AppState;
use chrono::Local;
use std::time::Instant;
use tokio::sync::mpsc::Sender;

/// Parses input text for leading/embedded numbers and computes the next incremented value (+2).
pub fn parse_and_increment_text(text: &str) -> Option<(i64, String)> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }

    let mut num_chars = String::new();
    let mut prefix_len = 0;

    for (i, c) in text.char_indices() {
        if c.is_ascii_digit() {
            num_chars.push(c);
        } else if num_chars.is_empty() && (c == ' ' || c == '_') {
            prefix_len = i + c.len_utf8();
            continue;
        } else {
            break;
        }
    }

    if let Ok(num) = num_chars.parse::<i64>() {
        let suffix = &text[prefix_len + num_chars.len()..];
        let next_num = num + 2;
        let prefix = &text[..prefix_len];
        let new_text = format!("{}{}{}", prefix, next_num, suffix);
        Some((next_num, new_text))
    } else {
        None
    }
}

/// Queue execution rules:
/// Places entered numbers into the queue first without sending immediately.
/// Additional custom queue validation logic can be added here cleanly.
pub async fn process_queue_input(
    app: &mut AppState,
    text: String,
    tx: &Sender<AppEvent>,
) {
    let parsed_inc = parse_and_increment_text(&text);

    if !app.queue_mode || parsed_inc.is_none() {
        // Standard non-numeric text or Queue Mode disabled: Direct send
        let now_instant = Instant::now();
        let nonce = format!("n-{}", chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0));
        let current_time_str = Local::now().format("%H:%M:%S%.3f").to_string();

        app.outbound_timers.insert(nonce.clone(), now_instant);

        app.messages.push(DiscordMessage {
            nonce: nonce.clone(),
            author: app.self_username.clone(),
            content: text.clone(),
            timestamp: format!("{} | ...", current_time_str),
            status: MessageStatus::Sending,
        });

        if !app.messages.is_empty() {
            app.list_state.select(Some(app.messages.len() - 1));
        }

        let _ = tx.send(AppEvent::HttpSendChat { nonce, text }).await;
    } else if let Some((num, next_text)) = parsed_inc {
        // Numeric input in Queue Mode:
        // Always place the item directly into queue first (does not send instantly)
        let current_queued_item = QueuedItem {
            content: text,
            number: num - 2,
        };

        let next_queued_item = QueuedItem {
            content: next_text,
            number: num,
        };

        app.queue.push(current_queued_item.clone());
        let _ = tx.send(AppEvent::EnqueueNumberItem(current_queued_item)).await;

        app.queue.push(next_queued_item.clone());
        let _ = tx.send(AppEvent::EnqueueNumberItem(next_queued_item)).await;
    }
}
