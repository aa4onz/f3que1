use crate::app::state::AppState;
use crate::models::{AppEvent, DiscordMessage, MessageStatus, QueuedItem};
use chrono::Local;
use std::time::Instant;
use tokio::sync::mpsc::Sender;

/// Dispatches direct messages for non-queueable items.
pub async fn send_direct_message(app: &mut AppState, text: String, tx: &Sender<AppEvent>) {
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
}

/// Enqueues the parsed item into the queue without sending it immediately.
pub async fn enqueue_parsed_item(
    app: &mut AppState,
    text: String,
    number: i64,
    tx: &Sender<AppEvent>,
) {
    let queued_item = QueuedItem {
        content: text,
        number,
        was_empty: false,
    };

    app.queue.push(queued_item.clone());
    let _ = tx.send(AppEvent::EnqueueNumberItem(queued_item)).await;
}
