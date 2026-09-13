use crate::models::ProxyResponse;
use crate::proxy::queue::execute_queued_reaction;
use crate::proxy::state::ProxyState;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;

/// Rules governing when a stealth queue reaction should trigger:
/// 1. Queue Mode must be active.
/// 2. Zero check: If top item number is 0 (or content "0"), clear entire queue and do not send.
/// 3. Size check: Requires queue size >= 2 to process.
/// 4. Sender check:
///    - Triggers ONLY on incoming WebSocket MESSAGE_CREATE payload (100% realtime, 0ms HTTP REST overhead).
///    - Self message check: Never trigger on own messages.
///    - Bot check: If sender is a bot or webhook, cancel and clear queue immediately, and schedule another clear after 1 second.
///    - Duplicate check: Ignore messages that have already triggered a reaction.
pub async fn evaluate_and_trigger_queue(
    message_data: &serde_json::Value,
    channel_id: &str,
    state: &ProxyState,
    discord_token: String,
    http_client: Arc<reqwest::Client>,
    gw_broadcast_tx: broadcast::Sender<ProxyResponse>,
) {
    if channel_id.is_empty() {
        return;
    }

    // Rule 1: Queue Mode must be active
    if !state.is_queue_mode_enabled().await {
        return;
    }

    // Rule 2 & Rule 3: Check queue existence, zero check, and queue size check
    {
        let map = state.active_queue.read().await;
        if let Some(q) = map.get(channel_id) {
            if let Some(top_item) = q.first() {
                // Rule 2: Zero check -> if top item is 0, clear queue and exit
                if top_item.number == 0 || top_item.content.trim() == "0" {
                    drop(map);
                    let cleared = state.clear_queue(channel_id).await;
                    let _ = gw_broadcast_tx.send(ProxyResponse::QueueSync { queue: cleared });
                    return;
                }
            } else {
                return;
            }

            // Rule 3: Size check -> Queue size must be at least 2 to process
            if q.len() < 2 {
                return;
            }
        } else {
            return;
        }
    }

    let msg_id = message_data["id"].as_str().unwrap_or("");
    
    // Check and set processed message atomically to prevent duplicate trigger races
    if !msg_id.is_empty() {
        if !state.try_mark_message_processed(channel_id, msg_id).await {
            return;
        }
    }

    let author_id = message_data["author"]["id"].as_str().unwrap_or("");
    let author_uname = message_data["author"]["username"].as_str().unwrap_or("");
    
    // Enhanced Bot Detection: Check boolean, bot discriminator, webhook_id, or message type
    let is_bot = message_data["author"]["bot"].as_bool().unwrap_or(false)
        || message_data["webhook_id"].is_string()
        || message_data["type"].as_u64().map_or(false, |t| t != 0 && t != 19); // 0 = DEFAULT, 19 = REPLY

    // Rule 4: Cannot trigger on own message
    if state.is_self_author(author_id, author_uname).await {
        return;
    }

    // Rule 4: If message sender is a bot -> clear queue immediately & again after 1s
    if is_bot {
        let cleared = state.clear_queue(channel_id).await;
        let _ = gw_broadcast_tx.send(ProxyResponse::QueueSync { queue: cleared });

        let state_clone = state.clone();
        let channel_id_clone = channel_id.to_string();
        let tx_clone = gw_broadcast_tx.clone();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let cleared_delayed = state_clone.clear_queue(&channel_id_clone).await;
            let _ = tx_clone.send(ProxyResponse::QueueSync { queue: cleared_delayed });
        });
        return;
    }

    // Trigger exactly ONE next item and preserve remaining queue
    if let Some((item, remaining_q)) = state.pop_next_item(channel_id).await {
        execute_queued_reaction(
            item,
            channel_id.to_string(),
            discord_token,
            http_client,
            gw_broadcast_tx,
            remaining_q,
            state.clone(),
        )
        .await;
    }
}
