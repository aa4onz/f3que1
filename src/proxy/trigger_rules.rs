use crate::models::ProxyResponse;
use crate::proxy::queue::execute_queued_reaction;
use crate::proxy::state::ProxyState;
use std::sync::Arc;
use tokio::sync::broadcast;

/// Rules governing when a stealth queue reaction should trigger:
/// 1. Queue Mode must be active.
/// 2. Zero check: If top item number is 0 (or content "0"), clear entire queue and do not send.
/// 3. Queue size requirement: Queue must contain AT LEAST 2 items (queue size > 1).
/// 4. First number added when queue was empty rule:
///    If the top item was added when queue was empty (`was_empty == true`), it bypasses all sender/bot/response
///    filters and triggers immediately as soon as queue size > 1 (without waiting for external messages).
/// 5. Standard rules for subsequent items (`was_empty == false`):
///    - Requires an incoming message event (`message_data` is present).
///    - Self message check: Never trigger on own messages.
///    - Bot check: If sender is a bot, cancel and clear queue immediately.
pub async fn evaluate_and_trigger_queue(
    message_data: Option<&serde_json::Value>,
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

    let is_first_when_empty = {
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
                top_item.was_empty
            } else {
                return;
            }
        } else {
            return;
        }
    };

    // Rule 3: Queue size requirement: Queue size must be > 1 (at least 2 items in queue)
    {
        let map = state.active_queue.read().await;
        if let Some(q) = map.get(channel_id) {
            if q.len() <= 1 {
                return;
            }
        } else {
            return;
        }
    }

    // If was_empty == true, bypass sender filter, bot checks, and waiting for other responses.
    if !is_first_when_empty {
        // Standard Rules for subsequent items require an incoming message event
        let data = match message_data {
            Some(d) => d,
            None => return,
        };

        let author_id = data["author"]["id"].as_str().unwrap_or("");
        let author_uname = data["author"]["username"].as_str().unwrap_or("");
        let is_bot = data["author"]["bot"].as_bool().unwrap_or(false);

        // Rule 5a: Cannot trigger on own message
        if state.is_self_author(author_id, author_uname).await {
            return;
        }

        // Rule 5b: If message sender is a bot -> clear queue immediately
        if is_bot {
            let cleared = state.clear_queue(channel_id).await;
            let _ = gw_broadcast_tx.send(ProxyResponse::QueueSync { queue: cleared });
            return;
        }
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
        )
        .await;
    }
}
