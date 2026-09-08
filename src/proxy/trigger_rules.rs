use crate::models::ProxyResponse;
use crate::proxy::queue::execute_queued_reaction;
use crate::proxy::state::ProxyState;
use std::sync::Arc;
use tokio::sync::broadcast;

/// Rules governing when a stealth queue reaction should trigger:
/// 1. Self message check: Never trigger on own messages (cannot send twice in a row).
/// 2. Bot check: If sender is a bot, cancel and clear queue immediately.
/// 3. Queue size requirement: Queue must contain AT LEAST 2 items (first item waits for 2nd item).
/// 4. Zero check: If top item number is 0 (or content "0"), clear entire queue and do not send.
pub async fn evaluate_and_trigger_queue(
    data: &serde_json::Value,
    state: &ProxyState,
    discord_token: String,
    http_client: Arc<reqwest::Client>,
    gw_broadcast_tx: broadcast::Sender<ProxyResponse>,
) {
    let cid = data["channel_id"].as_str().unwrap_or("");
    if cid.is_empty() {
        return;
    }

    let author_id = data["author"]["id"].as_str().unwrap_or("");
    let author_uname = data["author"]["username"].as_str().unwrap_or("");
    let is_bot = data["author"]["bot"].as_bool().unwrap_or(false);

    // Rule 1: Queue Mode must be active
    if !state.is_queue_mode_enabled().await {
        return;
    }

    // Rule 2: Cannot trigger on own message (prevents sending twice in a row)
    if state.is_self_author(author_id, author_uname).await {
        return;
    }

    // Rule 3: If message sender is a bot -> clear queue immediately
    if is_bot {
        let cleared = state.clear_queue(cid).await;
        let _ = gw_broadcast_tx.send(ProxyResponse::QueueSync { queue: cleared });
        return;
    }

    // Rule 4: Inspect queue before popping (must have >= 2 items to trigger)
    {
        let map = state.active_queue.read().await;
        if let Some(q) = map.get(cid) {
            if q.len() < 2 {
                // Wait until at least 2 items are present in queue
                return;
            }

            // Check if top item is '0' -> if so, clear queue and exit
            if let Some(top_item) = q.first() {
                if top_item.number == 0 || top_item.content.trim() == "0" {
                    drop(map);
                    let cleared = state.clear_queue(cid).await;
                    let _ = gw_broadcast_tx.send(ProxyResponse::QueueSync { queue: cleared });
                    return;
                }
            }
        } else {
            return;
        }
    }

    // Rule 5: Trigger exactly ONE next item and preserve remaining queue
    if let Some((item, remaining_q)) = state.pop_next_item(cid).await {
        execute_queued_reaction(
            item,
            cid.to_string(),
            discord_token,
            http_client,
            gw_broadcast_tx,
            remaining_q,
        )
        .await;
    }
}
