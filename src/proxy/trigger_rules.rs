use crate::models::{ProxyResponse, QueuedItem};
use crate::proxy::queue::execute_queued_reaction;
use crate::proxy::state::ProxyState;
use std::sync::Arc;
use tokio::sync::broadcast;

/// Rules governing when a stealth queue reaction should trigger:
/// 1. Self message check: Never trigger on own messages.
/// 2. Bot check: If sender is a bot, cancel and clear queue immediately.
/// 3. Cannot send twice in a row: Queue only triggers if the last author was someone else.
/// 4. Duplicate sender protection: Only trigger once on valid user message, then pop/clear.
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

    // Rule 1: Check if queue mode is enabled
    if !state.is_queue_mode_enabled().await {
        return;
    }

    // Rule 2: If message author is self -> do not trigger queue (cannot send twice in a row)
    if state.is_self_author(author_id, author_uname).await {
        return;
    }

    // Rule 3: If sender is a bot -> clear queue immediately and stop
    if is_bot {
        let cleared = state.clear_queue(cid).await;
        let _ = gw_broadcast_tx.send(ProxyResponse::QueueSync { queue: cleared });
        return;
    }

    // Rule 4: Valid user message received -> trigger exactly ONE queued item, then stop
    if let Some((item, _)) = state.pop_next_item(cid).await {
        // Clear remaining items so duplicate messages from others do not trigger again
        let cleared_remaining = state.clear_queue(cid).await;

        execute_queued_reaction(
            item,
            cid.to_string(),
            discord_token,
            http_client,
            gw_broadcast_tx,
            cleared_remaining,
        )
        .await;
    }
}
