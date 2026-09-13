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
/// 4. Sender & Duplicate check:
///    - Supports both instant WebSocket event payload AND REST API fallback.
///    - Atomically marks message ID as processed to strictly guarantee 1 trigger per Discord message.
///    - Bot check: Explicitly checks bot IDs (510016054391734273, 639599059036012605) alongside standard bot flags.
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

    // Fetch last message from Discord API if no direct gateway event payload was provided
    let fetched_last_msg;
    let data = match message_data {
        Some(d) => d,
        None => {
            let url = format!(
                "https://discord.com/api/v10/channels/{}/messages?limit=1",
                channel_id
            );
            let res = http_client
                .get(&url)
                .header("Authorization", &discord_token)
                .send()
                .await;

            if let Ok(resp) = res {
                if let Ok(arr) = resp.json::<serde_json::Value>().await {
                    if let Some(first_msg) = arr.get(0) {
                        fetched_last_msg = first_msg.clone();
                        &fetched_last_msg
                    } else {
                        return;
                    }
                } else {
                    return;
                }
            } else {
                return;
            }
        }
    };

    let msg_id = data["id"].as_str().unwrap_or("");

    // ATOMIC DEDUPLICATION: Ensures REST fallback and WebSocket payload never conflict or double-trigger
    if !msg_id.is_empty() {
        if !state.try_mark_message_processed(channel_id, msg_id).await {
            return;
        }
    }

    let author_id = data["author"]["id"].as_str().unwrap_or("");
    let author_uname = data["author"]["username"].as_str().unwrap_or("");

    // Enhanced Bot Detection with explicit Bot IDs for fast & reliable lookup
    let is_known_bot_id = author_id == "510016054391734273" || author_id == "639599059036012605";
    let is_bot = is_known_bot_id
        || data["author"]["bot"].as_bool().unwrap_or(false)
        || data["webhook_id"].is_string()
        || data["type"].as_u64().map_or(false, |t| t != 0 && t != 19);

    // Cannot trigger on own message
    if state.is_self_author(author_id, author_uname).await {
        return;
    }

    // Bot message -> trigger top item immediately and clear the rest of the queue
    if is_bot {
        if let Some((item, _)) = state.pop_next_item(channel_id).await {
            let cleared = state.clear_queue(channel_id).await;
            execute_queued_reaction(
                item,
                channel_id.to_string(),
                discord_token,
                http_client,
                gw_broadcast_tx.clone(),
                cleared,
                state.clone(),
            )
            .await;
        } else {
            let cleared = state.clear_queue(channel_id).await;
            let _ = gw_broadcast_tx.send(ProxyResponse::QueueSync { queue: cleared });
        }

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

    // Trigger exactly ONE next item
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
