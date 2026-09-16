use crate::models::ProxyResponse;
use crate::proxy::queue::execute_queued_reaction;
use crate::proxy::state::ProxyState;
use crate::proxy::utils::generate_snowflake_nonce;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;

/// Rules governing when a stealth queue reaction should trigger:
/// 1. Queue Mode must be active.
/// 2. Zero check: If top item number is 0 (or content "0"), clear entire queue and do not send.
/// 3. Emptiness check: Requires queue to be non-empty to process.
/// 4. RAM Cache Lookup: Evaluates strictly against `state.get_cached_chat(...)`, falling back to HTTP GET once if empty.
/// 5. Sender & Duplicate check: Atomically marks message ID as processed. Bot check empties queue and emits failure.
pub async fn evaluate_and_trigger_queue(
    _message_data: Option<&serde_json::Value>,
    channel_id: &str,
    state: &ProxyState,
    discord_token: String,
    http_client: Arc<reqwest::Client>,
    gw_broadcast_tx: broadcast::Sender<ProxyResponse>,
    force_skip_delay: bool,
) {
    if channel_id.is_empty() {
        return;
    }

    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    // Rule 1: Queue Mode must be active
    if !state.is_queue_mode_enabled().await {
        return;
    }

    // Rule 2 & Rule 3: Check queue existence, zero check, and queue non-empty check
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

            // Rule 3: Emptiness check -> Queue must not be empty to process
            if q.is_empty() {
                return;
            }
        } else {
            return;
        }
    }

    // Read the latest message strictly from the RAM chat cache
    let data = match state.get_cached_chat(channel_id).await {
        Some(cached) => cached,
        None => {
            // One-time fallback fetch if proxy app just started up and cache is unpopulated
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
                        let fetched = first_msg.clone();
                        state.update_cached_chat(channel_id, fetched.clone()).await;
                        fetched
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

    state.last_live_event_time.store(now_ms, Ordering::SeqCst);
    state.last_sender_was_me.store(false, Ordering::SeqCst);

    let msg_id = data["id"].as_str().unwrap_or("");

    // ATOMIC DEDUPLICATION: Ensures duplicate execution is avoided for processed messages
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

    // Bot message -> Empty queue and emit top queue item as failed
    if is_bot {
        if let Some((top_item, _)) = state.pop_next_item(channel_id).await {
            let cleared = state.clear_queue(channel_id).await;
            let _ = gw_broadcast_tx.send(ProxyResponse::QueueSync { queue: cleared });

            let nonce = generate_snowflake_nonce();
            let _ = gw_broadcast_tx.send(ProxyResponse::QueuedMessageFailed {
                nonce,
                content: top_item.content,
                error: Some("Rate Limited / Cancelled on Bot Message".to_string()),
            });
        } else {
            let cleared = state.clear_queue(channel_id).await;
            let _ = gw_broadcast_tx.send(ProxyResponse::QueueSync { queue: cleared });
        }
        return;
    }

    // Circuit breaker: ensure strictly one thread triggers back-to-back
    if state
        .last_sender_was_me
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
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
            force_skip_delay,
        )
        .await;
    }
}
