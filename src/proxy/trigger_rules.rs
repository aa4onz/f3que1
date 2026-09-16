use crate::models::{DeliveryStatus, ProxyResponse, ReactionDelayMode};
use crate::proxy::state::ProxyState;
use crate::proxy::utils::generate_snowflake_nonce;
use rand::Rng;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
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

    let msg_id = data["id"].as_str().unwrap_or("");

    // ATOMIC DEDUPLICATION: Ensures duplicate execution is avoided for processed messages
    if !msg_id.is_empty() {
        if !state.try_mark_message_processed(channel_id, msg_id).await {
            return;
        }
    }

    let author_id = data["author"]["id"].as_str().unwrap_or("");
    let author_uname = data["author"]["username"].as_str().unwrap_or("");

    // Cannot trigger on own message
    if state.is_self_author(author_id, author_uname).await {
        // Reset the sender lock because our message was sent and hit the gateway!
        state.last_sender_was_me.store(false, Ordering::SeqCst);
        return;
    }

    // Enhanced Bot Detection with explicit Bot IDs for fast & reliable lookup
    let is_known_bot_id = author_id == "510016054391734273" || author_id == "639599059036012605";
    let is_bot = is_known_bot_id
        || data["author"]["bot"].as_bool().unwrap_or(false)
        || data["webhook_id"].is_string()
        || data["type"].as_u64().map_or(false, |t| t != 0 && t != 19);

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

    // 🔒 CIRCUIT BREAKER: Strictly block execution if a thread is already running or sending
    if state
        .last_sender_was_me
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        return; // Thread finishes here immediately!
    }

    // Only update live event time if this thread safely acquired the lock
    state.last_live_event_time.store(now_ms, Ordering::SeqCst);

    // Reaction Delay
    if !force_skip_delay {
        let delay_mode = state.get_reaction_delay_mode().await;
        let delay_ms = match delay_mode {
            ReactionDelayMode::Normal => rand::thread_rng().gen_range(200..=300),
            ReactionDelayMode::Fast => rand::thread_rng().gen_range(0..=200),
            ReactionDelayMode::Instant => 0,
        };
        if delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        }
    }

    // Peek top item and set DeliveryStatus::Sending
    let top_item = match state.peek_top_item(channel_id).await {
        Some(item) => item,
        None => {
            state.last_sender_was_me.store(false, Ordering::SeqCst);
            return;
        }
    };

    let updated_q = state
        .update_item_status(channel_id, 0, DeliveryStatus::Sending)
        .await;
    let _ = gw_broadcast_tx.send(ProxyResponse::QueueSync { queue: updated_q });

    // Inline HTTP POST Execution
    let msg_url = format!("https://discord.com/api/v10/channels/{}/messages", channel_id);
    let nonce = generate_snowflake_nonce();
    let payload = serde_json::json!({
        "content": top_item.content,
        "nonce": nonce
    });

    let res = http_client
        .post(&msg_url)
        .header("Authorization", &discord_token)
        .header("Content-Type", "application/json")
        .json(&payload)
        .send()
        .await;

    match res {
        Ok(resp) if resp.status().is_success() => {
            if let Some((_, remaining_q)) = state.pop_next_item(channel_id).await {
                let _ = gw_broadcast_tx.send(ProxyResponse::QueueSync { queue: remaining_q });
            }
            // Note: Keep lock active until Discord confirmation hits the WebSocket gateway.
            // This stops users from fast-spamming messages while your HTTP request is in transit!
        }
        Ok(resp) => {
            let err_text = resp
                .text()
                .await
                .unwrap_or_else(|_| "Rate Limited / Rejected".to_string());
            let updated_q = state
                .update_item_status(channel_id, 0, DeliveryStatus::Failed)
                .await;

            // Release lock on failure so the system doesn't permanently freeze
            state.last_sender_was_me.store(false, Ordering::SeqCst);

            let _ = gw_broadcast_tx.send(ProxyResponse::QueueSync { queue: updated_q });
            let _ = gw_broadcast_tx.send(ProxyResponse::QueuedMessageFailed {
                nonce,
                content: top_item.content,
                error: Some(err_text),
            });
        }
        Err(e) => {
            let updated_q = state
                .update_item_status(channel_id, 0, DeliveryStatus::Failed)
                .await;

            // Release lock on failure so the system doesn't permanently freeze
            state.last_sender_was_me.store(false, Ordering::SeqCst);

            let _ = gw_broadcast_tx.send(ProxyResponse::QueueSync { queue: updated_q });
            let _ = gw_broadcast_tx.send(ProxyResponse::QueuedMessageFailed {
                nonce,
                content: top_item.content,
                error: Some(e.to_string()),
            });
        }
    }
}
