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

    println!("[DEBUG] 🟢 Entry triggered for channel: {}", channel_id);

    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    // Rule 1: Queue Mode must be active
    if !state.is_queue_mode_enabled().await {
        println!("[DEBUG] 🛑 Stopped at Rule 1: Queue Mode Disabled");
        return;
    }

    // Rule 2 & Rule 3: Check queue existence, zero check, and queue non-empty check
    {
        let map = state.active_queue.read().await;
        if let Some(q) = map.get(channel_id) {
            if let Some(top_item) = q.first() {
                // Rule 2: Zero check -> if top item is 0, clear queue and exit
                if top_item.number == 0 || top_item.content.trim() == "0" {
                    println!("[DEBUG] 🛑 Stopped at Rule 2: Zero Check matched '0'. Clearing queue.");
                    drop(map);
                    let cleared = state.clear_queue(channel_id).await;
                    let _ = gw_broadcast_tx.send(ProxyResponse::QueueSync { queue: cleared });
                    return;
                }
            } else {
                println!("[DEBUG] 🛑 Stopped: Top item in queue is None.");
                return;
            }

            // Rule 3: Emptiness check -> Queue must not be empty to process
            if q.is_empty() {
                println!("[DEBUG] 🛑 Stopped at Rule 3: Queue is empty");
                return;
            }
        } else {
            println!("[DEBUG] 🛑 Stopped: No active queue found for this channel ID.");
            return;
        }
    }

    // Read the latest message strictly from the RAM chat cache
    let data = match state.get_cached_chat(channel_id).await {
        Some(cached) => {
            println!("[DEBUG] Cache lookup hit. Content length parsed.");
            cached
        }
        None => {
            println!("[DEBUG] ⚠️ Cache empty. Attempting one-time fallback HTTP GET fetch...");
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
                        println!("[DEBUG] 🛑 Fallback fetch returned an empty array.");
                        return;
                    }
                } else {
                    println!("[DEBUG] 🛑 Fallback fetch JSON parsing failed.");
                    return;
                }
            } else {
                println!("[DEBUG] 🛑 Fallback fetch network request failed.");
                return;
            }
        }
    };

    let msg_id = data["id"].as_str().unwrap_or("");
    println!(
        "[DEBUG] Current message ID: {} | Text snippet: {:?}",
        msg_id,
        data["content"].as_str().unwrap_or("")
    );

    // ATOMIC DEDUPLICATION: Ensures duplicate execution is avoided for processed messages
    if !msg_id.is_empty() {
        if !state.try_mark_message_processed(channel_id, msg_id).await {
            println!(
                "[DEBUG] 🛑 Stopped at Deduplication Check: Message {} was already handled.",
                msg_id
            );
            return;
        }
    }

    let author_id = data["author"]["id"].as_str().unwrap_or("");
    let author_uname = data["author"]["username"].as_str().unwrap_or("");
    println!(
        "[DEBUG] Message Creator ID: {} | Username: {}",
        author_id, author_uname
    );

    // Cannot trigger on own message
    if state.is_self_author(author_id, author_uname).await {
        println!(
            "[DEBUG] ✨ Stopped at Self-Author Check: This is OUR own bot message. Safely flipping lock back to FALSE."
        );
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
        println!(
            "[DEBUG] 🛑 Stopped at Bot Check: Target message belongs to another external bot. Clearing active queue."
        );
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

    // 🟩 SELF-HEALING OVERRIDE TIMEOUT: Place directly above the circuit breaker!
    // If the system lock gets stuck at TRUE for over 1.5 seconds without a gateway clean-up event, 
    // forcefully break it so regular channel triggers can recover.
    if state.last_sender_was_me.load(Ordering::SeqCst) {
        let last_event = state.last_live_event_time.load(Ordering::SeqCst);
        if now_ms.saturating_sub(last_event) > 1500 {
            println!("[DEBUG] 🛡️ Self-Healing: Resetting stuck lock back to FALSE.");
            state.last_sender_was_me.store(false, Ordering::SeqCst);
        }
    }

    // 🔒 CIRCUIT BREAKER: Check lock status
    println!(
        "[DEBUG] 🔒 Checking Circuit Breaker gate. Current lock memory status: {}",
        state.last_sender_was_me.load(Ordering::SeqCst)
    );
    if state
        .last_sender_was_me
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        println!(
            "[DEBUG] ❌ CRITICAL: BLOCKED BY CIRCUIT BREAKER! last_sender_was_me is TRUE. This thread is dropping out!"
        );
        return;
    }

    println!("[DEBUG] 🔓 PASSED CIRCUIT BREAKER. Memory lock successfully engaged to TRUE.");
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
            println!(
                "[DEBUG] 🛑 Aborting API send: Queue became empty during delay phase. Reverting lock back to FALSE."
            );
            state.last_sender_was_me.store(false, Ordering::SeqCst);
            return;
        }
    };

    println!(
        "[DEBUG] 📤 Dispatching HTTP POST request payload for number '{}'...",
        top_item.content
    );
    let updated_q = state
        .update_item_status(channel_id, 0, DeliveryStatus::Sending)
        .await;
    let _ = gw_broadcast_tx.send(ProxyResponse::QueueSync { queue: updated_q });

    // Inline HTTP POST Execution
    let msg_url = format!(
        "https://discord.com/api/v10/channels/{}/messages",
        channel_id
    );
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
            println!("[DEBUG] ✅ Discord API response 200 OK received successfully.");
            if let Some((_, remaining_q)) = state.pop_next_item(channel_id).await {
                let _ = gw_broadcast_tx.send(ProxyResponse::QueueSync { queue: remaining_q });
            }
        }
        Ok(resp) => {
            let err_text = resp
                .text()
                .await
                .unwrap_or_else(|_| "Rate Limited / Rejected".to_string());
            println!("[DEBUG] ❌ Discord API rejected message payload: {}", err_text);
            let updated_q = state
                .update_item_status(channel_id, 0, DeliveryStatus::Failed)
                .await;

            state.last_sender_was_me.store(false, Ordering::SeqCst);

            let _ = gw_broadcast_tx.send(ProxyResponse::QueueSync { queue: updated_q });
            let _ = gw_broadcast_tx.send(ProxyResponse::QueuedMessageFailed {
                nonce,
                content: top_item.content,
                error: Some(err_text),
            });
        }
        Err(e) => {
            println!("[DEBUG] ❌ Network request connection error encountered: {}", e);
            let updated_q = state
                .update_item_status(channel_id, 0, DeliveryStatus::Failed)
                .await;

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
