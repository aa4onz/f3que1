// src/bin/remote_proxy.rs
use fast_discord_tui::models::{ProxyAction, ProxyResponse};
use futures_util::{SinkExt, StreamExt};
use rand::Rng;
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, ACCEPT_LANGUAGE, USER_AGENT};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, Mutex, RwLock};
use tokio::time::{interval, sleep, Duration};
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::{accept_async, connect_async};

#[derive(Deserialize)]
struct GatewayPayload {
    op: u8,
    #[serde(default)]
    d: serde_json::Value,
    #[serde(default)]
    t: Option<String>,
}

/// Generates realistic human reaction jitter following a realistic skewed distribution (120ms - 380ms)
fn generate_human_reaction_jitter(hardware_delay: u64) -> u64 {
    let mut rng = rand::thread_rng();
    // Skewed human cognitive reaction latency (base reaction + hardware baseline)
    let cognitive_delay: u64 = match rng.gen_range(1..=100) {
        1..=70 => rng.gen_range(140..=260),  // Standard fast human reaction (70% probability)
        71..=92 => rng.gen_range(261..=380), // Normal human variance (22% probability)
        _ => rng.gen_range(381..=520),       // Slight hesitation/lag (8% probability)
    };
    hardware_delay.saturating_add(cognitive_delay)
}

/// Generates Discord Snowflake ID formatted string for nonces
fn generate_snowflake_nonce() -> String {
    let discord_epoch: u64 = 1420070400000;
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    
    let timestamp_part = now_ms.saturating_sub(discord_epoch);
    let worker_id: u64 = 1;
    let process_id: u64 = 1;
    let increment: u64 = rand::thread_rng().gen_range(0..=4095);

    let snowflake = (timestamp_part << 22) | (worker_id << 17) | (process_id << 12) | increment;
    snowflake.to_string()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    let discord_token = env::var("DISCORD_TOKEN")
        .expect("DISCORD_TOKEN environment variable must be set on the proxy server");

    let port = env::var("PORT").unwrap_or_else(|_| "8080".to_string());
    let addr = format!("0.0.0.0:{}", port);

    let listener = TcpListener::bind(&addr).await?;
    println!("Remote stealth proxy server listening on ws://{}", addr);

    let (gw_tx, _) = broadcast::channel::<ProxyResponse>(512);

    // Global Queue and Stealth State across channels
    let active_queue: Arc<RwLock<HashMap<String, Vec<i64>>>> = Arc::new(RwLock::new(HashMap::new()));
    let queue_mode_enabled = Arc::new(RwLock::new(true));
    let hardware_delay_ms = Arc::new(RwLock::new(45u64));
    let self_user_id = Arc::new(RwLock::new(String::new()));

    // Shared Gateway Write Stream for Typing events
    let gw_write_arc: Arc<Mutex<Option<futures_util::stream::SplitSink<tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>, Message>>>> = Arc::new(Mutex::new(None));

    // HTTP Client initialization
    let mut default_headers = HeaderMap::new();
    default_headers.insert(ACCEPT, HeaderValue::from_static("*/*"));
    default_headers.insert(ACCEPT_LANGUAGE, HeaderValue::from_static("en-US,en;q=0.9"));
    default_headers.insert(USER_AGENT, HeaderValue::from_static("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"));

    let http_client = Arc::new(
        reqwest::Client::builder()
            .tcp_nodelay(true)
            .tcp_keepalive(Duration::from_secs(15))
            .default_headers(default_headers)
            .build()?
    );

    // Spawn Gateway WebSocket Loop
    let gw_broadcast_tx = gw_tx.clone();
    let token_clone = discord_token.clone();
    let queue_ref = Arc::clone(&active_queue);
    let queue_mode_ref = Arc::clone(&queue_mode_enabled);
    let delay_ref = Arc::clone(&hardware_delay_ms);
    let client_ref = Arc::clone(&http_client);
    let gw_writer_ref = Arc::clone(&gw_write_arc);
    let self_id_ref = Arc::clone(&self_user_id);

    tokio::spawn(async move {
        let gw_url = "wss://gateway.discord.gg/?v=10&encoding=json";
        loop {
            if let Ok((ws, _)) = connect_async(gw_url).await {
                let (write, mut read) = ws.split();
                *gw_writer_ref.lock().await = Some(write);

                if let Some(Ok(Message::Text(t))) = read.next().await {
                    if let Ok(p) = serde_json::from_str::<GatewayPayload>(&t) {
                        if p.op == 10 {
                            let heartbeat_interval = p.d["heartbeat_interval"].as_u64().unwrap_or(41250);
                            let identify = serde_json::json!({
                                "op": 2,
                                "d": {
                                    "token": token_clone,
                                    "capabilities": 16381,
                                    "properties": {
                                        "$os": "Windows",
                                        "$browser": "Chrome",
                                        "$device": "",
                                        "system_locale": "en-US",
                                        "browser_user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
                                        "browser_version": "128.0.0.0",
                                        "os_version": "10",
                                        "referrer": "https://discord.com/",
                                        "referring_domain": "discord.com"
                                    },
                                    "presence": {
                                        "status": "online",
                                        "since": 0,
                                        "activities": [],
                                        "afk": false
                                    },
                                    "compress": false
                                }
                            });

                            if let Some(w) = gw_writer_ref.lock().await.as_mut() {
                                let _ = w.send(Message::Text(identify.to_string())).await;
                            }

                            // Heartbeat loop
                            let writer_hb = Arc::clone(&gw_writer_ref);
                            tokio::spawn(async move {
                                let mut hb_timer = interval(Duration::from_millis(heartbeat_interval));
                                loop {
                                    hb_timer.tick().await;
                                    let hb_payload = serde_json::json!({ "op": 1, "d": null }).to_string();
                                    let mut lock = writer_hb.lock().await;
                                    if let Some(w) = lock.as_mut() {
                                        if w.send(Message::Text(hb_payload)).await.is_err() {
                                            break;
                                        }
                                    } else {
                                        break;
                                    }
                                }
                            });

                            while let Some(Ok(msg_text)) = read.next().await {
                                if let Message::Text(txt) = msg_text {
                                    if let Ok(pay) = serde_json::from_str::<GatewayPayload>(&txt) {
                                        if pay.op == 0 {
                                            let event_type = pay.t.unwrap_or_default();
                                            let data = pay.d.clone();

                                            if event_type == "READY" {
                                                if let Some(uid) = data["user"]["id"].as_str() {
                                                    *self_id_ref.write().await = uid.to_string();
                                                }
                                            }

                                            // Gate Trigger: Listen for opponent messages when Queue is NOT EMPTY
                                            if event_type == "MESSAGE_CREATE" {
                                                let cid = data["channel_id"].as_str().unwrap_or("");
                                                let author_id = data["author"]["id"].as_str().unwrap_or("");
                                                let my_id = self_id_ref.read().await.clone();

                                                let is_mode_on = *queue_mode_ref.read().await;

                                                if is_mode_on && !cid.is_empty() && author_id != my_id {
                                                    let next_num = {
                                                        let mut q_map = queue_ref.write().await;
                                                        if let Some(q) = q_map.get_mut(cid) {
                                                            if !q.is_empty() {
                                                                Some(q.remove(0))
                                                            } else {
                                                                None
                                                            }
                                                        } else {
                                                            None
                                                        }
                                                    };

                                                    if let Some(num) = next_num {
                                                        let remaining_q = queue_ref.read().await.get(cid).cloned().unwrap_or_default();
                                                        let _ = gw_broadcast_tx.send(ProxyResponse::QueueSync { queue: remaining_q });

                                                        // Stealth Delay & Hardware Mimicry Exec
                                                        let base_hw_delay = *delay_ref.read().await;
                                                        let total_human_delay = generate_human_reaction_jitter(base_hw_delay);
                                                        let token_sub = token_clone.clone();
                                                        let client_sub = Arc::clone(&client_ref);
                                                        let cid_sub = cid.to_string();

                                                        tokio::spawn(async move {
                                                            // 1. Wait realistic cognitive processing time before typing
                                                            let typing_lead_delay = total_human_delay / 3;
                                                            sleep(Duration::from_millis(typing_lead_delay)).await;

                                                            let typing_url = format!("https://discord.com/api/v10/channels/{}/typing", cid_sub);
                                                            let _ = client_sub.post(&typing_url)
                                                                .header("Authorization", &token_sub)
                                                                .header("Content-Length", "0")
                                                                .send()
                                                                .await;

                                                            // 2. Wait remaining typing delay
                                                            let remaining_delay = total_human_delay.saturating_sub(typing_lead_delay);
                                                            sleep(Duration::from_millis(remaining_delay)).await;

                                                            // 3. Send HTTP POST message with Snowflake Nonce
                                                            let msg_url = format!("https://discord.com/api/v10/channels/{}/messages", cid_sub);
                                                            let nonce = generate_snowflake_nonce();
                                                            let payload = serde_json::json!({
                                                                "content": num.to_string(),
                                                                "nonce": nonce
                                                            });

                                                            let _ = client_sub.post(&msg_url)
                                                                .header("Authorization", &token_sub)
                                                                .header("Content-Type", "application/json")
                                                                .json(&payload)
                                                                .send()
                                                                .await;
                                                        });
                                                    }
                                                }
                                            }

                                            let response = ProxyResponse::GatewayEvent {
                                                event_type,
                                                data,
                                            };
                                            let _ = gw_broadcast_tx.send(response);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    });

    while let Ok((stream, _)) = listener.accept().await {
        let _ = stream.set_nodelay(true);
        let token = discord_token.clone();
        let client = Arc::clone(&http_client);
        let mut client_gw_rx = gw_tx.subscribe();
        let queue_map_ref = Arc::clone(&active_queue);
        let mode_flag_ref = Arc::clone(&queue_mode_enabled);
        let hw_delay_ref = Arc::clone(&hardware_delay_ms);

        tokio::spawn(async move {
            if let Ok(ws_stream) = accept_async(stream).await {
                let (write, mut read) = ws_stream.split();
                let write_arc = Arc::new(tokio::sync::Mutex::new(write));

                let subscribed_cid = Arc::new(tokio::sync::RwLock::new(String::new()));
                let client_cid_ref = Arc::clone(&subscribed_cid);

                // Forward Gateway events from Discord to client with channel filtering
                let gw_writer = Arc::clone(&write_arc);
                tokio::spawn(async move {
                    while let Ok(event) = client_gw_rx.recv().await {
                        let should_send = match &event {
                            ProxyResponse::GatewayEvent { event_type, data } => {
                                if event_type == "READY" {
                                    true
                                } else if event_type == "MESSAGE_CREATE" {
                                    let active_cid = client_cid_ref.read().await;
                                    data["channel_id"].as_str() == Some(active_cid.as_str())
                                } else {
                                    false
                                }
                            }
                            _ => true,
                        };

                        if should_send {
                            if let Ok(json_str) = serde_json::to_string(&event) {
                                let mut w = gw_writer.lock().await;
                                if w.send(Message::Text(json_str)).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                });

                while let Some(Ok(msg)) = read.next().await {
                    match msg {
                        Message::Ping(data) => {
                            let mut w = write_arc.lock().await;
                            let _ = w.send(Message::Pong(data)).await;
                        }
                        Message::Text(text) => {
                            if let Ok(action) = serde_json::from_str::<ProxyAction>(&text) {
                                match action {
                                    ProxyAction::SetQueueMode { enabled } => {
                                        *mode_flag_ref.write().await = enabled;
                                    }
                                    ProxyAction::UpdateHardwareDelay { delay_ms } => {
                                        *hw_delay_ref.write().await = delay_ms;
                                    }
                                    ProxyAction::ClearQueue { channel_id } => {
                                        let mut map = queue_map_ref.write().await;
                                        if let Some(q) = map.get_mut(&channel_id) {
                                            q.clear();
                                        }
                                        let sync_resp = ProxyResponse::QueueSync { queue: Vec::new() };
                                        let resp_json = serde_json::to_string(&sync_resp).unwrap();
                                        let mut w = write_arc.lock().await;
                                        let _ = w.send(Message::Text(resp_json)).await;
                                    }
                                    ProxyAction::EnqueueNumber { channel_id, number } => {
                                        let mut map = queue_map_ref.write().await;
                                        let q = map.entry(channel_id.clone()).or_insert_with(Vec::new);

                                        if q.is_empty() {
                                            // First item popped and transmitted immediately
                                            let token_sub = token.clone();
                                            let client_sub = Arc::clone(&client);
                                            let base_hw_delay = *hw_delay_ref.read().await;

                                            tokio::spawn(async move {
                                                let total_delay = generate_human_reaction_jitter(base_hw_delay);
                                                let lead_typing_delay = total_delay / 3;
                                                sleep(Duration::from_millis(lead_typing_delay)).await;

                                                let typing_url = format!("https://discord.com/api/v10/channels/{}/typing", channel_id);
                                                let _ = client_sub.post(&typing_url)
                                                    .header("Authorization", &token_sub)
                                                    .header("Content-Length", "0")
                                                    .send()
                                                    .await;

                                                let remaining_delay = total_delay.saturating_sub(lead_typing_delay);
                                                sleep(Duration::from_millis(remaining_delay)).await;

                                                let msg_url = format!("https://discord.com/api/v10/channels/{}/messages", channel_id);
                                                let nonce = generate_snowflake_nonce();
                                                let payload = serde_json::json!({
                                                    "content": number.to_string(),
                                                    "nonce": nonce
                                                });

                                                let _ = client_sub.post(&msg_url)
                                                    .header("Authorization", &token_sub)
                                                    .header("Content-Type", "application/json")
                                                    .json(&payload)
                                                    .send()
                                                    .await;
                                            });
                                        } else {
                                            q.push(number);
                                        }

                                        let sync_resp = ProxyResponse::QueueSync { queue: q.clone() };
                                        let resp_json = serde_json::to_string(&sync_resp).unwrap();
                                        let mut w = write_arc.lock().await;
                                        let _ = w.send(Message::Text(resp_json)).await;
                                    }
                                    ProxyAction::SubscribeChannel { channel_id } => {
                                        *subscribed_cid.write().await = channel_id;
                                    }
                                    ProxyAction::Ping => {
                                        let resp = serde_json::to_string(&ProxyResponse::Pong).unwrap();
                                        let mut w = write_arc.lock().await;
                                        let _ = w.send(Message::Text(resp)).await;
                                    }
                                    ProxyAction::SendMessage { channel_id, content, nonce } => {
                                        let ack_json = serde_json::to_string(&ProxyResponse::Ack { nonce: nonce.clone() }).unwrap();
                                        let w_arc_ack = Arc::clone(&write_arc);
                                        tokio::spawn(async move {
                                            let mut w = w_arc_ack.lock().await;
                                            let _ = w.send(Message::Text(ack_json)).await;
                                        });

                                        let url = format!("https://discord.com/api/v10/channels/{}/messages", channel_id);
                                        let payload = serde_json::json!({ "content": content, "nonce": nonce });
                                        let client_ref = Arc::clone(&client);
                                        let token_ref = token.clone();
                                        let w_arc_res = Arc::clone(&write_arc);

                                        tokio::spawn(async move {
                                            let res = client_ref.post(&url)
                                                .header("Authorization", &token_ref)
                                                .header("Content-Type", "application/json")
                                                .json(&payload)
                                                .send()
                                                .await;

                                            let response = match res {
                                                Ok(resp) if resp.status().is_success() => ProxyResponse::MessageResult {
                                                    nonce,
                                                    success: true,
                                                    error: None,
                                                },
                                                Ok(resp) => {
                                                    let err = resp.text().await.unwrap_or_else(|_| "Rejected".to_string());
                                                    ProxyResponse::MessageResult {
                                                        nonce,
                                                        success: false,
                                                        error: Some(err),
                                                    }
                                                }
                                                Err(e) => ProxyResponse::MessageResult {
                                                    nonce,
                                                    success: false,
                                                    error: Some(e.to_string()),
                                                },
                                            };

                                            if let Ok(json_resp) = serde_json::to_string(&response) {
                                                let mut w = w_arc_res.lock().await;
                                                let _ = w.send(Message::Text(json_resp)).await;
                                            }
                                        });
                                    }
                                    ProxyAction::SendTyping { channel_id } => {
                                        let url = format!("https://discord.com/api/v10/channels/{}/typing", channel_id);
                                        let client_ref = Arc::clone(&client);
                                        let token_ref = token.clone();
                                        tokio::spawn(async move {
                                            let _ = client_ref.post(&url)
                                                .header("Authorization", &token_ref)
                                                .header("Content-Length", "0")
                                                .send()
                                                .await;
                                        });
                                    }
                                    ProxyAction::FetchHistory { channel_id, limit } => {
                                        let url = format!("https://discord.com/api/v10/channels/{}/messages?limit={}", channel_id, limit);
                                        let client_ref = Arc::clone(&client);
                                        let token_ref = token.clone();
                                        let w_arc_hist = Arc::clone(&write_arc);

                                        tokio::spawn(async move {
                                            let res = client_ref.get(&url)
                                                .header("Authorization", &token_ref)
                                                .send()
                                                .await;

                                            if let Ok(resp) = res {
                                                if let Ok(json_data) = resp.json::<serde_json::Value>().await {
                                                    let resp_struct = ProxyResponse::ChannelHistory {
                                                        channel_id,
                                                        messages: json_data,
                                                    };
                                                    if let Ok(json_resp) = serde_json::to_string(&resp_struct) {
                                                        let mut w = w_arc_hist.lock().await;
                                                        let _ = w.send(Message::Text(json_resp)).await;
                                                    }
                                                }
                                            }
                                        });
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
        });
    }

    Ok(())
}
