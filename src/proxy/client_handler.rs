use crate::models::{ProxyAction, ProxyResponse};
use crate::proxy::state::ProxyState;
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::{broadcast, Mutex, RwLock};
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::protocol::Message;

pub async fn handle_client_connection(
    stream: TcpStream,
    discord_token: String,
    http_client: Arc<reqwest::Client>,
    state: ProxyState,
    gw_broadcast_tx: broadcast::Sender<ProxyResponse>,
) {
    let mut client_gw_rx = gw_broadcast_tx.subscribe();

    if let Ok(ws_stream) = accept_async(stream).await {
        let (write, mut read) = ws_stream.split();
        let write_arc = Arc::new(Mutex::new(write));

        let subscribed_cid = Arc::new(RwLock::new(String::new()));
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
                                *state.queue_mode_enabled.write().await = enabled;
                                if !enabled {
                                    let mut map = state.active_queue.write().await;
                                    for q in map.values_mut() {
                                        q.clear();
                                    }
                                    let sync_resp = ProxyResponse::QueueSync { queue: Vec::new() };
                                    let resp_json = serde_json::to_string(&sync_resp).unwrap();
                                    let mut w = write_arc.lock().await;
                                    let _ = w.send(Message::Text(resp_json)).await;
                                }
                            }
                            ProxyAction::UpdateHardwareDelay { delay_ms } => {
                                *state.hardware_delay_ms.write().await = delay_ms;
                            }
                            ProxyAction::ClearQueue { channel_id } => {
                                let mut map = state.active_queue.write().await;
                                if let Some(q) = map.get_mut(&channel_id) {
                                    q.clear();
                                }
                                let sync_resp = ProxyResponse::QueueSync { queue: Vec::new() };
                                let resp_json = serde_json::to_string(&sync_resp).unwrap();
                                let mut w = write_arc.lock().await;
                                let _ = w.send(Message::Text(resp_json)).await;
                            }
                            ProxyAction::EnqueueNumber { channel_id, item } => {
                                let mut map = state.active_queue.write().await;
                                let q = map.entry(channel_id.clone()).or_insert_with(Vec::new);
                                q.push(item);

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
                                let client_ref = Arc::clone(&http_client);
                                let token_ref = discord_token.clone();
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
                                let client_ref = Arc::clone(&http_client);
                                let token_ref = discord_token.clone();
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
                                let client_ref = Arc::clone(&http_client);
                                let token_ref = discord_token.clone();
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
}
