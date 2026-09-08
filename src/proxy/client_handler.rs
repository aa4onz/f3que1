use crate::models::{ProxyAction, ProxyResponse};
use crate::proxy::state::ProxyState;
use futures_util::{stream::SplitSink, SinkExt, StreamExt};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::{broadcast, Mutex, RwLock};
use tokio_tungstenite::{accept_async, WebSocketStream};
use tokio_tungstenite::tungstenite::protocol::Message;

type WsWriter = SplitSink<WebSocketStream<TcpStream>, Message>;

async fn send_resp(writer: &Arc<Mutex<WsWriter>>, resp: &ProxyResponse) -> bool {
    if let Ok(json_str) = serde_json::to_string(resp) {
        let mut w = writer.lock().await;
        w.send(Message::Text(json_str)).await.is_ok()
    } else {
        false
    }
}

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
                    if !send_resp(&gw_writer, &event).await {
                        break;
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
                                state.set_queue_mode(enabled).await;
                                if !enabled {
                                    send_resp(&write_arc, &ProxyResponse::QueueSync { queue: Vec::new() }).await;
                                }
                            }
                            ProxyAction::UpdateHardwareDelay { delay_ms } => {
                                state.set_hardware_delay(delay_ms).await;
                            }
                            ProxyAction::ClearQueue { channel_id } => {
                                let cleared = state.clear_queue(&channel_id).await;
                                send_resp(&write_arc, &ProxyResponse::QueueSync { queue: cleared }).await;
                            }
                            ProxyAction::EnqueueNumber { channel_id, item } => {
                                let updated_q = state.enqueue_item(&channel_id, item).await;
                                send_resp(&write_arc, &ProxyResponse::QueueSync { queue: updated_q }).await;
                            }
                            ProxyAction::SubscribeChannel { channel_id } => {
                                *subscribed_cid.write().await = channel_id;
                            }
                            ProxyAction::Ping => {
                                send_resp(&write_arc, &ProxyResponse::Pong).await;
                            }
                            ProxyAction::SendMessage { channel_id, content, nonce } => {
                                let w_arc_ack = Arc::clone(&write_arc);
                                let ack_nonce = nonce.clone();
                                tokio::spawn(async move {
                                    send_resp(&w_arc_ack, &ProxyResponse::Ack { nonce: ack_nonce }).await;
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

                                    send_resp(&w_arc_res, &response).await;
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
                                            send_resp(&w_arc_hist, &resp_struct).await;
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
