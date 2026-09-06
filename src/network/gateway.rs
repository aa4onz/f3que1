// src/network/gateway.rs
use crate::app::state::AppState;
use crate::models::{AppEvent, DiscordMessage, GatewayPayload, MessageStatus};
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc::Sender, watch, Mutex};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

pub async fn run_gateway_loop(
    app_state: Arc<Mutex<AppState>>, 
    event_tx: Sender<AppEvent>,
    channel_id_rx: watch::Receiver<String>,
) {
    let url = "wss://gateway.discord.gg/?v=10&encoding=json";
    loop {
        let token = {
            let state = app_state.lock().await;
            state.token.clone()
        };
        if token.is_empty() { break; }

        if let Ok((ws, _resp)) = connect_async(url).await {
            let (mut write, mut read) = ws.split();
            
            if let Some(Ok(Message::Text(t))) = read.next().await {
                if let Ok(p) = serde_json::from_str::<GatewayPayload>(&t) {
                    if p.op == 10 {
                        let interval = p.d["heartbeat_interval"].as_u64().unwrap_or(41250);
                        let id = serde_json::json!({"op": 2, "d": {"token": token.clone(), "properties": {"$os": "windows", "$browser": "chrome", "$device": "pc"}}});
                        let _ = write.send(Message::Text(id.to_string())).await;
                        
                        let state_clone = Arc::clone(&app_state);
                        let shared_w = Arc::new(Mutex::new(write));
                        let h_write = Arc::clone(&shared_w);

                        let last_hb_send = Arc::new(Mutex::new(Option::<Instant>::None));
                        let last_hb_send_clone = Arc::clone(&last_hb_send);

                        tokio::spawn(async move {
                            loop {
                                tokio::time::sleep(std::time::Duration::from_millis(interval)).await;
                                if state_clone.lock().await.token.is_empty() { break; }
                                *last_hb_send_clone.lock().await = Some(Instant::now());
                                if h_write.lock().await.send(Message::Text(serde_json::json!({"op": 1, "d": null}).to_string())).await.is_err() { break; }
                            }
                        });

                        let w_tx = event_tx.clone();

                        while let Some(Ok(Message::Text(msg_text))) = read.next().await {
                            if let Ok(pay) = serde_json::from_str::<GatewayPayload>(&msg_text) {
                                if pay.op == 11 {
                                    let now_inst = Instant::now();
                                    let rtt = if let Some(sent_at) = *last_hb_send.lock().await {
                                        now_inst.duration_since(sent_at).as_millis() as u64
                                    } else {
                                        0
                                    };

                                    let estimated_one_way = (rtt / 2) as i64;
                                    let clock_offset = estimated_one_way;

                                    let _ = w_tx.send(AppEvent::UpdateGatewayRtt {
                                        rtt_ms: rtt,
                                        offset_ms: clock_offset,
                                    }).await;
                                } else if pay.op == 0 {
                                    let ev = pay.t.as_deref().unwrap_or("");

                                    if ev == "READY" {
                                        let name = pay.d["user"]["global_name"]
                                            .as_str()
                                            .filter(|s| !s.is_empty())
                                            .unwrap_or_else(|| pay.d["user"]["username"].as_str().unwrap_or(""));
                                        if !name.is_empty() {
                                            let _ = w_tx.send(AppEvent::SetSelfUsername(name.to_string())).await;
                                        }
                                    } else if ev == "MESSAGE_CREATE" {
                                        let current_target_cid = channel_id_rx.borrow().clone();
                                        if pay.d["channel_id"].as_str() == Some(&current_target_cid) {
                                            let msg_id_str = pay.d["id"].as_str().unwrap_or("0");
                                            let current_time_str = chrono::Local::now().format("%H:%M:%S%.3f").to_string();

                                            let transit_time_str = if let Ok(msg_id) = msg_id_str.parse::<u64>() {
                                                let discord_epoch_ms = (msg_id >> 22) + 1420070400000;
                                                let local_now_ms = chrono::Utc::now().timestamp_millis();
                                                let diff = if local_now_ms >= discord_epoch_ms as i64 {
                                                    (local_now_ms - discord_epoch_ms as i64) as u64
                                                } else {
                                                    1
                                                };

                                                format!("{} | {}ms", current_time_str, diff)
                                            } else {
                                                current_time_str
                                            };

                                            let nonce = pay.d["nonce"]
                                                .as_str()
                                                .filter(|s| !s.is_empty())
                                                .unwrap_or_else(|| pay.d["id"].as_str().unwrap_or(""))
                                                .to_string();

                                            let member_nick = pay.d["member"]["nick"].as_str().filter(|s| !s.is_empty());
                                            let global_name = pay.d["author"]["global_name"].as_str().filter(|s| !s.is_empty());
                                            let username = pay.d["author"]["username"].as_str().unwrap_or("Unknown");

                                            let author = member_nick
                                                .or(global_name)
                                                .unwrap_or(username)
                                                .to_string();

                                            let _ = w_tx.send(AppEvent::IncomingMessage(DiscordMessage {
                                                nonce,
                                                author,
                                                content: pay.d["content"].as_str().unwrap_or("").to_string(),
                                                timestamp: transit_time_str,
                                                status: MessageStatus::Delivered,
                                            })).await;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        let _ = event_tx.send(AppEvent::GatewayClosed).await;
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    }
}
