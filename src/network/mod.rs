// src/network/mod.rs
pub mod gateway;
pub mod http;

use crate::app::state::AppState;
use crate::models::{AppEvent, DiscordApiMessage, DiscordMessage, MessageStatus, ProxyAction, ProxyResponse};
use crate::network::http::DiscordHttpClient;
use chrono::Local;
use crossterm::event::EventStream;
use futures_util::{SinkExt, StreamExt};
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, ACCEPT_LANGUAGE, USER_AGENT};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, mpsc::Sender, watch, Mutex};
use tokio::time::interval;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

pub fn spawn_network_handlers(
    app_state: Arc<Mutex<AppState>>, 
    event_tx: Sender<AppEvent>, 
    net_rx: mpsc::Receiver<AppEvent>,
    channel_id_rx: watch::Receiver<String>,
) {
    // 1. Terminal Key & Mouse input loop (0ms latency local handling)
    let input_tx = event_tx.clone();
    tokio::spawn(async move {
        let mut reader = EventStream::new();
        while let Some(Ok(ev)) = reader.next().await {
            if input_tx.send(AppEvent::Terminal(ev)).await.is_err() {
                break;
            }
        }
    });

    let target_url = {
        let state = app_state.try_lock().expect("Failed to lock state at initialization");
        state.token.clone()
    };

    let is_proxy_mode = target_url.starts_with("ws://") || target_url.starts_with("wss://");

    if is_proxy_mode {
        // Mode 2: Remote Proxy Mode
        let worker_tx = event_tx.clone();
        let mut outbound_rx = net_rx;
        let proxy_cid_rx = channel_id_rx;

        tokio::spawn(async move {
            loop {
                if let Ok((ws_stream, _)) = connect_async(&target_url).await {
                    let (mut write, mut read) = ws_stream.split();
                    let write_arc = Arc::new(Mutex::new(write));

                    // Send initial SubscribeChannel to remote proxy
                    let initial_cid = proxy_cid_rx.borrow().clone();
                    if !initial_cid.is_empty() {
                        let sub_payload = serde_json::to_string(&ProxyAction::SubscribeChannel {
                            channel_id: initial_cid,
                        }).unwrap();
                        let mut w = write_arc.lock().await;
                        let _ = w.send(Message::Text(sub_payload)).await;
                    }

                    // Keep remote proxy updated when channel changes
                    let sub_writer = Arc::clone(&write_arc);
                    let mut channel_watch = proxy_cid_rx.clone();
                    tokio::spawn(async move {
                        while channel_watch.changed().await.is_ok() {
                            let new_cid = channel_watch.borrow().clone();
                            if !new_cid.is_empty() {
                                let payload = serde_json::to_string(&ProxyAction::SubscribeChannel {
                                    channel_id: new_cid,
                                }).unwrap();
                                let mut w = sub_writer.lock().await;
                                if w.send(Message::Text(payload)).await.is_err() {
                                    break;
                                }
                            }
                        }
                    });

                    let ping_writer = Arc::clone(&write_arc);
                    let mut keepalive = interval(Duration::from_secs(15));
                    tokio::spawn(async move {
                        loop {
                            keepalive.tick().await;
                            let ping_payload = serde_json::to_string(&ProxyAction::Ping).unwrap();
                            let mut w = ping_writer.lock().await;
                            if w.send(Message::Text(ping_payload)).await.is_err() {
                                break;
                            }
                        }
                    });

                    let incoming_tx = worker_tx.clone();
                    let recv_cid_rx = proxy_cid_rx.clone();

                    let read_handle = tokio::spawn(async move {
                        while let Some(Ok(msg)) = read.next().await {
                            match msg {
                                Message::Text(txt) => {
                                    if let Ok(resp) = serde_json::from_str::<ProxyResponse>(&txt) {
                                        match resp {
                                            ProxyResponse::QueueSync { queue } => {
                                                let _ = incoming_tx.send(AppEvent::UpdateQueueState(queue)).await;
                                            }
                                            ProxyResponse::Ack { nonce } => {
                                                let _ = incoming_tx.send(AppEvent::MessageSent {
                                                    nonce,
                                                    timestamp: String::new(),
                                                }).await;
                                            }
                                            ProxyResponse::MessageResult { nonce, success, .. } => {
                                                if success {
                                                    let _ = incoming_tx.send(AppEvent::MessageSent {
                                                        nonce,
                                                        timestamp: String::new(),
                                                    }).await;
                                                } else {
                                                    let _ = incoming_tx.send(AppEvent::MessageFailed { nonce }).await;
                                                }
                                            }
                                            ProxyResponse::ChannelHistory { messages, .. } => {
                                                if let Ok(raw_msgs) = serde_json::from_value::<Vec<DiscordApiMessage>>(messages) {
                                                    let mut parsed = Vec::with_capacity(raw_msgs.len());
                                                    for m in raw_msgs.into_iter().rev() {
                                                        let author = m.author.global_name.unwrap_or(m.author.username);
                                                        let time_formatted = match chrono::DateTime::parse_from_rfc3339(&m.timestamp) {
                                                            Ok(dt) => dt.with_timezone(&Local).format("%H:%M:%S%.3f").to_string(),
                                                            Err(_) => Local::now().format("%H:%M:%S%.3f").to_string(),
                                                        };
                                                        let nonce_str = match m.nonce {
                                                            Some(serde_json::Value::String(s)) => s,
                                                            Some(serde_json::Value::Number(n)) => n.to_string(),
                                                            _ => m.id.clone(),
                                                        };
                                                        parsed.push(DiscordMessage {
                                                            nonce: nonce_str,
                                                            author,
                                                            content: m.content,
                                                            timestamp: time_formatted,
                                                            status: MessageStatus::Delivered,
                                                        });
                                                    }
                                                    let _ = incoming_tx.send(AppEvent::LoadChannelHistory(parsed)).await;
                                                }
                                            }
                                            ProxyResponse::GatewayEvent { event_type, data } => {
                                                if event_type == "READY" {
                                                    let name = data["user"]["global_name"]
                                                        .as_str()
                                                        .filter(|s| !s.is_empty())
                                                        .unwrap_or_else(|| data["user"]["username"].as_str().unwrap_or(""));
                                                    if !name.is_empty() {
                                                        let _ = incoming_tx.send(AppEvent::SetSelfUsername(name.to_string())).await;
                                                    }
                                                } else if event_type == "MESSAGE_CREATE" {
                                                    let current_target_cid = recv_cid_rx.borrow().clone();
                                                    if data["channel_id"].as_str() == Some(&current_target_cid) {
                                                        let current_time_str = Local::now().format("%H:%M:%S%.3f").to_string();
                                                        let nonce = data["nonce"]
                                                            .as_str()
                                                            .filter(|s| !s.is_empty())
                                                            .unwrap_or_else(|| data["id"].as_str().unwrap_or(""))
                                                            .to_string();

                                                        let member_nick = data["member"]["nick"].as_str().filter(|s| !s.is_empty());
                                                        let global_name = data["author"]["global_name"].as_str().filter(|s| !s.is_empty());
                                                        let username = data["author"]["username"].as_str().unwrap_or("Unknown");

                                                        let author = member_nick
                                                            .or(global_name)
                                                            .unwrap_or(username)
                                                            .to_string();

                                                        let _ = incoming_tx.send(AppEvent::IncomingMessage(DiscordMessage {
                                                            nonce,
                                                            author,
                                                            content: data["content"].as_str().unwrap_or("").to_string(),
                                                            timestamp: current_time_str,
                                                            status: MessageStatus::Delivered,
                                                        })).await;
                                                    }
                                                }
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }
                    });

                    while let Some(job) = outbound_rx.recv().await {
                        let active_cid = proxy_cid_rx.borrow().clone();
                        let action = match job {
                            AppEvent::ToggleQueueMode => {
                                let state_mode = app_state.lock().await.queue_mode;
                                Some(ProxyAction::SetQueueMode { enabled: state_mode })
                            }
                            AppEvent::ClearQueue => Some(ProxyAction::ClearQueue { channel_id: active_cid }),
                            AppEvent::EnqueueNumberItem(num) => Some(ProxyAction::EnqueueNumber { channel_id: active_cid, number: num }),
                            AppEvent::UpdateHardwareDelay(ms) => Some(ProxyAction::UpdateHardwareDelay { delay_ms: ms }),
                            AppEvent::FetchChannelHistory(cid) => Some(ProxyAction::FetchHistory {
                                channel_id: cid,
                                limit: 50,
                            }),
                            AppEvent::HttpTriggerTyping => Some(ProxyAction::SendTyping {
                                channel_id: active_cid,
                            }),
                            AppEvent::HttpSendChat { nonce, text } => Some(ProxyAction::SendMessage {
                                channel_id: active_cid,
                                content: text,
                                nonce,
                            }),
                            _ => None,
                        };

                        if let Some(act) = action {
                            if let Ok(payload_str) = serde_json::to_string(&act) {
                                let w_arc = Arc::clone(&write_arc);
                                tokio::spawn(async move {
                                    let mut w = w_arc.lock().await;
                                    let _ = w.send(Message::Text(payload_str)).await;
                                });
                            }
                        }
                    }

                    read_handle.abort();
                }

                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        });
    } else {
        // Mode 1: Direct Discord Mode
        let gw_state = Arc::clone(&app_state);
        let gw_tx = event_tx.clone();
        let gw_cid_rx = channel_id_rx;
        tokio::spawn(async move {
            gateway::run_gateway_loop(gw_state, gw_tx, gw_cid_rx).await;
        });

        let mut default_headers = HeaderMap::new();
        default_headers.insert(ACCEPT, HeaderValue::from_static("*/*"));
        default_headers.insert(ACCEPT_LANGUAGE, HeaderValue::from_static("en-US,en;q=0.9"));
        default_headers.insert(USER_AGENT, HeaderValue::from_static("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"));

        let reqwest_client = reqwest::Client::builder()
            .tcp_nodelay(true)
            .tcp_keepalive(Duration::from_secs(15))
            .default_headers(default_headers)
            .build()
            .expect("Failed to build HTTP client");

        let http_client = Arc::new(DiscordHttpClient::new(reqwest_client, target_url));
        let worker_tx = event_tx;
        let mut outbound_rx = net_rx;

        tokio::spawn(async move {
            while let Some(job) = outbound_rx.recv().await {
                let client = Arc::clone(&http_client);
                let tx = worker_tx.clone();

                match job {
                    AppEvent::FetchChannelHistory(cid) => {
                        tokio::spawn(async move {
                            if let Ok(msgs) = client.fetch_messages(&cid, 50).await {
                                let _ = tx.send(AppEvent::LoadChannelHistory(msgs)).await;
                            }
                        });
                    }
                    AppEvent::HttpTriggerTyping => {
                        let cid = client.token.clone();
                        tokio::spawn(async move {
                            let _ = client.send_typing(&cid).await;
                        });
                    }
                    AppEvent::HttpSendChat { nonce, text } => {
                        let cid = client.token.clone();
                        tokio::spawn(async move {
                            match client.send_message(&cid, &text, &nonce).await {
                                Ok(res) if res.status().is_success() => {
                                    let _ = tx.send(AppEvent::MessageSent {
                                        nonce,
                                        timestamp: String::new(),
                                    }).await;
                                }
                                _ => {
                                    let _ = tx.send(AppEvent::MessageFailed { nonce }).await;
                                }
                            }
                        });
                    }
                    _ => {}
                }
            }
        });
    }
}
