use crate::models::{ProxyAction, ProxyResponse};
use crate::proxy::actions::{handle_action, send_resp};
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
                        handle_action(
                            action,
                            &state,
                            &write_arc,
                            &discord_token,
                            &http_client,
                            &subscribed_cid,
                            &gw_broadcast_tx,
                        )
                        .await;
                    }
                }
                _ => {}
            }
        }

        // Local client app disconnected or closed -> clear all stealth queues immediately
        state.clear_all_queues().await;
        let active_cid = subscribed_cid.read().await.clone();
        if !active_cid.is_empty() {
            let _ = gw_broadcast_tx.send(ProxyResponse::QueueSync { queue: Vec::new() });
        }
    }
}
