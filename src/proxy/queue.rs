use crate::models::{QueuedItem, ProxyResponse};
use crate::proxy::state::ProxyState;
use crate::proxy::utils::generate_snowflake_nonce;
use rand::Rng;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::time::Duration;

pub async fn execute_queued_reaction(
    item: QueuedItem,
    channel_id: String,
    discord_token: String,
    http_client: Arc<reqwest::Client>,
    gw_broadcast_tx: broadcast::Sender<ProxyResponse>,
    remaining_queue: Vec<QueuedItem>,
    state: ProxyState,
) {
    // Notify connected client of the updated queue state immediately
    let _ = gw_broadcast_tx.send(ProxyResponse::QueueSync {
        queue: remaining_queue,
    });

    // Humanized reaction delay (200ms - 300ms) before sending payload
    tokio::spawn(async move {
        let delay_ms = rand::thread_rng().gen_range(200..=300);
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;

        let msg_url = format!("https://discord.com/api/v10/channels/{}/messages", channel_id);
        let nonce = generate_snowflake_nonce();
        let payload = serde_json::json!({
            "content": item.content,
            "nonce": nonce
        });

        let res = http_client
            .post(&msg_url)
            .header("Authorization", &discord_token)
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await;

        // If message fails (rate limited 429, rejected, network error, etc.), clear queue immediately
        let is_success = match res {
            Ok(resp) => resp.status().is_success(),
            Err(_) => false,
        };

        if !is_success {
            let cleared = state.clear_queue(&channel_id).await;
            let _ = gw_broadcast_tx.send(ProxyResponse::QueueSync { queue: cleared });
        }
    });
}
