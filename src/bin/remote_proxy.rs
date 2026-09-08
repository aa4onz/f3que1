use fast_discord_tui::models::ProxyResponse;
use fast_discord_tui::proxy::client_handler::handle_client_connection;
use fast_discord_tui::proxy::discord_gw::{run_discord_gateway, SharedGwWriter};
use fast_discord_tui::proxy::state::ProxyState;
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, ACCEPT_LANGUAGE, USER_AGENT};
use std::env;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, Mutex};
use tokio::time::Duration;

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

    let proxy_state = ProxyState::new();

    // Shared Gateway Write Stream for Typing events
    let gw_write_arc: SharedGwWriter = Arc::new(Mutex::new(None));

    // HTTP Client initialization
    let mut default_headers = HeaderMap::new();
    default_headers.insert(ACCEPT, HeaderValue::from_static("*/*"));
    default_headers.insert(ACCEPT_LANGUAGE, HeaderValue::from_static("en-US,en;q=0.9"));
    default_headers.insert(
        USER_AGENT,
        HeaderValue::from_static(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
        ),
    );

    let http_client = Arc::new(
        reqwest::Client::builder()
            .tcp_nodelay(true)
            .tcp_keepalive(Duration::from_secs(15))
            .default_headers(default_headers)
            .build()?,
    );

    // Spawn Gateway WebSocket Loop
    let gw_broadcast_tx = gw_tx.clone();
    let token_clone = discord_token.clone();
    let client_ref = Arc::clone(&http_client);
    let gw_writer_ref = Arc::clone(&gw_write_arc);
    let state_clone = proxy_state.clone();

    tokio::spawn(async move {
        run_discord_gateway(
            token_clone,
            state_clone,
            gw_broadcast_tx,
            client_ref,
            gw_writer_ref,
        )
        .await;
    });

    while let Ok((stream, _)) = listener.accept().await {
        let _ = stream.set_nodelay(true);
        let token = discord_token.clone();
        let client = Arc::clone(&http_client);
        let gw_tx_clone = gw_tx.clone();
        let state_conn = proxy_state.clone();

        tokio::spawn(async move {
            handle_client_connection(stream, token, client, state_conn, gw_tx_clone).await;
        });
    }

    Ok(())
}
