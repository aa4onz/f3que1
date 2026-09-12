use fast_discord_tui::models::ProxyResponse;
use fast_discord_tui::proxy::client_handler::handle_client_connection;
use fast_discord_tui::proxy::discord_gw::{run_discord_gateway, SharedGwWriter};
use fast_discord_tui::proxy::state::ProxyState;
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, ACCEPT_LANGUAGE, USER_AGENT};
use std::env;
use std::io::{self, Write};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, Mutex};
use tokio::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Determine profile from command line args (e.g. `1`, `--profile 1`, `--profile1`, `-1`) or PROFILE env var
    let args: Vec<String> = env::args().collect();
    let profile_num = if args.len() > 1 {
        let mut raw_arg = args[1].as_str();
        if raw_arg == "--profile" && args.len() > 2 {
            raw_arg = args[2].as_str();
        }
        let clean = raw_arg
            .trim_start_matches('-')
            .trim_start_matches("profile")
            .trim();
        if clean.is_empty() {
            "1".to_string()
        } else {
            clean.to_string()
        }
    } else {
        env::var("PROFILE").unwrap_or_else(|_| "1".to_string())
    };

    let profile_env = format!(".env.profile_{}", profile_num);
    if std::path::Path::new(&profile_env).exists() {
        dotenvy::from_filename(&profile_env).ok();
        println!("Loaded configuration from {}", profile_env);
    } else {
        dotenvy::dotenv().ok();
        println!("Loaded configuration from .env");
    }

    let mut discord_token = env::var("DISCORD_TOKEN").unwrap_or_default();

    if discord_token.trim().is_empty() {
        println!("\n⚠️ DISCORD_TOKEN not found in environment or dotenv file!");
        print!("Enter Discord Token for Remote Proxy Profile {}: ", profile_num);
        io::stdout().flush()?;

        let mut token_input = String::new();
        io::stdin().read_line(&mut token_input)?;
        discord_token = token_input.trim().to_string();

        if discord_token.is_empty() {
            eprintln!("Error: Discord Token cannot be empty. Exiting.");
            std::process::exit(1);
        }

        // Save token to the profile .env file for future runs
        let target_file = if std::path::Path::new(&profile_env).exists() || !std::path::Path::new(".env").exists() {
            &profile_env
        } else {
            ".env"
        };

        let env_content = format!("\nDISCORD_TOKEN={}\n", discord_token);
        use std::fs::OpenOptions;
        if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(target_file) {
            let _ = file.write_all(env_content.as_bytes());
            println!("Saved DISCORD_TOKEN to {} for future runs.", target_file);
        }
    }

    // Assign default port dynamically based on profile number (e.g., Profile 1 -> 8080, Profile 2 -> 8081)
    let default_port = match profile_num.parse::<u16>() {
        Ok(n) if n > 0 => (8080 + n - 1).to_string(),
        _ => "8080".to_string(),
    };

    let port = env::var("PORT").unwrap_or(default_port);
    let addr = format!("0.0.0.0:{}", port);

    let listener = TcpListener::bind(&addr).await?;
    println!("Remote stealth proxy server (Profile {}) listening on ws://{}", profile_num, addr);

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
