use fast_discord_tui::app::state::AppState;
use fast_discord_tui::models::AppEvent;
use fast_discord_tui::network;
use fast_discord_tui::tui;

use std::env;
use std::io::{self, Write};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio::time::{interval, Duration};

fn sanitize_proxy_url(raw_url: &str) -> String {
    let mut url = raw_url.trim().trim_end_matches('/').to_string();
    if url.starts_with("https://") {
        url = url.replacen("https://", "wss://", 1);
    } else if url.starts_with("http://") {
        url = url.replacen("http://", "ws://", 1);
    } else if !url.starts_with("wss://") && !url.starts_with("ws://") {
        url = format!("wss://{}", url);
    }
    url
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    loop {
        println!("========================================");
        println!("       Fast Discord TUI Client          ");
        println!("========================================");
        println!("[1] Direct Discord Mode (No Remote Proxy)");
        println!("[2] Remote Proxy Mode (USA Codespace)");
        print!("Select Mode [1 or 2]: ");
        io::stdout().flush()?;

        let mut mode_choice = String::new();
        io::stdin().read_line(&mut mode_choice)?;
        let is_proxy_mode = mode_choice.trim() == "2";

        let mut token_or_proxy = String::new();
        let mut url_input = String::new();

        if is_proxy_mode {
            println!("\nSelect Remote Proxy Profile:");
            println!("[1] Remote Profile 1");
            println!("[2] Remote Profile 2");
            println!("[3] Remote Profile 3");
            print!("Select Profile [1, 2, or 3]: ");
            io::stdout().flush()?;

            let mut profile_choice = String::new();
            io::stdin().read_line(&mut profile_choice)?;
            let profile_num = profile_choice.trim().to_string();
            let profile_num = if profile_num.is_empty() { "1".to_string() } else { profile_num };

            // Load .env.profile_X if present
            let profile_env = format!(".env.profile_{}", profile_num);
            if std::path::Path::new(&profile_env).exists() {
                dotenvy::from_filename(&profile_env).ok();
            } else {
                dotenvy::dotenv().ok();
            }

            let cache_filename = format!(".proxy_cache_profile_{}", profile_num);

            // Check order: 1. Environment Variable, 2. Cached file, 3. Manual User Prompt
            if let Ok(env_proxy) = env::var("PROXY_URL").or_else(|_| env::var("REMOTE_PROXY_URL")) {
                if !env_proxy.trim().is_empty() {
                    token_or_proxy = env_proxy.trim().to_string();
                    let _ = std::fs::write(&cache_filename, &token_or_proxy);
                }
            }

            if token_or_proxy.is_empty() {
                if std::path::Path::new(&cache_filename).exists() {
                    token_or_proxy = std::fs::read_to_string(&cache_filename)?.trim().to_string();
                } else if std::path::Path::new(".proxy_cache").exists() && !["1", "2", "3"].contains(&profile_num.as_str()) {
                    token_or_proxy = std::fs::read_to_string(".proxy_cache")?.trim().to_string();
                }
            }

            if token_or_proxy.is_empty() {
                print!("Enter Remote Proxy WebSocket URL for Profile {} (e.g. wss://...): ", profile_num);
                io::stdout().flush()?;
                io::stdin().read_line(&mut token_or_proxy)?;
                token_or_proxy = token_or_proxy.trim().to_string();
                if !token_or_proxy.is_empty() {
                    token_or_proxy = sanitize_proxy_url(&token_or_proxy);
                    let _ = std::fs::write(&cache_filename, &token_or_proxy);
                }
            } else {
                token_or_proxy = sanitize_proxy_url(&token_or_proxy);
            }
        } else {
            // Direct Discord Mode
            dotenvy::dotenv().ok();

            if let Ok(env_token) = env::var("DISCORD_TOKEN") {
                if !env_token.trim().is_empty() {
                    token_or_proxy = env_token.trim().to_string();
                    let _ = std::fs::write(".token_cache", &token_or_proxy);
                }
            }

            if token_or_proxy.is_empty() {
                if std::path::Path::new(".token_cache").exists() {
                    token_or_proxy = std::fs::read_to_string(".token_cache")?.trim().to_string();
                } else {
                    print!("Enter Discord Token: ");
                    io::stdout().flush()?;
                    io::stdin().read_line(&mut token_or_proxy)?;
                    token_or_proxy = token_or_proxy.trim().to_string();
                    if !token_or_proxy.is_empty() {
                        let _ = std::fs::write(".token_cache", &token_or_proxy);
                    }
                }
            }
        }

        // Channel ID resolution (Env variable -> Cache file -> Prompt)
        if let Ok(env_cid) = env::var("TARGET_CHANNEL_ID") {
            if !env_cid.trim().is_empty() {
                url_input = env_cid.trim().to_string();
            }
        }

        if url_input.is_empty() {
            if std::path::Path::new(".channel_cache").exists() {
                url_input = std::fs::read_to_string(".channel_cache")?.trim().to_string();
            } else {
                print!("Enter direct Discord Channel URL link: ");
                io::stdout().flush()?;
                io::stdin().read_line(&mut url_input)?;
                url_input = url_input.trim().to_string();
            }
        }

        let clean_url = url_input.trim_end_matches('/');
        let target_channel_id = clean_url.split('/').last().unwrap_or("").split('?').next().unwrap_or("").to_string();
        if target_channel_id.is_empty() || !target_channel_id.chars().all(|c| c.is_numeric()) {
            println!("Error: Invalid Discord Channel URL provided!");
            let _ = std::fs::remove_file(".channel_cache");
            continue;
        }

        std::fs::write(".channel_cache", &target_channel_id)?;

        crossterm::terminal::enable_raw_mode()?;
        let mut stdout = io::stdout();
        crossterm::queue!(
            stdout,
            crossterm::terminal::EnterAlternateScreen,
            crossterm::cursor::Hide,
            crossterm::event::EnableMouseCapture
        )?;
        let backend = ratatui::backend::CrosstermBackend::new(stdout);
        let mut terminal = ratatui::Terminal::new(backend)?;

        let mut initial_state = AppState::new(token_or_proxy.clone());
        initial_state.set_target_channel_id(target_channel_id.clone());
        let channel_id_rx = initial_state.channel_id_rx.clone();
        let app_state = Arc::new(Mutex::new(initial_state));

        let (event_tx, mut event_rx) = mpsc::channel::<AppEvent>(512);
        let (net_tx, net_rx) = mpsc::channel::<AppEvent>(256);

        network::spawn_network_handlers(Arc::clone(&app_state), event_tx.clone(), net_rx, channel_id_rx);

        // Initial history fetch
        let _ = net_tx.send(AppEvent::FetchChannelHistory(target_channel_id)).await;

        // Draw initial frame immediately
        {
            let mut state = app_state.lock().await;
            terminal.draw(|f| {
                tui::render(f, &mut state);
            })?;
        }

        // Ticker task for driving visual character typing updates continuously
        let tick_tx = event_tx.clone();
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_millis(20));
            loop {
                ticker.tick().await;
                // Dummy terminal wake event to drive frame rendering smoothly
            }
        });

        while let Some(event) = event_rx.recv().await {
            let mut state = app_state.lock().await;
            let mut should_exit = false;

            match &event {
                AppEvent::HttpTriggerTyping 
                | AppEvent::HttpSendChat { .. } 
                | AppEvent::FetchChannelHistory(_) 
                | AppEvent::EnqueueNumberItem(_) 
                | AppEvent::UpdateHardwareDelay(_)
                | AppEvent::UpdateReactionDelayMode(_)
                | AppEvent::TriggerTopQueue => {
                    let n_tx = net_tx.clone();
                    let ev_clone = event.clone();
                    tokio::spawn(async move {
                        let _ = n_tx.send(ev_clone).await;
                    });
                }
                AppEvent::ToggleQueueMode | AppEvent::ClearQueue => {
                    let n_tx = net_tx.clone();
                    let ev_clone = event.clone();
                    tokio::spawn(async move {
                        let _ = n_tx.send(ev_clone).await;
                    });
                }
                _ => {}
            }

            if state.handle_event(event, &event_tx).await {
                should_exit = true;
            }

            while let Ok(next_event) = event_rx.try_recv() {
                match &next_event {
                    AppEvent::HttpTriggerTyping 
                    | AppEvent::HttpSendChat { .. } 
                    | AppEvent::FetchChannelHistory(_) 
                    | AppEvent::EnqueueNumberItem(_) 
                    | AppEvent::UpdateHardwareDelay(_)
                    | AppEvent::UpdateReactionDelayMode(_)
                    | AppEvent::TriggerTopQueue => {
                        let n_tx = net_tx.clone();
                        let ev_clone = next_event.clone();
                        tokio::spawn(async move {
                            let _ = n_tx.send(ev_clone).await;
                        });
                    }
                    AppEvent::ToggleQueueMode | AppEvent::ClearQueue => {
                        let n_tx = net_tx.clone();
                        let ev_clone = next_event.clone();
                        tokio::spawn(async move {
                            let _ = n_tx.send(ev_clone).await;
                        });
                    }
                    _ => {}
                }

                if state.handle_event(next_event, &event_tx).await {
                    should_exit = true;
                }
            }

            state.step_simulated_typing();

            terminal.draw(|f| {
                tui::render(f, &mut state);
            })?;

            if should_exit { break; }
        }

        crossterm::terminal::disable_raw_mode()?;
        crossterm::execute!(
            terminal.backend_mut(),
            crossterm::terminal::LeaveAlternateScreen,
            crossterm::cursor::Show,
            crossterm::event::DisableMouseCapture
        )?;
    }
}
