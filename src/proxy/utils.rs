use rand::Rng;

/// Generates Discord Snowflake ID formatted string for nonces
pub fn generate_snowflake_nonce() -> String {
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
