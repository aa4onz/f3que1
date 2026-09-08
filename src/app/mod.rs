// src/app/mod.rs
pub mod state;
pub mod handlers;
pub mod queue_logic;
pub mod queue_parser;
pub mod queue_sender;

// Re-export so the rest of your app can still use `app::AppState` directly
pub use state::AppState;
