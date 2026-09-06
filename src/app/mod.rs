// src/app/mod.rs
pub mod state;
pub mod handlers;

// Re-export so the rest of your app can still use `app::AppState` directly
pub use state::AppState;
