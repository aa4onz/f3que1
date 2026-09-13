// src/tui/mod.rs
pub mod theme;
pub mod components;
pub mod modals;

use crate::app::state::ActiveModal;
use crate::app::AppState;
use ratatui::{
    layout::{Constraint, Direction, Layout},
    Frame,
};

pub fn render(f: &mut Frame, state: &mut AppState) {
    let screen_size = f.size();

    let is_proxy_mode = state.token.starts_with("ws://") || state.token.starts_with("wss://");
    let show_sidebar = is_proxy_mode && state.queue_mode;

    // 1. Horizontal Layout Split: Sidebar (Queue) on left (if enabled) and Right Area on right
    let columns = if show_sidebar {
        Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Percentage(28), // Left Queue Box
                Constraint::Percentage(72), // Right Workspace
            ])
            .split(screen_size)
    } else {
        Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Percentage(0),
                Constraint::Percentage(100), // Full width when Queue is disabled or local mode
            ])
            .split(screen_size)
    };

    if show_sidebar {
        let queue_widget = components::render_queue_sidebar(
            state.queue_mode,
            &state.queue,
            state.hardware_delay_ms,
            state.reaction_delay_mode,
        );
        f.render_widget(queue_widget, columns[0]);
    }

    // 2. Fixed Vertical Layout Split: Chat Messages area (top), Middle Box (preview or empty spacer), Bottom Input Box
    let right_workspace = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(5),    // Top: Chat Messages List
            Constraint::Length(3), // Middle: Preview Box / Space Margin
            Constraint::Length(3), // Bottom: Main Input Box
        ])
        .split(columns[1]);

    let msg_list = components::render_messages(
        &state.messages,
        &state.self_username,
        state.show_timestamp,
        state.show_latency,
    );
    f.render_stateful_widget(msg_list, right_workspace[0], &mut state.list_state);

    // Middle Box: Render preview box when queue mode is active and queue is non-empty
    if state.queue_mode && !state.queue.is_empty() {
        let preview_box = components::render_queue_preview_box(&state.preview_typed_text);
        f.render_widget(preview_box, right_workspace[1]);
    }

    // Bottom Main Input Box (Always stays in exact fixed position)
    let input_box = components::render_input_box(&state.input_text);
    f.render_widget(input_box, right_workspace[2]);

    // Render Modal Overlays
    match state.active_modal {
        ActiveModal::LogoutPrompt => {
            modals::render_logout_modal(f, screen_size);
        }
        ActiveModal::SwitchChannelPrompt => {
            modals::render_switch_channel_modal(f, screen_size, &state.modal_input);
        }
        ActiveModal::None => {}
    }
}
