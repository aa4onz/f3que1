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

    // 1. Split screen horizontally into Left Sidebar (Queue) and Right Main Area (Chat + Input)
    let columns = if show_sidebar {
        Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Percentage(28), // Left: Queue Box Sidebar
                Constraint::Percentage(72), // Right: Chat Area + Input Box
            ])
            .split(screen_size)
    } else {
        Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Percentage(0),
                Constraint::Percentage(100), // Full width when Queue is disabled or local
            ])
            .split(screen_size)
    };

    // Render Left Sidebar only when Queue Mode is active in Proxy Mode
    if show_sidebar {
        let queue_widget = components::render_queue_sidebar(
            state.queue_mode,
            &state.queue,
            state.hardware_delay_ms,
            state.reaction_delay_mode,
        );
        f.render_widget(queue_widget, columns[0]);
    }

    // 2. Split Right Main Area vertically into Messages List, Middle Queue Preview Box (when active), and Send Input Box
    let right_workspace = if show_sidebar && !state.queue.is_empty() {
        Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Min(5),    // Top: Chat Messages List
                Constraint::Length(3), // Middle: Top Queue Simulated Typing Box
                Constraint::Length(3), // Bottom: Send Message Input Box
            ])
            .split(columns[1])
    } else {
        Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Min(5),    // Top: Chat Messages List
                Constraint::Length(3), // Bottom: Send Message Input Box
            ])
            .split(columns[1])
    };

    let msg_list = components::render_messages(
        &state.messages,
        &state.self_username,
        state.show_timestamp,
        state.show_latency,
    );
    f.render_stateful_widget(msg_list, right_workspace[0], &mut state.list_state);

    if show_sidebar && !state.queue.is_empty() {
        let preview_box = components::render_queue_preview_box(&state.preview_typed_text);
        f.render_widget(preview_box, right_workspace[1]);

        let input_box = components::render_input_box(&state.input_text);
        f.render_widget(input_box, right_workspace[2]);
    } else {
        let input_box = components::render_input_box(&state.input_text);
        f.render_widget(input_box, right_workspace[1]);
    }

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
