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

    // 1. Split screen horizontally into Left Sidebar (Queue) and Right Main Area (Chat + Input)
    let columns = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(28), // Left: Queue Box Sidebar
            Constraint::Percentage(72), // Right: Chat Area + Input Box
        ])
        .split(screen_size);

    // 2. Split Right Main Area vertically into Messages List and Send Input Box
    let right_workspace = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(5),    // Top: Chat Messages List
            Constraint::Length(3), // Bottom: Send Message Input Box (Aligned directly under Chat)
        ])
        .split(columns[1]);

    let queue_widget = components::render_queue_sidebar(
        state.queue_mode,
        &state.queue,
        state.hardware_delay_ms,
        state.reaction_delay_mode,
    );
    f.render_widget(queue_widget, columns[0]);

    let msg_list = components::render_messages(
        &state.messages,
        &state.self_username,
        state.show_timestamp,
        state.show_latency,
    );
    f.render_stateful_widget(msg_list, right_workspace[0], &mut state.list_state);

    let input_box = components::render_input_box(&state.input_text);
    f.render_widget(input_box, right_workspace[1]);

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
