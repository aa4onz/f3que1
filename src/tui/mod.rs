// src/tui/mod.rs
pub mod theme;
pub mod components;
pub mod modals;

use crate::app::state::ActiveModal;
use crate::app::AppState;
use ratatui::{
    layout::{Constraint, Direction, Layout},
    widgets::Block,
    Frame,
};

pub fn render(f: &mut Frame, state: &mut AppState) {
    let screen_size = f.size();

    let is_proxy_mode = state.token.starts_with("ws://") || state.token.starts_with("wss://");
    let show_sidebar_content = is_proxy_mode && state.queue_mode;
    let show_preview_box = state.queue_mode && !state.queue.is_empty();

    // 1. Horizontal Layout Split: Left Sidebar (28%), Right Workspace (72%)
    let columns = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(28), // Left Queue Box / Margin
            Constraint::Percentage(72), // Right Workspace (Chat + Inputs)
        ])
        .split(screen_size);

    if show_sidebar_content {
        let queue_widget = components::render_queue_sidebar(
            state.queue_mode,
            &state.queue,
            state.hardware_delay_ms,
            state.reaction_delay_mode,
        );
        f.render_widget(queue_widget, columns[0]);
    } else {
        // Render blank layout margin when queue mode is disabled or in direct mode
        f.render_widget(Block::default(), columns[0]);
    }

    // 2. Dynamic Vertical Layout Split:
    // When preview box is active -> 3 slots: Messages, Simulated Preview Box, Main Input Box
    // When preview box is inactive -> 2 slots: Messages, Main Input Box (no extra gap)
    if show_preview_box {
        let right_workspace = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Min(5),    // Top: Chat Messages List
                Constraint::Length(3), // Middle: Simulated Preview Input Box
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

        let preview_box = components::render_queue_preview_box(&state.preview_typed_text);
        f.render_widget(preview_box, right_workspace[1]);

        let input_box = components::render_input_box(&state.input_text);
        f.render_widget(input_box, right_workspace[2]);
    } else {
        let right_workspace = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Min(5),    // Top: Chat Messages List
                Constraint::Length(3), // Bottom: Main Input Box (directly below messages)
            ])
            .split(columns[1]);

        let msg_list = components::render_messages(
            &state.messages,
            &state.self_username,
            state.show_timestamp,
            state.show_latency,
        );
        f.render_stateful_widget(msg_list, right_workspace[0], &mut state.list_state);

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
