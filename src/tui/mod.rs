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

    // Responsive horizontal layout with proper margins
    let middle_area = if screen_size.width > 100 {
        let horizontal_chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Percentage(5),
                Constraint::Percentage(90),
                Constraint::Percentage(5),
            ])
            .split(screen_size);
        horizontal_chunks[1]
    } else {
        screen_size
    };

    let vertical_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Queue Monitor Header
            Constraint::Min(5),    // Messages Chat List
            Constraint::Length(3), // Input Text Box
        ])
        .split(middle_area);

    let queue_widget = components::render_queue_monitor(
        state.queue_mode,
        &state.queue,
        state.hardware_delay_ms,
    );
    f.render_widget(queue_widget, vertical_chunks[0]);

    let msg_list = components::render_messages(
        &state.messages,
        &state.self_username,
        state.show_timestamp,
        state.show_latency,
    );
    f.render_stateful_widget(msg_list, vertical_chunks[1], &mut state.list_state);

    let input_box = components::render_input_box(&state.input_text);
    f.render_widget(input_box, vertical_chunks[2]);

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
