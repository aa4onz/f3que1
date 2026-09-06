// src/tui/theme.rs
use ratatui::style::{Color, Modifier, Style};

pub struct Theme {
    pub active_border: Style,
    pub idle_border: Style,
    pub system_text: Style,
    pub self_message: Style,
    pub peer_message: Style,
    pub error_text: Style,
}

impl Default for Theme {
    fn default() -> Self {
        Self {
            active_border: Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
            idle_border: Style::default().fg(Color::DarkGray),
            system_text: Style::default().fg(Color::DarkGray),
            self_message: Style::default().fg(Color::Blue).add_modifier(Modifier::BOLD),
            peer_message: Style::default().fg(Color::Green).add_modifier(Modifier::BOLD),
            error_text: Style::default().fg(Color::Red),
        }
    }
}
