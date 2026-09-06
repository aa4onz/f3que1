// src/tui/modals.rs
use ratatui::{
    layout::Rect,
    widgets::{Block, Borders, Clear, Paragraph},
    Frame,
};

pub fn render_logout_modal(f: &mut Frame, screen_size: Rect) {
    let modal_area = Rect::new(
        screen_size.width / 4,
        screen_size.height / 3,
        screen_size.width / 2,
        7,
    );
    f.render_widget(Clear, modal_area);
    let block = Paragraph::new("\n Clear saved token and exit?\n\n Press [Y] to confirm or [N/Esc] to cancel.")
        .block(Block::default().title(" Logout / Switch Token ").borders(Borders::ALL));
    f.render_widget(block, modal_area);
}

pub fn render_switch_channel_modal(f: &mut Frame, screen_size: Rect, modal_input: &str) {
    let modal_area = Rect::new(
        screen_size.width / 6,
        screen_size.height / 3,
        (screen_size.width * 2) / 3,
        7,
    );
    f.render_widget(Clear, modal_area);
    let modal_text = format!("\n Enter Channel URL / ID:\n > {}\n\n Press [Enter] to Jump | [Esc] Cancel", modal_input);
    let block = Paragraph::new(modal_text)
        .block(Block::default().title(" Jump to Specific Channel ").borders(Borders::ALL));
    f.render_widget(block, modal_area);
}
