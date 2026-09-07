// src/tui/components.rs
use crate::models::{DiscordMessage, MessageStatus};
use ratatui::{
    style::{Color, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, Paragraph},
};

pub fn render_queue_monitor<'a>(queue_mode: bool, queue: &[i64], hw_delay: u64) -> Paragraph<'a> {
    let mode_str = if queue_mode {
        Span::styled("ENABLED", Style::default().fg(Color::Green))
    } else {
        Span::styled("DISABLED", Style::default().fg(Color::Red))
    };

    let queue_str = if queue.is_empty() {
        Span::styled("[ EMPTY ]", Style::default().fg(Color::DarkGray))
    } else {
        let items: Vec<String> = queue.iter().map(|n| n.to_string()).collect();
        Span::styled(format!("[ {} ]", items.join(" ➔ ")), Style::default().fg(Color::Yellow))
    };

    let line = Line::from(vec![
        Span::raw(" Queue Mode: "),
        mode_str,
        Span::raw(" | Items: "),
        queue_str,
        Span::raw(" | Hardware Delay: "),
        Span::styled(format!("{}ms", hw_delay), Style::default().fg(Color::Cyan)),
        Span::raw(" | [F6/Ctrl+Q: Toggle Mode | F7/Ctrl+C: Clear Queue]"),
    ]);

    Paragraph::new(line)
        .block(Block::default().borders(Borders::ALL).title(" Stealth Queue Engine "))
}

pub fn render_messages<'a>(
    messages: &'a [DiscordMessage],
    self_user: &str,
    show_time: bool,
    show_lat: bool,
) -> List<'a> {
    let msgs: Vec<ListItem> = messages.iter().map(|m| {
        let is_me = m.author == self_user;
        let author_color = if is_me { Color::Blue } else { Color::Green };
        let header_style = Style::default().fg(author_color);

        let content_color = match m.status {
            MessageStatus::Sending => Color::DarkGray,
            MessageStatus::Failed => Color::Red,
            MessageStatus::Delivered => Color::White,
        };

        let status_indicator = match m.status {
            MessageStatus::Sending => " [...]",
            MessageStatus::Failed => " [❌]",
            MessageStatus::Delivered => "",
        };

        let header_line = if show_time || show_lat {
            let mut parts = m.timestamp.split('|');
            let time_part = parts.next().unwrap_or("").trim();
            let lat_part = parts.next().unwrap_or("").trim();

            let meta_str = match (show_time && !time_part.is_empty(), show_lat && !lat_part.is_empty()) {
                (true, true) => format!("[{} | {}]", time_part, lat_part),
                (true, false) => format!("[{}]", time_part),
                (false, true) => format!("[{}]", lat_part),
                (false, false) => String::new(),
            };

            if !meta_str.is_empty() {
                Line::from(vec![
                    Span::styled(&m.author, header_style),
                    Span::raw(" "),
                    Span::styled(meta_str, header_style),
                    Span::styled(status_indicator, header_style),
                ])
            } else {
                Line::from(vec![
                    Span::styled(&m.author, header_style),
                    Span::styled(status_indicator, header_style),
                ])
            }
        } else {
            Line::from(vec![
                Span::styled(&m.author, header_style),
                Span::styled(status_indicator, header_style),
            ])
        };

        let content_line = Line::from(vec![
            Span::raw("  "),
            Span::styled(&m.content, Style::default().fg(content_color)),
        ]);

        ListItem::new(vec![header_line, content_line])
    }).collect();

    let time_status = if show_time { "F2: Hide Time" } else { "F2: Show Time" };
    let lat_status = if show_lat { "F3: Hide Latency" } else { "F3: Show Latency" };
    let title_text = format!(" messages [{} | {} | p/Ctrl+G: Channel | Ctrl+X/F4: Switch Token] ", time_status, lat_status);

    List::new(msgs)
        .block(Block::default()
            .borders(Borders::ALL)
            .title(title_text))
}

pub fn render_input_box<'a>(input_text: &'a str) -> Paragraph<'a> {
    let prompt_span = Span::raw("> ");
    let text_span = Span::styled(
        input_text,
        Style::default().fg(Color::Yellow)
    );
    let input_line = Line::from(vec![prompt_span, text_span]);

    Paragraph::new(input_line)
        .block(Block::default().borders(Borders::ALL))
}
