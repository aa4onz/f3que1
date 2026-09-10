// src/tui/components.rs
use crate::models::{DiscordMessage, MessageStatus, QueuedItem, ReactionDelayMode};
use ratatui::{
    style::{Color, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, Paragraph},
};

pub fn render_queue_sidebar<'a>(queue_mode: bool, queue: &[QueuedItem], hw_delay: u64, delay_mode: ReactionDelayMode) -> List<'a> {
    let mode_span = if queue_mode {
        Span::styled("ENABLED", Style::default().fg(Color::Green))
    } else {
        Span::styled("DISABLED", Style::default().fg(Color::Red))
    };

    let delay_str = match delay_mode {
        ReactionDelayMode::Normal => "200-300ms",
        ReactionDelayMode::Fast => "0-200ms",
        ReactionDelayMode::Instant => "0ms",
    };

    let mut items: Vec<ListItem> = Vec::new();

    items.push(ListItem::new(Line::from(vec![
        Span::raw("Status: "),
        mode_span,
    ])));

    items.push(ListItem::new(Line::from(vec![
        Span::raw("HW Delay: "),
        Span::styled(format!("{}ms", hw_delay), Style::default().fg(Color::Cyan)),
    ])));

    items.push(ListItem::new(Line::from(vec![
        Span::raw("Rx Delay (F10): "),
        Span::styled(delay_str, Style::default().fg(Color::Magenta)),
    ])));

    items.push(ListItem::new(Line::from(Span::styled("────────────────────", Style::default().fg(Color::DarkGray)))));

    if queue.is_empty() {
        items.push(ListItem::new(Span::styled("(Empty Queue)", Style::default().fg(Color::DarkGray))));
    } else {
        for (idx, q_item) in queue.iter().enumerate() {
            let label = format!("[#{}] {}", idx + 1, q_item.content);
            items.push(ListItem::new(Span::styled(label, Style::default().fg(Color::Yellow))));
        }
    }

    List::new(items)
        .block(Block::default().borders(Borders::ALL).title(" Stealth Queue "))
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

    let time_status = if show_time { "F6: Hide Time" } else { "F6: Show Time" };
    let lat_status = if show_lat { "F7: Hide Latency" } else { "F7: Show Latency" };
    let title_text = format!(" Messages [{} | {} | F5/Ctrl+G: Channel | F2/Ctrl+Q: Queue | F3/Ctrl+C: Clear Queue | F1: Trigger Top | F10: Delay] ", time_status, lat_status);

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
        .block(Block::default().borders(Borders::ALL).title(" Send Message "))
}
