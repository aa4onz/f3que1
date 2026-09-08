use crate::app::queue_parser::parse_number_text;
use crate::app::queue_sender::{enqueue_parsed_item, send_direct_message};
use crate::app::state::AppState;
use crate::models::AppEvent;
use tokio::sync::mpsc::Sender;

/// Orchestrates parsing and sending/queueing execution.
pub async fn process_queue_input(
    app: &mut AppState,
    text: String,
    tx: &Sender<AppEvent>,
) {
    let parsed = parse_number_text(&text);

    if !app.queue_mode || parsed.is_none() {
        send_direct_message(app, text, tx).await;
    } else if let Some((num, original_text)) = parsed {
        // Number entered in Queue Mode: Enqueues the exact original text/number as typed
        enqueue_parsed_item(app, original_text, num, tx).await;
    }
}
