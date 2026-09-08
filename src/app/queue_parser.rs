/// Extracts the original number and text from input text without modifying or adding numbers.
pub fn parse_number_text(text: &str) -> Option<(i64, String)> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }

    let mut num_chars = String::new();

    for (_i, c) in text.char_indices() {
        if c.is_ascii_digit() {
            num_chars.push(c);
        } else if num_chars.is_empty() && (c == ' ' || c == '_') {
            continue;
        } else {
            break;
        }
    }

    if let Ok(num) = num_chars.parse::<i64>() {
        Some((num, text.to_string()))
    } else {
        None
    }
}
