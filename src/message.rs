//! The two message types that cross the server.
//!
//! [`Command`] is what a client asked for, parsed from an input line.
//! [`ServerMessage`] is what the server sends back. Keeping them apart means a
//! message stays structured until the writer task renders it, so the wire
//! format lives in exactly one place.

use std::fmt;

/// Something a client asked the server to do, parsed from one input line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Ordinary chat text, to be broadcast to everyone else.
    Text(String),
    /// A `/word` the server does not recognise.
    Unknown { name: String },
}

impl Command {
    /// Parses one line of client input.
    ///
    /// Returns `None` for a blank line, which is not worth acting on.
    #[must_use]
    pub fn parse(line: &str) -> Option<Self> {
        // Clears the line terminator along with any trailing whitespace.
        let line = line.trim_end();

        if line.is_empty() {
            return None;
        }

        if let Some(rest) = line.strip_prefix('/') {
            let name = rest.split_whitespace().next().unwrap_or_default().to_owned();
            return Some(Self::Unknown { name });
        }

        Some(Self::Text(line.to_owned()))
    }
}

/// Something the server sends to a client.
///
/// [`Display`](fmt::Display) renders one line *without* its terminator; the
/// writer task appends the newline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerMessage {
    /// Chat text from another client.
    Chat { from: String, body: String },
    /// A client joined the server.
    Joined { nickname: String },
    /// A client left the server.
    Left { nickname: String },
    /// Server-to-client text: prompts, greetings, errors, goodbyes.
    Notice(String),
}

impl ServerMessage {
    pub fn chat(from: impl Into<String>, body: impl Into<String>) -> Self {
        Self::Chat {
            from: from.into(),
            body: body.into(),
        }
    }

    pub fn joined(nickname: impl Into<String>) -> Self {
        Self::Joined {
            nickname: nickname.into(),
        }
    }

    pub fn left(nickname: impl Into<String>) -> Self {
        Self::Left {
            nickname: nickname.into(),
        }
    }

    pub fn notice(text: impl Into<String>) -> Self {
        Self::Notice(text.into())
    }
}

impl fmt::Display for ServerMessage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Chat { from, body } => write!(formatter, "{from}: {body}"),
            Self::Joined { nickname } => write!(formatter, "{nickname} joined the server"),
            Self::Left { nickname } => write!(formatter, "{nickname} left the server"),
            Self::Notice(text) => write!(formatter, "{text}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Command, ServerMessage};

    #[test]
    fn plain_text_parses_as_chat() {
        assert_eq!(
            Command::parse("hello everyone"),
            Some(Command::Text("hello everyone".to_owned()))
        );
    }

    #[test]
    fn line_terminators_are_trimmed() {
        assert_eq!(
            Command::parse("hello\r\n"),
            Some(Command::Text("hello".to_owned()))
        );
    }

    #[test]
    fn leading_whitespace_is_preserved() {
        assert_eq!(
            Command::parse("  indented  "),
            Some(Command::Text("  indented".to_owned()))
        );
    }

    #[test]
    fn blank_lines_parse_to_nothing() {
        assert_eq!(Command::parse(""), None);
        assert_eq!(Command::parse("\r\n"), None);
        assert_eq!(Command::parse("   \n"), None);
    }

    #[test]
    fn a_slash_word_parses_as_an_unknown_command() {
        assert_eq!(
            Command::parse("/nick alice"),
            Some(Command::Unknown {
                name: "nick".to_owned()
            })
        );
    }

    #[test]
    fn a_bare_slash_is_an_unknown_command_with_no_name() {
        assert_eq!(
            Command::parse("/"),
            Some(Command::Unknown {
                name: String::new()
            })
        );
    }

    #[test]
    fn messages_render_one_line_without_a_terminator() {
        assert_eq!(ServerMessage::chat("alice", "hi").to_string(), "alice: hi");
        assert_eq!(
            ServerMessage::joined("bob").to_string(),
            "bob joined the server"
        );
        assert_eq!(
            ServerMessage::left("bob").to_string(),
            "bob left the server"
        );
        assert_eq!(ServerMessage::notice("careful").to_string(), "careful");
    }
}
