use std::net::SocketAddr;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::{broadcast, mpsc, oneshot},
    task::JoinHandle,
};

use crate::{
    message::{Command, ServerMessage},
    state::{Client, ClientId, ServerState},
};

/// How many messages may queue for one client before we start dropping them.
const CLIENT_BUFFER: usize = 100;

/// Bytes read from the socket in a single `read` call.
const READ_CHUNK: usize = 1024;

/// A client sending more than this without a newline is disconnected, so a
/// misbehaving peer cannot grow our buffer without bound.
const MAX_LINE_BYTES: usize = 8 * 1024;

/// Longest nickname a client may choose, in characters.
const MAX_NICKNAME_CHARS: usize = 32;

#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {
    #[error("error reading from client: {0}")]
    Read(#[from] std::io::Error),
    #[error("client sent a line longer than {MAX_LINE_BYTES} bytes")]
    LineTooLong,
}

/// Why a client's session ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Disconnect {
    /// The peer closed the connection.
    Client,
    /// Writing to the peer failed, so its writer task gave up.
    WriteFailed,
    /// The whole server is going away.
    ServerShutdown,
}

/// Drives one client for its entire lifetime: names it, registers it, serves
/// it, and cleans it up.
pub async fn handle_connection(
    socket: TcpStream,
    address: SocketAddr,
    state: ServerState,
    mut shutdown: broadcast::Receiver<()>,
) {
    let (read_half, mut write_half) = socket.into_split();
    let mut reader = LineReader::new(read_half);

    // The client is not in the registry until it has a name, so it neither
    // receives chat nor appears to anyone else while it is being prompted.
    let nickname = match ask_for_nickname(&mut write_half, &mut reader, &mut shutdown).await {
        Ok(Some(nickname)) => nickname,
        Ok(None) => {
            println!("Client {address} left before choosing a nickname");
            return;
        }
        Err(e) => {
            eprintln!("Error naming client {address}: {e}");
            return;
        }
    };

    let id = ClientId::next();
    let (sender, receiver) = mpsc::channel::<ServerMessage>(CLIENT_BUFFER);
    let (writer_died_tx, writer_died_rx) = oneshot::channel();

    state
        .add_client(Client::new(id, address, nickname.clone(), sender))
        .await;
    let writer = spawn_writer(write_half, receiver, id, writer_died_tx);

    println!("Client {id} ({nickname}) connected from {address}");

    state
        .send_to(
            id,
            &ServerMessage::notice(format!("Welcome to the server, {nickname}!")),
        )
        .await;
    state
        .broadcast(&ServerMessage::joined(&nickname), Some(id))
        .await;

    let outcome = read_loop(
        &mut reader,
        &state,
        id,
        &nickname,
        writer_died_rx,
        &mut shutdown,
    )
    .await;

    match outcome {
        Ok(Disconnect::ServerShutdown) => {
            state
                .send_to(
                    id,
                    &ServerMessage::notice(format!(
                        "Goodbye {nickname}, the server is shutting down"
                    )),
                )
                .await;
        }
        Ok(Disconnect::Client) => println!("Client {id} ({nickname}) disconnected"),
        Ok(Disconnect::WriteFailed) => {
            eprintln!("Client {id} ({nickname}) dropped: writing failed");
        }
        Err(ConnectionError::LineTooLong) => {
            eprintln!("Client {id} ({nickname}) dropped: line exceeded {MAX_LINE_BYTES} bytes");
            state
                .send_to(id, &ServerMessage::notice("Message too long, disconnecting"))
                .await;
        }
        Err(ref e) => eprintln!("Error handling client {id} ({nickname}): {e}"),
    }

    // Dropping this client's `Sender` lets its writer task finish flushing
    // whatever is still queued and then exit.
    state.remove_client(id).await;

    if !matches!(outcome, Ok(Disconnect::ServerShutdown)) {
        state.broadcast(&ServerMessage::left(&nickname), None).await;
    }

    // Wait for the queued bytes to reach the socket before we return, so a
    // graceful shutdown really does deliver the goodbye message.
    if let Err(e) = writer.await {
        eprintln!("Writer task for client {id} ({nickname}) failed: {e}");
    }
}

/// Prompts until the client supplies a usable nickname.
///
/// Writes straight to the socket rather than through a channel, because the
/// client has no writer task until it is registered. Returns `Ok(None)` if the
/// client leaves, or the server shuts down, before choosing one.
async fn ask_for_nickname(
    write_half: &mut OwnedWriteHalf,
    reader: &mut LineReader,
    shutdown: &mut broadcast::Receiver<()>,
) -> Result<Option<String>, ConnectionError> {
    loop {
        write_line(write_half, &ServerMessage::notice("Enter your nickname:")).await?;

        let line = tokio::select! {
            line = reader.next_line() => line?,
            _ = shutdown.recv() => {
                let goodbye = ServerMessage::notice("Server is shutting down");
                let _ = write_line(write_half, &goodbye).await;
                return Ok(None);
            }
        };

        let Some(line) = line else {
            return Ok(None);
        };

        let nickname = String::from_utf8_lossy(&line).trim().to_owned();

        if nickname.is_empty() {
            write_line(
                write_half,
                &ServerMessage::notice("A nickname cannot be empty."),
            )
            .await?;
            continue;
        }

        if nickname.chars().count() > MAX_NICKNAME_CHARS {
            write_line(
                write_half,
                &ServerMessage::notice(format!(
                    "A nickname can be at most {MAX_NICKNAME_CHARS} characters."
                )),
            )
            .await?;
            continue;
        }

        return Ok(Some(nickname));
    }
}

/// Reads commands from the client and acts on them, until the client leaves,
/// its writer dies, or the server shuts down.
async fn read_loop(
    reader: &mut LineReader,
    state: &ServerState,
    id: ClientId,
    nickname: &str,
    mut writer_died: oneshot::Receiver<()>,
    shutdown: &mut broadcast::Receiver<()>,
) -> Result<Disconnect, ConnectionError> {
    loop {
        tokio::select! {
            line = reader.next_line() => {
                match line? {
                    Some(line) => {
                        let line = String::from_utf8_lossy(&line);
                        if let Some(command) = Command::parse(&line) {
                            handle_command(command, state, id, nickname).await;
                        }
                    }
                    None => return Ok(Disconnect::Client),
                }
            }
            _ = &mut writer_died => return Ok(Disconnect::WriteFailed),
            _ = shutdown.recv() => return Ok(Disconnect::ServerShutdown),
        }
    }
}

async fn handle_command(command: Command, state: &ServerState, id: ClientId, nickname: &str) {
    match command {
        Command::Text(body) => {
            state
                .broadcast(&ServerMessage::chat(nickname, body), Some(id))
                .await;
        }
        Command::Unknown { name } => {
            state
                .send_to(id, &ServerMessage::notice(format!("Unknown command: /{name}")))
                .await;
        }
    }
}

/// Turns the socket's arbitrary chunks into whole newline-terminated lines.
struct LineReader {
    read_half: OwnedReadHalf,
    /// Bytes received but not yet returned as a complete line.
    buffer: Vec<u8>,
}

impl LineReader {
    const fn new(read_half: OwnedReadHalf) -> Self {
        Self {
            read_half,
            buffer: Vec::new(),
        }
    }

    /// Returns the next line, including its terminator, or `None` at end of
    /// stream.
    ///
    /// Cancel-safe: the only await point is `read`, which consumes nothing when
    /// cancelled, and anything already received stays in `buffer`.
    async fn next_line(&mut self) -> Result<Option<Vec<u8>>, ConnectionError> {
        let mut chunk = [0u8; READ_CHUNK];

        loop {
            if let Some(newline) = self.buffer.iter().position(|byte| *byte == b'\n') {
                return Ok(Some(self.buffer.drain(..=newline).collect()));
            }

            // No newline in sight and the buffer is already oversized.
            if self.buffer.len() > MAX_LINE_BYTES {
                return Err(ConnectionError::LineTooLong);
            }

            let read = self.read_half.read(&mut chunk).await?;
            if read == 0 {
                return Ok(None);
            }
            self.buffer
                .extend_from_slice(chunk.get(..read).unwrap_or_default());
        }
    }
}

/// Renders one message onto the socket, adding the line terminator.
async fn write_line(
    write_half: &mut OwnedWriteHalf,
    message: &ServerMessage,
) -> std::io::Result<()> {
    write_half.write_all(format!("{message}\n").as_bytes()).await
}

/// Owns the client's write half and drains its queue onto the socket.
///
/// Signals `died` if a write fails, so the read side knows to give up too.
fn spawn_writer(
    mut write_half: OwnedWriteHalf,
    mut receiver: mpsc::Receiver<ServerMessage>,
    id: ClientId,
    died: oneshot::Sender<()>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(message) = receiver.recv().await {
            if let Err(e) = write_line(&mut write_half, &message).await {
                eprintln!("Error writing to client {id}: {e}");
                let _ = died.send(());
                return;
            }
        }

        // Every `Sender` is gone: the client was removed from the registry, so
        // close our side of the socket cleanly.
        let _ = write_half.shutdown().await;
    })
}
