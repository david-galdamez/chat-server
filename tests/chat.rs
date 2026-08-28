//! End-to-end tests: they drive the real server over real TCP sockets.

use std::{net::SocketAddr, time::Duration};

use chat_server::server;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines},
    net::{
        TcpListener, TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::oneshot,
    task::JoinHandle,
    time::timeout,
};

/// How long a test waits for something that should happen.
const WAIT: Duration = Duration::from_secs(5);

/// How long a test waits to convince itself something should *not* happen.
const QUIET: Duration = Duration::from_millis(250);

const PROMPT: &str = "Enter your nickname:";

/// A server bound to an ephemeral port, shut down when the test ends.
struct TestServer {
    address: SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    handle: Option<JoinHandle<()>>,
}

impl TestServer {
    async fn start() -> anyhow::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (shutdown, signal) = oneshot::channel();

        let handle = tokio::spawn(async move {
            server::run(listener, async move {
                let _ = signal.await;
            })
            .await;
        });

        Ok(Self {
            address,
            shutdown: Some(shutdown),
            handle: Some(handle),
        })
    }

    /// Signals shutdown and waits for the server to finish.
    async fn stop(&mut self) -> anyhow::Result<()> {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(handle) = self.handle.take() {
            timeout(WAIT, handle).await??;
        }
        Ok(())
    }
}

/// A chat client speaking the server's newline-delimited protocol.
struct Client {
    lines: Lines<BufReader<OwnedReadHalf>>,
    write_half: OwnedWriteHalf,
}

impl Client {
    /// Opens a socket without completing the nickname handshake.
    async fn dial(address: SocketAddr) -> anyhow::Result<Self> {
        let stream = TcpStream::connect(address).await?;
        let (read_half, write_half) = stream.into_split();
        Ok(Self {
            lines: BufReader::new(read_half).lines(),
            write_half,
        })
    }

    /// Connects and completes the handshake under `nickname`.
    async fn connect(address: SocketAddr, nickname: &str) -> anyhow::Result<Self> {
        let mut client = Self::dial(address).await?;

        let prompt = client.expect_line().await?;
        anyhow::ensure!(prompt == PROMPT, "unexpected prompt: {prompt}");

        client.send(nickname).await?;

        let welcome = client.expect_line().await?;
        anyhow::ensure!(
            welcome == format!("Welcome to the server, {nickname}!"),
            "unexpected greeting: {welcome}"
        );
        Ok(client)
    }

    async fn send(&mut self, text: &str) -> anyhow::Result<()> {
        self.write_half
            .write_all(format!("{text}\n").as_bytes())
            .await?;
        Ok(())
    }

    /// Reads the next line, failing the test if none arrives in time.
    async fn expect_line(&mut self) -> anyhow::Result<String> {
        let line = timeout(WAIT, self.lines.next_line()).await??;
        line.ok_or_else(|| anyhow::anyhow!("connection closed while awaiting a line"))
    }

    /// Reads the next line, expecting the server to have closed the socket.
    async fn expect_closed(&mut self) -> anyhow::Result<()> {
        // Goodbye notices may still be in flight; keep reading past them.
        while timeout(WAIT, self.lines.next_line()).await??.is_some() {}
        Ok(())
    }

    /// Asserts nothing arrives for a short while.
    async fn expect_silence(&mut self) -> anyhow::Result<()> {
        match timeout(QUIET, self.lines.next_line()).await {
            Err(_elapsed) => Ok(()),
            Ok(line) => anyhow::bail!("expected silence, received {:?}", line?),
        }
    }
}

#[tokio::test]
async fn a_message_reaches_the_other_client_but_not_its_sender() -> anyhow::Result<()> {
    let mut server = TestServer::start().await?;
    let mut alice = Client::connect(server.address, "alice").await?;
    let mut bob = Client::connect(server.address, "bob").await?;

    let joined = alice.expect_line().await?;
    anyhow::ensure!(joined == "bob joined the server", "got: {joined}");

    alice.send("hello everyone").await?;

    let received = bob.expect_line().await?;
    anyhow::ensure!(received == "hello everyone", "got: {received}");
    alice.expect_silence().await?;

    server.stop().await
}

#[tokio::test]
async fn the_welcome_goes_to_the_joiner_and_the_notice_to_everyone_else() -> anyhow::Result<()> {
    let mut server = TestServer::start().await?;

    // `connect` already asserts the joiner receives its own welcome.
    let mut alice = Client::connect(server.address, "alice").await?;
    let bob = Client::connect(server.address, "bob").await?;

    let notice = alice.expect_line().await?;
    anyhow::ensure!(notice == "bob joined the server", "got: {notice}");

    drop(bob);
    server.stop().await
}

#[tokio::test]
async fn nicknames_appear_in_the_join_and_leave_notices() -> anyhow::Result<()> {
    let mut server = TestServer::start().await?;
    let mut alice = Client::connect(server.address, "alice").await?;
    let bob = Client::connect(server.address, "bob_the_builder").await?;

    let joined = alice.expect_line().await?;
    anyhow::ensure!(joined == "bob_the_builder joined the server", "got: {joined}");

    drop(bob);

    let left = alice.expect_line().await?;
    anyhow::ensure!(left == "bob_the_builder left the server", "got: {left}");

    server.stop().await
}

#[tokio::test]
async fn an_empty_nickname_is_rejected_and_reprompted() -> anyhow::Result<()> {
    let mut server = TestServer::start().await?;
    let mut client = Client::dial(server.address).await?;

    let prompt = client.expect_line().await?;
    anyhow::ensure!(prompt == PROMPT, "got: {prompt}");

    client.send("   ").await?;

    let rejection = client.expect_line().await?;
    anyhow::ensure!(rejection.contains("cannot be empty"), "got: {rejection}");
    let reprompt = client.expect_line().await?;
    anyhow::ensure!(reprompt == PROMPT, "got: {reprompt}");

    // A valid nickname on the second attempt is accepted.
    client.send("alice").await?;
    let welcome = client.expect_line().await?;
    anyhow::ensure!(welcome == "Welcome to the server, alice!", "got: {welcome}");

    server.stop().await
}

#[tokio::test]
async fn an_over_long_nickname_is_rejected_and_reprompted() -> anyhow::Result<()> {
    let mut server = TestServer::start().await?;
    let mut client = Client::dial(server.address).await?;

    let prompt = client.expect_line().await?;
    anyhow::ensure!(prompt == PROMPT, "got: {prompt}");

    client.send(&"n".repeat(33)).await?;

    let rejection = client.expect_line().await?;
    anyhow::ensure!(rejection.contains("at most 32"), "got: {rejection}");
    let reprompt = client.expect_line().await?;
    anyhow::ensure!(reprompt == PROMPT, "got: {reprompt}");

    server.stop().await
}

#[tokio::test]
async fn a_client_that_leaves_before_naming_itself_is_never_announced() -> anyhow::Result<()> {
    let mut server = TestServer::start().await?;
    let mut alice = Client::connect(server.address, "alice").await?;

    let shy = Client::dial(server.address).await?;
    drop(shy);

    // Alice hears nothing about a client that never got a nickname.
    alice.expect_silence().await?;

    server.stop().await
}

#[tokio::test]
async fn many_clients_all_receive_a_broadcast() -> anyhow::Result<()> {
    let mut server = TestServer::start().await?;
    let mut speaker = Client::connect(server.address, "speaker").await?;

    let mut listeners = Vec::new();
    for index in 0..5 {
        listeners.push(Client::connect(server.address, &format!("listener{index}")).await?);
    }

    // Drain the join notices the speaker accumulated.
    for _ in 0..listeners.len() {
        let _ = speaker.expect_line().await?;
    }

    speaker.send("broadcast to all").await?;

    for listener in &mut listeners {
        // Each listener sees join notices for those who arrived after it,
        // then the broadcast itself.
        loop {
            let line = listener.expect_line().await?;
            if line == "broadcast to all" {
                break;
            }
            anyhow::ensure!(line.contains("joined the server"), "got: {line}");
        }
    }

    server.stop().await
}

#[tokio::test]
async fn blank_lines_are_not_broadcast() -> anyhow::Result<()> {
    let mut server = TestServer::start().await?;
    let mut alice = Client::connect(server.address, "alice").await?;
    let mut bob = Client::connect(server.address, "bob").await?;

    let _ = alice.expect_line().await?;

    alice.send("").await?;
    bob.expect_silence().await?;

    alice.send("still here").await?;
    let received = bob.expect_line().await?;
    anyhow::ensure!(received == "still here", "got: {received}");

    server.stop().await
}

#[tokio::test]
async fn carriage_returns_are_stripped_from_messages() -> anyhow::Result<()> {
    let mut server = TestServer::start().await?;
    let mut alice = Client::connect(server.address, "alice").await?;
    let mut bob = Client::connect(server.address, "bob").await?;

    let _ = alice.expect_line().await?;

    // What a telnet client sends.
    alice.write_half.write_all(b"from telnet\r\n").await?;

    let received = bob.expect_line().await?;
    anyhow::ensure!(received == "from telnet", "got: {received}");

    server.stop().await
}

#[tokio::test]
async fn shutting_down_closes_every_client_connection() -> anyhow::Result<()> {
    let mut server = TestServer::start().await?;
    let mut alice = Client::connect(server.address, "alice").await?;
    let mut bob = Client::connect(server.address, "bob").await?;

    let _ = alice.expect_line().await?;

    server.stop().await?;

    alice.expect_closed().await?;
    bob.expect_closed().await?;
    Ok(())
}

#[tokio::test]
async fn an_over_long_line_disconnects_only_that_client() -> anyhow::Result<()> {
    let mut server = TestServer::start().await?;
    let mut alice = Client::connect(server.address, "alice").await?;
    let mut flooder = Client::connect(server.address, "flooder").await?;

    let joined = alice.expect_line().await?;
    anyhow::ensure!(joined == "flooder joined the server", "got: {joined}");

    // Well past MAX_LINE_BYTES, with no newline anywhere.
    let flood = vec![b'x'; 16 * 1024];
    let _ = flooder.write_half.write_all(&flood).await;

    // The flooder is cut off...
    flooder.expect_closed().await?;

    // ...and Alice is still served.
    let left = alice.expect_line().await?;
    anyhow::ensure!(left == "flooder left the server", "got: {left}");

    alice.send("still working").await?;
    server.stop().await
}
