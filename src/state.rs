use std::{
    collections::HashMap,
    fmt,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use tokio::sync::{
    Mutex,
    mpsc::{Sender, error::TrySendError},
};

use crate::message::ServerMessage;

/// Hands out a fresh id for every connection this process accepts.
static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);

/// A stable, unique handle for a connected client.
///
/// Ids are used instead of socket addresses because the OS recycles ephemeral
/// ports: a new client can be handed the address a departing one just released,
/// so an address is not a safe key for the registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ClientId(u64);

impl ClientId {
    /// Allocates the next unused id.
    #[must_use]
    pub fn next() -> Self {
        // Relaxed is enough: we only need each caller to get a distinct value,
        // not any ordering against other memory.
        Self(NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed))
    }
}

impl fmt::Display for ClientId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "#{}", self.0)
    }
}

/// Everything the server knows about one connected client.
#[derive(Debug, Clone)]
pub struct Client {
    id: ClientId,
    address: SocketAddr,
    nickname: String,
    sender: Sender<ServerMessage>,
}

impl Client {
    #[must_use]
    pub const fn new(
        id: ClientId,
        address: SocketAddr,
        nickname: String,
        sender: Sender<ServerMessage>,
    ) -> Self {
        Self {
            id,
            address,
            nickname,
            sender,
        }
    }

    #[must_use]
    pub const fn id(&self) -> ClientId {
        self.id
    }

    #[must_use]
    pub const fn address(&self) -> SocketAddr {
        self.address
    }

    /// The name other clients see.
    #[must_use]
    pub fn nickname(&self) -> &str {
        &self.nickname
    }
}

/// The registry of connected clients, shared across every connection task.
///
/// Cloning is cheap: every clone points at the same underlying map.
#[derive(Debug, Clone, Default)]
pub struct ServerState {
    clients: Arc<Mutex<HashMap<ClientId, Client>>>,
}

impl ServerState {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn add_client(&self, client: Client) {
        self.clients.lock().await.insert(client.id, client);
    }

    pub async fn remove_client(&self, id: ClientId) {
        self.clients.lock().await.remove(&id);
    }

    pub async fn client_count(&self) -> usize {
        self.clients.lock().await.len()
    }

    /// Every connected client, oldest first.
    pub async fn clients(&self) -> Vec<Client> {
        let mut clients: Vec<Client> = self.clients.lock().await.values().cloned().collect();
        clients.sort_unstable_by_key(Client::id);
        clients
    }

    /// Queues `message` for every connected client except `exclude`.
    pub async fn broadcast(&self, message: &ServerMessage, exclude: Option<ClientId>) {
        // Clone the registry so the lock is never held across a `try_send`,
        // and never held while `remove_client` takes it again below.
        let clients = self.clients.lock().await.clone();

        let mut closed = Vec::new();
        for client in clients.values().filter(|client| exclude != Some(client.id)) {
            if !deliver(client, message) {
                closed.push(client.id);
            }
        }

        for id in closed {
            self.remove_client(id).await;
        }
    }

    /// Queues `message` for a single client.
    pub async fn send_to(&self, id: ClientId, message: &ServerMessage) {
        let client = self.clients.lock().await.get(&id).cloned();

        match client {
            Some(client) => {
                if !deliver(&client, message) {
                    self.remove_client(id).await;
                }
            }
            None => eprintln!("Client {id} is not connected"),
        }
    }
}

/// Hands `message` to a client's writer task without blocking.
///
/// Returns `false` when the client's channel is closed and the client should be
/// dropped from the registry. A full channel means the client cannot keep up:
/// the message is dropped, but the client stays connected.
fn deliver(client: &Client, message: &ServerMessage) -> bool {
    match client.sender.try_send(message.clone()) {
        Ok(()) => true,
        Err(TrySendError::Full(_)) => {
            eprintln!(
                "Client {} ({}) is too slow to keep up, dropping message",
                client.id, client.nickname
            );
            true
        }
        Err(TrySendError::Closed(_)) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::{Client, ClientId, ServerState, deliver};
    use crate::message::ServerMessage;
    use std::net::SocketAddr;
    use tokio::sync::mpsc::{self, Sender};

    fn address(port: u16) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], port))
    }

    fn client(nickname: &str, sender: Sender<ServerMessage>) -> Client {
        Client::new(ClientId::next(), address(1), nickname.to_owned(), sender)
    }

    #[test]
    fn ids_are_never_handed_out_twice() {
        let first = ClientId::next();
        let second = ClientId::next();
        assert_ne!(first, second);
    }

    #[tokio::test]
    async fn tracks_clients_as_they_come_and_go() {
        let state = ServerState::new();
        let (first_tx, _first_rx) = mpsc::channel(4);
        let (second_tx, _second_rx) = mpsc::channel(4);

        let alice = client("alice", first_tx);
        let bob = client("bob", second_tx);
        state.add_client(alice.clone()).await;
        state.add_client(bob.clone()).await;

        assert_eq!(state.client_count().await, 2);
        let nicknames: Vec<String> = state
            .clients()
            .await
            .iter()
            .map(|client| client.nickname().to_owned())
            .collect();
        assert_eq!(nicknames, vec!["alice".to_owned(), "bob".to_owned()]);

        state.remove_client(alice.id()).await;
        assert_eq!(state.client_count().await, 1);
    }

    #[tokio::test]
    async fn two_clients_from_the_same_address_stay_separate() {
        let state = ServerState::new();
        let (first_tx, _first_rx) = mpsc::channel(4);
        let (second_tx, _second_rx) = mpsc::channel(4);

        // Same address, as happens when the OS recycles an ephemeral port.
        let old = client("old", first_tx);
        let new = client("new", second_tx);
        state.add_client(old.clone()).await;
        state.add_client(new.clone()).await;

        // Removing the departing client must not evict its replacement.
        state.remove_client(old.id()).await;

        assert_eq!(state.client_count().await, 1);
        let remaining = state.clients().await;
        assert!(matches!(remaining.first(), Some(client) if client.nickname() == "new"));
    }

    #[tokio::test]
    async fn broadcast_skips_the_excluded_client() {
        let state = ServerState::new();
        let (sender, mut receiver) = mpsc::channel(4);
        let (excluded_tx, mut excluded_rx) = mpsc::channel(4);

        state.add_client(client("alice", sender)).await;
        let excluded = client("bob", excluded_tx);
        state.add_client(excluded.clone()).await;

        let message = ServerMessage::chat("alice", "hello");
        state.broadcast(&message, Some(excluded.id())).await;

        assert_eq!(receiver.recv().await, Some(message));
        assert!(excluded_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn send_to_reaches_only_that_client() {
        let state = ServerState::new();
        let (sender, mut receiver) = mpsc::channel(4);
        let (other_tx, mut other_rx) = mpsc::channel(4);

        let alice = client("alice", sender);
        state.add_client(alice.clone()).await;
        state.add_client(client("bob", other_tx)).await;

        let message = ServerMessage::notice("just for you");
        state.send_to(alice.id(), &message).await;

        assert_eq!(receiver.recv().await, Some(message));
        assert!(other_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn broadcast_drops_clients_whose_channel_is_closed() {
        let state = ServerState::new();
        let (sender, receiver) = mpsc::channel(4);
        state.add_client(client("alice", sender)).await;

        // The writer task going away is what closes the channel.
        drop(receiver);
        state.broadcast(&ServerMessage::notice("anyone there?"), None).await;

        assert_eq!(state.client_count().await, 0);
    }

    #[test]
    fn a_full_channel_drops_the_message_but_keeps_the_client() {
        let (sender, _receiver) = mpsc::channel(1);
        let alice = client("alice", sender);

        assert!(deliver(&alice, &ServerMessage::notice("first")));
        // Capacity is 1 and nothing has been received yet.
        assert!(deliver(&alice, &ServerMessage::notice("second")));
    }

    #[test]
    fn a_closed_channel_reports_the_client_as_gone() {
        let (sender, receiver) = mpsc::channel(1);
        drop(receiver);
        assert!(!deliver(
            &client("alice", sender),
            &ServerMessage::notice("anyone?")
        ));
    }
}
