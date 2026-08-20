use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use tokio::{io::AsyncWriteExt, net::tcp::OwnedWriteHalf, sync::Mutex};

#[derive(Debug, Clone)]
pub struct ServerState {
    clients: Arc<Mutex<HashMap<SocketAddr, OwnedWriteHalf>>>,
}

impl ServerState {
    pub fn new() -> Self {
        Self {
            clients: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn add_client(&self, new_write_half: OwnedWriteHalf, address: SocketAddr) {
        let clients = Arc::clone(&self.clients);
        clients.lock().await.insert(address, new_write_half);
    }

    pub async fn write_to_clients(&self, message: &[u8], client_address: SocketAddr) {
        let clients = Arc::clone(&self.clients);
        for (_, client) in clients
            .lock()
            .await
            .iter_mut()
            .filter(|(addr, _)| **addr != client_address)
        {
            let _ = client.write_all(message).await;
        }
    }

    pub async fn write_to_all_clients(&self, message: &[u8]) {
        let clients = Arc::clone(&self.clients);
        for (_, client) in clients.lock().await.iter_mut() {
            let _ = client.write_all(message).await;
        }
    }

    pub async fn remove_client(&self, address: SocketAddr) {
        let clients = Arc::clone(&self.clients);
        clients.lock().await.remove(&address);
    }
}
