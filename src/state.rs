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
        self.clients.lock().await.insert(address, new_write_half);
    }

    pub async fn write_to_clients(&self, message: &[u8], exclude: Option<SocketAddr>) {
        for (_, client) in self.clients.lock().await.iter_mut().filter(|(addr, _)| {
            if let Some(client_addr) = exclude {
                client_addr != **addr
            } else {
                true
            }
        }) {
            let _ = client.write_all(message).await;
        }
    }

    pub async fn remove_client(&self, address: SocketAddr) {
        self.clients.lock().await.remove(&address);
    }
}
