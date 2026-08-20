use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use tokio::sync::{Mutex, mpsc::UnboundedSender};

#[derive(Debug, Clone)]
pub struct ServerState {
    clients: Arc<Mutex<HashMap<SocketAddr, UnboundedSender<Vec<u8>>>>>,
}

impl ServerState {
    pub fn new() -> Self {
        Self {
            clients: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn add_client(&self, sender: UnboundedSender<Vec<u8>>, address: SocketAddr) {
        self.clients.lock().await.insert(address, sender);
    }

    pub async fn write_to_clients(&self, message: &[u8], exclude: Option<SocketAddr>) {
        let clients = self.clients.lock().await.clone();
        for (_, client) in clients.iter().filter(|(addr, _)| exclude != Some(**addr)) {
            let _ = client.send(message.to_vec());
        }
    }

    pub async fn remove_client(&self, address: SocketAddr) {
        self.clients.lock().await.remove(&address);
    }
}
