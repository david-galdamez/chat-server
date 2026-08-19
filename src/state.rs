use std::sync::Arc;

use tokio::{io::AsyncWriteExt, net::TcpStream, sync::Mutex};

#[derive(Debug, Clone)]
pub struct ServerState {
    clients: Arc<Mutex<Vec<TcpStream>>>,
}

impl ServerState {
    pub fn new() -> Self {
        Self {
            clients: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub async fn add_client(&mut self, new_client: TcpStream) {
        let clients = Arc::clone(&self.clients);
        clients.lock().await.push(new_client);
    }

    pub async fn write_to_all_clients(&mut self, message: &[u8], bytes: usize) {
        let clients = Arc::clone(&self.clients);
        for client in clients.lock().await.iter_mut() {
            let _ = client.write_all(&message[0..bytes]).await;
        }
    }
}
