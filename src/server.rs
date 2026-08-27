use tokio::{io::AsyncWriteExt, net::TcpListener};

use crate::{connection::handle_and_read_connection, state::ServerState};

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("Connection error: {0}")]
    ConnectionError(#[from] std::io::Error),
}

pub async fn server(listener: TcpListener) -> Result<(), ServerError> {
    let state = ServerState::new();

    loop {
        let (socket, address) = match listener.accept().await {
            Ok(res) => res,
            Err(e) => {
                eprintln!("Error accepting connection: {e}");
                continue;
            }
        };

        let (read_half, mut write_half) = socket.into_split();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        state.add_client(tx, address).await;
        let remove_state = state.clone();

        tokio::spawn(async move {
            while let Some(message) = rx.recv().await {
                if let Err(e) = write_half.write_all(&message).await {
                    eprintln!("Error writing to client {address}: {e}");
                    remove_state.remove_client(address).await;
                    let _ = shutdown_tx.send(());
                    break;
                }
            }
        });

        let client_state = state.clone();
        tokio::spawn(async move {
            let welcome_message = format!("Welcome to the server, client {address}\n");
            client_state
                .write_to_clients(welcome_message.as_bytes(), None)
                .await;

            match handle_and_read_connection(read_half, &client_state, address, shutdown_rx).await {
                Ok(()) => {
                    println!("Client {address} disconnected");
                }
                Err(e) => {
                    eprintln!("Error handling connection for client {address}: {e}");
                }
            }

            let disconnect_message = format!("Client {address} disconnected\n");
            client_state.remove_client(address).await;
            client_state
                .write_to_clients(disconnect_message.as_bytes(), None)
                .await;
        });
    }
}
