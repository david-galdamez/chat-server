use tokio::net::TcpListener;

use crate::{
    connection::{HandlerError, handle_and_read_connection},
    state::ServerState,
};

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("Connection error: {0}")]
    ConnectionError(#[from] std::io::Error),
    #[error("Handler error: {0}")]
    HandlerError(#[from] HandlerError),
    // #[error("State error: {0}")]
    // StateError(),
}

pub async fn server(listener: TcpListener) -> Result<(), ServerError> {
    let state = ServerState::new();

    loop {
        let (socket, address) = listener.accept().await?;
        let (read_half, write_half) = socket.into_split();
        state.add_client(write_half, address).await;
        let welcome_message = format!("Welcome to the server, client {address}\n");

        state
            .write_to_clients(welcome_message.as_bytes(), None)
            .await;

        let client_state = state.clone();
        tokio::spawn(async move {
            match handle_and_read_connection(read_half, &client_state, address).await {
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
