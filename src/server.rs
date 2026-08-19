use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

use crate::state::ServerState;

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("Connection error: {0}")]
    ConnectionError(#[from] std::io::Error),
    // #[error("State error: {0}")]
    // StateError(),
}

pub async fn server(listener: TcpListener, clients: &mut ServerState) -> Result<(), ServerError> {
    println!("Server listening on {}", listener.local_addr()?);
    loop {
        let (mut socket, _) = listener.accept().await?;

        clients.add_client(socket).await;

        tokio::spawn(async move {
            let mut buf = [0; 1024];

            loop {
                let n = match socket.read(&mut buf).await {
                    Ok(n) if n == 0 => return, // Connection closed
                    Ok(n) => n,
                    Err(e) => {
                        eprintln!("Failed to read from socket: {:?}", e);
                        return;
                    }
                };

                clients.write_to_all_clients(&buf, n).await;
            }
        });
    }
}
