use std::net::SocketAddr;

use tokio::{io::AsyncReadExt, net::tcp::OwnedReadHalf};

use crate::state::ServerState;

#[derive(Debug, thiserror::Error)]
pub enum HandlerError {
    #[error("Error reading message bytes: {0}")]
    ReadingError(#[from] std::io::Error),
}

pub async fn handle_and_read_connection(
    mut read_half: OwnedReadHalf,
    state: &ServerState,
    address: SocketAddr,
) -> Result<(), HandlerError> {
    let mut buffer = [0u8; 1024];
    loop {
        let n = read_half.read(&mut buffer).await?;
        if n == 0 {
            return Ok(());
        }

        state
            .write_to_clients(buffer.get(..n).unwrap_or_default(), Some(address))
            .await;
    }
}
