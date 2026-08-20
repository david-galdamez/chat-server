use std::net::SocketAddr;

use tokio::{io::AsyncReadExt, net::tcp::OwnedReadHalf, sync::oneshot};

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
    mut shutdown_rx: oneshot::Receiver<()>,
) -> Result<(), HandlerError> {
    let mut buffer = [0u8; 1024];
    loop {
        tokio::select! {
            result = read_half.read(&mut buffer) => {
                let result = result?;
                if result == 0 {
                    return Ok(());
                }

                state
                    .write_to_clients(buffer.get(..result).unwrap_or_default(), Some(address))
                    .await;
            }
            _ = &mut shutdown_rx => {
                return Ok(());
            }
        }
    }
}
