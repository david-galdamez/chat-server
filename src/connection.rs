use tokio::net::TcpStream;

use crate::state::ServerState;

pub async fn handle_connection(mut socket: TcpStream, state: ServerState) {}
