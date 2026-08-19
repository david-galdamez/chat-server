use tokio::net::TcpListener;

mod connection;
mod server;
mod state;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:5000").await?;
    let mut clients = state::ServerState::new();

    server::server(listener, &mut clients).await?;

    Ok(())
}
