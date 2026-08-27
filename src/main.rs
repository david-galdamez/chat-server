use tokio::net::TcpListener;

mod connection;
mod server;
mod state;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:5000").await?;
    println!("Server listening on {}", listener.local_addr()?);
    server::server(listener).await?;

    Ok(())
}
