use chat_server::server;
use tokio::net::TcpListener;

const DEFAULT_ADDRESS: &str = "127.0.0.1:5000";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind(DEFAULT_ADDRESS).await?;
    println!("Server listening on {}", listener.local_addr()?);

    server::run(listener, shutdown_signal()).await;

    println!("Server stopped");
    Ok(())
}

/// Resolves when the process is asked to terminate.
async fn shutdown_signal() {
    match tokio::signal::ctrl_c().await {
        Ok(()) => println!("Received Ctrl-C, shutting down"),
        Err(e) => eprintln!("Failed to listen for Ctrl-C: {e}"),
    }
}
