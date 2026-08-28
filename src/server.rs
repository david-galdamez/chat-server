use std::future::Future;

use tokio::{net::TcpListener, sync::broadcast, task::JoinSet};

use crate::{connection::handle_connection, state::ServerState};

/// Accepts connections until `shutdown` resolves, then waits for every client
/// task to finish before returning.
pub async fn run<F>(listener: TcpListener, shutdown: F)
where
    F: Future<Output = ()> + Send,
{
    let state = ServerState::new();
    // Every connection subscribes; dropping the sender tells them all to stop.
    let (shutdown_tx, _) = broadcast::channel::<()>(1);
    let mut connections = JoinSet::new();

    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            incoming = listener.accept() => {
                match incoming {
                    Ok((socket, address)) => {
                        println!("Accepted connection from {address}");
                        connections.spawn(handle_connection(
                            socket,
                            address,
                            state.clone(),
                            shutdown_tx.subscribe(),
                        ));
                    }
                    // One failed accept should not take the server down.
                    Err(e) => eprintln!("Error accepting connection: {e}"),
                }
            }
            () = &mut shutdown => break,
        }
    }

    println!("Shutting down, closing {} connection(s)", connections.len());

    // Closes every subscriber's receiver, waking each connection task.
    drop(shutdown_tx);

    while connections.join_next().await.is_some() {}
}
