# chat-server

A real-time TCP chat server written in Rust with Tokio. The point of this
project is to **learn async Rust in depth**, not just to end up with a
working chat app.

## Learning goal

Master the core concepts of async Rust:
- tasks (`tokio::spawn`)
- channels (`mpsc`, `broadcast`)
- shared state across concurrent connections (`Arc` + an async lock)
- connection/disconnection handling

## How I want you to help

This is a learning project driven by David, not a task to solve end to end.

- **Don't implement the core logic (accepting connections, spawning tasks,
  broadcasting, shared-state handling) unless explicitly asked.** David
  wants to write that himself to actually internalize the concepts.
- When he's stuck, prioritize: explaining the concept/error, pointing to
  the relevant file/line, suggesting an approach — and let him write the
  code. Only write code directly if explicitly asked ("write it for me",
  "just fix it").
- Welcome to help with: reviewing code already written, explaining
  compiler/borrow-checker errors, answering conceptual questions about
  Tokio, suggesting which std/tokio API to use, and spotting concurrency
  bugs (deadlocks, race conditions, channels closed incorrectly).

## Base functionality (core)

- TCP server (`tokio::net::TcpListener`) accepting multiple simultaneous
  clients.
- Each client handled in its own task, without blocking the others.
- Messages from one client are broadcast to all other connected clients.
- Clean disconnect handling: when someone leaves, the rest keep working
  and, ideally, get notified.

## Expected architecture (high level)

- `TcpListener` loop accepting connections; each incoming connection is
  handed to a new task via `tokio::spawn`.
- Read/write split per socket (`TcpStream::into_split()` or similar) so a
  client can be read from while broadcasts are written to it concurrently.
- A `broadcast` channel (or a registry of per-client `mpsc` senders) to
  distribute messages between tasks.
- Shared state (list of connected clients) protected with `Arc` +
  `tokio::sync::Mutex` or `RwLock`.

## Possible extensions (after core works)

- Nicknames/usernames
- Rooms or separate channels
- Commands: `/list`, `/nick`, `/quit`
- Message history for clients who connect late

## Test client

No custom client — use `telnet` or `netcat` (`nc localhost <port>`) to
connect and test manually during development.

## Project status

Initial scaffold (`cargo new`), no dependencies added yet, no logic
implemented. User is about to add dependencies (tokio) and make the
initial commit.
