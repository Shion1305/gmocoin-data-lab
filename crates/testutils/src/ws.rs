//! Deterministic fake GMO Coin WebSocket server for integration tests.

use std::sync::Arc;

use anyhow::Result;
use futures::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::{
    net::TcpListener,
    sync::{broadcast, oneshot},
};
use tokio_tungstenite::{accept_async, tungstenite::Message};
use url::Url;

/// Test WebSocket server that broadcasts pre-baked GMO shaped messages.
pub struct FakeWsServer {
    url: Url,
    sender: broadcast::Sender<Value>,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl FakeWsServer {
    /// Spawns a new fake server bound to localhost, replaying the provided messages.
    pub async fn spawn(initial_messages: Vec<Value>) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let url = Url::parse(&format!("ws://{}", addr))?;

        let initial = Arc::new(initial_messages);
        let (sender, _) = broadcast::channel::<Value>(1024);
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();

        let sender_clone = sender.clone();
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => break,
                    accept = listener.accept() => {
                        match accept {
                            Ok((stream, _peer)) => {
                                let sender = sender_clone.clone();
                                let initial = Arc::clone(&initial);
                                tokio::spawn(async move {
                                    if let Err(err) = handle_connection(stream, sender, initial).await {
                                        tracing::warn!(error = %err, "fake ws connection closed with error");
                                    }
                                });
                            }
                            Err(err) => {
                                tracing::error!(error = %err, "fake ws accept failed");
                                break;
                            }
                        }
                    }
                }
            }
        });

        // Emit the initial messages once all handlers have subscribed by broadcasting.
        Ok(Self {
            url,
            sender,
            shutdown: Some(shutdown_tx),
            task: Some(task),
        })
    }

    /// Returns the websocket URL clients should connect to.
    pub fn url(&self) -> Url {
        self.url.clone()
    }

    /// Broadcasts a message to all connected clients.
    pub fn send(&self, message: Value) -> Result<()> {
        self.sender
            .send(message)
            .map(|_| ())
            .map_err(|err| anyhow::anyhow!(err))
    }

    /// Gracefully shuts the server down.
    pub async fn shutdown(mut self) -> Result<()> {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
        Ok(())
    }
}

impl Drop for FakeWsServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

async fn handle_connection(
    stream: tokio::net::TcpStream,
    sender: broadcast::Sender<Value>,
    initial: Arc<Vec<Value>>,
) -> Result<()> {
    let mut ws = accept_async(stream).await?;
    let mut rx = sender.subscribe();

    // Send initial ack after receiving the first subscription request.
    if let Some(Ok(Message::Text(_subscription))) = ws.next().await {
        let ack = json!({
            "channel": "status",
            "data": { "status": 0, "msg": "ok" }
        });
        ws.send(Message::Text(ack.to_string())).await?;
    }

    // Replay initial messages to the newly connected consumer.
    for msg in initial.iter() {
        ws.send(Message::Text(msg.to_string())).await?;
    }

    loop {
        tokio::select! {
            incoming = ws.next() => match incoming {
                Some(Ok(Message::Ping(payload))) => {
                    ws.send(Message::Pong(payload)).await?;
                }
                Some(Ok(Message::Pong(_))) => {
                    // no-op
                }
                Some(Ok(Message::Close(_))) | None => break,
                Some(Ok(Message::Text(_))) => {
                    // Ignore subsequent client messages.
                }
                Some(Ok(Message::Binary(_))) => {},
                Some(Ok(Message::Frame(_))) => {},
                Some(Err(err)) => {
                    tracing::warn!(error = %err, "fake ws client disconnected");
                    break;
                }
            },
            result = rx.recv() => {
                match result {
                    Ok(msg) => {
                        if ws.send(Message::Text(msg.to_string())).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }

    Ok(())
}
