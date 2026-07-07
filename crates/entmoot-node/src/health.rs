//! Small Kubernetes-oriented health endpoint.
//!
//! `/healthz` means the process event loop is alive. `/readyz` means the
//! broker reached the state where Zenoh is open and MQTT listeners were bound;
//! future drain mode can make this return 503 before shutdown.

use crate::Broker;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::warn;

pub async fn serve(listener: TcpListener, broker: Arc<Broker>) {
    loop {
        let (mut stream, _) = match listener.accept().await {
            Ok(x) => x,
            Err(e) => {
                warn!("health accept failed: {e}");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let broker = broker.clone();
        tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            let n = match tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf)).await {
                Ok(Ok(n)) => n,
                _ => 0,
            };
            let req = String::from_utf8_lossy(&buf[..n]);
            let path = req
                .lines()
                .next()
                .and_then(|line| line.split_whitespace().nth(1))
                .unwrap_or("/");
            let (status, body) = match path {
                "/healthz" => ("200 OK", "ok\n".to_string()),
                "/readyz" => ("200 OK", format!("ready node={} zid={}\n", broker.cfg.id, broker.session.zid())),
                _ => ("404 Not Found", "not found\n".to_string()),
            };
            let resp = format!(
                "HTTP/1.1 {status}\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(resp.as_bytes()).await;
        });
    }
}
