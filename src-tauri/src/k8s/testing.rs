//! Мок API-сервера для тестов: `kube::Client` поверх `tower_test`, ответы — функцией от запроса;
//! `serve` — то же на локальном сокете, для клиента из профиля.

use http::{Request, Response};
use http_body_util::BodyExt;
use kube::client::Body;

/// Запрос, который увидел мок.
#[derive(Debug, Clone)]
pub struct Seen {
    pub method: http::Method,
    /// Путь вместе со строкой запроса: `/api/v1/pods?&limit=500`.
    pub uri: String,
    pub body: serde_json::Value,
}

impl Seen {
    pub fn path(&self) -> &str {
        self.uri.split('?').next().unwrap_or("")
    }
}

/// Клиент, все запросы которого обрабатывает `handler` (код ответа и JSON). Задача завершается,
/// когда клиент удалён, и возвращает список увиденных запросов.
pub fn mock_client(
    handler: impl Fn(&Seen) -> (u16, serde_json::Value) + Send + 'static,
) -> (kube::Client, tokio::task::JoinHandle<Vec<Seen>>) {
    mock_raw(move |s| {
        let (code, json) = handler(s);
        (code, serde_json::to_vec(&json).unwrap())
    })
}

/// Как `mock_client`, но тело ответа — байты как есть (текст `/readyz`, строки лога, события
/// watch по строке на событие).
pub fn mock_raw(
    handler: impl Fn(&Seen) -> (u16, Vec<u8>) + Send + 'static,
) -> (kube::Client, tokio::task::JoinHandle<Vec<Seen>>) {
    let (svc, mut handle) = tower_test::mock::pair::<Request<Body>, Response<Body>>();
    let task = tokio::spawn(async move {
        let mut seen = Vec::new();
        while let Some((req, send)) = handle.next_request().await {
            let method = req.method().clone();
            let uri = req
                .uri()
                .path_and_query()
                .map(|p| p.to_string())
                .unwrap_or_default();
            let bytes = req.into_body().collect().await.unwrap().to_bytes();
            let body = if bytes.is_empty() {
                serde_json::Value::Null
            } else {
                serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null)
            };
            let s = Seen { method, uri, body };
            let (code, bytes) = handler(&s);
            seen.push(s);
            send.send_response(
                Response::builder()
                    .status(code)
                    .body(Body::from(bytes))
                    .unwrap(),
            );
        }
        seen
    });
    (kube::Client::new(svc, "default"), task)
}

/// Ответ `serve`: тело целиком или поток, который держится открытым, пока не отменят `close`
/// (watch, follow лога).
pub enum Reply {
    Full(u16, Vec<u8>),
    Held(Vec<u8>, tokio_util::sync::CancellationToken),
}

/// API-сервер на 127.0.0.1 без TLS — для тестов, которым нужен клиент из профиля
/// (`AppState::connected` с exec-плагином). `handler` получает запрос и токен из заголовка
/// `Authorization`; каждый ответ закрывает соединение. Возвращает порт.
pub async fn serve(handler: impl Fn(&Seen, Option<&str>) -> Reply + Send + Sync + 'static) -> u16 {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let handler = std::sync::Arc::new(handler);
    tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            let handler = handler.clone();
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                let mut read = async |buf: &mut Vec<u8>| match sock.read(&mut chunk).await {
                    Ok(0) | Err(_) => false,
                    Ok(n) => {
                        buf.extend_from_slice(&chunk[..n]);
                        true
                    }
                };
                let head_end = loop {
                    if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                        break i + 4;
                    }
                    if !read(&mut buf).await {
                        return;
                    }
                };
                let head = String::from_utf8_lossy(&buf[..head_end]).into_owned();
                let header = |name: &str| {
                    head.split("\r\n").skip(1).find_map(|l| {
                        let (k, v) = l.split_once(':')?;
                        k.eq_ignore_ascii_case(name).then(|| v.trim().to_string())
                    })
                };
                let length: usize = header("content-length")
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0);
                while buf.len() < head_end + length {
                    if !read(&mut buf).await {
                        return;
                    }
                }
                let mut first = head.split(' ');
                let method = first.next().unwrap_or("GET").parse().unwrap();
                let uri = first.next().unwrap_or("/").to_string();
                let body = serde_json::from_slice(&buf[head_end..head_end + length])
                    .unwrap_or(serde_json::Value::Null);
                let token = header("authorization")
                    .and_then(|v| v.strip_prefix("Bearer ").map(str::to_string));
                let seen = Seen { method, uri, body };
                match handler(&seen, token.as_deref()) {
                    Reply::Full(code, bytes) => {
                        let head = format!(
                            "HTTP/1.1 {code} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                            bytes.len()
                        );
                        let _ = sock.write_all(&[head.into_bytes(), bytes].concat()).await;
                    }
                    Reply::Held(bytes, close) => {
                        let mut out = b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ntransfer-encoding: chunked\r\nconnection: close\r\n\r\n".to_vec();
                        if !bytes.is_empty() {
                            out.extend_from_slice(format!("{:x}\r\n", bytes.len()).as_bytes());
                            out.extend_from_slice(&bytes);
                            out.extend_from_slice(b"\r\n");
                        }
                        let _ = sock.write_all(&out).await;
                        close.cancelled().await;
                        let _ = sock.write_all(b"0\r\n\r\n").await;
                    }
                }
                let _ = sock.shutdown().await;
            });
        }
    });
    port
}

/// Ответ API-сервера об ошибке (`kind: Status`).
pub fn status(code: u16, reason: &str, message: &str) -> (u16, serde_json::Value) {
    (
        code,
        serde_json::json!({"kind": "Status", "apiVersion": "v1", "metadata": {}, "status": "Failure",
            "message": message, "reason": reason, "code": code}),
    )
}
