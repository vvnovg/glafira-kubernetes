//! Мок API-сервера для тестов: `kube::Client` поверх `tower_test`, ответы — функцией от запроса.

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
            let (code, json) = handler(&s);
            seen.push(s);
            send.send_response(
                Response::builder()
                    .status(code)
                    .body(Body::from(serde_json::to_vec(&json).unwrap()))
                    .unwrap(),
            );
        }
        seen
    });
    (kube::Client::new(svc, "default"), task)
}

/// Ответ API-сервера об ошибке (`kind: Status`).
pub fn status(code: u16, reason: &str, message: &str) -> (u16, serde_json::Value) {
    (
        code,
        serde_json::json!({"kind": "Status", "apiVersion": "v1", "metadata": {}, "status": "Failure",
            "message": message, "reason": reason, "code": code}),
    )
}
