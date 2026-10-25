//! Приём redirect на `http://127.0.0.1:<порт>/callback`: одна страница с кодом авторизации.

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

use crate::error::{AppError, AppResult, ErrorCode};

/// Что пришло в redirect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Callback {
    Code {
        code: String,
        state: String,
    },
    Denied {
        error: String,
    },
    /// Другой путь (например, `/favicon.ico`) — ждём дальше.
    Other,
}

/// Разбирает первую строку HTTP-запроса: `GET /callback?code=…&state=… HTTP/1.1`.
pub fn parse_request_line(line: &str) -> Callback {
    let mut parts = line.split_whitespace();
    let (Some("GET"), Some(target)) = (parts.next(), parts.next()) else {
        return Callback::Other;
    };
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    if path != "/callback" {
        return Callback::Other;
    }
    let mut code = None;
    let mut state = None;
    let mut error = None;
    for (k, v) in url::form_urlencoded::parse(query.as_bytes()) {
        match k.as_ref() {
            "code" => code = Some(v.into_owned()),
            "state" => state = Some(v.into_owned()),
            "error" => error = Some(v.into_owned()),
            _ => {}
        }
    }
    match (error, code) {
        (Some(error), _) => Callback::Denied { error },
        (None, Some(code)) => Callback::Code {
            code,
            state: state.unwrap_or_default(),
        },
        (None, None) => Callback::Denied {
            error: "missing code".into(),
        },
    }
}

// Страница видна в браузере до возврата в Glafira; текст — по-английски, как у `oc login --web`.
const PAGE: &str = "<!doctype html><html><head><meta charset=\"utf-8\"><title>Glafira</title></head>\
<body style=\"font-family:sans-serif;padding:32px\"><h2>Glafira for Kubernetes</h2>\
<p>Signed in — return to Glafira. You can close this tab.</p></body></html>";

const DENIED_PAGE: &str = "<!doctype html><html><head><meta charset=\"utf-8\"><title>Glafira</title></head>\
<body style=\"font-family:sans-serif;padding:32px\"><h2>Glafira for Kubernetes</h2>\
<p>Sign-in failed — see the Glafira window for details.</p></body></html>";

pub struct CallbackServer {
    listener: TcpListener,
    pub redirect_uri: String,
}

impl CallbackServer {
    /// Слушает случайный порт только на 127.0.0.1.
    pub async fn bind() -> AppResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        Ok(Self {
            listener,
            redirect_uri: format!("http://127.0.0.1:{port}/callback"),
        })
    }

    /// Ждёт redirect с `expected_state` не дольше `limit`; `cancel` прерывает ожидание.
    pub async fn wait(
        self,
        expected_state: &str,
        limit: Duration,
        cancel: oneshot::Receiver<()>,
    ) -> AppResult<String> {
        let accept = async {
            loop {
                let (mut stream, _) = self.listener.accept().await?;
                let mut buf = vec![0u8; 8192];
                let mut len = 0;
                // Достаточно первой строки; ждём конец заголовков, но не больше 8 КБ.
                while len < buf.len() {
                    let n = stream.read(&mut buf[len..]).await?;
                    if n == 0 {
                        break;
                    }
                    len += n;
                    if buf[..len].windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
                let head = String::from_utf8_lossy(&buf[..len]);
                let line = head.lines().next().unwrap_or("");
                let (status, page, result) = match parse_request_line(line) {
                    Callback::Other => ("404 Not Found", "", None),
                    Callback::Denied { error } => (
                        "200 OK",
                        DENIED_PAGE,
                        Some(Err(app_err!(
                            ErrorCode::AuthFailed,
                            "errors.oauth.denied",
                            "Sign-in was not completed: {{error}}",
                            error = error
                        ))),
                    ),
                    Callback::Code { state, .. } if state != expected_state => (
                        "200 OK",
                        DENIED_PAGE,
                        Some(Err(app_err!(
                            ErrorCode::AuthFailed,
                            "errors.oauth.stateMismatch",
                            "The sign-in response does not match the request — try again"
                        ))),
                    ),
                    Callback::Code { code, .. } => ("200 OK", PAGE, Some(Ok(code))),
                };
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{page}",
                    page.len()
                );
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.shutdown().await;
                if let Some(r) = result {
                    return r;
                }
            }
        };
        tokio::select! {
            r = accept => r,
            _ = cancel => Err(cancelled()),
            _ = tokio::time::sleep(limit) => Err(app_err!(
                ErrorCode::Timeout,
                "errors.oauth.timeout",
                "Sign-in in the browser was not completed in time"
            )),
        }
    }
}

pub fn cancelled() -> AppError {
    app_err!(
        ErrorCode::AuthFailed,
        "errors.oauth.cancelled",
        "Sign-in was cancelled"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_lines() {
        assert_eq!(
            parse_request_line("GET /callback?code=abc%2F1&state=s1 HTTP/1.1"),
            Callback::Code {
                code: "abc/1".into(),
                state: "s1".into()
            }
        );
        assert_eq!(
            parse_request_line("GET /callback?error=access_denied&state=s HTTP/1.1"),
            Callback::Denied {
                error: "access_denied".into()
            }
        );
        assert_eq!(
            parse_request_line("GET /favicon.ico HTTP/1.1"),
            Callback::Other
        );
        assert_eq!(
            parse_request_line("POST /callback HTTP/1.1"),
            Callback::Other
        );
        assert_eq!(
            parse_request_line("GET /callback HTTP/1.1"),
            Callback::Denied {
                error: "missing code".into()
            }
        );
    }

    async fn get(url: &str) -> (u16, String) {
        crate::tls::ensure_crypto_provider();
        let r = reqwest::Client::new().get(url).send().await.unwrap();
        (r.status().as_u16(), r.text().await.unwrap())
    }

    #[tokio::test]
    async fn returns_code_after_ignoring_other_paths() {
        let server = CallbackServer::bind().await.unwrap();
        let base = server.redirect_uri.replace("/callback", "");
        assert!(server.redirect_uri.starts_with("http://127.0.0.1:"));
        let (_tx, rx) = oneshot::channel();
        let wait =
            tokio::spawn(async move { server.wait("s1", Duration::from_secs(10), rx).await });
        let (code, _) = get(&format!("{base}/favicon.ico")).await;
        assert_eq!(code, 404);
        let (code, page) = get(&format!("{base}/callback?code=c-1&state=s1")).await;
        assert_eq!(code, 200);
        assert!(page.contains("Signed in"));
        assert_eq!(wait.await.unwrap().unwrap(), "c-1");
    }

    #[tokio::test]
    async fn wrong_state_and_denial_are_errors() {
        for (query, key) in [
            ("code=c&state=other", "errors.oauth.stateMismatch"),
            ("error=access_denied", "errors.oauth.denied"),
        ] {
            let server = CallbackServer::bind().await.unwrap();
            let url = format!("{}?{query}", server.redirect_uri);
            let (_tx, rx) = oneshot::channel();
            let wait =
                tokio::spawn(async move { server.wait("s1", Duration::from_secs(10), rx).await });
            let (_, page) = get(&url).await;
            assert!(page.contains("Sign-in failed"));
            assert_eq!(wait.await.unwrap().unwrap_err().key, key);
        }
    }

    #[tokio::test]
    async fn cancel_and_timeout() {
        let server = CallbackServer::bind().await.unwrap();
        let (tx, rx) = oneshot::channel();
        let wait = tokio::spawn(async move { server.wait("s", Duration::from_secs(10), rx).await });
        tx.send(()).unwrap();
        assert_eq!(
            wait.await.unwrap().unwrap_err().key,
            "errors.oauth.cancelled"
        );

        let server = CallbackServer::bind().await.unwrap();
        let (_tx, rx) = oneshot::channel();
        let e = server
            .wait("s", Duration::from_millis(50), rx)
            .await
            .unwrap_err();
        assert_eq!(e.key, "errors.oauth.timeout");
    }
}
