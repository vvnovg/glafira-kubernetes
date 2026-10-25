//! Приём redirect на `http://127.0.0.1:<порт>/callback`: одна страница с кодом авторизации.

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinSet;
use tokio::time::timeout;

use crate::error::{AppError, AppResult, ErrorCode};

/// Что пришло в redirect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Callback {
    Code {
        code: String,
        state: String,
    },
    /// Отказ (`error=…`); `state` — как в запросе авторизации (RFC 6749, 4.1.2.1), пустой, если
    /// его нет.
    Denied {
        error: String,
        state: String,
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
    let state = state.unwrap_or_default();
    match (error, code) {
        (Some(error), _) => Callback::Denied { error, state },
        (None, Some(code)) => Callback::Code { code, state },
        (None, None) => Callback::Denied {
            error: "missing code".into(),
            state,
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

/// Сколько ждать заголовок запроса от одного соединения. Браузеры открывают соединения заранее
/// (preconnect) и ничего по ним не шлют: такое соединение закрываем, остальные это не задерживает.
const HEADER_TIMEOUT: Duration = Duration::from_secs(5);
/// Пауза после сбоя `accept` (например, нехватка дескрипторов), чтобы не крутиться вхолостую.
const ACCEPT_RETRY_PAUSE: Duration = Duration::from_millis(50);

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

    /// Ждёт redirect с `expected_state` не дольше `limit`: код или отказ (`error=…`) с этим `state`.
    ///
    /// `cancel` прерывает ожидание: и `send(())`, и сброс (drop) отправителя — приёмник `oneshot`
    /// завершается в обоих случаях, поэтому вызывающий держит отправитель, пока вход не закончен.
    ///
    /// Каждое соединение обслуживается отдельной задачей: молчащее или оборванное соединение,
    /// чужой путь и код или отказ с чужим или пропущенным `state` (другая вкладка, стороннее
    /// приложение) вход не прерывают — ожидание продолжается до настоящего redirect, отмены или
    /// `limit`.
    pub async fn wait(
        self,
        expected_state: &str,
        limit: Duration,
        cancel: oneshot::Receiver<()>,
    ) -> AppResult<String> {
        let serve = async {
            // Незавершённые задачи отменяются вместе с `JoinSet`, когда ожидание закончено.
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = self.listener.accept() => match accepted {
                        Ok((stream, _)) => {
                            connections.spawn(serve_connection(
                                stream,
                                expected_state.to_owned(),
                                HEADER_TIMEOUT,
                            ));
                        }
                        // Сброшенное до `accept` соединение — не повод прерывать вход.
                        Err(_) => tokio::time::sleep(ACCEPT_RETRY_PAUSE).await,
                    },
                    Some(finished) = connections.join_next() => {
                        if let Ok(Some(result)) = finished {
                            return result;
                        }
                    }
                }
            }
        };
        tokio::select! {
            r = serve => r,
            _ = cancel => Err(cancelled()),
            _ = tokio::time::sleep(limit) => Err(app_err!(
                ErrorCode::Timeout,
                "errors.oauth.timeout",
                "Sign-in in the browser was not completed in time"
            )),
        }
    }
}

/// Читает заголовок запроса (до пустой строки, не больше 8 КБ) и возвращает его первую строку.
/// Заголовок читается целиком, иначе закрытие сокета с непрочитанными данными сбросит соединение
/// (RST), и браузер не получит страницу.
async fn read_request_line(stream: &mut TcpStream) -> std::io::Result<String> {
    let mut buf = vec![0u8; 8192];
    let mut len = 0;
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
    Ok(head.lines().next().unwrap_or("").to_owned())
}

/// Одно соединение: читает запрос (не дольше `header_timeout`), отвечает страницей и возвращает
/// итог, если запрос завершает вход. `None` — соединение лишнее (молчание, обрыв, ошибка чтения,
/// чужой путь, чужой `state`), ожидание продолжается.
async fn serve_connection(
    mut stream: TcpStream,
    expected_state: String,
    header_timeout: Duration,
) -> Option<AppResult<String>> {
    let line = timeout(header_timeout, read_request_line(&mut stream))
        .await
        .ok()?
        .ok()?;
    if line.is_empty() {
        return None;
    }
    let (status, page, result) = match parse_request_line(&line) {
        Callback::Other => ("404 Not Found", "", None),
        // Ответ не на наш запрос (код или отказ с чужим или пропущенным `state`): показываем
        // страницу ошибки, но вход не прерываем — настоящий redirect ещё может прийти.
        Callback::Code { state, .. } | Callback::Denied { state, .. }
            if state != expected_state =>
        {
            ("200 OK", DENIED_PAGE, None)
        }
        Callback::Denied { error, .. } => (
            "200 OK",
            DENIED_PAGE,
            Some(Err(app_err!(
                ErrorCode::AuthFailed,
                "errors.oauth.denied",
                "Sign-in was not completed: {{error}}",
                error = error
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
    result
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
                error: "access_denied".into(),
                state: "s".into()
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
                error: "missing code".into(),
                state: String::new()
            }
        );
    }

    async fn get(url: &str) -> (u16, String) {
        crate::tls::ensure_crypto_provider();
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let r = client.get(url).send().await.unwrap();
        (r.status().as_u16(), r.text().await.unwrap())
    }

    /// Порт сервера из его `redirect_uri`.
    fn port_of(server: &CallbackServer) -> u16 {
        url::Url::parse(&server.redirect_uri)
            .unwrap()
            .port()
            .unwrap()
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
    async fn silent_connection_does_not_block_the_callback() {
        let server = CallbackServer::bind().await.unwrap();
        let url = format!("{}?code=c-1&state=s1", server.redirect_uri);
        // Браузер открыл соединение заранее (preconnect) и ничего не шлёт.
        let _silent = tokio::net::TcpStream::connect(("127.0.0.1", port_of(&server)))
            .await
            .unwrap();
        let started = std::time::Instant::now();
        let (_tx, rx) = oneshot::channel();
        let wait = tokio::spawn(async move { server.wait("s1", Duration::from_secs(3), rx).await });
        let request = tokio::spawn(async move { get(&url).await });
        assert_eq!(wait.await.unwrap().unwrap(), "c-1");
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "ответ ждал молчащее соединение: {:?}",
            started.elapsed()
        );
        let (status, page) = request.await.unwrap();
        assert_eq!(status, 200);
        assert!(page.contains("Signed in"));
    }

    #[tokio::test]
    async fn stalled_connection_is_dropped_after_the_header_timeout() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        for sent in [&b""[..], &b"GET /callback?code=c-1&sta"[..]] {
            let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
            client.write_all(sent).await.unwrap();
            let (stream, _) = listener.accept().await.unwrap();
            let started = std::time::Instant::now();
            let result = serve_connection(stream, "s1".into(), Duration::from_millis(100)).await;
            assert!(result.is_none());
            assert!(started.elapsed() < Duration::from_secs(2));
            // Сервер закрыл соединение: клиент видит конец потока и не получил страницу.
            let mut rest = Vec::new();
            client.read_to_end(&mut rest).await.unwrap();
            assert!(rest.is_empty());
        }
    }

    #[tokio::test]
    async fn closed_and_reset_connections_do_not_end_the_login() {
        let server = CallbackServer::bind().await.unwrap();
        let url = format!("{}?code=c-1&state=s1", server.redirect_uri);
        let port = port_of(&server);
        // Закрытое сразу соединение и сброшенное (RST).
        drop(
            tokio::net::TcpStream::connect(("127.0.0.1", port))
                .await
                .unwrap(),
        );
        let reset = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .unwrap();
        reset.set_zero_linger().unwrap();
        drop(reset);
        let (_tx, rx) = oneshot::channel();
        let wait =
            tokio::spawn(async move { server.wait("s1", Duration::from_secs(10), rx).await });
        let (status, _) = get(&url).await;
        assert_eq!(status, 200);
        assert_eq!(wait.await.unwrap().unwrap(), "c-1");
    }

    #[tokio::test]
    async fn wrong_state_gets_failure_page_and_waiting_goes_on() {
        let server = CallbackServer::bind().await.unwrap();
        let redirect = server.redirect_uri.clone();
        let (_tx, rx) = oneshot::channel();
        let wait =
            tokio::spawn(async move { server.wait("s1", Duration::from_secs(10), rx).await });
        let (status, page) = get(&format!("{redirect}?code=forged&state=other")).await;
        assert_eq!(status, 200);
        assert!(page.contains("Sign-in failed"));
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!wait.is_finished(), "чужой state не должен завершать вход");
        let (status, page) = get(&format!("{redirect}?code=c-2&state=s1")).await;
        assert_eq!(status, 200);
        assert!(page.contains("Signed in"));
        assert_eq!(wait.await.unwrap().unwrap(), "c-2");
    }

    #[tokio::test]
    async fn forged_denial_is_ignored_until_the_real_callback() {
        let server = CallbackServer::bind().await.unwrap();
        let redirect = server.redirect_uri.clone();
        let (_tx, rx) = oneshot::channel();
        let wait =
            tokio::spawn(async move { server.wait("s1", Duration::from_secs(10), rx).await });
        // Отказ с чужим state, без state и «пустой» redirect — не ответ на наш запрос.
        for forged in [
            format!("{redirect}?error=access_denied&state=other"),
            format!("{redirect}?error=access_denied"),
            redirect.clone(),
        ] {
            let (status, page) = get(&forged).await;
            assert_eq!(status, 200);
            assert!(page.contains("Sign-in failed"), "{forged}");
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert!(!wait.is_finished(), "{forged} завершил вход");
        }
        let (_, page) = get(&format!("{redirect}?code=c-3&state=s1")).await;
        assert!(page.contains("Signed in"));
        assert_eq!(wait.await.unwrap().unwrap(), "c-3");
    }

    #[tokio::test]
    async fn denial_is_an_error() {
        let server = CallbackServer::bind().await.unwrap();
        let url = format!("{}?error=access_denied&state=s1", server.redirect_uri);
        let (_tx, rx) = oneshot::channel();
        let wait =
            tokio::spawn(async move { server.wait("s1", Duration::from_secs(10), rx).await });
        let (_, page) = get(&url).await;
        assert!(page.contains("Sign-in failed"));
        let e = wait.await.unwrap().unwrap_err();
        assert_eq!(e.key, "errors.oauth.denied");
        assert_eq!(e.params["error"], "access_denied");
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

        // Сброс отправителя тоже отменяет ожидание.
        let server = CallbackServer::bind().await.unwrap();
        let (tx, rx) = oneshot::channel::<()>();
        let wait = tokio::spawn(async move { server.wait("s", Duration::from_secs(10), rx).await });
        drop(tx);
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
