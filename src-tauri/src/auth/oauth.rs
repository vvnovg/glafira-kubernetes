//! OAuth-сервер OpenShift: метаданные, адрес авторизации, обмен кода на токен, страница токена.

use std::time::Duration;

use serde::Deserialize;
use tokio::sync::oneshot;

use crate::auth::callback::{CallbackServer, cancelled};
use crate::auth::pkce;
use crate::error::{AppError, AppResult, ErrorCode};
use crate::k8s::errors::{chain_text, from_transport_text, timeout};
use crate::storage::profile::{CaConfig, ConnectionProfile, ProxyConfig, pem_certificates};

/// OAuth-клиент, которым входит `oc login --web`: публичный, с loopback-redirect.
pub const CLIENT_ID: &str = "openshift-cli-client";
/// Сколько ждать входа в браузере.
pub const LOGIN_TIMEOUT: Duration = Duration::from_secs(300);

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct OAuthMetadata {
    pub issuer: String,
    pub authorization_endpoint: String,
    pub token_endpoint: String,
}

fn from_reqwest(e: &reqwest::Error) -> AppError {
    if e.is_timeout() {
        timeout().with_details(chain_text(e))
    } else {
        from_transport_text(&chain_text(e))
    }
}

/// HTTP-клиент к API-серверу и OAuth-серверу с настройками TLS и прокси профиля. Сертификат
/// OAuth-сервера часто выпущен CA ingress, а не CA API-сервера: CA профиля добавляется к
/// системным корням, а не заменяет их.
///
/// Ограничение: `tls_server_name` профиля здесь не учитывается — у `reqwest` нет переопределения
/// SNI и имени для проверки сертификата, оно берётся из адреса. Кластеру, которому нужно
/// другое имя, вход через браузер не подойдёт.
pub fn http_client(p: &ConnectionProfile) -> AppResult<reqwest::Client> {
    http_client_with(p, |b| b)
}

/// Как `http_client`, но `tune` меняет сборщик перед сборкой (например, политику переадресаций).
pub fn http_client_with(
    p: &ConnectionProfile,
    tune: impl FnOnce(reqwest::ClientBuilder) -> reqwest::ClientBuilder,
) -> AppResult<reqwest::Client> {
    crate::tls::ensure_crypto_provider();
    let mut b = reqwest::Client::builder()
        .connect_timeout(Duration::from_millis(p.connect_timeout_ms))
        .timeout(Duration::from_millis(p.request_timeout_ms))
        .user_agent(concat!("glafira-kubernetes/", env!("CARGO_PKG_VERSION")));
    match &p.ca {
        CaConfig::System => {}
        CaConfig::Pem { pem } => {
            let bad_pem = || {
                app_err!(
                    ErrorCode::InvalidInput,
                    "errors.connections.caPemInvalid",
                    "The CA certificate must be in PEM format (-----BEGIN CERTIFICATE-----)"
                )
            };
            let certs = pem_certificates(pem)
                .map_err(|_| bad_pem())?
                .iter()
                .map(|der| reqwest::Certificate::from_der(der))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| bad_pem())?;
            b = b.tls_certs_merge(certs);
        }
        CaConfig::Insecure => b = b.danger_accept_invalid_certs(true),
    }
    b = match &p.proxy {
        ProxyConfig::System => b,
        ProxyConfig::None => b.no_proxy(),
        ProxyConfig::Url { url } => b.proxy(reqwest::Proxy::all(url.trim()).map_err(|_| {
            app_err!(
                ErrorCode::InvalidInput,
                "errors.connections.badProxyUrl",
                "Invalid proxy address “{{url}}”: use http://host:port",
                url = crate::storage::profile::redact_url(url)
            )
        })?),
    };
    tune(b)
        .build()
        .map_err(|e| AppError::internal_with(format!("http client: {}", chain_text(&e))))
}

/// Нет OAuth-сервера OpenShift (обычный Kubernetes, спец. §5.2): способ входа в форме доступен
/// на любой платформе, но вход завершается этой ошибкой с подсказкой.
fn not_available() -> AppError {
    app_err!(
        ErrorCode::AuthFailed,
        "errors.oauth.notAvailable",
        "This cluster has no OpenShift OAuth server, so sign-in via browser is not available — use a token, a kubeconfig context or an exec plugin"
    )
}

/// `GET <API>/.well-known/oauth-authorization-server` — адреса OAuth-сервера кластера.
pub async fn fetch_metadata(http: &reqwest::Client, api_url: &str) -> AppResult<OAuthMetadata> {
    let url = format!(
        "{}/.well-known/oauth-authorization-server",
        api_url.trim_end_matches('/')
    );
    let r = http.get(&url).send().await.map_err(|e| from_reqwest(&e))?;
    if !r.status().is_success() {
        return Err(not_available().with_details(format!("{url}: HTTP {}", r.status())));
    }
    let meta = r
        .json::<OAuthMetadata>()
        .await
        .map_err(|e| not_available().with_details(chain_text(&e)))?;
    // Адреса из ответа сервера открываются в браузере (`authorization_endpoint`, `issuer`) и
    // получают код с verifier (`token_endpoint`): только http(s), без `file:` и прочих схем.
    for (name, value) in [
        ("issuer", &meta.issuer),
        ("authorization_endpoint", &meta.authorization_endpoint),
        ("token_endpoint", &meta.token_endpoint),
    ] {
        if !is_http_url(value) {
            return Err(not_available().with_details(format!("{url}: {name}: {value}")));
        }
    }
    Ok(meta)
}

/// Абсолютный адрес `http://` или `https://` с именем хоста.
fn is_http_url(value: &str) -> bool {
    url::Url::parse(value)
        .is_ok_and(|u| matches!(u.scheme(), "http" | "https") && u.host_str().is_some())
}

/// Адрес страницы входа: Authorization Code + PKCE (S256). Параметры добавляются к строке запроса
/// адреса `authorization_endpoint`, если она там уже есть.
pub fn authorize_url(
    meta: &OAuthMetadata,
    redirect_uri: &str,
    challenge: &str,
    state: &str,
) -> String {
    let params = [
        ("client_id", CLIENT_ID),
        ("response_type", "code"),
        ("redirect_uri", redirect_uri),
        ("scope", "user:full"),
        ("code_challenge", challenge),
        ("code_challenge_method", "S256"),
        ("state", state),
    ];
    match url::Url::parse(&meta.authorization_endpoint) {
        Ok(mut u) => {
            u.query_pairs_mut().extend_pairs(params);
            u.into()
        }
        // Адрес, не прошедший `fetch_metadata`: строку запроса дописываем как есть.
        Err(_) => {
            let query = url::form_urlencoded::Serializer::new(String::new())
                .extend_pairs(params)
                .finish();
            let sep = if meta.authorization_endpoint.contains('?') {
                '&'
            } else {
                '?'
            };
            format!("{}{sep}{query}", meta.authorization_endpoint)
        }
    }
}

/// Страница «Display Token» OAuth-сервера: запасной путь, если вход через браузер недоступен.
pub fn token_request_page(meta: &OAuthMetadata) -> String {
    format!("{}/oauth/token/request", meta.issuer.trim_end_matches('/'))
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
}

#[derive(Deserialize, Default)]
struct TokenError {
    #[serde(default)]
    error: String,
    #[serde(default)]
    error_description: String,
}

/// Обмен кода авторизации на токен доступа.
pub async fn exchange_code(
    http: &reqwest::Client,
    meta: &OAuthMetadata,
    code: &str,
    verifier: &str,
    redirect_uri: &str,
) -> AppResult<String> {
    let r = http
        .post(&meta.token_endpoint)
        .form(&[
            ("grant_type", "authorization_code"),
            ("code", code),
            ("redirect_uri", redirect_uri),
            ("client_id", CLIENT_ID),
            ("code_verifier", verifier),
        ])
        .send()
        .await
        .map_err(|e| from_reqwest(&e))?;
    let status = r.status();
    let text = r.text().await.map_err(|e| from_reqwest(&e))?;
    if !status.is_success() {
        let err: TokenError = serde_json::from_str(&text).unwrap_or_default();
        let reason = [err.error_description, err.error]
            .into_iter()
            .find(|s| !s.is_empty())
            .unwrap_or_else(|| format!("HTTP {status}"));
        return Err(app_err!(
            ErrorCode::AuthFailed,
            "errors.oauth.rejected",
            "The OAuth server rejected the sign-in: {{reason}}",
            reason = reason
        ));
    }
    let bad_response = || {
        app_err!(
            ErrorCode::AuthFailed,
            "errors.oauth.badTokenResponse",
            "The OAuth server returned an unexpected response"
        )
    };
    let token = serde_json::from_str::<TokenResponse>(&text)
        .map_err(|e| bad_response().with_details(e.to_string()))?
        .access_token;
    if token.trim().is_empty() {
        return Err(bad_response().with_details("empty access_token"));
    }
    Ok(token)
}

/// Полный вход: метаданные → loopback-сервер → браузер → код → токен. Токен не сохраняется
/// здесь: это делает команда (keychain).
///
/// `cancel` действует с самого начала: и пока запрашиваются метаданные (после отмены браузер не
/// открывается), и пока ждём redirect. Он срабатывает и по `send(())`, и по сбросу (drop)
/// отправителя — приёмник `oneshot` завершается в обоих случаях, поэтому вызывающий держит
/// отправитель, пока вход не закончен.
pub async fn login(
    p: &ConnectionProfile,
    open_browser: impl FnOnce(&str) -> AppResult<()>,
    mut cancel: oneshot::Receiver<()>,
    limit: Duration,
) -> AppResult<String> {
    let http = http_client(p)?;
    // Всё до открытия браузера идёт наперегонки с отменой; завершённый `Receiver` после этого
    // не опрашивается: при отмене возвращаемся сразу.
    let prepare = async {
        let meta = fetch_metadata(&http, &p.api_url).await?;
        let server = CallbackServer::bind().await?;
        Ok::<_, AppError>((meta, server))
    };
    let (meta, server) = tokio::select! {
        biased;
        _ = &mut cancel => return Err(cancelled()),
        prepared = prepare => prepared?,
    };
    let redirect_uri = server.redirect_uri.clone();
    let pkce = pkce::generate()?;
    let state = pkce::random_token()?;
    open_browser(&authorize_url(
        &meta,
        &redirect_uri,
        &pkce.challenge,
        &state,
    ))?;
    let code = server.wait(&state, limit, cancel).await?;
    exchange_code(&http, &meta, &code, &pkce.verifier, &redirect_uri).await
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;
    use crate::storage::profile::sample_profile;

    /// Профиль для тестов: без системного прокси, чтобы запросы к 127.0.0.1 не зависели от машины.
    fn test_profile() -> ConnectionProfile {
        let mut p = sample_profile("a");
        p.proxy = ProxyConfig::None;
        p
    }

    /// Клиент «браузера» в тестах: тоже без системного прокси.
    fn plain_client() -> reqwest::Client {
        crate::tls::ensure_crypto_provider();
        reqwest::Client::builder().no_proxy().build().unwrap()
    }

    /// Простой HTTP-сервер для тестов: отвечает `routes(метод, путь, тело)`; записывает запросы.
    async fn fake_server(
        routes: impl Fn(&str, &str, &str) -> (u16, String) + Send + Sync + 'static,
    ) -> (String, Arc<Mutex<Vec<String>>>) {
        fake_server_delayed(Duration::ZERO, routes).await
    }

    /// То же, но каждый ответ отправляется через `delay` после получения запроса.
    async fn fake_server_delayed(
        delay: Duration,
        routes: impl Fn(&str, &str, &str) -> (u16, String) + Send + Sync + 'static,
    ) -> (String, Arc<Mutex<Vec<String>>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let log = Arc::new(Mutex::new(Vec::new()));
        let (routes, log2) = (Arc::new(routes), log.clone());
        tokio::spawn(async move {
            loop {
                let Ok((mut s, _)) = listener.accept().await else {
                    return;
                };
                let (routes, log) = (routes.clone(), log2.clone());
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut chunk = [0u8; 4096];
                    loop {
                        let Ok(n) = s.read(&mut chunk).await else {
                            return;
                        };
                        if n == 0 {
                            return;
                        }
                        buf.extend_from_slice(&chunk[..n]);
                        let text = String::from_utf8_lossy(&buf).to_string();
                        if let Some(end) = text.find("\r\n\r\n") {
                            let len = text[..end]
                                .lines()
                                .find_map(|l| {
                                    l.to_ascii_lowercase()
                                        .strip_prefix("content-length:")
                                        .map(|v| v.trim().parse::<usize>().unwrap())
                                })
                                .unwrap_or(0);
                            if buf.len() >= end + 4 + len {
                                let mut first = text.lines().next().unwrap().split(' ');
                                let (method, path) = (first.next().unwrap(), first.next().unwrap());
                                let body = &text[end + 4..end + 4 + len];
                                log.lock().unwrap().push(format!("{method} {path} {body}"));
                                let (code, json) = routes(method, path, body);
                                tokio::time::sleep(delay).await;
                                let resp = format!(
                                    "HTTP/1.1 {code} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{json}",
                                    json.len()
                                );
                                // Клиент мог уйти (отмена входа) — для сервера это не ошибка.
                                let _ = s.write_all(resp.as_bytes()).await;
                                return;
                            }
                        }
                    }
                });
            }
        });
        (base, log)
    }

    fn meta(base: &str) -> OAuthMetadata {
        OAuthMetadata {
            issuer: base.into(),
            authorization_endpoint: format!("{base}/oauth/authorize"),
            token_endpoint: format!("{base}/oauth/token"),
        }
    }

    #[test]
    fn authorize_url_has_pkce_and_loopback_redirect() {
        let url = authorize_url(
            &meta("https://oauth-openshift.apps.c"),
            "http://127.0.0.1:5000/callback",
            "CH",
            "ST",
        );
        let parsed = url::Url::parse(&url).unwrap();
        assert_eq!(parsed.path(), "/oauth/authorize");
        let q: std::collections::HashMap<_, _> = parsed.query_pairs().into_owned().collect();
        assert_eq!(q["client_id"], "openshift-cli-client");
        assert_eq!(q["response_type"], "code");
        assert_eq!(q["redirect_uri"], "http://127.0.0.1:5000/callback");
        assert_eq!(q["code_challenge"], "CH");
        assert_eq!(q["code_challenge_method"], "S256");
        assert_eq!(q["state"], "ST");
        assert_eq!(
            token_request_page(&meta("https://oauth-openshift.apps.c/")),
            "https://oauth-openshift.apps.c/oauth/token/request"
        );
    }

    #[test]
    fn authorize_url_keeps_the_query_of_the_endpoint() {
        let mut m = meta("https://oauth-openshift.apps.c");
        m.authorization_endpoint = "https://oauth-openshift.apps.c/oauth/authorize?x=1".into();
        let url = authorize_url(&m, "http://127.0.0.1:5000/callback", "CH", "ST");
        let parsed = url::Url::parse(&url).unwrap();
        assert_eq!(parsed.path(), "/oauth/authorize");
        let q: Vec<(String, String)> = parsed.query_pairs().into_owned().collect();
        assert_eq!(q[0], ("x".to_string(), "1".to_string()));
        assert!(q.contains(&("state".to_string(), "ST".to_string())));
        assert!(q.contains(&("code_challenge".to_string(), "CH".to_string())));
        assert_eq!(url.matches('?').count(), 1, "один «?» в адресе: {url}");
        // Адрес, который не разобрать как URL, получает параметры без потери строки запроса.
        for (endpoint, prefix) in [
            ("/oauth/authorize", "/oauth/authorize?client_id="),
            ("/oauth/authorize?x=1", "/oauth/authorize?x=1&client_id="),
        ] {
            m.authorization_endpoint = endpoint.into();
            let url = authorize_url(&m, "http://127.0.0.1:5000/callback", "CH", "ST");
            assert!(url.starts_with(prefix), "{url}");
        }
    }

    #[tokio::test]
    async fn metadata_and_its_absence() {
        let (base, _) = fake_server(|_, path, _| match path {
            "/.well-known/oauth-authorization-server" => (
                200,
                r#"{"issuer":"https://o","authorization_endpoint":"https://o/oauth/authorize","token_endpoint":"https://o/oauth/token","scopes_supported":["user:full"]}"#.into(),
            ),
            _ => (404, "{}".into()),
        })
        .await;
        let http = http_client(&test_profile()).unwrap();
        let m = fetch_metadata(&http, &format!("{base}/")).await.unwrap();
        assert_eq!(m.token_endpoint, "https://o/oauth/token");
        let (base, log) = fake_server(|_, _, _| (404, "{}".into())).await;
        let e = fetch_metadata(&http, &format!("{base}/k8s/clusters/c-1"))
            .await
            .unwrap_err();
        assert_eq!(
            (e.code, e.key.as_str()),
            (ErrorCode::AuthFailed, "errors.oauth.notAvailable")
        );
        assert_eq!(
            log.lock().unwrap()[0],
            "GET /k8s/clusters/c-1/.well-known/oauth-authorization-server ",
            "адрес API с путём"
        );
    }

    #[tokio::test]
    async fn metadata_endpoints_must_be_http() {
        let http = http_client(&test_profile()).unwrap();
        for (field, bad) in [
            ("authorization_endpoint", "file:///etc/passwd"),
            ("token_endpoint", "javascript:alert(1)"),
            ("issuer", "ftp://o/"),
            ("issuer", "not a url"),
        ] {
            let mut doc = serde_json::json!({
                "issuer": "https://o",
                "authorization_endpoint": "https://o/oauth/authorize",
                "token_endpoint": "https://o/oauth/token",
            });
            doc[field] = bad.into();
            let (base, _) = fake_server(move |_, _, _| (200, doc.to_string())).await;
            let e = fetch_metadata(&http, &base).await.unwrap_err();
            assert_eq!(
                (e.code, e.key.as_str()),
                (ErrorCode::AuthFailed, "errors.oauth.notAvailable"),
                "{field} = {bad}"
            );
            assert!(e.details.unwrap().contains(field), "{field} в details");
        }
    }

    #[tokio::test]
    async fn code_exchange_success_and_rejection() {
        let (base, log) = fake_server(|_, _, body| {
            if body.contains("code=good") {
                (
                    200,
                    r#"{"access_token":"sha256~tok","token_type":"Bearer","expires_in":86400}"#
                        .into(),
                )
            } else if body.contains("code=empty") {
                (200, r#"{"access_token":""}"#.into())
            } else {
                (
                    400,
                    r#"{"error":"invalid_grant","error_description":"code expired"}"#.into(),
                )
            }
        })
        .await;
        let http = http_client(&test_profile()).unwrap();
        let t = exchange_code(
            &http,
            &meta(&base),
            "good",
            "VER",
            "http://127.0.0.1:1/callback",
        )
        .await
        .unwrap();
        assert_eq!(t, "sha256~tok");
        let req = log.lock().unwrap()[0].clone();
        assert!(req.starts_with("POST /oauth/token "));
        for part in [
            "grant_type=authorization_code",
            "client_id=openshift-cli-client",
            "code_verifier=VER",
            "redirect_uri=http%3A%2F%2F127.0.0.1%3A1%2Fcallback",
        ] {
            assert!(req.contains(part), "{part} in {req}");
        }
        let e = exchange_code(&http, &meta(&base), "bad", "V", "r")
            .await
            .unwrap_err();
        assert_eq!(e.key, "errors.oauth.rejected");
        assert_eq!(e.params["reason"], "code expired");
        let e = exchange_code(&http, &meta(&base), "empty", "V", "r")
            .await
            .unwrap_err();
        assert_eq!(
            e.key, "errors.oauth.badTokenResponse",
            "пустой access_token"
        );
    }

    #[tokio::test]
    async fn full_login_through_fake_oauth_server() {
        let (base, token_log) = fake_server(move |method, path, body| match (method, path) {
            ("POST", "/oauth/token") if body.contains("code=the-code") => {
                (200, r#"{"access_token":"sha256~new"}"#.into())
            }
            _ => (404, "{}".into()),
        })
        .await;
        // Метаданные указывают на тот же фейковый сервер.
        let (meta_base, _) = {
            let b = base.clone();
            fake_server(move |_, _, _| {
                (
                    200,
                    serde_json::json!({"issuer": b, "authorization_endpoint": format!("{b}/oauth/authorize"),
                        "token_endpoint": format!("{b}/oauth/token")})
                    .to_string(),
                )
            })
            .await
        };
        let mut p = test_profile();
        p.api_url = meta_base;
        let (_tx, rx) = tokio::sync::oneshot::channel();
        // «Браузер»: сразу проходит redirect с кодом и тем же state; запоминает code_challenge.
        let challenge = Arc::new(Mutex::new(String::new()));
        let seen = challenge.clone();
        let browser = move |url: &str| {
            let u = url::Url::parse(url).unwrap();
            let q: std::collections::HashMap<_, _> = u.query_pairs().into_owned().collect();
            *seen.lock().unwrap() = q["code_challenge"].clone();
            let callback = format!("{}?code=the-code&state={}", q["redirect_uri"], q["state"]);
            tokio::spawn(async move {
                plain_client().get(callback).send().await.unwrap();
            });
            Ok(())
        };
        let token = login(&p, browser, rx, Duration::from_secs(10))
            .await
            .unwrap();
        assert_eq!(token, "sha256~new");
        // Обмен кода идёт с verifier, из которого получен challenge авторизации.
        let request = token_log.lock().unwrap()[0].clone();
        let body = request.strip_prefix("POST /oauth/token ").unwrap();
        let form: std::collections::HashMap<_, _> = url::form_urlencoded::parse(body.as_bytes())
            .into_owned()
            .collect();
        let challenge = challenge.lock().unwrap().clone();
        assert!(!challenge.is_empty());
        assert_eq!(pkce::challenge_for(&form["code_verifier"]), challenge);
    }

    #[tokio::test]
    async fn cancel_while_fetching_metadata_never_opens_the_browser() {
        // Метаданные придут, но через 2 с: отмена приходит раньше.
        let (api, _) = fake_server_delayed(Duration::from_secs(2), |_, _, _| {
            (
                200,
                serde_json::json!({"issuer": "https://o", "authorization_endpoint": "https://o/a",
                    "token_endpoint": "https://o/t"})
                .to_string(),
            )
        })
        .await;
        let mut p = test_profile();
        p.api_url = api;
        let opened = Arc::new(AtomicBool::new(false));
        let flag = opened.clone();
        let browser = move |_: &str| {
            flag.store(true, Ordering::SeqCst);
            Ok(())
        };
        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let _ = tx.send(());
        });
        let started = std::time::Instant::now();
        let e = login(&p, browser, rx, Duration::from_secs(10))
            .await
            .unwrap_err();
        assert_eq!(e.key, "errors.oauth.cancelled");
        assert!(
            !opened.load(Ordering::SeqCst),
            "браузер открыт после отмены"
        );
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "отмена ждала метаданные: {:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn bad_proxy_and_ca_are_input_errors() {
        let mut p = test_profile();
        p.proxy = ProxyConfig::Url { url: "::".into() };
        assert_eq!(
            http_client(&p).unwrap_err().key,
            "errors.connections.badProxyUrl"
        );
        // Профиль, записанный в обход проверки: пароль прокси в ошибку не попадает.
        p.proxy = ProxyConfig::Url {
            url: "http://u:s3cr3t@[::1".into(),
        };
        let e = http_client(&p).unwrap_err();
        assert_eq!(e.key, "errors.connections.badProxyUrl");
        assert_eq!(e.params["url"], "http://[::1");
        let json = serde_json::to_string(&e).unwrap();
        assert!(!json.contains("s3cr3t") && !json.contains('@'), "{json}");
        let mut p = test_profile();
        p.ca = CaConfig::Pem { pem: "nope".into() };
        assert_eq!(
            http_client(&p).unwrap_err().key,
            "errors.connections.caPemInvalid"
        );
        let mut p = test_profile();
        p.ca = CaConfig::Pem {
            pem: crate::storage::profile::TEST_CERT_PEM.into(),
        };
        assert!(http_client(&p).is_ok());
    }
}
