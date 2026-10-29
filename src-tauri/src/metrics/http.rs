//! Прямой HTTP к источнику метрик (спец. §7.1): Thanos Querier по маршруту OpenShift и свой
//! URL из профиля. Только GET к `/api/v1/query` и `/api/v1/query_range` — других запросов
//! клиент не умеет; токен (подключения или свой) уходит заголовком `Authorization`. TLS и
//! прокси — как у подключения (`auth::oauth::http_client`: CA добавляется к системным корням —
//! сертификат маршрута часто выпущен CA ingress), свой CA источника — так же.

use futures::future::BoxFuture;
use secrecy::{ExposeSecret, SecretString};

use super::prometheus::{Failure, MetricsSource, PromApi, Reason, SourceInfo};
use crate::error::{AppError, AppResult, ErrorCode};
use crate::storage::profile::{CaConfig, ConnectionProfile, MetricsCa};

/// Источник метрик по адресу: база без `/api/v1`, токен — по выбранной авторизации.
pub struct HttpSource {
    client: reqwest::Client,
    base: url::Url,
    token: Option<SecretString>,
    info: SourceInfo,
    tenancy: bool,
}

/// `Debug` без токена.
impl std::fmt::Debug for HttpSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpSource")
            .field("base", &self.base.as_str())
            .field("token", &self.token.as_ref().map(|_| "***"))
            .field("info", &self.info)
            .finish()
    }
}

/// HTTP-клиент источника: TLS, прокси и таймауты профиля; CA источника — свой или как у
/// подключения. Переадресациям клиент не следует: за ними токен ушёл бы по другому пути, а
/// источник делает только GET к `/api/v1/query` и `/api/v1/query_range`.
pub fn client(profile: &ConnectionProfile, ca: &MetricsCa) -> AppResult<reqwest::Client> {
    let no_redirects = |b: reqwest::ClientBuilder| b.redirect(reqwest::redirect::Policy::none());
    match ca {
        MetricsCa::Connection => crate::auth::oauth::http_client_with(profile, no_redirects),
        MetricsCa::Pem { pem } => crate::auth::oauth::http_client_with(
            &ConnectionProfile {
                ca: CaConfig::Pem { pem: pem.clone() },
                ..profile.clone()
            },
            no_redirects,
        ),
    }
}

impl HttpSource {
    pub fn new(
        client: reqwest::Client,
        base: url::Url,
        token: Option<SecretString>,
        info: SourceInfo,
        tenancy: bool,
    ) -> Self {
        Self {
            client,
            base,
            token,
            info,
            tenancy,
        }
    }

    fn url(&self, api: PromApi) -> String {
        format!("{}{}", self.base.as_str().trim_end_matches('/'), api.path())
    }

    async fn fetch(&self, api: PromApi, params: Vec<(String, String)>) -> Result<String, Failure> {
        let mut url = url::Url::parse(&self.url(api))
            .map_err(|e| Failure::from_error(AppError::internal_with(e.to_string())))?;
        url.query_pairs_mut().extend_pairs(&params);
        let mut req = self.client.get(url);
        if let Some(t) = &self.token {
            req = req.bearer_auth(t.expose_secret());
        }
        let resp = req.send().await.map_err(|e| transport(&e))?;
        let status = resp.status().as_u16();
        let body = resp.text().await.map_err(|e| transport(&e))?;
        match status {
            200..=299 => Ok(body),
            // Переадресация не выполняется: источник с ней — не Prometheus API по этому адресу.
            300..=399 => Err(Failure::bad_response(format!(
                "redirects are not followed (HTTP {status})"
            ))),
            // У Prometheus ошибка запроса — 400/422 с телом `{"status":"error",…}`.
            400 | 422 => Err(Failure::bad_response(
                body.chars().take(300).collect::<String>(),
            )),
            _ => Err(http_failure(status, &self.base)),
        }
    }
}

impl MetricsSource for HttpSource {
    fn info(&self) -> &SourceInfo {
        &self.info
    }

    fn tenancy(&self) -> bool {
        self.tenancy
    }

    fn get<'a>(
        &'a self,
        api: PromApi,
        params: Vec<(String, String)>,
    ) -> BoxFuture<'a, Result<String, Failure>> {
        Box::pin(self.fetch(api, params))
    }
}

/// Сеть и TLS: тот же разбор, что у API-сервера.
fn transport(e: &reqwest::Error) -> Failure {
    let error = if e.is_timeout() {
        crate::k8s::errors::timeout()
    } else {
        crate::k8s::errors::from_transport_text(&crate::k8s::errors::chain_text(e))
    };
    Failure::from_error(error)
}

/// Ответ источника с кодом ошибки; адрес — в тексте для человека (без имени и пароля).
fn http_failure(status: u16, base: &url::Url) -> Failure {
    let reason = match status {
        401 | 403 => Reason::Forbidden,
        404 => Reason::NotFound,
        _ => Reason::Unavailable,
    };
    let code = match reason {
        Reason::Forbidden => ErrorCode::Forbidden,
        Reason::NotFound => ErrorCode::NotFound,
        _ => ErrorCode::ConnectionFailed,
    };
    Failure {
        reason,
        error: AppError::localized(
            code,
            crate::ltext!(
                "errors.metrics.httpStatus",
                "The metrics source {{url}} answered with code {{status}}",
                url = crate::storage::profile::without_userinfo(base),
                status = status
            ),
        ),
    }
}

/// Локальный HTTP-сервер для тестов источников: свой адрес и свой Thanos в `select`.
#[cfg(test)]
pub mod fake {
    use std::sync::{Arc, Mutex};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// Ответ `(код, тело)` по первой строке запроса; запросы с заголовками записываются.
    /// Коды 3xx идут с `Location`: без него клиент переадресацию не выполнит.
    pub async fn server(
        answer: impl Fn(&str) -> (u16, String) + Send + Sync + 'static,
    ) -> (url::Url, Arc<Mutex<Vec<String>>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base =
            url::Url::parse(&format!("http://{}/prefix", listener.local_addr().unwrap())).unwrap();
        let log = Arc::new(Mutex::new(Vec::new()));
        let (answer, seen) = (Arc::new(answer), log.clone());
        tokio::spawn(async move {
            while let Ok((mut s, _)) = listener.accept().await {
                let (answer, seen) = (answer.clone(), seen.clone());
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut chunk = [0u8; 4096];
                    while !String::from_utf8_lossy(&buf).contains("\r\n\r\n") {
                        let Ok(n) = s.read(&mut chunk).await else {
                            return;
                        };
                        if n == 0 {
                            return;
                        }
                        buf.extend_from_slice(&chunk[..n]);
                    }
                    let head = String::from_utf8_lossy(&buf).to_string();
                    let line = head.lines().next().unwrap_or_default().to_string();
                    seen.lock().unwrap().push(head);
                    let (code, body) = answer(&line);
                    let location = if (300..400).contains(&code) {
                        "Location: /elsewhere\r\n"
                    } else {
                        ""
                    };
                    let resp = format!(
                        "HTTP/1.1 {code} X\r\n{location}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = s.write_all(resp.as_bytes()).await;
                });
            }
        });
        (base, log)
    }
}

#[cfg(test)]
mod tests {
    use super::fake::server;
    use super::*;
    use crate::metrics::prometheus::{SourceKind, SourceOrigin, probe, query};
    use crate::storage::profile::sample_profile;

    fn source(base: url::Url, token: Option<&str>, tenancy: bool) -> HttpSource {
        let mut p = sample_profile("a");
        p.proxy = crate::storage::profile::ProxyConfig::None;
        HttpSource::new(
            client(&p, &MetricsCa::Connection).unwrap(),
            base,
            token.map(|t| SecretString::from(t.to_string())),
            SourceInfo {
                kind: SourceKind::Thanos,
                origin: SourceOrigin::Profile,
                location: "x".into(),
                history: true,
            },
            tenancy,
        )
    }

    const ONE: &str = r#"{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1,"1"]}]}}"#;

    #[tokio::test]
    async fn get_goes_to_the_api_path_with_the_token() {
        let (base, log) = server(|_| (200, ONE.into())).await;
        let s = source(base, Some("sha256~secret"), true);
        probe(&s, 5).await.unwrap();
        query(&s, "team-a", "up{a=\"b\"}", 7).await.unwrap();
        let seen = log.lock().unwrap();
        assert!(
            seen[0].starts_with("GET /prefix/api/v1/query?query=vector%281%29&time=5 "),
            "{}",
            seen[0]
        );
        assert!(
            seen[0].contains("authorization: Bearer sha256~secret"),
            "{}",
            seen[0]
        );
        assert!(
            seen[1].starts_with(
                "GET /prefix/api/v1/query?query=up%7Ba%3D%22b%22%7D&time=7&namespace=team-a "
            ),
            "{}",
            seen[1]
        );
        assert!(!format!("{s:?}").contains("secret"), "{s:?}");
    }

    #[tokio::test]
    async fn redirects_are_not_followed() {
        let (base, log) = server(|_| (302, String::new())).await;
        let s = source(base, Some("sha256~secret"), false);
        let f = probe(&s, 1).await.unwrap_err();
        assert_eq!(f.reason, Reason::BadResponse);
        assert_eq!(f.error.key, "errors.metrics.badResponse");
        assert!(f.error.details.unwrap().contains("302"));
        assert_eq!(
            log.lock().unwrap().len(),
            1,
            "за переадресацией с токеном не ходят"
        );
    }

    #[tokio::test]
    async fn answers_become_reasons() {
        let (base, _) = server(|line| {
            if line.contains("time=1") {
                (403, "forbidden".into())
            } else if line.contains("time=2") {
                (404, "nope".into())
            } else if line.contains("time=3") {
                (503, "down".into())
            } else {
                (
                    400,
                    r#"{"status":"error","errorType":"bad_data","error":"parse error"}"#.into(),
                )
            }
        })
        .await;
        let s = source(base.clone(), None, false);
        for (t, reason) in [
            (1, Reason::Forbidden),
            (2, Reason::NotFound),
            (3, Reason::Unavailable),
            (4, Reason::BadResponse),
        ] {
            let f = probe(&s, t).await.unwrap_err();
            assert_eq!(f.reason, reason, "{t}");
        }
        let f = probe(&s, 1).await.unwrap_err();
        assert_eq!(f.error.key, "errors.metrics.httpStatus");
        assert_eq!(f.error.params["status"], "403");
        assert!(f.error.params["url"].ends_with("/prefix"));
        // Порт закрыт — источник недоступен.
        let closed = url::Url::parse("http://127.0.0.1:1").unwrap();
        let f = probe(&source(closed, None, false), 1).await.unwrap_err();
        assert!(
            matches!(f.reason, Reason::Unavailable | Reason::Timeout),
            "{f:?}"
        );
    }
}
