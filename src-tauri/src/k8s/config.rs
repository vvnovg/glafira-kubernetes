//! Сборка `kube::Config` из профиля подключения (спец. §5.1–5.2). Секреты читаются из keychain;
//! сеть здесь не используется.

use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use kube::config::AuthInfo;
use secrecy::SecretString;

use crate::error::{AppError, AppResult, ErrorCode};
use crate::secrets::{CLIENT_KEY, SecretStore, TOKEN, secret_key};
use crate::storage::profile::{
    AuthConfig, CaConfig, ConnectionProfile, ProxyConfig, pem_certificates, without_userinfo,
};

/// Значение переменной окружения (в тестах подменяется).
pub type EnvLookup<'a> = &'a dyn Fn(&str) -> Option<String>;

/// Переменные окружения процесса.
pub fn process_env(name: &str) -> Option<String> {
    std::env::var(name).ok()
}

/// Прокси для `host` из `HTTPS_PROXY` / `https_proxy` с учётом `NO_PROXY` / `no_proxy`
/// (`*`, точное имя, домен с точкой или без).
pub fn system_proxy(host: &str, env: EnvLookup) -> Option<String> {
    let get = |a: &str, b: &str| env(a).or_else(|| env(b)).filter(|v| !v.trim().is_empty());
    let proxy = get("HTTPS_PROXY", "https_proxy")?;
    let host = host.to_ascii_lowercase();
    if let Some(no_proxy) = get("NO_PROXY", "no_proxy") {
        let excluded = no_proxy.split(',').map(str::trim).any(|entry| {
            let entry = entry.to_ascii_lowercase();
            let domain = entry.trim_start_matches('.');
            entry == "*" || host == domain || host.ends_with(&format!(".{domain}"))
        });
        if excluded {
            return None;
        }
    }
    Some(proxy.trim().to_string())
}

/// Адрес прокси без имени и пароля для текста ошибки. Если адрес не разбирается как URL с
/// хостом (нет схемы, пробел в хосте), отбрасывается всё до последней «@».
fn redact_proxy(raw: &str) -> String {
    match url::Url::parse(raw) {
        Ok(u) if u.host().is_some() => without_userinfo(&u),
        _ => match raw.rsplit_once('@') {
            Some((head, tail)) => match head.split_once("://") {
                Some((scheme, _)) => format!("{scheme}://{tail}"),
                None => tail.to_string(),
            },
            None => raw.to_string(),
        },
    }
}

fn invalid_url(p: &ConnectionProfile) -> AppError {
    app_err!(
        ErrorCode::InvalidInput,
        "errors.connections.badApiUrl",
        "Invalid API server address “{{url}}”",
        url = p.api_url
    )
}

/// Токен из keychain. Нет токена у входа через браузер — нужно войти (`SESSION_EXPIRED`),
/// у токена, вставленного вручную, — вставить его (`AUTH_FAILED`).
fn token(p: &ConnectionProfile, secrets: &dyn SecretStore) -> AppResult<String> {
    match secrets.get(&secret_key(&p.id, TOKEN))? {
        Some(t) if !t.trim().is_empty() => Ok(t.trim().to_string()),
        _ if p.auth == AuthConfig::Browser => Err(app_err!(
            ErrorCode::SessionExpired,
            "errors.connections.loginRequired",
            "Sign in to the cluster: use “Sign in via browser”"
        )),
        _ => Err(app_err!(
            ErrorCode::AuthFailed,
            "errors.connections.tokenMissing",
            "No token is saved for this connection — paste a token"
        )),
    }
}

/// `kube::Config` профиля. Таймаут ответа не задаётся: он оборвал бы долгие потоки (логи, watch);
/// таймаут запроса ставит вызывающий код (`request_timeout`). У exec-плагина учётных данных здесь
/// нет: их выдаёт запуск плагина, и их подставляет `apply_exec_credential`.
pub fn build_kube_config(
    p: &ConnectionProfile,
    secrets: &dyn SecretStore,
    env: EnvLookup,
) -> AppResult<kube::Config> {
    let uri: http::Uri = p.api_url.trim().parse().map_err(|_| invalid_url(p))?;
    let host = uri.host().ok_or_else(|| invalid_url(p))?.to_string();
    let mut c = kube::Config::new(uri);
    c.connect_timeout = Some(Duration::from_millis(p.connect_timeout_ms));
    c.read_timeout = None;
    c.write_timeout = Some(request_timeout(p));
    // Встроенный повтор kube (429/503/504 до 15 раз) скрыл бы за таймаутом запроса ошибки
    // «слишком много запросов» и «сервер недоступен» и повторял бы изменяющие запросы.
    c.default_retry = false;
    c.tls_server_name = p.tls_server_name.clone();
    match &p.ca {
        CaConfig::System => {}
        CaConfig::Pem { pem } => {
            c.root_cert = Some(pem_certificates(pem).map_err(|_| {
                app_err!(
                    ErrorCode::InvalidInput,
                    "errors.connections.caPemInvalid",
                    "The CA certificate must be in PEM format (-----BEGIN CERTIFICATE-----)"
                )
            })?);
        }
        CaConfig::Insecure => c.accept_invalid_certs = true,
    }
    let proxy = match &p.proxy {
        ProxyConfig::System => system_proxy(&host, env),
        ProxyConfig::None => None,
        ProxyConfig::Url { url } => Some(url.trim().to_string()),
    };
    c.proxy_url = proxy
        .map(|u| {
            // Адрес из HTTPS_PROXY может нести пароль: в ошибке он показан без имени и пароля.
            let bad = || {
                app_err!(
                    ErrorCode::InvalidInput,
                    "errors.connections.badProxyUrl",
                    "Invalid proxy address “{{url}}”: use http://host:port",
                    url = redact_proxy(&u)
                )
            };
            // Другие схемы kube отвергнет позже, с полным адресом в тексте ошибки.
            let uri = u.parse::<http::Uri>().map_err(|_| bad())?;
            match uri.scheme_str() {
                Some("http" | "https") => Ok(uri),
                _ => Err(bad()),
            }
        })
        .transpose()?;
    c.auth_info = match &p.auth {
        AuthConfig::Exec(_) => AuthInfo::default(),
        AuthConfig::Token | AuthConfig::Browser => AuthInfo {
            token: Some(SecretString::from(token(p, secrets)?)),
            ..Default::default()
        },
        AuthConfig::ClientCert { certificate_pem } => {
            let key = secrets
                .get(&secret_key(&p.id, CLIENT_KEY))?
                .filter(|k| !k.trim().is_empty())
                .ok_or_else(|| {
                    app_err!(
                        ErrorCode::AuthFailed,
                        "errors.connections.clientKeyMissing",
                        "No client key is saved for this connection — add the key (PEM)"
                    )
                })?;
            AuthInfo {
                client_certificate_data: Some(STANDARD.encode(certificate_pem.as_bytes())),
                client_key_data: Some(SecretString::from(STANDARD.encode(key.as_bytes()))),
                ..Default::default()
            }
        }
    };
    c.headers.push((
        http::header::USER_AGENT,
        http::HeaderValue::from_static(concat!("glafira-kubernetes/", env!("CARGO_PKG_VERSION"))),
    ));
    Ok(c)
}

/// Учётные данные, которые выдал exec-плагин: клиентский сертификат или токен.
pub fn apply_exec_credential(c: &mut kube::Config, cred: &crate::auth::exec::PluginCredential) {
    use secrecy::ExposeSecret;
    c.auth_info = match (&cred.client_certificate_pem, &cred.client_key_pem) {
        (Some(cert), Some(key)) => AuthInfo {
            client_certificate_data: Some(STANDARD.encode(cert.as_bytes())),
            client_key_data: Some(SecretString::from(
                STANDARD.encode(key.expose_secret().as_bytes()),
            )),
            ..Default::default()
        },
        _ => AuthInfo {
            token: cred.token.clone(),
            ..Default::default()
        },
    };
}

/// Таймаут одного запроса к API-серверу.
pub fn request_timeout(p: &ConnectionProfile) -> Duration {
    Duration::from_millis(p.request_timeout_ms)
}

#[cfg(test)]
mod tests {
    use secrecy::ExposeSecret;

    use super::*;
    use crate::secrets::MemoryStore;
    use crate::storage::profile::{TEST_CERT_PEM, sample_profile};

    const TEST_KEY_PEM: &str = include_str!("../storage/testdata/test-key.pem");

    fn no_env(_: &str) -> Option<String> {
        None
    }

    fn with_token(id: &str) -> MemoryStore {
        let s = MemoryStore::default();
        s.set(&secret_key(id, TOKEN), " sha256~abc \n").unwrap();
        s
    }

    #[test]
    fn token_profile_builds_bearer_config() {
        let p = sample_profile("a");
        let c = build_kube_config(&p, &with_token("a"), &no_env).unwrap();
        assert_eq!(
            c.cluster_url.to_string(),
            "https://api.dev.example.com:6443/"
        );
        assert_eq!(
            c.auth_info.token.as_ref().unwrap().expose_secret(),
            "sha256~abc"
        );
        assert_eq!(c.connect_timeout, Some(Duration::from_secs(10)));
        assert_eq!(c.read_timeout, None, "долгие потоки не обрываются");
        assert!(c.root_cert.is_none() && !c.accept_invalid_certs);
        assert!(c.proxy_url.is_none());
        assert!(c.headers.iter().any(|(k, v)| k == http::header::USER_AGENT
            && v.to_str().unwrap().starts_with("glafira-kubernetes/")));
    }

    #[test]
    fn missing_token_depends_on_method() {
        let mut p = sample_profile("a");
        let e = build_kube_config(&p, &MemoryStore::default(), &no_env).unwrap_err();
        assert_eq!(
            (e.code, e.key.as_str()),
            (ErrorCode::AuthFailed, "errors.connections.tokenMissing")
        );
        p.auth = AuthConfig::Browser;
        let e = build_kube_config(&p, &MemoryStore::default(), &no_env).unwrap_err();
        assert_eq!(
            (e.code, e.key.as_str()),
            (
                ErrorCode::SessionExpired,
                "errors.connections.loginRequired"
            )
        );
    }

    #[test]
    fn ca_modes() {
        let mut p = sample_profile("a");
        p.ca = CaConfig::Pem {
            pem: format!("{TEST_CERT_PEM}{TEST_CERT_PEM}"),
        };
        let c = build_kube_config(&p, &with_token("a"), &no_env).unwrap();
        assert_eq!(c.root_cert.unwrap().len(), 2);
        p.ca = CaConfig::Insecure;
        let c = build_kube_config(&p, &with_token("a"), &no_env).unwrap();
        assert!(c.accept_invalid_certs);
        p.ca = CaConfig::Pem { pem: "x".into() };
        let e = build_kube_config(&p, &with_token("a"), &no_env).unwrap_err();
        assert_eq!(e.key, "errors.connections.caPemInvalid");
    }

    #[test]
    fn client_certificate_goes_base64_and_needs_a_key() {
        let mut p = sample_profile("a");
        p.auth = AuthConfig::ClientCert {
            certificate_pem: TEST_CERT_PEM.into(),
        };
        let e = build_kube_config(&p, &MemoryStore::default(), &no_env).unwrap_err();
        assert_eq!(e.key, "errors.connections.clientKeyMissing");
        let secrets = MemoryStore::default();
        secrets
            .set(&secret_key("a", CLIENT_KEY), TEST_KEY_PEM)
            .unwrap();
        let c = build_kube_config(&p, &secrets, &no_env).unwrap();
        let cert = STANDARD
            .decode(c.auth_info.client_certificate_data.unwrap())
            .unwrap();
        assert_eq!(cert, TEST_CERT_PEM.as_bytes());
        let key = STANDARD
            .decode(c.auth_info.client_key_data.unwrap().expose_secret())
            .unwrap();
        assert_eq!(key, TEST_KEY_PEM.as_bytes());
        assert!(c.auth_info.token.is_none());
    }

    #[tokio::test]
    async fn every_auth_mode_produces_a_client() {
        let secrets = with_token("a");
        secrets
            .set(&secret_key("a", CLIENT_KEY), TEST_KEY_PEM)
            .unwrap();
        for (auth, ca) in [
            (AuthConfig::Token, CaConfig::System),
            (
                AuthConfig::Browser,
                CaConfig::Pem {
                    pem: TEST_CERT_PEM.into(),
                },
            ),
            (
                AuthConfig::ClientCert {
                    certificate_pem: TEST_CERT_PEM.into(),
                },
                CaConfig::Insecure,
            ),
        ] {
            let mut p = sample_profile("a");
            p.auth = auth.clone();
            p.ca = ca;
            let c = build_kube_config(&p, &secrets, &no_env).unwrap();
            assert!(kube::Client::try_from(c).is_ok(), "{auth:?}");
        }
    }

    #[test]
    fn proxy_modes() {
        let env = |name: &str| match name {
            "HTTPS_PROXY" => Some("http://proxy.corp:3128".to_string()),
            "NO_PROXY" => Some("localhost, .internal.corp,example.org".to_string()),
            _ => None,
        };
        assert_eq!(
            system_proxy("api.c", &env).as_deref(),
            Some("http://proxy.corp:3128")
        );
        for host in [
            "localhost",
            "api.internal.corp",
            "example.org",
            "api.example.org",
        ] {
            assert_eq!(system_proxy(host, &env), None, "{host}");
        }
        assert_eq!(system_proxy("api.c", &no_env), None);
        let star = |name: &str| match name {
            "https_proxy" => Some("http://p:1".to_string()),
            "no_proxy" => Some("*".to_string()),
            _ => None,
        };
        assert_eq!(system_proxy("api.c", &star), None);

        let mut p = sample_profile("a");
        let c = build_kube_config(&p, &with_token("a"), &env).unwrap();
        assert_eq!(c.proxy_url.unwrap().to_string(), "http://proxy.corp:3128/");
        p.proxy = ProxyConfig::None;
        assert!(
            build_kube_config(&p, &with_token("a"), &env)
                .unwrap()
                .proxy_url
                .is_none()
        );
        p.proxy = ProxyConfig::Url {
            url: "http://other:8080".into(),
        };
        let c = build_kube_config(&p, &with_token("a"), &no_env).unwrap();
        assert_eq!(c.proxy_url.unwrap().to_string(), "http://other:8080/");
    }

    /// Адрес API с путём (прокси Rancher `…/k8s/clusters/<id>`): запросы идут под этот путь.
    #[tokio::test]
    async fn api_url_path_prefixes_every_request() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = Vec::new();
            let mut chunk = [0u8; 1024];
            while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
                let n = sock.read(&mut chunk).await.unwrap();
                buf.extend_from_slice(&chunk[..n]);
            }
            let body = r#"{"major":"1","minor":"31","gitVersion":"v1.31.4","gitCommit":"x","gitTreeState":"clean","buildDate":"2026-01-01T00:00:00Z","goVersion":"go1.23","compiler":"gc","platform":"linux/amd64"}"#;
            let resp = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            sock.write_all(resp.as_bytes()).await.unwrap();
            String::from_utf8(buf).unwrap()
        });
        let mut p = sample_profile("a");
        // Простой HTTP — только чтобы не поднимать TLS в тесте; профиль с http:// не сохранить.
        p.api_url = format!("http://127.0.0.1:{port}/k8s/clusters/c-x1");
        p.proxy = ProxyConfig::None;
        let c = build_kube_config(&p, &with_token("a"), &no_env).unwrap();
        let client = kube::Client::try_from(c).unwrap();
        let v = client.apiserver_version().await.unwrap();
        assert_eq!(v.git_version, "v1.31.4");
        let request = server.await.unwrap();
        assert!(
            request.starts_with("GET /k8s/clusters/c-x1/version "),
            "{request}"
        );
        assert!(
            request
                .to_lowercase()
                .contains("authorization: bearer sha256~abc")
        );
    }

    #[tokio::test]
    async fn exec_profile_gets_credentials_from_the_plugin() {
        let mut p = sample_profile("a");
        p.auth = AuthConfig::Exec(crate::storage::profile::sample_exec());
        let mut c = build_kube_config(&p, &MemoryStore::default(), &no_env).unwrap();
        assert!(c.auth_info.token.is_none() && c.auth_info.client_key_data.is_none());
        let mut cred = crate::auth::exec::PluginCredential {
            token: Some(SecretString::from("t-exec")),
            client_certificate_pem: None,
            client_key_pem: None,
            expires_at: None,
        };
        apply_exec_credential(&mut c, &cred);
        assert_eq!(
            c.auth_info.token.as_ref().unwrap().expose_secret(),
            "t-exec"
        );
        cred.client_certificate_pem = Some(TEST_CERT_PEM.into());
        cred.client_key_pem = Some(SecretString::from(TEST_KEY_PEM));
        apply_exec_credential(&mut c, &cred);
        assert!(c.auth_info.token.is_none(), "сертификат вместо токена");
        assert!(kube::Client::try_from(c).is_ok());
    }

    #[test]
    fn tls_server_name_and_timeouts_come_from_profile() {
        let mut p = sample_profile("a");
        p.tls_server_name = Some("api.internal".into());
        p.connect_timeout_ms = 2_000;
        p.request_timeout_ms = 5_000;
        let c = build_kube_config(&p, &with_token("a"), &no_env).unwrap();
        assert_eq!(c.tls_server_name.as_deref(), Some("api.internal"));
        assert_eq!(c.connect_timeout, Some(Duration::from_secs(2)));
        assert_eq!(request_timeout(&p), Duration::from_secs(5));
    }

    /// Конфиг профиля с системным прокси из `HTTPS_PROXY`.
    fn config_with_env_proxy(value: &str) -> AppResult<kube::Config> {
        let value = value.to_string();
        let env = move |name: &str| (name == "HTTPS_PROXY").then(|| value.clone());
        build_kube_config(&sample_profile("a"), &with_token("a"), &env)
    }

    /// Имя и пароль прокси доходят до клиента: kube отправит их как `Proxy-Authorization: Basic`.
    #[test]
    fn proxy_credentials_reach_the_client() {
        let c = config_with_env_proxy("http://user:pass@proxy:3128").unwrap();
        let uri = c.proxy_url.unwrap();
        assert_eq!(uri.authority().unwrap().as_str(), "user:pass@proxy:3128");
    }

    /// Плохой адрес прокси даёт `badProxyUrl`, а пароль не попадает ни в параметр, ни в текст.
    #[test]
    fn bad_proxy_url_is_reported_without_credentials() {
        for (raw, shown) in [
            ("socks5://user:pass@proxy:1080", "socks5://proxy:1080"),
            ("http://user:pa ss@proxy:3128", "http://proxy:3128"),
            ("user:pass@proxy:3128", "proxy:3128"),
            ("http://user:pass@pro xy:3128", "http://pro xy:3128"),
        ] {
            let e = config_with_env_proxy(raw).unwrap_err();
            assert_eq!(
                (e.code, e.key.as_str()),
                (ErrorCode::InvalidInput, "errors.connections.badProxyUrl"),
                "{raw}"
            );
            assert_eq!(e.params["url"], shown, "{raw}");
            let json = serde_json::to_string(&e).unwrap();
            for secret in ["pass", "pa ss", "user"] {
                assert!(!json.contains(secret), "{raw}: {json}");
            }
        }
    }

    /// Принимаются только `http` и `https`: остальные схемы kube всё равно отвергнет,
    /// но уже с полным адресом в тексте.
    #[test]
    fn only_http_and_https_proxies_are_accepted() {
        let mut p = sample_profile("a");
        p.proxy = ProxyConfig::Url {
            url: "ftp://proxy:21".into(),
        };
        let e = build_kube_config(&p, &with_token("a"), &no_env).unwrap_err();
        assert_eq!(
            (e.code, e.key.as_str()),
            (ErrorCode::InvalidInput, "errors.connections.badProxyUrl")
        );
        for url in ["http://proxy:3128", "https://proxy:3128"] {
            p.proxy = ProxyConfig::Url { url: url.into() };
            assert!(
                build_kube_config(&p, &with_token("a"), &no_env).is_ok(),
                "{url}"
            );
        }
    }

    /// Встроенный повтор kube (429/503/504 до 15 раз) скрыл бы классификацию ошибок и
    /// повторял бы изменяющие запросы.
    #[test]
    fn builtin_retry_is_disabled() {
        let c = build_kube_config(&sample_profile("a"), &with_token("a"), &no_env).unwrap();
        assert!(!c.default_retry);
    }
}
