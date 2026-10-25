//! Профиль подключения к кластеру Kubernetes (спец. §5.1). Секреты в профиле не хранятся: токен и ключ
//! клиентского сертификата лежат в keychain (`secrets::TOKEN`, `secrets::CLIENT_KEY`).

use serde::{Deserialize, Serialize};

use crate::error::{AppError, AppResult, ErrorCode};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Environment {
    Local,
    Dev,
    Test,
    Stage,
    Prod,
}

/// Как Glafira входит в кластер (спец. §5.2).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "method", rename_all = "camelCase")]
pub enum AuthConfig {
    /// Токен вставлен вручную (`oc whoami -t`, токен ServiceAccount).
    Token,
    /// Вход через браузер в OAuth OpenShift; полученный токен хранится так же, как у `Token`.
    Browser,
    /// Клиентский сертификат: сертификат (PEM) — в профиле, ключ — в keychain.
    #[serde(rename_all = "camelCase")]
    ClientCert { certificate_pem: String },
}

impl AuthConfig {
    /// Токен хранится в keychain (`Token` и `Browser`).
    pub fn uses_token(&self) -> bool {
        matches!(self, AuthConfig::Token | AuthConfig::Browser)
    }
}

/// Чему доверять при проверке сертификата API-сервера.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "camelCase")]
pub enum CaConfig {
    /// Системное хранилище сертификатов ОС.
    System,
    /// Только перечисленные сертификаты (один или несколько PEM), как `certificate-authority`
    /// в kubeconfig: системные корни при этом не используются.
    Pem { pem: String },
    /// Без проверки сертификата (`insecure-skip-tls-verify`); может быть запрещено политикой.
    Insecure,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "camelCase")]
pub enum ProxyConfig {
    /// Переменные окружения `HTTPS_PROXY` / `NO_PROXY`.
    System,
    None,
    Url {
        url: String,
    },
}

fn default_ca() -> CaConfig {
    CaConfig::System
}

fn default_proxy() -> ProxyConfig {
    ProxyConfig::System
}

fn default_request_timeout() -> u64 {
    30_000
}

fn default_connect_timeout() -> u64 {
    10_000
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionProfile {
    pub id: String,
    pub name: String,
    pub color: String,
    pub environment: Environment,
    pub read_only: bool,
    /// Адрес API-сервера: `https://api.<кластер>:6443`; допускается путь — например, прокси Rancher
    /// `https://rancher.example.com/k8s/clusters/c-xxxxx`.
    pub api_url: String,
    pub auth: AuthConfig,
    #[serde(default = "default_ca")]
    pub ca: CaConfig,
    /// Имя для проверки сертификата, если оно отличается от хоста адреса (`tls-server-name`).
    #[serde(default)]
    pub tls_server_name: Option<String>,
    #[serde(default = "default_proxy")]
    pub proxy: ProxyConfig,
    /// Избранные проекты: закрепляются наверху дерева.
    #[serde(default)]
    pub favorite_namespaces: Vec<String>,
    /// Проекты, которые показываются, если кластер не отдаёт список проектов.
    #[serde(default)]
    pub manual_namespaces: Vec<String>,
    #[serde(default = "default_request_timeout")]
    pub request_timeout_ms: u64,
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout_ms: u64,
    #[serde(default)]
    pub folder_id: Option<String>,
    #[serde(default)]
    pub order: i64,
    /// Поля из более новой версии Glafira: сохраняются при записи как есть.
    /// `ConnectionStore::upsert` входящий `extra` не принимает: у существующего профиля
    /// остаётся сохранённый, новый получает пустой.
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

/// Приводит поля к каноническому виду до проверки: адрес без пробелов и `/` на конце,
/// списки проектов без пустых строк и повторов.
pub fn normalize_profile(p: &mut ConnectionProfile) {
    p.api_url = p.api_url.trim().trim_end_matches('/').to_string();
    p.tls_server_name = p
        .tls_server_name
        .take()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    for list in [&mut p.favorite_namespaces, &mut p.manual_namespaces] {
        let mut seen = std::collections::HashSet::new();
        list.retain_mut(|n| {
            *n = n.trim().to_string();
            !n.is_empty() && seen.insert(n.clone())
        });
    }
}

/// Сертификаты из PEM-текста (DER). Ошибка — если текст не PEM или в нём нет ни одного сертификата.
pub fn pem_certificates(text: &str) -> Result<Vec<Vec<u8>>, String> {
    let blocks = pem::parse_many(text.as_bytes()).map_err(|e| e.to_string())?;
    let certs: Vec<Vec<u8>> = blocks
        .into_iter()
        .filter(|b| b.tag() == "CERTIFICATE")
        .map(|b| b.into_contents())
        .collect();
    if certs.is_empty() {
        return Err("no CERTIFICATE blocks".into());
    }
    Ok(certs)
}

/// Имя проекта (namespace) по правилам DNS-1123: строчные латинские буквы, цифры и `-`,
/// начало и конец — буква или цифра, не длиннее 63 символов.
pub fn is_valid_namespace(name: &str) -> bool {
    let bytes = name.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 63
        && bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-')
        && bytes[0] != b'-'
        && bytes[bytes.len() - 1] != b'-'
}

fn invalid(key_and_text: crate::i18n::LocalizedText) -> AppError {
    AppError::localized(ErrorCode::InvalidInput, key_and_text)
}

pub fn validate_profile(p: &ConnectionProfile) -> AppResult<()> {
    if p.id.trim().is_empty() {
        return Err(invalid(ltext!(
            "errors.connections.noId",
            "The connection has no ID"
        )));
    }
    if p.name.trim().is_empty() {
        return Err(invalid(ltext!(
            "errors.connections.emptyName",
            "Connection name cannot be empty"
        )));
    }
    let url = url::Url::parse(p.api_url.trim()).map_err(|_| {
        invalid(ltext!(
            "errors.connections.badApiUrl",
            "Invalid API server address “{{url}}”",
            url = p.api_url
        ))
    })?;
    if url.query().is_some() || url.fragment().is_some() {
        return Err(invalid(ltext!(
            "errors.connections.badApiUrl",
            "Invalid API server address “{{url}}”",
            url = p.api_url
        )));
    }
    if url.scheme() != "https" || url.host_str().is_none_or(str::is_empty) {
        return Err(invalid(ltext!(
            "errors.connections.httpsRequired",
            "The API server address must start with https:// and contain a host"
        )));
    }
    if let AuthConfig::ClientCert { certificate_pem } = &p.auth
        && pem_certificates(certificate_pem).is_err()
    {
        return Err(invalid(ltext!(
            "errors.connections.certificatePemInvalid",
            "The client certificate must be in PEM format (-----BEGIN CERTIFICATE-----)"
        )));
    }
    if let CaConfig::Pem { pem } = &p.ca
        && pem_certificates(pem).is_err()
    {
        return Err(invalid(ltext!(
            "errors.connections.caPemInvalid",
            "The CA certificate must be in PEM format (-----BEGIN CERTIFICATE-----)"
        )));
    }
    if let ProxyConfig::Url { url } = &p.proxy {
        let ok = url::Url::parse(url.trim())
            .is_ok_and(|u| matches!(u.scheme(), "http" | "https") && u.host_str().is_some());
        if !ok {
            return Err(invalid(ltext!(
                "errors.connections.badProxyUrl",
                "Invalid proxy address “{{url}}”: use http://host:port",
                url = url
            )));
        }
    }
    if let Some(ns) = p
        .favorite_namespaces
        .iter()
        .chain(&p.manual_namespaces)
        .find(|n| !is_valid_namespace(n))
    {
        return Err(invalid(ltext!(
            "errors.connections.badNamespace",
            "“{{name}}” is not a valid project name: use lowercase letters, digits and “-”",
            name = ns
        )));
    }
    if p.request_timeout_ms < 1_000 || p.connect_timeout_ms < 1_000 {
        return Err(invalid(ltext!(
            "errors.connections.timeoutTooSmall",
            "Timeouts must be at least 1000 ms"
        )));
    }
    Ok(())
}

#[cfg(test)]
pub fn sample_profile(id: &str) -> ConnectionProfile {
    ConnectionProfile {
        id: id.to_string(),
        name: format!("conn-{id}"),
        color: "#4f8cff".into(),
        environment: Environment::Dev,
        read_only: false,
        api_url: "https://api.dev.example.com:6443".into(),
        auth: AuthConfig::Token,
        ca: CaConfig::System,
        tls_server_name: None,
        proxy: ProxyConfig::System,
        favorite_namespaces: Vec::new(),
        manual_namespaces: Vec::new(),
        request_timeout_ms: 30_000,
        connect_timeout_ms: 10_000,
        folder_id: None,
        order: 0,
        extra: Default::default(),
    }
}

/// Самоподписанный сертификат для тестов (CN=glafira-test).
#[cfg(test)]
pub const TEST_CERT_PEM: &str = include_str!("testdata/test-cert.pem");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_shape_matches_frontend_contract() {
        let mut p = sample_profile("x");
        p.auth = AuthConfig::ClientCert {
            certificate_pem: "PEM".into(),
        };
        p.ca = CaConfig::Pem { pem: "CA".into() };
        p.proxy = ProxyConfig::Url {
            url: "http://proxy:3128".into(),
        };
        let v = serde_json::to_value(&p).unwrap();
        assert_eq!(v["apiUrl"], "https://api.dev.example.com:6443");
        assert_eq!(
            v["auth"],
            serde_json::json!({"method": "clientCert", "certificatePem": "PEM"})
        );
        assert_eq!(v["ca"], serde_json::json!({"mode": "pem", "pem": "CA"}));
        assert_eq!(
            v["proxy"],
            serde_json::json!({"mode": "url", "url": "http://proxy:3128"})
        );
        assert_eq!(v["readOnly"], false);
        assert_eq!(v["requestTimeoutMs"], 30_000);
        assert_eq!(v["connectTimeoutMs"], 10_000);
        let back: ConnectionProfile = serde_json::from_value(v).unwrap();
        assert_eq!(back, p);
    }

    #[test]
    fn minimal_json_gets_defaults() {
        let p: ConnectionProfile = serde_json::from_value(serde_json::json!({
            "id": "a", "name": "A", "color": "#fff", "environment": "dev", "readOnly": false,
            "apiUrl": "https://api.c:6443", "auth": {"method": "browser"}
        }))
        .unwrap();
        assert_eq!(p.ca, CaConfig::System);
        assert_eq!(p.proxy, ProxyConfig::System);
        assert_eq!(p.auth, AuthConfig::Browser);
        assert!(p.auth.uses_token());
        assert_eq!(
            (p.request_timeout_ms, p.connect_timeout_ms),
            (30_000, 10_000)
        );
        assert!(p.favorite_namespaces.is_empty());
    }

    #[test]
    fn normalization_trims_and_dedups() {
        let mut p = sample_profile("a");
        p.api_url = " https://api.c:6443/ ".into();
        p.tls_server_name = Some("  ".into());
        p.favorite_namespaces = vec![" team-a ".into(), "".into(), "team-a".into(), "b".into()];
        normalize_profile(&mut p);
        assert_eq!(p.api_url, "https://api.c:6443");
        assert_eq!(p.tls_server_name, None);
        p.api_url = "https://rancher.corp/k8s/clusters/c-x1/ ".into();
        normalize_profile(&mut p);
        assert_eq!(
            p.api_url, "https://rancher.corp/k8s/clusters/c-x1",
            "путь сохраняется"
        );
        assert_eq!(p.favorite_namespaces, vec!["team-a", "b"]);
    }

    #[test]
    fn namespace_names() {
        for ok in ["a", "team-a", "a1", "0abc", &"a".repeat(63)] {
            assert!(is_valid_namespace(ok), "{ok}");
        }
        for bad in ["", "-a", "a-", "Team", "a_b", "a.b", &"a".repeat(64)] {
            assert!(!is_valid_namespace(bad), "{bad}");
        }
    }

    #[test]
    fn pem_certificates_need_a_certificate_block() {
        assert_eq!(pem_certificates(TEST_CERT_PEM).unwrap().len(), 1);
        let two = format!("{TEST_CERT_PEM}\n{TEST_CERT_PEM}");
        assert_eq!(pem_certificates(&two).unwrap().len(), 2);
        assert!(pem_certificates("not pem").is_err());
        assert!(
            pem_certificates("-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n")
                .is_err()
        );
    }

    type Change = Box<dyn Fn(&mut ConnectionProfile)>;

    #[test]
    fn validation_accepts_sample_and_rejects_bad_fields() {
        assert!(validate_profile(&sample_profile("a")).is_ok());
        let cases: Vec<(&str, Change)> = vec![
            ("errors.connections.noId", Box::new(|p| p.id = " ".into())),
            (
                "errors.connections.emptyName",
                Box::new(|p| p.name = "".into()),
            ),
            (
                "errors.connections.badApiUrl",
                Box::new(|p| p.api_url = "https://".into()),
            ),
            (
                "errors.connections.badApiUrl",
                Box::new(|p| p.api_url = "https://api.c:6443/k8s?cluster=x".into()),
            ),
            (
                "errors.connections.httpsRequired",
                Box::new(|p| p.api_url = "http://api.c:6443".into()),
            ),
            (
                "errors.connections.certificatePemInvalid",
                Box::new(|p| {
                    p.auth = AuthConfig::ClientCert {
                        certificate_pem: "x".into(),
                    }
                }),
            ),
            (
                "errors.connections.caPemInvalid",
                Box::new(|p| p.ca = CaConfig::Pem { pem: "x".into() }),
            ),
            (
                "errors.connections.badProxyUrl",
                Box::new(|p| {
                    p.proxy = ProxyConfig::Url {
                        url: "socks://x".into(),
                    }
                }),
            ),
            (
                "errors.connections.badNamespace",
                Box::new(|p| p.manual_namespaces = vec!["Bad".into()]),
            ),
            (
                "errors.connections.timeoutTooSmall",
                Box::new(|p| p.connect_timeout_ms = 10),
            ),
        ];
        for (key, change) in cases {
            let mut p = sample_profile("a");
            change(&mut p);
            let e = validate_profile(&p).unwrap_err();
            assert_eq!((e.code, e.key.as_str()), (ErrorCode::InvalidInput, key));
        }
        let mut p = sample_profile("a");
        p.auth = AuthConfig::ClientCert {
            certificate_pem: TEST_CERT_PEM.into(),
        };
        p.ca = CaConfig::Pem {
            pem: TEST_CERT_PEM.into(),
        };
        p.proxy = ProxyConfig::Url {
            url: "http://proxy.corp:3128".into(),
        };
        assert!(validate_profile(&p).is_ok());
        p.api_url = "https://rancher.corp/k8s/clusters/c-x1".into();
        assert!(
            validate_profile(&p).is_ok(),
            "адрес с путём (прокси Rancher)"
        );
    }

    #[test]
    fn profile_json_never_contains_secrets() {
        let v = serde_json::to_string(&sample_profile("a"))
            .unwrap()
            .to_lowercase();
        for word in ["\"token\":", "password", "clientkey"] {
            assert!(!v.contains(word), "{word}");
        }
    }
}
