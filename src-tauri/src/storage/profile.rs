//! Профиль подключения к кластеру Kubernetes (спец. §5.1). Токен и ключ клиентского сертификата в
//! профиле не хранятся, они лежат в keychain (`secrets::TOKEN`, `secrets::CLIENT_KEY`); секретные
//! значения exec-плагина остаются в профиле, как в kubeconfig (`ExecAuth`).

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
    /// Exec-плагин kubeconfig из списка разрешённых (спец. §5.2); учётные данные выдаёт плагин,
    /// в keychain ничего не хранится.
    Exec(ExecAuth),
}

/// Exec-плагин: что и как запускать. Секретные значения (переменные окружения вроде
/// `AWS_SECRET_ACCESS_KEY`, аргументы вроде `--oidc-client-secret`) остаются в профиле, как в
/// kubeconfig, откуда плагин обычно приходит; интерфейс их скрывает, экспорт не пишет, а `Debug`
/// не печатает.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecAuth {
    /// Абсолютный путь к команде: имя из kubeconfig разрешается при импорте или кнопкой «Найти»
    /// в форме — у приложения из Dock нет PATH оболочки.
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: Vec<ExecEnv>,
    /// `client.authentication.k8s.io/v1` или `v1beta1`.
    #[serde(default = "default_exec_api_version")]
    pub api_version: String,
    /// Передать плагину адрес и CA кластера (`provideClusterInfo` в kubeconfig).
    #[serde(default)]
    pub provide_cluster_info: bool,
    /// Отпечаток строки запуска, которую подтвердил человек (`auth::exec::fingerprint`);
    /// изменённая строка требует нового подтверждения. Ставят только сохранение из формы
    /// и импорт kubeconfig по явному согласию. Импорт файла обмена подтверждения не ставит:
    /// новое подключение приходит неподтверждённым, а у существующего прежнее остаётся, только
    /// пока строка запуска (с локальными секретными значениями) не изменилась.
    #[serde(default)]
    pub approved: Option<String>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecEnv {
    pub name: String,
    pub value: String,
}

/// Вместо секретных значений `Debug` печатает это.
pub(crate) const REDACTED: &str = "***";

/// Значение переменной с секретным именем не печатается, чтобы не попасть в лог или сообщение
/// об ошибке.
impl std::fmt::Debug for ExecEnv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = if crate::auth::exec::is_secret_name(&self.name) {
            REDACTED
        } else {
            self.value.as_str()
        };
        f.debug_struct("ExecEnv")
            .field("name", &self.name)
            .field("value", &value)
            .finish()
    }
}

/// Секретные значения аргументов и (через `ExecEnv`) переменных окружения не печатаются.
impl std::fmt::Debug for ExecAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecAuth")
            .field("command", &self.command)
            .field(
                "args",
                &crate::auth::exec::mask_secret_args(&self.args, REDACTED),
            )
            .field("env", &self.env)
            .field("api_version", &self.api_version)
            .field("provide_cluster_info", &self.provide_cluster_info)
            .field("approved", &self.approved)
            .finish()
    }
}

pub const EXEC_API_V1: &str = "client.authentication.k8s.io/v1";
pub const EXEC_API_V1BETA1: &str = "client.authentication.k8s.io/v1beta1";

fn default_exec_api_version() -> String {
    EXEC_API_V1.to_string()
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

/// Диапазоны PEM-текста, занятые блоками с приватным ключом: тег содержит `PRIVATE KEY`
/// (`PRIVATE KEY`, `RSA PRIVATE KEY`, `EC PRIVATE KEY`, `ENCRYPTED PRIVATE KEY`). Блок идёт от
/// `-----BEGIN` до конца `-----END …-----`, незавершённый — до конца текста. Разбор не зависит
/// от того, корректно ли тело блока: ключ находится и в повреждённом PEM.
fn private_key_ranges(text: &str) -> Vec<std::ops::Range<usize>> {
    const BEGIN: &str = "-----BEGIN ";
    const END: &str = "-----END ";
    const DASHES: &str = "-----";
    let mut ranges = Vec::new();
    let mut pos = 0;
    while let Some(found) = text[pos..].find(BEGIN) {
        let start = pos + found;
        let label_start = start + BEGIN.len();
        let Some(label_len) = text[label_start..].find(DASHES) else {
            break;
        };
        let body = label_start + label_len + DASHES.len();
        if !text[label_start..label_start + label_len].contains("PRIVATE KEY") {
            // Следующий блок ищем сразу за заголовком этого: так не пропускается ключ,
            // стоящий внутри незавершённого блока другого вида.
            pos = body;
            continue;
        }
        let end = text[body..]
            .find(END)
            .and_then(|e| {
                let tail = body + e + END.len();
                text[tail..].find(DASHES).map(|d| tail + d + DASHES.len())
            })
            .unwrap_or(text.len());
        ranges.push(start..end);
        pos = end;
    }
    ranges
}

/// В PEM-тексте есть блок с приватным ключом (секреты хранятся только в keychain, спец. §5.1).
pub fn pem_has_private_key(text: &str) -> bool {
    !private_key_ranges(text).is_empty()
}

/// PEM-текст без блоков с приватным ключом; остальное (сертификаты) остаётся как есть.
pub fn strip_private_keys(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut pos = 0;
    for range in private_key_ranges(text) {
        out.push_str(&text[pos..range.start]);
        pos = range.end;
    }
    out.push_str(&text[pos..]);
    out
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

/// В адресе есть имя пользователя или пароль (`https://user:pass@host`).
fn has_userinfo(u: &url::Url) -> bool {
    !u.username().is_empty() || u.password().is_some()
}

/// Адрес без имени и пароля и без завершающего «/» — для текста ошибки.
pub(crate) fn without_userinfo(u: &url::Url) -> String {
    let mut clean = u.clone();
    // Ошибка возможна только для адреса без хоста; вызывающий код такие адреса сюда не передаёт.
    let _ = clean.set_username("");
    let _ = clean.set_password(None);
    clean.as_str().trim_end_matches('/').to_string()
}

/// Адрес для текста ошибки без имени и пароля: ни один адрес, который вводил человек или который
/// читается из профиля и окружения, не показывается как есть. Если адрес не разбирается как URL
/// с хостом (нет схемы, пробел в хосте, пустой хост), отбрасывается всё до последней «@».
pub(crate) fn redact_url(raw: &str) -> String {
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

/// Ошибка «учётные данные в адресе»; в тексте адрес показан без имени и пароля.
fn credentials_in_url(u: &url::Url) -> AppError {
    invalid(ltext!(
        "errors.connections.credentialsInUrl",
        "Remove the user name and password from “{{url}}”: credentials do not belong in the address (a proxy with a password is set via the system HTTPS_PROXY)",
        url = without_userinfo(u)
    ))
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
    let bad_api_url = || {
        invalid(ltext!(
            "errors.connections.badApiUrl",
            "Invalid API server address “{{url}}”",
            url = redact_url(&p.api_url)
        ))
    };
    let url = url::Url::parse(p.api_url.trim()).map_err(|_| bad_api_url())?;
    if url.query().is_some() || url.fragment().is_some() {
        return Err(bad_api_url());
    }
    if url.scheme() != "https" || url.host_str().is_none_or(str::is_empty) {
        return Err(invalid(ltext!(
            "errors.connections.httpsRequired",
            "The API server address must start with https:// and contain a host"
        )));
    }
    if has_userinfo(&url) {
        return Err(credentials_in_url(&url));
    }
    if matches!(&p.auth, AuthConfig::ClientCert { certificate_pem } if pem_has_private_key(certificate_pem))
        || matches!(&p.ca, CaConfig::Pem { pem } if pem_has_private_key(pem))
    {
        return Err(invalid(ltext!(
            "errors.connections.pemContainsKey",
            "A certificate field contains a private key — put the key into the key field"
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
    if let AuthConfig::Exec(e) = &p.auth {
        validate_exec(e)?;
    }
    if let ProxyConfig::Url { url } = &p.proxy {
        let proxy = url::Url::parse(url.trim())
            .ok()
            .filter(|u| matches!(u.scheme(), "http" | "https") && u.host_str().is_some());
        let Some(proxy) = proxy else {
            return Err(invalid(ltext!(
                "errors.connections.badProxyUrl",
                "Invalid proxy address “{{url}}”: use http://host:port",
                url = redact_url(url)
            )));
        };
        if has_userinfo(&proxy) {
            return Err(credentials_in_url(&proxy));
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

/// Exec-плагин: абсолютный путь, команда из списка разрешённых, известная версия API,
/// корректные имена переменных окружения и ни одной запрещённой (`auth::exec::is_forbidden_env`).
fn validate_exec(e: &ExecAuth) -> AppResult<()> {
    if e.command.trim().is_empty() {
        return Err(invalid(ltext!(
            "errors.exec.commandMissing",
            "Specify the exec plugin command"
        )));
    }
    if !std::path::Path::new(&e.command).is_absolute() {
        return Err(invalid(crate::auth::exec::command_not_absolute(&e.command)));
    }
    crate::auth::exec::check_allowed(&e.command, &e.args).map_err(invalid)?;
    if e.api_version != EXEC_API_V1 && e.api_version != EXEC_API_V1BETA1 {
        return Err(invalid(ltext!(
            "errors.exec.badApiVersion",
            "Unsupported exec plugin API version “{{version}}”: use {{expected}}",
            version = e.api_version,
            expected = EXEC_API_V1
        )));
    }
    if let Some(v) = e
        .env
        .iter()
        .find(|v| v.name.trim().is_empty() || v.name.contains('=') || v.name.contains('\0'))
    {
        return Err(invalid(ltext!(
            "errors.exec.badEnvName",
            "Invalid environment variable name “{{name}}”",
            name = v.name
        )));
    }
    crate::auth::exec::check_env(&e.env).map_err(invalid)?;
    Ok(())
}

/// Exec-плагин для тестов: `kubectl oidc-login get-token` по абсолютному пути.
#[cfg(test)]
pub fn sample_exec() -> ExecAuth {
    ExecAuth {
        command: if cfg!(windows) {
            r"C:\tools\kubectl.exe".into()
        } else {
            "/usr/local/bin/kubectl".into()
        },
        args: vec![
            "oidc-login".into(),
            "get-token".into(),
            "--oidc-issuer-url=https://sso.corp/realms/k8s".into(),
        ],
        env: vec![ExecEnv {
            name: "HTTPS_PROXY".into(),
            value: "http://proxy:3128".into(),
        }],
        api_version: EXEC_API_V1.into(),
        provide_cluster_info: false,
        approved: None,
    }
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
    fn exec_json_shape() {
        let mut p = sample_profile("x");
        p.auth = AuthConfig::Exec(sample_exec());
        let v = serde_json::to_value(&p).unwrap();
        assert_eq!(v["auth"]["method"], "exec");
        assert_eq!(v["auth"]["args"][0], "oidc-login");
        assert_eq!(
            v["auth"]["env"][0],
            serde_json::json!({"name": "HTTPS_PROXY", "value": "http://proxy:3128"})
        );
        assert_eq!(v["auth"]["apiVersion"], EXEC_API_V1);
        assert_eq!(v["auth"]["approved"], serde_json::Value::Null);
        assert!(!p.auth.uses_token());
        let back: ConnectionProfile = serde_json::from_value(v).unwrap();
        assert_eq!(back, p);
        // Минимальный exec из более старого файла: версия API по умолчанию.
        let a: AuthConfig =
            serde_json::from_value(serde_json::json!({"method": "exec", "command": "/bin/oc"}))
                .unwrap();
        let AuthConfig::Exec(e) = a else {
            panic!("exec")
        };
        assert_eq!((e.api_version.as_str(), e.args.len()), (EXEC_API_V1, 0));
    }

    /// `{:?}` не печатает секретные значения: ни в переменных окружения, ни в аргументах; на диск
    /// (serde) они при этом пишутся как есть.
    #[test]
    fn exec_debug_hides_secret_values() {
        let mut e = sample_exec();
        e.env.push(ExecEnv {
            name: "AWS_SECRET_ACCESS_KEY".into(),
            value: "s3cr3t".into(),
        });
        e.args.extend([
            "--oidc-client-secret=top-secret".to_string(),
            "--client-secret".to_string(),
            "pair-secret".to_string(),
        ]);
        let mut p = sample_profile("x");
        p.auth = AuthConfig::Exec(e.clone());
        let texts = [
            format!("{e:?}"),
            format!("{:#?}", e),
            format!("{p:?}"),
            format!("{:?}", e.env),
            format!("{:?}", e.env[1]),
        ];
        for text in &texts {
            for secret in ["s3cr3t", "top-secret", "pair-secret"] {
                assert!(!text.contains(secret), "{secret} в {text}");
            }
        }
        let text = &texts[0];
        assert!(text.contains("AWS_SECRET_ACCESS_KEY"), "{text}");
        assert!(text.contains("http://proxy:3128"), "{text}");
        assert!(
            text.contains("--oidc-issuer-url=https://sso.corp/realms/k8s"),
            "{text}"
        );
        assert!(text.contains("--oidc-client-secret=***"), "{text}");
        let json = serde_json::to_string(&e).unwrap();
        assert!(json.contains("s3cr3t") && json.contains("pair-secret"));
    }

    type ExecChange = Box<dyn Fn(&mut ExecAuth)>;

    #[test]
    fn exec_validation() {
        let ok = |e: ExecAuth| {
            let mut p = sample_profile("a");
            p.auth = AuthConfig::Exec(e);
            validate_profile(&p)
        };
        assert!(ok(sample_exec()).is_ok());
        let cases: Vec<(&str, ExecChange)> = vec![
            (
                "errors.exec.commandMissing",
                Box::new(|e| e.command = " ".into()),
            ),
            (
                "errors.exec.commandNotAbsolute",
                Box::new(|e| e.command = "kubectl".into()),
            ),
            (
                "errors.exec.argsNotAllowed",
                Box::new(|e| e.args = vec!["get".into(), "pods".into()]),
            ),
            (
                "errors.exec.notAllowed",
                Box::new(|e| {
                    e.command = if cfg!(windows) {
                        r"C:\Windows\cmd.exe".into()
                    } else {
                        "/bin/sh".into()
                    }
                }),
            ),
            (
                "errors.exec.badApiVersion",
                Box::new(|e| e.api_version = "client.authentication.k8s.io/v1alpha1".into()),
            ),
            (
                "errors.exec.badEnvName",
                Box::new(|e| {
                    e.env = vec![ExecEnv {
                        name: "A=B".into(),
                        value: "x".into(),
                    }]
                }),
            ),
            (
                "errors.exec.optionBeforePlugin",
                Box::new(|e| {
                    e.args = [
                        "--insecure-skip-tls-verify",
                        "exec",
                        "oidc-login",
                        "get-token",
                    ]
                    .map(String::from)
                    .to_vec()
                }),
            ),
        ];
        for (key, change) in cases {
            let mut e = sample_exec();
            change(&mut e);
            let err = ok(e).unwrap_err();
            assert_eq!((err.code, err.key.as_str()), (ErrorCode::InvalidInput, key));
        }
        // Переменные загрузчика и интерпретаторов: строку с ними нельзя сохранить.
        for name in [
            "PATH",
            "LD_PRELOAD",
            "DYLD_INSERT_LIBRARIES",
            "NODE_OPTIONS",
        ] {
            let mut e = sample_exec();
            e.env.push(ExecEnv {
                name: name.into(),
                value: "/tmp/evil".into(),
            });
            let err = ok(e).unwrap_err();
            assert_eq!(
                (err.code, err.key.as_str(), err.params["name"].as_str()),
                (ErrorCode::InvalidInput, "errors.exec.envForbidden", name)
            );
        }
        let mut e = sample_exec();
        e.env.push(ExecEnv {
            name: "AWS_PROFILE".into(),
            value: "dev".into(),
        });
        assert!(ok(e).is_ok(), "настройка самой утилиты разрешена");
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

    /// Сертификат и приватный ключ в одном PEM — то, что получается из `cat client.crt client.key`.
    fn cert_with_key() -> String {
        format!("{TEST_CERT_PEM}\n{}", include_str!("testdata/test-key.pem"))
    }

    fn assert_invalid(p: &ConnectionProfile, key: &str) -> AppError {
        let e = validate_profile(p).unwrap_err();
        assert_eq!((e.code, e.key.as_str()), (ErrorCode::InvalidInput, key));
        e
    }

    #[test]
    fn private_key_in_a_certificate_field_is_rejected() {
        for tag in [
            "PRIVATE KEY",
            "RSA PRIVATE KEY",
            "EC PRIVATE KEY",
            "ENCRYPTED PRIVATE KEY",
        ] {
            let combined =
                format!("{TEST_CERT_PEM}\n-----BEGIN {tag}-----\nAAAA\n-----END {tag}-----\n");
            let mut p = sample_profile("a");
            p.auth = AuthConfig::ClientCert {
                certificate_pem: combined.clone(),
            };
            assert_invalid(&p, "errors.connections.pemContainsKey");
            let mut p = sample_profile("a");
            p.ca = CaConfig::Pem { pem: combined };
            assert_invalid(&p, "errors.connections.pemContainsKey");
        }
        let mut p = sample_profile("a");
        p.auth = AuthConfig::ClientCert {
            certificate_pem: cert_with_key(),
        };
        let e = assert_invalid(&p, "errors.connections.pemContainsKey");
        assert_eq!(
            e.message,
            "A certificate field contains a private key — put the key into the key field"
        );
        // Ключ раньше сертификата и ключ без сертификата — тоже ключ, а не «неверный PEM».
        p.auth = AuthConfig::ClientCert {
            certificate_pem: format!("{}\n{TEST_CERT_PEM}", include_str!("testdata/test-key.pem")),
        };
        assert_invalid(&p, "errors.connections.pemContainsKey");
        p.auth = AuthConfig::ClientCert {
            certificate_pem: include_str!("testdata/test-key.pem").into(),
        };
        assert_invalid(&p, "errors.connections.pemContainsKey");
        // Обычные сертификаты проходят по-прежнему.
        p.auth = AuthConfig::ClientCert {
            certificate_pem: TEST_CERT_PEM.into(),
        };
        p.ca = CaConfig::Pem {
            pem: format!("{TEST_CERT_PEM}\n{TEST_CERT_PEM}"),
        };
        assert!(validate_profile(&p).is_ok());
    }

    #[test]
    fn credentials_in_urls_are_rejected_without_echoing_them() {
        for url in [
            "https://user:s3cr3t@api.c:6443",
            "https://user@api.c:6443",
            "https://:s3cr3t@api.c:6443/k8s/clusters/c-x1",
        ] {
            let mut p = sample_profile("a");
            p.api_url = url.into();
            let e = assert_invalid(&p, "errors.connections.credentialsInUrl");
            assert!(e.params["url"].starts_with("https://api.c:6443"), "{url}");
            let all = serde_json::to_string(&e).unwrap();
            assert!(!all.contains("s3cr3t") && !all.contains('@'), "{all}");
        }
        for (url, shown) in [
            ("http://u:s3cr3t@proxy:3128", "http://proxy:3128"),
            ("https://u@proxy:3128/", "https://proxy:3128"),
        ] {
            let mut p = sample_profile("a");
            p.proxy = ProxyConfig::Url { url: url.into() };
            let e = assert_invalid(&p, "errors.connections.credentialsInUrl");
            assert_eq!(e.params["url"], shown, "{url}");
            let all = serde_json::to_string(&e).unwrap();
            assert!(!all.contains("s3cr3t") && !all.contains('@'), "{all}");
        }
        let mut p = sample_profile("a");
        p.api_url = "https://u:s3cr3t@api.c:6443".into();
        let e = validate_profile(&p).unwrap_err();
        assert_eq!(
            e.message,
            "Remove the user name and password from “https://api.c:6443”: credentials do not belong in the address (a proxy with a password is set via the system HTTPS_PROXY)"
        );
        // «@» в пути — не учётные данные; адрес без них проходит.
        p.api_url = "https://api.c:6443/k8s/clusters/c-x@1".into();
        p.proxy = ProxyConfig::Url {
            url: "http://proxy.corp:3128".into(),
        };
        assert!(validate_profile(&p).is_ok());
    }

    /// Адрес, не прошедший проверку, показывается в ошибке без имени и пароля — для адреса API
    /// и прокси, при любой причине отказа (не разбирается, запрос, чужая схема, нет хоста).
    #[test]
    fn invalid_addresses_are_reported_without_passwords() {
        let api = [
            (
                "https://u:s3cr3t@api.c:6443/k8s?x=1",
                "https://api.c:6443/k8s?x=1",
            ),
            (
                "https://u:s3cr3t@api.c:6443/k8s#f",
                "https://api.c:6443/k8s#f",
            ),
            ("https://u:s3cr3t@", "https://"),
            ("https://u:s3cr3t@api c:6443", "https://api c:6443"),
        ];
        for (url, shown) in api {
            let mut p = sample_profile("a");
            p.api_url = url.into();
            let e = assert_invalid(&p, "errors.connections.badApiUrl");
            assert_eq!(e.params["url"], shown, "{url}");
            let all = serde_json::to_string(&e).unwrap();
            assert!(
                !all.contains("s3cr3t") && !all.contains('@'),
                "{url}: {all}"
            );
        }
        let proxy = [
            ("socks://u:s3cr3t@proxy:1080", "socks://proxy:1080"),
            ("u:s3cr3t@proxy:1080", "proxy:1080"),
            ("http://u:s3cr3t@pro xy:3128", "http://pro xy:3128"),
            ("http://u:s3cr3t@", "http://"),
        ];
        for (url, shown) in proxy {
            let mut p = sample_profile("a");
            p.proxy = ProxyConfig::Url { url: url.into() };
            let e = assert_invalid(&p, "errors.connections.badProxyUrl");
            assert_eq!(e.params["url"], shown, "{url}");
            let all = serde_json::to_string(&e).unwrap();
            assert!(
                !all.contains("s3cr3t") && !all.contains('@'),
                "{url}: {all}"
            );
        }
        // Без пароля адрес показывается как есть.
        let mut p = sample_profile("a");
        p.proxy = ProxyConfig::Url {
            url: "socks://proxy:1080".into(),
        };
        let e = assert_invalid(&p, "errors.connections.badProxyUrl");
        assert_eq!(e.params["url"], "socks://proxy:1080");
    }

    #[test]
    fn strip_private_keys_keeps_only_the_certificates() {
        let key = include_str!("testdata/test-key.pem");
        let body = key.lines().nth(1).unwrap();
        let want = pem_certificates(TEST_CERT_PEM).unwrap();
        for text in [
            cert_with_key(),
            format!("{key}\n{TEST_CERT_PEM}"),
            format!("{TEST_CERT_PEM}\n-----BEGIN EC PRIVATE KEY-----\nAAAA\n"),
            format!("{TEST_CERT_PEM}\n-----BEGIN RSA PRIVATE KEY-----\nAAAA\n-----END"),
            format!(
                "{TEST_CERT_PEM}\n{key}\n-----BEGIN ENCRYPTED PRIVATE KEY-----\nBBBB\n-----END ENCRYPTED PRIVATE KEY-----\n"
            ),
        ] {
            assert!(pem_has_private_key(&text), "{text}");
            let stripped = strip_private_keys(&text);
            assert!(
                !stripped.contains("PRIVATE KEY") && !stripped.contains(body),
                "{stripped}"
            );
            assert_eq!(pem_certificates(&stripped).unwrap(), want, "{stripped}");
        }
        // Без ключа текст не меняется ни на байт.
        let plain = format!("# CA\n{TEST_CERT_PEM}\n{TEST_CERT_PEM}");
        assert!(!pem_has_private_key(&plain));
        assert_eq!(strip_private_keys(&plain), plain);
        // Ключ внутри незавершённого блока другого вида тоже находится и вырезается.
        let nested = format!("-----BEGIN CERTIFICATE-----\nAAAA\n{key}");
        assert!(pem_has_private_key(&nested));
        assert!(!strip_private_keys(&nested).contains(body));
    }
}
