//! Пошаговая проверка подключения (спец. §5.3): конфигурация → TCP → TLS → вход → discovery →
//! проекты. Останавливается на первом неуспешном шаге и называет его.

use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;

use crate::error::{AppError, AppResult, ErrorCode};
use crate::i18n::LocalizedText;
use crate::k8s::cluster::{Discovered, discover, list_project_names, whoami};
use crate::k8s::config::{build_kube_config, process_env, request_timeout};
use crate::k8s::errors::from_kube;
use crate::policy::PolicyStatus;
use crate::secrets::SecretStore;
use crate::storage::profile::{AuthConfig, CaConfig, ConnectionProfile};

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TestStep {
    pub name: &'static str,
    pub ok: bool,
    pub message: LocalizedText,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<AppError>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TestReport {
    pub ok: bool,
    pub steps: Vec<TestStep>,
}

fn passed(name: &'static str, message: LocalizedText) -> TestStep {
    TestStep {
        name,
        ok: true,
        message,
        error: None,
    }
}

fn failed_step(name: &'static str, e: AppError) -> TestStep {
    TestStep {
        name,
        ok: false,
        message: e.text(),
        error: Some(e),
    }
}

/// Запрещено ли подключение политикой (спец. §12).
pub fn ensure_allowed(p: &ConnectionProfile, policy: &PolicyStatus) -> AppResult<()> {
    if p.ca == CaConfig::Insecure && !policy.allow_insecure_tls {
        return Err(app_err!(
            ErrorCode::InvalidInput,
            "errors.connections.insecureForbidden",
            "Connections without TLS certificate verification are forbidden by your organization"
        ));
    }
    if matches!(p.auth, AuthConfig::Exec(_)) && !policy.allow_exec_auth {
        return Err(app_err!(
            ErrorCode::InvalidInput,
            "errors.connections.execForbidden",
            "Connections with exec plugins are forbidden by your organization"
        ));
    }
    Ok(())
}

/// Куда открыть TCP: прокси, если он задан, иначе API-сервер (порт по умолчанию — 443).
pub fn tcp_target(config: &kube::Config) -> (String, u16, bool) {
    let (uri, via_proxy) = match &config.proxy_url {
        Some(p) => (p, true),
        None => (&config.cluster_url, false),
    };
    let host = uri
        .host()
        .unwrap_or("")
        .trim_start_matches('[')
        .trim_end_matches(']');
    let port = uri
        .port_u16()
        .unwrap_or(if uri.scheme_str() == Some("http") {
            80
        } else {
            443
        });
    (host.to_string(), port, via_proxy)
}

/// TCP-соединение с `host:port` за `limit`. Возвращает адрес, с которым соединились.
pub async fn tcp_probe(host: &str, port: u16, limit: Duration) -> AppResult<String> {
    let addrs: Vec<_> =
        match tokio::time::timeout(limit, tokio::net::lookup_host((host, port))).await {
            Ok(Ok(a)) => a.collect(),
            Ok(Err(e)) => {
                return Err(app_err!(
                    ErrorCode::ConnectionFailed,
                    "errors.k8s.dns",
                    "Cannot resolve the server host name"
                )
                .with_details(e.to_string()));
            }
            Err(_) => return Err(crate::k8s::errors::timeout()),
        };
    let mut last = None;
    for addr in addrs {
        match tokio::time::timeout(limit, tokio::net::TcpStream::connect(addr)).await {
            Ok(Ok(_)) => return Ok(addr.to_string()),
            Ok(Err(e)) => last = Some(e.to_string()),
            Err(_) => last = Some("timed out".into()),
        }
    }
    Err(app_err!(
        ErrorCode::ConnectionFailed,
        "errors.k8s.connect",
        "Cannot connect to the server"
    )
    .with_details(last.unwrap_or_else(|| format!("{host}: no addresses"))))
}

/// Платформа, версия API-сервера и найденные группы API OpenShift и Gateway API
/// (названия видов не переводятся).
fn discovery_message(d: &Discovered) -> LocalizedText {
    let g = &d.api_groups;
    let found: Vec<&str> = [
        (g.routes, "Route"),
        (g.projects, "Project"),
        (g.deployment_configs, "DeploymentConfig"),
        (g.gateway_api, "Gateway API"),
    ]
    .into_iter()
    .filter_map(|(has, name)| has.then_some(name))
    .collect();
    if found.is_empty() {
        ltext!(
            "messages.connectionTest.discovery",
            "{{platform}} · API server {{version}}",
            platform = d.platform.label,
            version = d.server_version
        )
    } else {
        ltext!(
            "messages.connectionTest.discoveryGroups",
            "{{platform}} · API server {{version}} · found: {{groups}}",
            platform = d.platform.label,
            version = d.server_version,
            groups = found.join(", ")
        )
    }
}

/// Ошибка транспорта — до ответа API-сервера дело не дошло (TLS, DNS, соединение, таймаут,
/// сеть). Различаем по ключу, а не по коду: `k8s::errors` отдаёт те же коды и для HTTP-ответов
/// (429 — `Timeout`, 5xx — `ConnectionFailed`, прочие 4xx — `Internal`), а ответ сервера
/// доказывает, что TLS-рукопожатие прошло.
fn is_transport_failure(e: &AppError) -> bool {
    e.code == ErrorCode::TlsError
        || matches!(
            e.key.as_str(),
            "errors.k8s.dns" | "errors.k8s.connect" | "errors.k8s.timeout" | "errors.k8s.transport"
        )
}

/// Шаги после TCP на готовом клиенте: TLS и вход (одним запросом «кто я»), discovery, проекты.
pub async fn check_cluster(
    steps: &mut Vec<TestStep>,
    client: &kube::Client,
    p: &ConnectionProfile,
) -> bool {
    let limit = request_timeout(p);
    let tls_ok = if p.ca == CaConfig::Insecure {
        ltext!(
            "messages.connectionTest.tlsInsecure",
            "The server certificate is NOT verified (insecure mode)"
        )
    } else {
        ltext!(
            "messages.connectionTest.tlsOk",
            "The server certificate is trusted"
        )
    };
    match whoami(client, limit).await {
        Ok((user, _)) => {
            steps.push(passed("tls", tls_ok));
            steps.push(passed(
                "auth",
                ltext!(
                    "messages.connectionTest.authOk",
                    "Signed in as {{user}}",
                    user = user
                ),
            ));
        }
        // Ошибка транспорта: ответа сервера не было, значит, не прошёл TLS (или связь).
        Err(e) if is_transport_failure(&e) => {
            steps.push(failed_step("tls", e));
            return false;
        }
        // Любой ответ сервера (401, 403, 404, 400/422 или не-API страница) доказывает, что
        // TLS-рукопожатие прошло, — виноват вход.
        Err(e) => {
            steps.push(passed("tls", tls_ok));
            steps.push(failed_step("auth", e));
            return false;
        }
    }
    let projects_api = match discover(client, limit).await {
        Ok(d) => {
            steps.push(passed("discovery", discovery_message(&d)));
            d.api_groups.projects
        }
        Err(e) => {
            steps.push(failed_step("discovery", e));
            return false;
        }
    };
    match list_project_names(client, limit, projects_api).await {
        Ok((names, more)) => {
            let count = if more {
                format!("{}+", names.len())
            } else {
                names.len().to_string()
            };
            steps.push(passed(
                "projects",
                ltext!(
                    "messages.connectionTest.projectsOk",
                    "Available projects: {{count}}",
                    count = count
                ),
            ));
            true
        }
        Err(e) if e.code == ErrorCode::Forbidden && !p.manual_namespaces.is_empty() => {
            steps.push(passed(
                "projects",
                ltext!(
                    "messages.connectionTest.projectsManual",
                    "The cluster does not list projects for you; projects from the profile: {{count}}",
                    count = p.manual_namespaces.len()
                ),
            ));
            true
        }
        Err(e) => {
            steps.push(failed_step("projects", e));
            false
        }
    }
}

/// Проверяет и несохранённый профиль: секреты берутся из `secrets` (поверх keychain — форма).
pub async fn test_connection(
    p: &ConnectionProfile,
    secrets: Arc<dyn SecretStore>,
    policy: &PolicyStatus,
) -> TestReport {
    let mut steps = Vec::new();
    let profile = p.clone();
    let config = match ensure_allowed(p, policy) {
        Err(e) => Err(e),
        Ok(()) => tokio::task::spawn_blocking(move || {
            crate::storage::profile::validate_profile(&profile)?;
            build_kube_config(&profile, secrets.as_ref(), &process_env)
        })
        .await
        .unwrap_or_else(|e| Err(AppError::internal_with(e.to_string()))),
    };
    let config = match config {
        Ok(c) => c,
        Err(e) => return report(vec![failed_step("config", e)]),
    };
    steps.push(passed(
        "config",
        ltext!("messages.connectionTest.configOk", "Configuration is valid"),
    ));

    let (host, port, via_proxy) = tcp_target(&config);
    let connect_limit = Duration::from_millis(p.connect_timeout_ms);
    match tcp_probe(&host, port, connect_limit).await {
        Ok(address) if via_proxy => steps.push(passed(
            "tcp",
            ltext!(
                "messages.connectionTest.tcpProxyOk",
                "TCP connection to the proxy {{address}} established",
                address = address
            ),
        )),
        Ok(address) => steps.push(passed(
            "tcp",
            ltext!(
                "messages.connectionTest.tcpOk",
                "TCP connection to {{address}} established",
                address = address
            ),
        )),
        Err(e) => {
            steps.push(failed_step("tcp", e));
            return report(steps);
        }
    }

    let client = match kube::Client::try_from(config) {
        Ok(c) => c,
        Err(e) => {
            steps.push(failed_step("tls", from_kube(&e)));
            return report(steps);
        }
    };
    check_cluster(&mut steps, &client, p).await;
    report(steps)
}

fn report(steps: Vec<TestStep>) -> TestReport {
    TestReport {
        ok: steps.iter().all(|s| s.ok),
        steps,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::k8s::cluster::fixtures;
    use crate::k8s::testing::{mock_client, status};
    use crate::secrets::{MemoryStore, TOKEN, secret_key};
    use crate::storage::profile::sample_profile;

    fn names(steps: &[TestStep]) -> Vec<(&str, bool)> {
        steps.iter().map(|s| (s.name, s.ok)).collect()
    }

    #[tokio::test]
    async fn all_cluster_steps_pass_on_openshift() {
        let (client, _server) = mock_client(fixtures::openshift);
        let mut steps = Vec::new();
        assert!(check_cluster(&mut steps, &client, &sample_profile("a")).await);
        assert_eq!(
            names(&steps),
            [
                ("tls", true),
                ("auth", true),
                ("discovery", true),
                ("projects", true)
            ]
        );
        assert_eq!(steps[1].message.params["user"], "dev1");
        assert_eq!(
            steps[2].message.key,
            "messages.connectionTest.discoveryGroups"
        );
        assert_eq!(steps[2].message.params["platform"], "OpenShift");
        assert_eq!(
            steps[2].message.params["groups"],
            "Route, Project, DeploymentConfig"
        );
        assert_eq!(steps[3].message.params["count"], "2");
    }

    #[tokio::test]
    async fn plain_kubernetes_lists_namespaces() {
        let (client, server) = mock_client(|s| match s.path() {
            "/apis" => (
                200,
                serde_json::json!({"kind": "APIGroupList", "apiVersion": "v1",
                    "groups": [{"name": "apps", "versions": [{"groupVersion": "apps/v1", "version": "v1"}]}]}),
            ),
            "/api/v1/namespaces" => (
                200,
                serde_json::json!({"apiVersion": "v1", "kind": "NamespaceList", "metadata": {},
                    "items": [{"metadata": {"name": "default"}}, {"metadata": {"name": "team-a"}}]}),
            ),
            _ => fixtures::openshift(s),
        });
        let mut steps = Vec::new();
        assert!(check_cluster(&mut steps, &client, &sample_profile("a")).await);
        assert_eq!(steps[2].message.key, "messages.connectionTest.discovery");
        assert_eq!(steps[2].message.params["platform"], "Kubernetes 1.32");
        assert_eq!(steps[3].message.params["count"], "2");
        drop(client);
        let seen = server.await.unwrap();
        assert!(
            !seen
                .iter()
                .any(|s| s.path().contains("project.openshift.io"))
        );
    }

    #[tokio::test]
    async fn rejected_token_fails_auth_after_tls() {
        let (client, _server) = mock_client(|_| status(401, "Unauthorized", "Unauthorized"));
        let mut steps = Vec::new();
        assert!(!check_cluster(&mut steps, &client, &sample_profile("a")).await);
        assert_eq!(names(&steps), [("tls", true), ("auth", false)]);
        assert_eq!(
            steps[1].error.as_ref().unwrap().code,
            ErrorCode::SessionExpired
        );
    }

    #[tokio::test]
    async fn forbidden_projects_fall_back_to_profile_list() {
        let (client, _server) = mock_client(|s| match s.path() {
            "/apis/project.openshift.io/v1/projects" => status(
                403,
                "Forbidden",
                r#"projects.project.openshift.io is forbidden: User "u" cannot list resource "projects" in API group "project.openshift.io" at the cluster scope"#,
            ),
            _ => fixtures::openshift(s),
        });
        let mut p = sample_profile("a");
        let mut steps = Vec::new();
        assert!(!check_cluster(&mut steps, &client, &p).await);
        assert_eq!(
            steps[3].error.as_ref().unwrap().key,
            "errors.k8s.forbiddenCluster"
        );
        p.manual_namespaces = vec!["team-a".into()];
        let mut steps = Vec::new();
        assert!(check_cluster(&mut steps, &client, &p).await);
        assert_eq!(
            steps[3].message.key,
            "messages.connectionTest.projectsManual"
        );
    }

    /// Шаги проверки, когда сервер отвечает `answer` на любой запрос.
    async fn steps_for_answer(
        answer: (u16, serde_json::Value),
        p: &ConnectionProfile,
    ) -> (bool, Vec<TestStep>) {
        let (client, _server) = mock_client(move |_| answer.clone());
        let mut steps = Vec::new();
        let ok = check_cluster(&mut steps, &client, p).await;
        (ok, steps)
    }

    /// Любой ответ сервера — доказательство, что TLS-рукопожатие прошло: 403 на «кто я» — это
    /// проблема входа, а не сертификата.
    #[tokio::test]
    async fn forbidden_whoami_fails_auth_not_tls() {
        let (ok, steps) = steps_for_answer(
            status(
                403,
                "Forbidden",
                r#"selfsubjectreviews.authentication.k8s.io is forbidden: User "u" cannot create resource "selfsubjectreviews" in API group "authentication.k8s.io" at the cluster scope"#,
            ),
            &sample_profile("a"),
        )
        .await;
        assert!(!ok);
        assert_eq!(names(&steps), [("tls", true), ("auth", false)]);
        assert_eq!(steps[0].message.key, "messages.connectionTest.tlsOk");
        assert_eq!(steps[1].error.as_ref().unwrap().code, ErrorCode::Forbidden);
    }

    #[tokio::test]
    async fn not_found_whoami_fails_auth_not_tls() {
        // Статус API-сервера и «чужая» страница с кодом 404 (не `kind: Status`).
        for answer in [
            status(
                404,
                "NotFound",
                "the server could not find the requested resource",
            ),
            (404, serde_json::json!("<html>404 page not found</html>")),
        ] {
            let (ok, steps) = steps_for_answer(answer, &sample_profile("a")).await;
            assert!(!ok);
            assert_eq!(names(&steps), [("tls", true), ("auth", false)]);
            assert_eq!(steps[1].error.as_ref().unwrap().code, ErrorCode::NotFound);
        }
    }

    #[tokio::test]
    async fn rejected_request_fails_auth_not_tls() {
        for code in [400, 422] {
            let (ok, steps) =
                steps_for_answer(status(code, "Invalid", "bad request"), &sample_profile("a"))
                    .await;
            assert!(!ok);
            assert_eq!(names(&steps), [("tls", true), ("auth", false)], "{code}");
            assert_eq!(
                steps[1].error.as_ref().unwrap().code,
                ErrorCode::InvalidInput,
                "{code}"
            );
        }
    }

    /// Коды ошибок `k8s::errors` переиспользуются для HTTP-ответов: 429 — это `Timeout`, 5xx —
    /// `ConnectionFailed`, прочие 4xx — `Internal`. Но это ответы сервера, значит, TLS прошёл.
    #[tokio::test]
    async fn overloaded_and_unusual_server_answers_fail_auth_not_tls() {
        for (code, expected, key) in [
            (429, ErrorCode::Timeout, "errors.k8s.tooManyRequests"),
            (503, ErrorCode::ConnectionFailed, "errors.k8s.serverError"),
            (405, ErrorCode::Internal, "errors.k8s.apiError"),
        ] {
            let (ok, steps) = steps_for_answer(
                status(code, "Failure", "server says no"),
                &sample_profile("a"),
            )
            .await;
            assert!(!ok);
            assert_eq!(names(&steps), [("tls", true), ("auth", false)], "{code}");
            let e = steps[1].error.as_ref().unwrap();
            assert_eq!((e.code, e.key.as_str()), (expected, key), "{code}");
        }
    }

    /// Ответ не от API-сервера (например, HTML-страница): соединение и TLS в порядке.
    #[tokio::test]
    async fn unexpected_response_fails_auth_not_tls() {
        let (ok, steps) = steps_for_answer(
            (200, serde_json::json!("<html>Welcome</html>")),
            &sample_profile("a"),
        )
        .await;
        assert!(!ok);
        assert_eq!(names(&steps), [("tls", true), ("auth", false)]);
        let e = steps[1].error.as_ref().unwrap();
        assert_eq!(e.code, ErrorCode::Internal);
        assert_eq!(e.key, "errors.k8s.unexpectedResponse");
    }

    #[tokio::test]
    async fn insecure_profile_reports_unverified_certificate_when_server_answers() {
        let mut p = sample_profile("a");
        p.ca = CaConfig::Insecure;
        let (_, steps) = steps_for_answer(status(403, "Forbidden", "forbidden"), &p).await;
        assert_eq!(names(&steps), [("tls", true), ("auth", false)]);
        assert_eq!(steps[0].message.key, "messages.connectionTest.tlsInsecure");
    }

    /// Клиент, у которого каждый запрос обрывается транспортной ошибкой с текстом `text`.
    fn failing_client(text: &'static str) -> kube::Client {
        let (svc, mut handle) = tower_test::mock::pair::<
            http::Request<kube::client::Body>,
            http::Response<kube::client::Body>,
        >();
        tokio::spawn(async move {
            while let Some((_req, send)) = handle.next_request().await {
                send.send_error(std::io::Error::other(text));
            }
        });
        kube::Client::new(svc, "default")
    }

    /// Ошибки транспорта (до ответа сервера) по-прежнему относятся к шагу `tls`.
    #[tokio::test]
    async fn transport_errors_fail_tls() {
        for (text, code, key) in [
            (
                "invalid peer certificate: UnknownIssuer",
                ErrorCode::TlsError,
                "errors.k8s.caUntrusted",
            ),
            (
                "dns error: failed to lookup address information",
                ErrorCode::ConnectionFailed,
                "errors.k8s.dns",
            ),
            (
                "tcp connect error: connection refused",
                ErrorCode::ConnectionFailed,
                "errors.k8s.connect",
            ),
            (
                "operation timed out",
                ErrorCode::Timeout,
                "errors.k8s.timeout",
            ),
            (
                "something odd happened",
                ErrorCode::Internal,
                "errors.k8s.transport",
            ),
        ] {
            let client = failing_client(text);
            let mut steps = Vec::new();
            assert!(!check_cluster(&mut steps, &client, &sample_profile("a")).await);
            assert_eq!(names(&steps), [("tls", false)], "{text}");
            let e = steps[0].error.as_ref().unwrap();
            assert_eq!((e.code, e.key.as_str()), (code, key), "{text}");
        }
    }

    #[tokio::test]
    async fn tcp_probe_reaches_listener_and_reports_refused_port() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let addr = tcp_probe("127.0.0.1", port, Duration::from_secs(2))
            .await
            .unwrap();
        assert_eq!(addr, format!("127.0.0.1:{port}"));
        drop(listener);
        let e = tcp_probe("127.0.0.1", port, Duration::from_secs(2))
            .await
            .unwrap_err();
        assert_eq!(e.code, ErrorCode::ConnectionFailed);
    }

    #[test]
    fn tcp_goes_to_proxy_when_set() {
        let mut c = kube::Config::new("https://api.c:6443".parse().unwrap());
        assert_eq!(tcp_target(&c), ("api.c".into(), 6443, false));
        c.proxy_url = Some("http://proxy:3128".parse().unwrap());
        assert_eq!(tcp_target(&c), ("proxy".into(), 3128, true));
        let c = kube::Config::new("https://api.c".parse().unwrap());
        assert_eq!(tcp_target(&c).1, 443);
    }

    #[test]
    fn policy_forbids_insecure_tls_and_exec_plugins() {
        let open = PolicyStatus::default();
        let mut p = sample_profile("a");
        p.auth = AuthConfig::Exec(crate::storage::profile::sample_exec());
        assert!(ensure_allowed(&p, &open).is_ok());
        let strict = PolicyStatus {
            allow_exec_auth: false,
            ..PolicyStatus::default()
        };
        assert_eq!(
            ensure_allowed(&p, &strict).unwrap_err().key,
            "errors.connections.execForbidden"
        );
        assert!(ensure_allowed(&sample_profile("b"), &strict).is_ok());
    }

    #[tokio::test]
    async fn config_errors_stop_before_network() {
        let open = PolicyStatus::default();
        let strict = PolicyStatus {
            allow_insecure_tls: false,
            ..PolicyStatus::default()
        };
        // Нет токена.
        let r = test_connection(
            &sample_profile("a"),
            Arc::new(MemoryStore::default()),
            &open,
        )
        .await;
        assert!(!r.ok);
        assert_eq!(names(&r.steps), [("config", false)]);
        assert_eq!(
            r.steps[0].error.as_ref().unwrap().key,
            "errors.connections.tokenMissing"
        );
        // Политика запрещает insecure.
        let mut p = sample_profile("a");
        p.ca = CaConfig::Insecure;
        let secrets = MemoryStore::default();
        secrets.set(&secret_key("a", TOKEN), "t").unwrap();
        let r = test_connection(&p, Arc::new(secrets), &strict).await;
        assert_eq!(
            r.steps[0].error.as_ref().unwrap().key,
            "errors.connections.insecureForbidden"
        );
        // Невалидный профиль.
        let mut p = sample_profile("a");
        p.api_url = "http://api.c:6443".into();
        let r = test_connection(&p, Arc::new(MemoryStore::default()), &open).await;
        assert_eq!(
            r.steps[0].error.as_ref().unwrap().key,
            "errors.connections.httpsRequired"
        );
    }

    #[tokio::test]
    async fn closed_port_fails_tcp_step() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let mut p = sample_profile("a");
        p.api_url = format!("https://127.0.0.1:{port}");
        p.proxy = crate::storage::profile::ProxyConfig::None;
        let secrets = MemoryStore::default();
        secrets.set(&secret_key("a", TOKEN), "t").unwrap();
        let r = test_connection(&p, Arc::new(secrets), &PolicyStatus::default()).await;
        assert_eq!(names(&r.steps), [("config", true), ("tcp", false)]);
    }
}
