//! Выбор источника метрик подключения (спец. §7.1). `auto` берёт первый работающий источник
//! истории: у `openshift` — Thanos Querier, у остальных — Prometheus через прокси API-сервера;
//! без него — `metrics.k8s.io` (только текущее значение). Свой URL из профиля другим не
//! заменяется. Выбор, отвергнутые источники и причины видны в интерфейсе и модели; выбор
//! живёт в подключении (`Connected::metrics`) 5 минут, «Обновить» выбирает заново.

use std::sync::Arc;
use std::time::Duration;

use secrecy::SecretString;
use serde::Serialize;
use tokio::time::Instant;

use super::discovery::{ProxySource, THANOS_ROUTE, candidates, thanos_url};
use super::http::{HttpSource, client};
use super::prometheus::{
    Failure, MetricsSource, Reason, SourceInfo, SourceKind, SourceOrigin, probe, probe_in,
    reason_text,
};
use crate::error::{AppError, AppResult, ErrorCode};
use crate::i18n::LocalizedText;
use crate::k8s::read::ReadClient;
use crate::platform::{PlatformInfo, PlatformKind};
use crate::storage::profile::{ConnectionProfile, MetricsAuth, MetricsConfig};

/// Сколько живёт выбор источника.
pub const SELECTION_TTL: Duration = Duration::from_secs(5 * 60);

/// Источник, который не подошёл.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Rejected {
    pub source: SourceInfo,
    pub reason: Reason,
    /// Причина словами (для человека).
    pub message: LocalizedText,
    /// Ошибка с подробностями (для человека: адрес, код ответа).
    pub error: AppError,
}

/// Режим профиля: `auto`, `thanos` или `prometheus`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum Mode {
    Auto,
    Thanos,
    Prometheus,
}

/// Что выбрано и почему: для «Ресурсов», проверки подключения и модели.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceStatus {
    pub mode: Mode,
    /// Источник истории; `None` — его нет.
    pub selected: Option<SourceInfo>,
    pub rejected: Vec<Rejected>,
    /// Группа `metrics.k8s.io` в кластере есть: без источника истории берутся текущие значения
    /// (только в режиме `auto`).
    pub pod_metrics: bool,
    /// Когда выбирали (мс Unix).
    pub checked_at_ms: i64,
}

impl SourceStatus {
    /// Ошибка «нет ни одного источника» с причинами (спец. §15 `METRICS_UNAVAILABLE`).
    pub fn unavailable(&self) -> AppError {
        let reasons = self
            .rejected
            .iter()
            .map(|r| r.message.text.clone())
            .collect::<Vec<_>>()
            .join("; ");
        crate::app_err!(
            ErrorCode::MetricsUnavailable,
            "errors.metrics.unavailable",
            "No metrics source is available"
        )
        .with_details(reasons)
    }
}

pub struct Selected {
    pub history: Option<Arc<dyn MetricsSource>>,
    pub status: SourceStatus,
}

/// Что нужно для выбора: чтение кластера, профиль, платформа и токены.
pub struct Inputs {
    pub reader: ReadClient,
    pub profile: ConnectionProfile,
    pub platform: PlatformInfo,
    /// Токен подключения (если вход по токену): уходит Thanos и своему источнику.
    pub connection_token: Option<SecretString>,
    /// Свой токен источника из keychain (`metricsToken`).
    pub metrics_token: Option<SecretString>,
}

fn rejected(source: SourceInfo, f: Failure) -> Rejected {
    Rejected {
        message: reason_text(f.reason, &source),
        reason: f.reason,
        error: f.error,
        source,
    }
}

/// Выбирает источник истории и проверяет `metrics.k8s.io`.
pub async fn select(i: &Inputs, now_s: i64) -> Selected {
    let mut tried = Vec::new();
    let (mode, history) = match &i.profile.metrics {
        MetricsConfig::Auto => (Mode::Auto, auto(i, now_s, &mut tried).await),
        MetricsConfig::Thanos(_) => (Mode::Thanos, own(i, now_s, &mut tried).await),
        MetricsConfig::Prometheus(_) => (Mode::Prometheus, own(i, now_s, &mut tried).await),
    };
    // Свой источник другим не заменяется: `metrics.k8s.io` — только у автоматического.
    let pod_metrics = mode == Mode::Auto
        && history.is_none()
        && match super::pods::installed(&i.reader).await {
            Ok(()) => true,
            Err(f) => {
                tried.push(rejected(super::pods::info(), f));
                false
            }
        };
    Selected {
        status: SourceStatus {
            mode,
            selected: history.as_ref().map(|h| h.info().clone()),
            rejected: tried,
            pod_metrics,
            checked_at_ms: now_s * 1000,
        },
        history,
    }
}

async fn auto(i: &Inputs, now_s: i64, tried: &mut Vec<Rejected>) -> Option<Arc<dyn MetricsSource>> {
    if i.platform.kind == PlatformKind::Openshift {
        let info = SourceInfo {
            kind: SourceKind::Thanos,
            origin: SourceOrigin::Auto,
            location: THANOS_ROUTE.into(),
            history: true,
        };
        let found = thanos_url(&i.reader).await.and_then(|url| {
            let http = client(&i.profile, &crate::storage::profile::MetricsCa::Connection)
                .map_err(Failure::from_error)?;
            Ok(HttpSource::new(
                http,
                url,
                i.connection_token.clone(),
                info.clone(),
                false,
            ))
        });
        match found {
            Ok(src) => match probe(&src, now_s).await {
                Ok(()) => return Some(Arc::new(src)),
                Err(f) => tried.push(rejected(info, f)),
            },
            Err(f) => tried.push(rejected(info, f)),
        }
        return None;
    }
    let distribution = i.platform.distribution.map(|d| d.id());
    for svc in candidates(distribution) {
        let src = ProxySource::new(i.reader.clone(), svc);
        match probe(&src, now_s).await {
            Ok(()) => return Some(Arc::new(src)),
            Err(f) => tried.push(rejected(src.info().clone(), f)),
        }
    }
    None
}

/// Свой источник из профиля.
async fn own(i: &Inputs, now_s: i64, tried: &mut Vec<Rejected>) -> Option<Arc<dyn MetricsSource>> {
    let (kind, e) = match &i.profile.metrics {
        MetricsConfig::Thanos(e) => (SourceKind::Thanos, e),
        MetricsConfig::Prometheus(e) => (SourceKind::Prometheus, e),
        MetricsConfig::Auto => return None,
    };
    let info = SourceInfo {
        kind,
        origin: SourceOrigin::Profile,
        location: crate::storage::profile::redact_url(&e.url),
        history: true,
    };
    let token = match e.auth {
        MetricsAuth::Connection => i.connection_token.clone(),
        MetricsAuth::None => None,
        MetricsAuth::Bearer => match &i.metrics_token {
            Some(t) => Some(t.clone()),
            None => {
                tried.push(rejected(
                    info,
                    Failure {
                        reason: Reason::TokenMissing,
                        error: crate::app_err!(
                            ErrorCode::AuthFailed,
                            "errors.metrics.tokenMissing",
                            "The metrics source token is not saved"
                        ),
                    },
                ));
                return None;
            }
        },
    };
    let built = url::Url::parse(&e.url)
        .map_err(|_| Failure::bad_response("metrics url"))
        .and_then(|url| {
            let http = client(&i.profile, &e.ca).map_err(Failure::from_error)?;
            Ok(HttpSource::new(
                http,
                url,
                token,
                info.clone(),
                kind == SourceKind::Thanos,
            ))
        });
    match built {
        Ok(src) => {
            // Tenancy-endpoint без `namespace` отвечает 400: его проверка идёт в проекте
            // пользователя. Future чтения списка проектов держится в куче (стек debug-сборки).
            let namespace = if src.tenancy() {
                Box::pin(probe_namespace(i)).await
            } else {
                None
            };
            match probe_in(&src, now_s, namespace.as_deref()).await {
                Ok(()) => Some(Arc::new(src)),
                Err(f) => {
                    tried.push(rejected(info, f));
                    None
                }
            }
        }
        Err(f) => {
            tried.push(rejected(info, f));
            None
        }
    }
}

/// Проект пользователя для проверки tenancy-источника: первый из списков профиля (избранные,
/// затем заданные вручную), иначе первый, который отдаёт кластер. Нет ни того ни другого —
/// `None`: проверка пойдёт без проекта, и источник сам скажет, что ему нужен проект.
async fn probe_namespace(i: &Inputs) -> Option<String> {
    let listed = i
        .profile
        .favorite_namespaces
        .iter()
        .chain(&i.profile.manual_namespaces)
        .next();
    if let Some(name) = listed {
        return Some(name.clone());
    }
    let projects_api = i.platform.kind == PlatformKind::Openshift;
    let (names, _) = i.reader.project_names(projects_api).await.ok()?;
    names.into_iter().next()
}

/// Выбор, который живёт в подключении. Одновременные вызовы ждут один выбор.
#[derive(Default)]
pub struct MetricsCache {
    inner: tokio::sync::Mutex<Option<(Instant, Arc<Selected>)>>,
}

impl MetricsCache {
    /// Выбор из подключения или `select` (только он и читает то, что нужно для выбора: токен
    /// источника из keychain). Ошибка `select` не запоминается.
    pub async fn get_or_select<F: std::future::Future<Output = AppResult<Selected>>>(
        &self,
        refresh: bool,
        select: impl FnOnce() -> F,
    ) -> AppResult<Arc<Selected>> {
        let mut slot = self.inner.lock().await;
        if !refresh
            && let Some((at, s)) = slot.as_ref()
            && at.elapsed() < SELECTION_TTL
        {
            return Ok(s.clone());
        }
        let s = Arc::new(select().await?);
        *slot = Some((Instant::now(), s.clone()));
        Ok(s)
    }
}

fn now_s() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as i64)
}

/// Источник подключения `id` для команд и MCP: из подключения или выбор заново. Свой токен
/// источника читается из keychain (на blocking-потоке) только тогда, когда выбор действительно
/// идёт, а не при каждом чтении из кэша. Ответы 403 при пробах кандидатов не сбрасывают права
/// проекта, в котором идёт чтение: они собираются отдельно (`observe`).
pub async fn for_connection(
    st: &crate::state::AppState,
    id: &str,
    c: &crate::k8s::pool::Connected,
    refresh: bool,
) -> AppResult<Arc<Selected>> {
    let profile = st.connections.get(id)?;
    let pick = || {
        Box::pin(async move {
            let bearer = profile
                .metrics
                .endpoint()
                .is_some_and(|e| e.auth == MetricsAuth::Bearer);
            let metrics_token = if bearer {
                let secrets = st.secrets.clone();
                let key = crate::secrets::secret_key(id, crate::secrets::METRICS_TOKEN);
                crate::commands::blocking(move || secrets.get(&key))
                    .await?
                    .map(SecretString::from)
            } else {
                None
            };
            let inputs = Inputs {
                reader: c.reader(),
                profile,
                platform: c.info.platform.clone(),
                connection_token: c.bearer().cloned(),
                metrics_token,
            };
            Ok(select(&inputs, now_s()).await)
        })
    };
    let (s, _) = crate::k8s::read::observe(c.metrics.get_or_select(refresh, pick)).await;
    s
}

#[cfg(test)]
mod tests {
    use std::time::Duration as StdDuration;

    use super::*;
    use crate::k8s::testing::{mock_client, status};
    use crate::storage::profile::{MetricsCa, MetricsEndpoint, sample_profile};

    fn ok_vector() -> (u16, serde_json::Value) {
        (
            200,
            serde_json::json!({"status": "success", "data": {"resultType": "vector",
                "result": [{"metric": {}, "value": [1, "1"]}]}}),
        )
    }

    fn inputs(
        platform: PlatformKind,
        handler: impl Fn(&crate::k8s::testing::Seen) -> (u16, serde_json::Value) + Send + 'static,
    ) -> Inputs {
        let mut info = crate::platform::detect(&Default::default());
        info.kind = platform;
        Inputs {
            reader: ReadClient::new(mock_client(handler).0, StdDuration::from_secs(5)),
            profile: sample_profile("a"),
            platform: info,
            connection_token: None,
            metrics_token: None,
        }
    }

    #[tokio::test]
    async fn kubernetes_takes_the_first_prometheus_that_answers() {
        let i = inputs(PlatformKind::Kubernetes, |s| {
            if s.path().contains("services/http:prometheus-k8s:9090") {
                status(403, "Forbidden", "services \"prometheus-k8s\" is forbidden")
            } else if s
                .path()
                .contains("monitoring/services/http:prometheus-operated")
            {
                ok_vector()
            } else {
                status(404, "NotFound", "no")
            }
        });
        let s = select(&i, 100).await;
        assert_eq!(s.status.mode, Mode::Auto);
        let sel = s.status.selected.unwrap();
        assert_eq!(sel.location, "monitoring/prometheus-operated:9090");
        assert_eq!(sel.kind, SourceKind::Prometheus);
        assert_eq!(s.status.rejected.len(), 1);
        assert_eq!(s.status.rejected[0].reason, Reason::Forbidden);
        assert_eq!(
            s.status.rejected[0].message.key,
            "messages.metrics.proxyForbidden"
        );
        assert!(!s.status.pod_metrics, "с историей metrics.k8s.io не нужен");
        assert!(s.history.is_some());
    }

    #[tokio::test]
    async fn without_history_auto_falls_back_to_pod_metrics() {
        let i = inputs(PlatformKind::Kubernetes, |s| {
            if s.path() == "/apis/metrics.k8s.io/v1beta1" {
                (200, serde_json::json!({"kind": "APIResourceList"}))
            } else {
                status(404, "NotFound", "no")
            }
        });
        let s = select(&i, 100).await;
        assert!(s.history.is_none());
        assert!(s.status.pod_metrics);
        assert_eq!(s.status.rejected.len(), 4, "все кандидаты Prometheus");
        let e = s.status.unavailable();
        assert_eq!(e.code, ErrorCode::MetricsUnavailable);
        assert!(
            e.details
                .unwrap()
                .contains("monitoring/prometheus-k8s:9090")
        );
    }

    #[tokio::test]
    async fn openshift_asks_thanos_and_reports_missing_rights() {
        // Ни маршрута, ни домена приложений, ни OAuth-сервера: адрес Thanos не найти.
        let i = inputs(PlatformKind::Openshift, |s| match s.path() {
            "/apis/metrics.k8s.io/v1beta1" => (200, serde_json::json!({})),
            _ => status(403, "Forbidden", "no"),
        });
        let s = select(&i, 100).await;
        assert!(s.history.is_none());
        assert_eq!(
            s.status.rejected.len(),
            1,
            "прокси Prometheus у OpenShift не пробуется"
        );
        assert_eq!(s.status.rejected[0].source.kind, SourceKind::Thanos);
        assert_eq!(s.status.rejected[0].source.location, THANOS_ROUTE);
        assert_eq!(
            s.status.rejected[0].message.key,
            "messages.metrics.thanosForbidden"
        );
        assert!(s.status.pod_metrics);
    }

    #[tokio::test]
    async fn own_source_is_never_replaced() {
        let mut i = inputs(PlatformKind::Kubernetes, |_| ok_vector());
        i.profile.metrics = MetricsConfig::Prometheus(MetricsEndpoint {
            url: "https://user:pw@prometheus.invalid".into(),
            auth: MetricsAuth::Bearer,
            ca: MetricsCa::Connection,
        });
        let s = select(&i, 100).await;
        assert_eq!(s.status.mode, Mode::Prometheus);
        assert!(s.history.is_none() && !s.status.pod_metrics);
        assert_eq!(
            s.status.rejected.len(),
            1,
            "прокси и metrics.k8s.io не пробовали"
        );
        assert_eq!(s.status.rejected[0].reason, Reason::TokenMissing);
        assert_eq!(s.status.rejected[0].source.origin, SourceOrigin::Profile);
        assert!(!s.status.rejected[0].source.location.contains("pw"));
    }

    /// Свой Thanos на tenancy-endpoint: без `namespace` отвечает 400, с ним — 200.
    async fn tenancy_endpoint() -> (url::Url, std::sync::Arc<std::sync::Mutex<Vec<String>>>) {
        crate::metrics::http::fake::server(|line| {
            if line.contains("namespace=team-a") {
                (200, ok_vector().1.to_string())
            } else {
                (
                    400,
                    r#"{"status":"error","errorType":"bad_data","error":"The namespace parameter is required."}"#
                        .into(),
                )
            }
        })
        .await
    }

    /// Профиль со своим Thanos по адресу `base`, без авторизации и прокси.
    fn own_thanos(i: &mut Inputs, base: &url::Url) {
        i.profile.proxy = crate::storage::profile::ProxyConfig::None;
        i.profile.metrics = MetricsConfig::Thanos(MetricsEndpoint {
            url: base.to_string(),
            auth: MetricsAuth::None,
            ca: MetricsCa::Connection,
        });
    }

    /// Ответ списка проектов OpenShift и число таких запросов.
    fn projects(
        names: &'static [&'static str],
    ) -> (
        impl Fn(&crate::k8s::testing::Seen) -> (u16, serde_json::Value) + Send + 'static,
        std::sync::Arc<std::sync::atomic::AtomicUsize>,
    ) {
        let listed = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = listed.clone();
        let handler = move |s: &crate::k8s::testing::Seen| {
            if s.path() == "/apis/project.openshift.io/v1/projects" {
                counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let items: Vec<_> = names
                    .iter()
                    .map(|n| serde_json::json!({"metadata": {"name": n}}))
                    .collect();
                (
                    200,
                    serde_json::json!({"apiVersion": "project.openshift.io/v1",
                        "kind": "ProjectList", "metadata": {}, "items": items}),
                )
            } else {
                status(403, "Forbidden", "no")
            }
        };
        (handler, listed)
    }

    #[tokio::test]
    async fn own_thanos_on_a_tenancy_endpoint_is_probed_in_a_project_of_the_user() {
        // Проекта в профиле нет: берётся первый из списка кластера.
        let (base, log) = tenancy_endpoint().await;
        let (handler, listed) = projects(&["team-a", "team-b"]);
        let mut i = inputs(PlatformKind::Openshift, handler);
        own_thanos(&mut i, &base);
        let s = select(&i, 100).await;
        let sel = s.status.selected.expect("свой Thanos выбран");
        assert_eq!(
            (sel.kind, sel.origin),
            (SourceKind::Thanos, SourceOrigin::Profile)
        );
        assert_eq!(s.status.mode, Mode::Thanos);
        assert!(s.history.is_some() && s.status.rejected.is_empty());
        assert_eq!(listed.load(std::sync::atomic::Ordering::SeqCst), 1);
        let seen = log.lock().unwrap().clone();
        assert_eq!(seen.len(), 1, "{seen:?}");
        assert!(
            seen[0].starts_with(
                "GET /prefix/api/v1/query?query=vector%281%29&time=100&namespace=team-a "
            ),
            "{}",
            seen[0]
        );

        // Проект есть в профиле: избранные раньше заданных вручную, кластер не спрашивают.
        let (base, log) = tenancy_endpoint().await;
        let (handler, listed) = projects(&["team-b"]);
        let mut i = inputs(PlatformKind::Openshift, handler);
        own_thanos(&mut i, &base);
        i.profile.manual_namespaces = vec!["team-b".into()];
        i.profile.favorite_namespaces = vec!["team-a".into()];
        assert!(select(&i, 100).await.history.is_some());
        assert_eq!(listed.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert!(log.lock().unwrap()[0].contains("namespace=team-a"));
    }

    #[tokio::test]
    async fn own_thanos_without_any_project_is_probed_without_one_and_rejected() {
        let (base, log) = tenancy_endpoint().await;
        let mut i = inputs(PlatformKind::Openshift, |_| {
            status(403, "Forbidden", "projects are forbidden")
        });
        own_thanos(&mut i, &base);
        let s = select(&i, 100).await;
        assert!(s.history.is_none() && !s.status.pod_metrics);
        assert_eq!(s.status.rejected.len(), 1);
        assert_eq!(s.status.rejected[0].reason, Reason::BadResponse);
        assert!(!log.lock().unwrap()[0].contains("namespace="));
    }

    #[tokio::test]
    async fn own_source_that_rejects_the_credentials_is_not_sent_to_the_api_proxy() {
        let (base, _) = crate::metrics::http::fake::server(|_| (403, "forbidden".into())).await;
        let mut i = inputs(PlatformKind::Kubernetes, |_| ok_vector());
        own_thanos(&mut i, &base);
        i.profile.favorite_namespaces = vec!["team-a".into()];
        let s = select(&i, 100).await;
        assert_eq!(s.status.rejected[0].reason, Reason::Forbidden);
        assert_eq!(
            s.status.rejected[0].message.key,
            "messages.metrics.ownForbidden"
        );
    }

    #[tokio::test]
    async fn the_choice_is_kept_until_refresh_or_expiry() {
        let cache = MetricsCache::default();
        let calls = std::sync::atomic::AtomicUsize::new(0);
        let pick = || async {
            calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(select(
                &inputs(PlatformKind::Kubernetes, |_| status(404, "NotFound", "no")),
                1,
            )
            .await)
        };
        cache.get_or_select(false, pick).await.unwrap();
        cache.get_or_select(false, pick).await.unwrap();
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        cache.get_or_select(true, pick).await.unwrap();
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
        // Ошибка выбора не запоминается: следующий вызов выбирает заново.
        let failing = MetricsCache::default();
        let fail = || async { Err(crate::k8s::errors::timeout()) };
        assert!(failing.get_or_select(false, fail).await.is_err());
        failing.get_or_select(false, pick).await.unwrap();
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 3);
    }

    /// Хранилище секретов, которое считает чтения; токена в нём нет.
    #[derive(Default)]
    struct CountingStore(std::sync::atomic::AtomicUsize);

    impl crate::secrets::SecretStore for CountingStore {
        fn get(&self, _key: &str) -> AppResult<Option<String>> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(None)
        }
        fn set(&self, _key: &str, _value: &str) -> AppResult<()> {
            Ok(())
        }
        fn delete(&self, _key: &str) -> AppResult<()> {
            Ok(())
        }
    }

    /// Свой токен источника читается из keychain только при выборе, а не при каждом чтении из
    /// кэша подключения (вкладка «Ресурсы», проверка, MCP).
    #[tokio::test]
    async fn the_metrics_token_is_read_only_when_the_source_is_selected() {
        let dir = tempfile::tempdir().unwrap();
        let secrets = std::sync::Arc::new(CountingStore::default());
        let st = crate::state::AppState::new(dir.path(), secrets.clone()).unwrap();
        let mut p = sample_profile("a");
        p.metrics = MetricsConfig::Prometheus(MetricsEndpoint {
            url: "https://prometheus.invalid".into(),
            auth: MetricsAuth::Bearer,
            ca: MetricsCa::Connection,
        });
        st.connections.upsert(p).unwrap();
        let c = crate::k8s::pool::test_connected(|_| status(404, "NotFound", "no"));
        let reads = || secrets.0.load(std::sync::atomic::Ordering::SeqCst);
        for _ in 0..3 {
            let s = for_connection(&st, "a", &c, false).await.unwrap();
            assert_eq!(s.status.rejected[0].reason, Reason::TokenMissing);
        }
        assert_eq!(reads(), 1, "из кэша — без keychain");
        for_connection(&st, "a", &c, true).await.unwrap();
        assert_eq!(reads(), 2, "«Обновить» выбирает заново");
    }
}
