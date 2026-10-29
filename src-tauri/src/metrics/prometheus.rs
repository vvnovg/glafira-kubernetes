//! Prometheus HTTP API (спец. §7.1): трейт источника [`MetricsSource`] — только GET к
//! `/api/v1/query` и `/api/v1/query_range`, — разбор ответов и запросы поверх него. Thanos
//! Querier говорит на том же API; у своего Thanos запросы по проекту идут с параметром
//! `namespace` (tenancy-endpoint OpenShift без него отвечает 400).

use std::collections::BTreeMap;

use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};

use crate::error::{AppError, ErrorCode};
use crate::i18n::LocalizedText;

/// Путь API Prometheus: других запросов источники не делают.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PromApi {
    Query,
    QueryRange,
}

impl PromApi {
    pub fn path(self) -> &'static str {
        match self {
            PromApi::Query => "/api/v1/query",
            PromApi::QueryRange => "/api/v1/query_range",
        }
    }
}

/// Вид источника.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SourceKind {
    Thanos,
    Prometheus,
    MetricsServer,
}

/// Откуда источник: найден автопоиском платформы или задан в профиле.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SourceOrigin {
    Auto,
    Profile,
}

/// Источник для интерфейса и модели. `location` — для человека: маршрут или сервис кластера, у
/// своего источника — адрес из профиля (модель его не получает).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceInfo {
    pub kind: SourceKind,
    pub origin: SourceOrigin,
    pub location: String,
    /// Есть история (PromQL); у `metrics.k8s.io` — только текущее значение.
    pub history: bool,
}

/// Почему источник не подошёл: код для интерфейса и модели.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum Reason {
    /// 401/403: нет прав на мониторинг или на прокси к сервису.
    Forbidden,
    /// Нет сервиса, маршрута или группы API (404).
    NotFound,
    /// Сервис или сеть не отвечают (5xx, отказ соединения, TLS).
    Unavailable,
    Timeout,
    /// Ответ — не Prometheus API или запрос отвергнут (400).
    BadResponse,
    /// Свой источник требует токен, а его нет в хранилище секретов.
    TokenMissing,
}

/// Отказ источника: причина и ошибка с подробностями для человека.
#[derive(Debug, Clone)]
pub struct Failure {
    pub reason: Reason,
    pub error: AppError,
}

impl Failure {
    /// Причина по ошибке запроса к API-серверу или источнику.
    pub fn from_error(error: AppError) -> Self {
        let reason = match error.code {
            ErrorCode::Forbidden | ErrorCode::SessionExpired => Reason::Forbidden,
            ErrorCode::NotFound => Reason::NotFound,
            ErrorCode::Timeout => Reason::Timeout,
            ErrorCode::InvalidInput => Reason::BadResponse,
            _ if error.key == "errors.k8s.unexpectedResponse" => Reason::BadResponse,
            _ => Reason::Unavailable,
        };
        Self { reason, error }
    }

    pub fn bad_response(details: impl Into<String>) -> Self {
        Self {
            reason: Reason::BadResponse,
            error: crate::app_err!(
                ErrorCode::ConnectionFailed,
                "errors.metrics.badResponse",
                "The metrics source returned an unexpected response"
            )
            .with_details(details),
        }
    }
}

/// Источник метрик с историей: GET к API Prometheus. Реализации — прокси сервиса через
/// API-сервер (`ReadClient`) и прямой HTTP (`metrics::http::MetricsHttp`); в тестах — подмена.
pub trait MetricsSource: Send + Sync {
    fn info(&self) -> &SourceInfo;
    /// Запросы по проекту передают `namespace` (Thanos tenancy).
    fn tenancy(&self) -> bool {
        false
    }
    /// Тело ответа на GET `api` с параметрами `params` (уже без `namespace`).
    fn get<'a>(
        &'a self,
        api: PromApi,
        params: Vec<(String, String)>,
    ) -> BoxFuture<'a, Result<String, Failure>>;
}

/// Значение ряда в момент запроса.
#[derive(Debug, Clone, PartialEq)]
pub struct Sample {
    pub labels: BTreeMap<String, String>,
    pub value: f64,
}

impl Sample {
    pub fn label(&self, name: &str) -> &str {
        self.labels.get(name).map_or("", String::as_str)
    }
}

/// Ряд range-запроса: точки (Unix-секунды, значение) без `NaN`.
#[derive(Debug, Clone, PartialEq)]
pub struct Series {
    pub labels: BTreeMap<String, String>,
    pub points: Vec<(i64, f64)>,
}

impl Series {
    pub fn label(&self, name: &str) -> &str {
        self.labels.get(name).map_or("", String::as_str)
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Envelope {
    status: String,
    #[serde(default)]
    data: Option<Data>,
    #[serde(default)]
    error_type: Option<String>,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Deserialize)]
#[serde(tag = "resultType", content = "result", rename_all = "lowercase")]
enum Data {
    Vector(Vec<VectorItem>),
    Matrix(Vec<MatrixItem>),
    Scalar(Point),
    String(serde::de::IgnoredAny),
}

#[derive(Deserialize)]
struct VectorItem {
    #[serde(default)]
    metric: BTreeMap<String, String>,
    value: Point,
}

#[derive(Deserialize)]
struct MatrixItem {
    #[serde(default)]
    metric: BTreeMap<String, String>,
    #[serde(default)]
    values: Vec<Point>,
}

/// `[1700000000.123, "1.5"]`.
#[derive(Deserialize)]
struct Point(f64, String);

impl Point {
    /// Значение или `None` для `NaN`; `+Inf` остаётся бесконечностью.
    fn value(&self) -> Option<f64> {
        self.1.parse::<f64>().ok().filter(|v| !v.is_nan())
    }
}

fn envelope(body: &str) -> Result<Data, Failure> {
    let e: Envelope = serde_json::from_str(body)
        .map_err(|_| Failure::bad_response(body.chars().take(200).collect::<String>()))?;
    if e.status != "success" {
        return Err(Failure::bad_response(format!(
            "{}: {}",
            e.error_type.unwrap_or_default(),
            e.error.unwrap_or_default()
        )));
    }
    e.data
        .ok_or_else(|| Failure::bad_response("success without data"))
}

/// Ответ instant-запроса: ряды со значением (`NaN` отброшены; скаляр — ряд без меток).
pub fn parse_vector(body: &str) -> Result<Vec<Sample>, Failure> {
    match envelope(body)? {
        Data::Vector(items) => Ok(items
            .into_iter()
            .filter_map(|i| {
                Some(Sample {
                    value: i.value.value()?,
                    labels: i.metric,
                })
            })
            .collect()),
        Data::Scalar(p) => Ok(p
            .value()
            .map(|value| Sample {
                labels: BTreeMap::new(),
                value,
            })
            .into_iter()
            .collect()),
        _ => Err(Failure::bad_response("expected a vector")),
    }
}

/// Ответ range-запроса.
pub fn parse_matrix(body: &str) -> Result<Vec<Series>, Failure> {
    match envelope(body)? {
        Data::Matrix(items) => Ok(items
            .into_iter()
            .map(|i| Series {
                points: i
                    .values
                    .iter()
                    .filter_map(|p| Some((p.0 as i64, p.value()?)))
                    .collect(),
                labels: i.metric,
            })
            .collect()),
        _ => Err(Failure::bad_response("expected a matrix")),
    }
}

fn with_namespace(
    s: &dyn MetricsSource,
    namespace: &str,
    mut params: Vec<(String, String)>,
) -> Vec<(String, String)> {
    if s.tenancy() {
        params.push(("namespace".into(), namespace.into()));
    }
    params
}

/// Instant-запрос в момент `time` (Unix-секунды).
pub async fn query(
    s: &dyn MetricsSource,
    namespace: &str,
    promql: &str,
    time: i64,
) -> Result<Vec<Sample>, Failure> {
    let params = vec![
        ("query".to_string(), promql.to_string()),
        ("time".to_string(), time.to_string()),
    ];
    let body = s
        .get(PromApi::Query, with_namespace(s, namespace, params))
        .await?;
    parse_vector(&body)
}

/// Range-запрос `[start, end]` с шагом `step` секунд.
pub async fn query_range(
    s: &dyn MetricsSource,
    namespace: &str,
    promql: &str,
    start: i64,
    end: i64,
    step: u64,
) -> Result<Vec<Series>, Failure> {
    let params = vec![
        ("query".to_string(), promql.to_string()),
        ("start".to_string(), start.to_string()),
        ("end".to_string(), end.to_string()),
        ("step".to_string(), step.to_string()),
    ];
    let body = s
        .get(PromApi::QueryRange, with_namespace(s, namespace, params))
        .await?;
    parse_matrix(&body)
}

/// Проверка источника: `vector(1)` отвечает единицей.
pub async fn probe(s: &dyn MetricsSource, time: i64) -> Result<(), Failure> {
    probe_in(s, time, None).await
}

/// Проверка в проекте пользователя: tenancy-endpoint без `namespace` отвечает 400, поэтому у
/// такого источника проверка идёт с проектом. Без проекта (`None`) — как `probe`.
pub async fn probe_in(
    s: &dyn MetricsSource,
    time: i64,
    namespace: Option<&str>,
) -> Result<(), Failure> {
    let params = vec![
        ("query".to_string(), "vector(1)".to_string()),
        ("time".to_string(), time.to_string()),
    ];
    let params = match namespace {
        Some(namespace) => with_namespace(s, namespace, params),
        None => params,
    };
    let body = s.get(PromApi::Query, params).await?;
    match parse_vector(&body)?.first() {
        Some(v) if v.value == 1.0 => Ok(()),
        _ => Err(Failure::bad_response("vector(1) did not return 1")),
    }
}

impl SourceInfo {
    /// Название для человека: «Thanos Querier openshift-monitoring/thanos-querier»,
    /// «Prometheus monitoring/prometheus-k8s:9090», «metrics.k8s.io».
    pub fn label(&self) -> String {
        match self.kind {
            SourceKind::Thanos => format!("Thanos Querier {}", self.location),
            SourceKind::Prometheus => format!("Prometheus {}", self.location),
            SourceKind::MetricsServer => self.location.clone(),
        }
    }
}

/// Причина словами, с источником (`{{source}}`): для интерфейса и проверки подключения.
pub fn reason_text(r: Reason, info: &SourceInfo) -> LocalizedText {
    let source = info.label();
    match (r, info.kind) {
        // Свой источник опрашивается напрямую, не через API-сервер: права на прокси ни при чём.
        (Reason::Forbidden, _) if info.origin == SourceOrigin::Profile => crate::ltext!(
            "messages.metrics.ownForbidden",
            "{{source}}: the source rejected the credentials — check the authorization in the connection settings",
            source = source
        ),
        (Reason::Forbidden, SourceKind::Thanos) => crate::ltext!(
            "messages.metrics.thanosForbidden",
            "{{source}}: no rights to cluster monitoring (the cluster-monitoring-view role or similar is needed)",
            source = source
        ),
        (Reason::Forbidden, SourceKind::MetricsServer) => crate::ltext!(
            "messages.metrics.podMetricsForbidden",
            "{{source}}: no rights to read pod metrics",
            source = source
        ),
        (Reason::Forbidden, _) => crate::ltext!(
            "messages.metrics.proxyForbidden",
            "{{source}}: no rights to the source (through the API server — get services/proxy)",
            source = source
        ),
        (Reason::NotFound, SourceKind::MetricsServer) => crate::ltext!(
            "messages.metrics.podMetricsMissing",
            "{{source}}: not installed in the cluster",
            source = source
        ),
        (Reason::NotFound, _) => crate::ltext!(
            "messages.metrics.notFound",
            "{{source}}: not found",
            source = source
        ),
        (Reason::Unavailable, _) => crate::ltext!(
            "messages.metrics.unavailable",
            "{{source}}: does not respond",
            source = source
        ),
        (Reason::Timeout, _) => crate::ltext!(
            "messages.metrics.timeout",
            "{{source}}: did not respond in time",
            source = source
        ),
        (Reason::BadResponse, _) => crate::ltext!(
            "messages.metrics.badResponse",
            "{{source}}: answered, but not as Prometheus",
            source = source
        ),
        (Reason::TokenMissing, _) => crate::ltext!(
            "messages.metrics.tokenMissing",
            "{{source}}: the token is not saved — enter it in the connection settings",
            source = source
        ),
    }
}

/// Подмена источника для тестов: ответ по подстроке запроса, журнал запросов.
#[cfg(test)]
pub mod fake {
    use std::sync::Mutex;

    use super::*;

    type Answer = Box<dyn Fn(PromApi, &str) -> Result<String, Failure> + Send + Sync>;
    /// Запрос: путь API и параметры.
    pub type Request = (PromApi, Vec<(String, String)>);
    /// Метки ряда в ответе.
    pub type Labels<'a> = &'a [(&'a str, &'a str)];

    pub struct FakeSource {
        pub info: SourceInfo,
        pub tenancy: bool,
        answer: Answer,
        pub seen: Mutex<Vec<Request>>,
    }

    impl FakeSource {
        pub fn new(
            answer: impl Fn(PromApi, &str) -> Result<String, Failure> + Send + Sync + 'static,
        ) -> Self {
            Self {
                info: SourceInfo {
                    kind: SourceKind::Prometheus,
                    origin: SourceOrigin::Auto,
                    location: "monitoring/prometheus-k8s:9090".into(),
                    history: true,
                },
                tenancy: false,
                answer: Box::new(answer),
                seen: Mutex::new(Vec::new()),
            }
        }

        pub fn queries(&self) -> Vec<String> {
            self.seen
                .lock()
                .unwrap()
                .iter()
                .filter_map(|(_, p)| p.iter().find(|(k, _)| k == "query").map(|(_, v)| v.clone()))
                .collect()
        }
    }

    impl MetricsSource for FakeSource {
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
            let query = params
                .iter()
                .find(|(k, _)| k == "query")
                .map(|(_, v)| v.clone())
                .unwrap_or_default();
            self.seen.lock().unwrap().push((api, params));
            let r = (self.answer)(api, &query);
            Box::pin(async move { r })
        }
    }

    /// Ответ instant-запроса: ряды `(метки, значение)`.
    pub fn vector(rows: &[(Labels<'_>, f64)]) -> String {
        let result: Vec<_> = rows
            .iter()
            .map(|(labels, v)| {
                let metric: BTreeMap<_, _> = labels.iter().copied().collect();
                serde_json::json!({"metric": metric, "value": [1_700_000_000, v.to_string()]})
            })
            .collect();
        serde_json::json!({"status": "success", "data": {"resultType": "vector", "result": result}})
            .to_string()
    }

    /// Ответ range-запроса: ряды `(метки, точки)`.
    pub fn matrix(rows: &[(Labels<'_>, &[(i64, f64)])]) -> String {
        let result: Vec<_> = rows
            .iter()
            .map(|(labels, points)| {
                let metric: BTreeMap<_, _> = labels.iter().copied().collect();
                let values: Vec<_> = points
                    .iter()
                    .map(|(t, v)| serde_json::json!([t, v.to_string()]))
                    .collect();
                serde_json::json!({"metric": metric, "values": values})
            })
            .collect();
        serde_json::json!({"status": "success", "data": {"resultType": "matrix", "result": result}})
            .to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::fake::{FakeSource, matrix, vector};
    use super::*;

    #[test]
    fn vectors_scalars_and_matrices_are_read() {
        let v = parse_vector(&vector(&[(
            &[("container", "app"), ("stat", "cpu_p95")],
            0.25,
        )]))
        .unwrap();
        assert_eq!(v.len(), 1);
        assert_eq!((v[0].label("container"), v[0].value), ("app", 0.25));
        assert_eq!(v[0].label("missing"), "");
        let body = r#"{"status":"success","data":{"resultType":"vector","result":[
            {"metric":{"a":"1"},"value":[1700000000.5,"NaN"]},
            {"metric":{"a":"2"},"value":[1700000000.5,"+Inf"]}]}}"#;
        let v = parse_vector(body).unwrap();
        assert_eq!(v.len(), 1, "NaN отброшен");
        assert!(v[0].value.is_infinite());
        let s = parse_vector(
            r#"{"status":"success","data":{"resultType":"scalar","result":[1700000000,"1"]}}"#,
        )
        .unwrap();
        assert_eq!(s[0].value, 1.0);
        let m = parse_matrix(&matrix(&[(
            &[("container", "app")],
            &[(10, 1.0), (20, 2.5)],
        )]))
        .unwrap();
        assert_eq!(m[0].points, [(10, 1.0), (20, 2.5)]);
    }

    #[test]
    fn errors_and_strange_bodies_are_bad_responses() {
        for body in [
            r#"{"status":"error","errorType":"bad_data","error":"parse error"}"#,
            "<html>login</html>",
            r#"{"status":"success"}"#,
            r#"{"status":"success","data":{"resultType":"matrix","result":[]}}"#,
        ] {
            let e = parse_vector(body).unwrap_err();
            assert_eq!(e.reason, Reason::BadResponse, "{body}");
            assert_eq!(e.error.key, "errors.metrics.badResponse");
        }
        assert!(
            parse_vector(r#"{"status":"error","errorType":"bad_data","error":"parse error"}"#)
                .unwrap_err()
                .error
                .details
                .unwrap()
                .contains("parse error")
        );
    }

    #[test]
    fn failures_get_their_reason_from_the_error_code() {
        let cases = [
            (
                crate::k8s::errors::not_allowed("get", "services/proxy", Some("monitoring")),
                Reason::Forbidden,
            ),
            (
                crate::k8s::errors::from_status(
                    &kube::core::Status::failure("services \"x\" not found", "NotFound")
                        .with_code(404),
                ),
                Reason::NotFound,
            ),
            (
                crate::k8s::errors::from_status(
                    &kube::core::Status::failure("no endpoints", "ServiceUnavailable")
                        .with_code(503),
                ),
                Reason::Unavailable,
            ),
            (crate::k8s::errors::timeout(), Reason::Timeout),
        ];
        for (e, reason) in cases {
            assert_eq!(Failure::from_error(e.clone()).reason, reason, "{}", e.key);
        }
    }

    #[tokio::test]
    async fn thanos_tenancy_gets_the_namespace() {
        let mut s = FakeSource::new(|_, _| Ok(vector(&[(&[], 1.0)])));
        query(&s, "team-a", "up", 100).await.unwrap();
        s.tenancy = true;
        query_range(&s, "team-a", "up", 0, 600, 300)
            .await
            .unwrap_err();
        let seen = s.seen.lock().unwrap();
        assert!(!seen[0].1.iter().any(|(k, _)| k == "namespace"));
        assert_eq!(seen[0].0, PromApi::Query);
        assert!(seen[1].1.contains(&("namespace".into(), "team-a".into())));
        assert!(seen[1].1.contains(&("step".into(), "300".into())));
    }

    #[tokio::test]
    async fn probe_wants_vector_one() {
        let ok = FakeSource::new(|_, q| {
            assert_eq!(q, "vector(1)");
            Ok(vector(&[(&[], 1.0)]))
        });
        assert!(probe(&ok, 1).await.is_ok());
        let wrong = FakeSource::new(|_, _| Ok(vector(&[])));
        assert_eq!(
            probe(&wrong, 1).await.unwrap_err().reason,
            Reason::BadResponse
        );
    }

    #[tokio::test]
    async fn probe_in_passes_the_project_only_to_a_tenancy_source() {
        let mut s = FakeSource::new(|_, _| Ok(vector(&[(&[], 1.0)])));
        probe_in(&s, 1, Some("team-a")).await.unwrap();
        s.tenancy = true;
        probe_in(&s, 1, Some("team-a")).await.unwrap();
        probe_in(&s, 1, None).await.unwrap();
        probe(&s, 1).await.unwrap();
        let seen = s.seen.lock().unwrap();
        let has_namespace = |i: usize| seen[i].1.contains(&("namespace".into(), "team-a".into()));
        assert!(!has_namespace(0), "у обычного источника проекта нет");
        assert!(has_namespace(1));
        assert!(
            !has_namespace(2) && !has_namespace(3),
            "без проекта — как probe"
        );
    }

    #[test]
    fn forbidden_text_depends_on_where_the_source_is() {
        let info = |kind, origin| SourceInfo {
            kind,
            origin,
            location: "x".into(),
            history: true,
        };
        for (kind, origin, key) in [
            (
                SourceKind::Prometheus,
                SourceOrigin::Profile,
                "messages.metrics.ownForbidden",
            ),
            (
                SourceKind::Thanos,
                SourceOrigin::Profile,
                "messages.metrics.ownForbidden",
            ),
            (
                SourceKind::Prometheus,
                SourceOrigin::Auto,
                "messages.metrics.proxyForbidden",
            ),
            (
                SourceKind::Thanos,
                SourceOrigin::Auto,
                "messages.metrics.thanosForbidden",
            ),
            (
                SourceKind::MetricsServer,
                SourceOrigin::Auto,
                "messages.metrics.podMetricsForbidden",
            ),
        ] {
            let text = reason_text(Reason::Forbidden, &info(kind, origin));
            assert_eq!(text.key, key, "{kind:?} {origin:?}");
            assert_eq!(text.params["source"], info(kind, origin).label());
        }
        // Остальные причины от происхождения не зависят.
        let own = info(SourceKind::Prometheus, SourceOrigin::Profile);
        assert_eq!(
            reason_text(Reason::NotFound, &own).key,
            "messages.metrics.notFound"
        );
    }
}
