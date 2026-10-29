//! Статистика потребления workload'а (спец. §7.2): по каждому контейнеру за окно — CPU, память,
//! троттлинг, OOM и рестарты; ряды графиков; покрытие окна. Источник истории — PromQL-шаблоны
//! (`metrics::promql`), без него — одна точка `metrics.k8s.io`. Поды выбираются через
//! kube-state-metrics, если его метрики есть, иначе по имени.

use std::collections::BTreeMap;

use serde::Serialize;

use super::current::{ContainerCurrent, current};
use crate::error::{AppError, AppResult, ErrorCode};
use crate::k8s::pods::{WorkloadRef, workload_pods};
use crate::k8s::read::ReadClient;
use crate::k8s::workloads::{WorkloadKind, get_spec};
use crate::metrics::pods::{PodNow, pods_now};
use crate::metrics::prometheus::{
    Failure, MetricsSource, Sample, Series, SourceInfo, query, query_range,
};
use crate::metrics::promql::{self, Pods};
use crate::metrics::select::{Selected, SourceStatus};

/// Окна анализа, дни (спец. §7.2).
pub const WINDOWS: [u32; 4] = [1, 7, 14, 30];

pub fn check_window(days: u32) -> AppResult<u32> {
    if WINDOWS.contains(&days) {
        return Ok(days);
    }
    Err(crate::app_err!(
        ErrorCode::InvalidInput,
        "errors.resources.badWindow",
        "The analysis window must be 1, 7, 14 or 30 days, not {{days}}",
        days = days
    ))
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CpuStats {
    pub p50: Option<f64>,
    pub p95: Option<f64>,
    pub p99: Option<f64>,
    pub max: Option<f64>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryStats {
    pub p95: Option<f64>,
    pub p99: Option<f64>,
    pub max: Option<f64>,
}

/// Доля периодов CFS с троттлингом, 0–1.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ThrottlingStats {
    pub mean: Option<f64>,
    pub p95: Option<f64>,
}

/// Статистика контейнера за окно: «худший под» (спец. §7.2).
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerStats {
    pub cpu: CpuStats,
    pub memory: MemoryStats,
    pub throttling: ThrottlingStats,
    /// Эпизоды OOM за окно; `None` — нет kube-state-metrics.
    pub oom_count: Option<u32>,
    /// Последний OOM (Unix-секунды).
    pub oom_last: Option<i64>,
    /// Рестарты за окно; `None` — нет kube-state-metrics.
    pub restarts: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerUsage {
    #[serde(flatten)]
    pub current: ContainerCurrent,
    pub stats: ContainerStats,
}

/// Ряд графика по контейнеру: (Unix-секунды, значение).
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChartSeries {
    pub container: String,
    pub points: Vec<(i64, f64)>,
}

/// Отметка на графике: рестарты или OOM контейнера за шаг.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Marker {
    pub container: String,
    pub time: i64,
    pub count: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Charts {
    pub start: i64,
    pub end: i64,
    pub step_seconds: u64,
    pub cpu: Vec<ChartSeries>,
    pub memory: Vec<ChartSeries>,
    pub restarts: Vec<Marker>,
    pub ooms: Vec<Marker>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadUsage {
    pub namespace: String,
    pub kind: WorkloadKind,
    pub name: String,
    pub window_days: u32,
    /// Источник данных: история или `metrics.k8s.io`.
    pub source: SourceInfo,
    /// Выбор источника и отвергнутые.
    pub source_status: SourceStatus,
    /// Доля шагов окна с данными хотя бы по одному поду (0–1); у `metrics.k8s.io` — нет.
    pub coverage: Option<f64>,
    /// Сколько секунд от первой точки с данными до конца окна.
    pub data_seconds: Option<i64>,
    /// Метрики kube-state-metrics есть: поды выбраны по владельцам, OOM и рестарты известны.
    pub owner_metrics: bool,
    /// Желаемо реплик (у DaemonSet — подов).
    pub replicas: Option<i32>,
    /// Подов сверх желаемого при выкладке.
    pub surge: i32,
    pub containers: Vec<ContainerUsage>,
    pub charts: Option<Charts>,
    /// Только `metrics.k8s.io`: текущее потребление подов workload'а.
    pub pods_now: Vec<PodNow>,
}

fn failed(f: Failure) -> AppError {
    f.error
}

/// Значения показателей с меткой `stat` по контейнеру: `stat → контейнер → значение`.
fn by_stat(samples: &[Sample]) -> BTreeMap<String, BTreeMap<String, f64>> {
    let mut out: BTreeMap<String, BTreeMap<String, f64>> = BTreeMap::new();
    for s in samples {
        out.entry(s.label("stat").to_string())
            .or_default()
            .insert(s.label("container").to_string(), s.value);
    }
    out
}

fn series(list: Vec<Series>) -> Vec<ChartSeries> {
    let mut out: Vec<ChartSeries> = list
        .into_iter()
        .map(|s| ChartSeries {
            container: s.label("container").to_string(),
            points: s.points,
        })
        .collect();
    out.sort_by(|a, b| a.container.cmp(&b.container));
    out
}

fn markers(list: &[Series], event: &str) -> Vec<Marker> {
    let mut out: Vec<Marker> = list
        .iter()
        .filter(|s| s.label("event") == event)
        .flat_map(|s| {
            s.points
                .iter()
                .filter(|(_, v)| *v > 0.0)
                .map(|(t, v)| Marker {
                    container: s.label("container").to_string(),
                    time: *t,
                    count: *v,
                })
        })
        .collect();
    out.sort_by(|a, b| a.time.cmp(&b.time).then(a.container.cmp(&b.container)));
    out
}

/// Есть ли у проекта метрики kube-state-metrics.
async fn has_owner_metrics(src: &dyn MetricsSource, ns: &str, now: i64) -> Result<bool, Failure> {
    let v = query(src, ns, &promql::owner_metrics_present(ns), now).await?;
    Ok(v.first().is_some_and(|s| s.value > 0.0))
}

/// Статистика и графики по источнику истории.
async fn from_history(
    src: &dyn MetricsSource,
    pods: &Pods,
    days: u32,
    now: i64,
    owner: bool,
    containers: Vec<ContainerCurrent>,
) -> Result<(Vec<ContainerUsage>, Charts, Option<f64>, Option<i64>), Failure> {
    let ns = pods.namespace();
    let step = promql::chart_step_seconds(days);
    let start = now - i64::from(days) * 86_400;
    let q = [
        promql::workload_cpu(pods, days),
        promql::workload_memory(pods, days),
        promql::workload_throttling(pods, days),
        promql::workload_restarts(pods, days),
        promql::workload_coverage(pods, days),
        promql::workload_cpu_series(pods),
        promql::workload_memory_series(pods),
        promql::workload_events_series(pods, step),
    ];
    let (cpu, memory, throttling, restarts, coverage, cpu_series, memory_series, events) = tokio::try_join!(
        query(src, ns, &q[0], now),
        query(src, ns, &q[1], now),
        query(src, ns, &q[2], now),
        query(src, ns, &q[3], now),
        query(src, ns, &q[4], now),
        query_range(src, ns, &q[5], start, now, step),
        query_range(src, ns, &q[6], start, now, step),
        query_range(src, ns, &q[7], start, now, step),
    )?;
    let stats: BTreeMap<String, BTreeMap<String, f64>> = [cpu, memory, throttling, restarts]
        .iter()
        .flat_map(|v| by_stat(v))
        .collect();
    let get = |stat: &str, c: &str| stats.get(stat).and_then(|m| m.get(c)).copied();
    let usage = containers
        .into_iter()
        .map(|current| {
            let c = current.name.as_str();
            let count = |stat: &str| owner.then(|| get(stat, c).unwrap_or(0.0).round() as u32);
            let stats = ContainerStats {
                cpu: CpuStats {
                    p50: get("cpu_p50", c),
                    p95: get("cpu_p95", c),
                    p99: get("cpu_p99", c),
                    max: get("cpu_max", c),
                },
                memory: MemoryStats {
                    p95: get("memory_p95", c),
                    p99: get("memory_p99", c),
                    max: get("memory_max", c),
                },
                throttling: ThrottlingStats {
                    mean: get("throttling_mean", c),
                    p95: get("throttling_p95", c),
                },
                oom_count: count("oom_count"),
                oom_last: owner
                    .then(|| get("oom_last", c).map(|t| t as i64))
                    .flatten(),
                restarts: count("restarts"),
            };
            ContainerUsage { current, stats }
        })
        .collect();
    let cov = by_stat(&coverage);
    let steps = cov.get("steps").and_then(|m| m.get("")).copied();
    let first = cov.get("first").and_then(|m| m.get("")).copied();
    let all_steps = (u64::from(days) * 86_400 / promql::STEP_SECONDS) as f64;
    let charts = Charts {
        start,
        end: now,
        step_seconds: step,
        cpu: series(cpu_series),
        memory: series(memory_series),
        restarts: markers(&events, "restart"),
        ooms: markers(&events, "oom"),
    };
    Ok((
        usage,
        charts,
        Some(steps.map_or(0.0, |s| (s / all_steps).min(1.0))),
        first.map(|f| now - f as i64),
    ))
}

/// Одна точка `metrics.k8s.io`: максимум по подам workload'а.
fn from_pods_now(containers: Vec<ContainerCurrent>, now: &[PodNow]) -> Vec<ContainerUsage> {
    containers
        .into_iter()
        .map(|current| {
            let values = now
                .iter()
                .flat_map(|p| &p.containers)
                .filter(|c| c.name == current.name);
            let (cpu, memory) =
                values.fold((None, None), |(cpu, mem): (Option<f64>, Option<f64>), c| {
                    (
                        c.cpu.map(|v| cpu.map_or(v, |x| x.max(v))).or(cpu),
                        c.memory.map(|v| mem.map_or(v, |x| x.max(v))).or(mem),
                    )
                });
            ContainerUsage {
                current,
                stats: ContainerStats {
                    cpu: CpuStats {
                        max: cpu,
                        ..Default::default()
                    },
                    memory: MemoryStats {
                        max: memory,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            }
        })
        .collect()
}

/// Статистика workload'а за окно `days` по выбранному источнику; без источника истории —
/// текущие значения `metrics.k8s.io`, без них — `METRICS_UNAVAILABLE` с причинами.
pub async fn workload_usage(
    r: &ReadClient,
    sel: &Selected,
    namespace: &str,
    kind: WorkloadKind,
    name: &str,
    days: u32,
    now: i64,
) -> AppResult<WorkloadUsage> {
    let days = check_window(days)?;
    let spec = get_spec(r, namespace, kind, name).await?;
    let w = WorkloadRef {
        kind,
        name: name.to_string(),
    };
    let pods = match workload_pods(r, namespace, &w).await {
        Ok(l) => l.items,
        // Без прав на поды нет только пометки LimitRange.
        Err(e) if e.code == ErrorCode::Forbidden => Vec::new(),
        Err(e) => return Err(e),
    };
    let containers = current(spec.template.as_ref().and_then(|t| t.spec.as_ref()), &pods);
    let mut out = WorkloadUsage {
        namespace: namespace.to_string(),
        kind,
        name: name.to_string(),
        window_days: days,
        source: crate::metrics::pods::info(),
        source_status: sel.status.clone(),
        coverage: None,
        data_seconds: None,
        owner_metrics: false,
        replicas: spec.row.desired,
        surge: spec.surge,
        containers: Vec::new(),
        charts: None,
        pods_now: Vec::new(),
    };
    if let Some(src) = sel.history.as_deref() {
        let owner = has_owner_metrics(src, namespace, now)
            .await
            .map_err(failed)?;
        let selector = if owner {
            Pods::Owner {
                namespace: namespace.into(),
                kind,
                name: name.into(),
            }
        } else {
            Pods::Name {
                namespace: namespace.into(),
                kind,
                name: name.into(),
            }
        };
        let (usage, charts, coverage, data) =
            from_history(src, &selector, days, now, owner, containers)
                .await
                .map_err(failed)?;
        out.source = src.info().clone();
        out.owner_metrics = owner;
        out.containers = usage;
        out.charts = Some(charts);
        out.coverage = coverage;
        out.data_seconds = data;
        return Ok(out);
    }
    if !sel.status.pod_metrics {
        return Err(sel.status.unavailable());
    }
    let names: std::collections::BTreeSet<&str> = pods
        .iter()
        .filter_map(|p| p.metadata.name.as_deref())
        .collect();
    let now_list: Vec<PodNow> = pods_now(
        r,
        namespace,
        spec.selector.as_deref().filter(|s| !s.is_empty()),
    )
    .await
    .map_err(failed)?
    .into_iter()
    .filter(|p| names.contains(p.pod.as_str()))
    .collect();
    out.containers = from_pods_now(containers, &now_list);
    out.pods_now = now_list;
    Ok(out)
}

/// Текущее потребление контейнеров пода (вкладка «Ресурсы» пода).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PodUsage {
    pub source: SourceInfo,
    pub containers: Vec<crate::metrics::pods::ContainerNow>,
}

/// Текущее потребление пода: по источнику истории, без него — `metrics.k8s.io`.
pub async fn pod_usage(
    r: &ReadClient,
    sel: &Selected,
    namespace: &str,
    pod: &str,
    now: i64,
) -> AppResult<PodUsage> {
    crate::k8s::read::check_name(pod)?;
    if let Some(src) = sel.history.as_deref() {
        let v = query(src, namespace, &promql::pod_now(namespace, pod), now)
            .await
            .map_err(failed)?;
        let stats = by_stat(&v);
        let mut names: Vec<&String> = stats.values().flat_map(|m| m.keys()).collect();
        names.sort();
        names.dedup();
        let get = |s: &str, c: &str| stats.get(s).and_then(|m| m.get(c)).copied();
        return Ok(PodUsage {
            source: src.info().clone(),
            containers: names
                .into_iter()
                .map(|c| crate::metrics::pods::ContainerNow {
                    name: c.clone(),
                    cpu: get("cpu", c),
                    memory: get("memory", c),
                })
                .collect(),
        });
    }
    if !sel.status.pod_metrics {
        return Err(sel.status.unavailable());
    }
    let now_list = pods_now(r, namespace, None).await.map_err(failed)?;
    Ok(PodUsage {
        source: crate::metrics::pods::info(),
        containers: now_list
            .into_iter()
            .find(|p| p.pod == pod)
            .map(|p| p.containers)
            .unwrap_or_default(),
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use crate::k8s::testing::{mock_client, status};
    use crate::metrics::prometheus::PromApi;
    use crate::metrics::prometheus::fake::{FakeSource, matrix, vector};
    use crate::metrics::select::Mode;

    pub const NOW: i64 = 1_700_000_000;

    /// API-сервер с Deployment `web` (2 реплики, контейнер `app` с requests и limits) и его
    /// подом.
    pub fn cluster(s: &crate::k8s::testing::Seen) -> (u16, serde_json::Value) {
        match s.path() {
            "/apis/apps/v1/namespaces/team-a/deployments/web" => (
                200,
                serde_json::json!({"apiVersion": "apps/v1", "kind": "Deployment",
                    "metadata": {"name": "web", "namespace": "team-a", "resourceVersion": "7"},
                    "spec": {"replicas": 2, "selector": {"matchLabels": {"app": "web"}},
                        "template": {"metadata": {"labels": {"app": "web"}}, "spec": {"containers": [
                            {"name": "app", "image": "web:1",
                             "resources": {"requests": {"cpu": "500m", "memory": "512Mi"},
                                           "limits": {"cpu": "1", "memory": "1Gi"}}}]}}}}),
            ),
            "/api/v1/namespaces/team-a/pods" => (
                200,
                serde_json::json!({"apiVersion": "v1", "kind": "PodList", "metadata": {},
                    "items": [{"metadata": {"name": "web-7d9f8c6b5d-x2x4z", "namespace": "team-a",
                        "labels": {"app": "web", "pod-template-hash": "7d9f8c6b5d"}},
                        "spec": {"containers": [{"name": "app", "image": "web:1"}]},
                        "status": {"phase": "Running"}}]}),
            ),
            "/apis/metrics.k8s.io/v1beta1/namespaces/team-a/pods" => (
                200,
                serde_json::json!({"apiVersion": "metrics.k8s.io/v1beta1", "kind": "PodMetricsList",
                    "metadata": {}, "items": [
                        {"metadata": {"name": "web-7d9f8c6b5d-x2x4z"}, "containers": [
                            {"name": "app", "usage": {"cpu": "120m", "memory": "300Mi"}}]},
                        {"metadata": {"name": "other-1"}, "containers": [
                            {"name": "app", "usage": {"cpu": "9", "memory": "9Gi"}}]}]}),
            ),
            _ => status(404, "NotFound", "no"),
        }
    }

    pub fn reader() -> ReadClient {
        ReadClient::new(mock_client(cluster).0, Duration::from_secs(5))
    }

    pub fn source_status(pod_metrics: bool) -> SourceStatus {
        SourceStatus {
            mode: Mode::Auto,
            selected: None,
            rejected: vec![],
            pod_metrics,
            checked_at_ms: 0,
        }
    }

    /// Prometheus со статистикой `app` за 7 дней.
    pub fn prometheus(owner: bool) -> FakeSource {
        FakeSource::new(move |api, q| {
            let stat = |pairs: &[(&str, f64)]| {
                let labels: Vec<[(&str, &str); 2]> = pairs
                    .iter()
                    .map(|(s, _)| [("container", "app"), ("stat", *s)])
                    .collect();
                vector(
                    &labels
                        .iter()
                        .zip(pairs)
                        .map(|(l, (_, v))| (&l[..], *v))
                        .collect::<Vec<_>>(),
                )
            };
            Ok(match api {
                PromApi::Query if q.starts_with("count(kube_pod_owner") => {
                    vector(&if owner { vec![(&[][..], 3.0)] } else { vec![] })
                }
                PromApi::Query if q.contains("\"cpu_p50\"") => stat(&[
                    ("cpu_p50", 0.1),
                    ("cpu_p95", 0.2),
                    ("cpu_p99", 0.3),
                    ("cpu_max", 0.5),
                ]),
                PromApi::Query if q.contains("\"memory_p95\"") => stat(&[
                    ("memory_p95", 300e6),
                    ("memory_p99", 350e6),
                    ("memory_max", 400e6),
                ]),
                PromApi::Query if q.contains("\"throttling_mean\"") => {
                    stat(&[("throttling_mean", 0.02), ("throttling_p95", 0.05)])
                }
                PromApi::Query if q.contains("\"oom_count\"") => {
                    if owner {
                        stat(&[
                            ("oom_count", 2.0),
                            ("oom_last", 1_699_990_000.0),
                            ("restarts", 3.0),
                        ])
                    } else {
                        vector(&[])
                    }
                }
                PromApi::Query if q.contains("\"steps\"") => vector(&[
                    (&[("stat", "steps")][..], 1814.4),
                    (&[("stat", "first")][..], (NOW - 6 * 86_400) as f64),
                ]),
                PromApi::QueryRange if q.contains("\"event\"") => matrix(&[
                    (
                        &[("container", "app"), ("event", "restart")][..],
                        &[(NOW - 100, 1.0), (NOW - 50, 0.0)][..],
                    ),
                    (
                        &[("container", "app"), ("event", "oom")][..],
                        &[(NOW - 100, 1.0)][..],
                    ),
                ]),
                PromApi::QueryRange => matrix(&[(
                    &[("container", "app")][..],
                    &[(NOW - 3024, 0.2), (NOW, 0.25)][..],
                )]),
                PromApi::Query => vector(&[]),
            })
        })
    }

    #[tokio::test]
    async fn history_gives_statistics_charts_and_coverage() {
        let src = Arc::new(prometheus(true));
        let sel = Selected {
            history: Some(src.clone()),
            status: source_status(false),
        };
        let u = workload_usage(
            &reader(),
            &sel,
            "team-a",
            WorkloadKind::Deployment,
            "web",
            7,
            NOW,
        )
        .await
        .unwrap();
        assert!(u.owner_metrics);
        assert_eq!((u.replicas, u.surge), (Some(2), 1));
        assert_eq!(u.source.location, "monitoring/prometheus-k8s:9090");
        let app = &u.containers[0];
        assert_eq!(app.current.requests.cpu, Some(0.5));
        assert_eq!(app.stats.cpu.p95, Some(0.2));
        assert_eq!(app.stats.memory.max, Some(400e6));
        assert_eq!(app.stats.throttling.mean, Some(0.02));
        assert_eq!(
            (app.stats.oom_count, app.stats.restarts),
            (Some(2), Some(3))
        );
        assert_eq!(app.stats.oom_last, Some(1_699_990_000));
        assert!((u.coverage.unwrap() - 0.9).abs() < 1e-9, "{:?}", u.coverage);
        assert_eq!(u.data_seconds, Some(6 * 86_400));
        let charts = u.charts.unwrap();
        assert_eq!(charts.step_seconds, 3024);
        assert_eq!(charts.cpu[0].points.len(), 2);
        assert_eq!(charts.restarts.len(), 1, "нули — не отметки");
        assert_eq!(charts.ooms[0].time, NOW - 100);
        // С kube-state-metrics поды выбраны по владельцу, а не по имени.
        let queries = src.queries();
        assert!(queries.iter().any(|q| q.contains("kube_replicaset_owner")));
        assert!(!queries.iter().any(|q| q.contains("pod=~")));
    }

    #[tokio::test]
    async fn without_kube_state_metrics_pods_are_chosen_by_name_and_ooms_are_unknown() {
        let src = Arc::new(prometheus(false));
        let sel = Selected {
            history: Some(src.clone()),
            status: source_status(false),
        };
        let u = workload_usage(
            &reader(),
            &sel,
            "team-a",
            WorkloadKind::Deployment,
            "web",
            7,
            NOW,
        )
        .await
        .unwrap();
        assert!(!u.owner_metrics);
        assert_eq!(u.containers[0].stats.oom_count, None);
        assert_eq!(u.containers[0].stats.restarts, None);
        assert!(src.queries().iter().any(|q| q.contains("pod=~\"web-")));
    }

    #[tokio::test]
    async fn pod_metrics_give_one_point_of_the_workload_pods() {
        let sel = Selected {
            history: None,
            status: source_status(true),
        };
        let u = workload_usage(
            &reader(),
            &sel,
            "team-a",
            WorkloadKind::Deployment,
            "web",
            7,
            NOW,
        )
        .await
        .unwrap();
        assert_eq!(
            u.source.kind,
            crate::metrics::prometheus::SourceKind::MetricsServer
        );
        assert_eq!(u.pods_now.len(), 1, "чужой под не попал");
        let app = &u.containers[0].stats;
        assert_eq!(app.cpu.max, Some(0.12));
        assert_eq!(app.cpu.p95, None);
        assert!(u.charts.is_none() && u.coverage.is_none());
    }

    #[tokio::test]
    async fn no_source_and_a_bad_window_are_errors() {
        let sel = Selected {
            history: None,
            status: source_status(false),
        };
        let e = workload_usage(
            &reader(),
            &sel,
            "team-a",
            WorkloadKind::Deployment,
            "web",
            7,
            NOW,
        )
        .await
        .unwrap_err();
        assert_eq!(e.code, ErrorCode::MetricsUnavailable);
        let e = workload_usage(
            &reader(),
            &sel,
            "team-a",
            WorkloadKind::Deployment,
            "web",
            3,
            NOW,
        )
        .await
        .unwrap_err();
        assert_eq!(e.key, "errors.resources.badWindow");
    }

    #[tokio::test]
    async fn pod_usage_from_history_or_pod_metrics() {
        let src = Arc::new(FakeSource::new(|_, q| {
            assert!(q.contains("pod=\"web-1\""), "{q}");
            Ok(vector(&[
                (&[("container", "app"), ("stat", "cpu")][..], 0.3),
                (&[("container", "app"), ("stat", "memory")][..], 1e8),
            ]))
        }));
        let sel = Selected {
            history: Some(src),
            status: source_status(false),
        };
        let p = pod_usage(&reader(), &sel, "team-a", "web-1", NOW)
            .await
            .unwrap();
        assert_eq!(p.containers[0].cpu, Some(0.3));
        let sel = Selected {
            history: None,
            status: source_status(true),
        };
        let p = pod_usage(&reader(), &sel, "team-a", "web-7d9f8c6b5d-x2x4z", NOW)
            .await
            .unwrap();
        assert_eq!(p.containers[0].memory, Some(300.0 * 1024.0 * 1024.0));
    }
}
