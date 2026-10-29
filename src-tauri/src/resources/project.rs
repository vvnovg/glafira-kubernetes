//! Ресурсы проекта (спец. §7.4): по каждому workload'у — суммарные requests и limits работающих
//! подов против фактического потребления за окно (p95 CPU и максимум памяти «худшего пода»),
//! резерв, риски (троттлинг, OOM, память у лимита) и избыток. Метрики — агрегирующие запросы по
//! проекту (по поду и контейнеру); поды с workload'ами сопоставляет Rust: работающие — по
//! владельцу, удалённые за окно — по форме имени. Те же значения за 24 ч проверяют правила
//! `resources.*` (`health`).

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{Pod, ResourceQuota};
use kube::api::ListParams;
use serde::Serialize;
use tokio::time::Instant;

use super::current::Amounts;
use super::recommend::QuotaRoom;
use crate::error::AppResult;
use crate::k8s::pods::{OwnerRef, owner_of};
use crate::k8s::quantity::parse_quantity;
use crate::k8s::read::ReadClient;
use crate::k8s::workloads::{KindError, WorkloadKind, cron_owner, list_workloads};
use crate::metrics::prometheus::{Failure, MetricsSource, SourceInfo, query};
use crate::metrics::promql;
use crate::metrics::select::{Selected, SourceStatus};
use crate::platform::ApiGroups;
use crate::sync::MutexExt;

/// Дедлайн сбора по проекту.
pub const PROJECT_DEADLINE: Duration = Duration::from_secs(45);
/// Сколько живёт собранное по проекту.
pub const PROJECT_TTL: Duration = Duration::from_secs(5 * 60);
/// Троттлинг контейнера с CPU limit в среднем — риск (`resources.cpuThrottled`).
pub const THROTTLED_MEAN: f64 = 0.25;
/// Максимум памяти от memory-limit — риск (`resources.memoryNearLimit`).
pub const MEMORY_NEAR_LIMIT: f64 = 0.90;
/// Избыток CPU (`resources.overProvisioned`): request ≥ 3 × p95 и разница ≥ 0,5 ядра на под.
pub const CPU_OVER_FACTOR: f64 = 3.0;
pub const CPU_OVER_MIN: f64 = 0.5;
/// Избыток памяти: request ≥ 2 × максимум и разница ≥ 512Mi на под.
pub const MEMORY_OVER_FACTOR: f64 = 2.0;
pub const MEMORY_OVER_MIN: f64 = 512.0 * 1024.0 * 1024.0;

/// Потребление контейнера одного пода за окно.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Use {
    pub cpu_p95: Option<f64>,
    pub memory_max: Option<f64>,
    pub throttling_mean: Option<f64>,
    pub oom_count: Option<u32>,
}

impl Use {
    /// Худшее из двух подов.
    pub(crate) fn worst(self, o: Self) -> Self {
        Self {
            cpu_p95: larger(self.cpu_p95, o.cpu_p95),
            memory_max: larger(self.memory_max, o.memory_max),
            throttling_mean: larger(self.throttling_mean, o.throttling_mean),
            oom_count: match (self.oom_count, o.oom_count) {
                (Some(a), Some(b)) => Some(a + b),
                (a, b) => a.or(b),
            },
        }
    }
}

/// Потребление по поду и контейнеру за окно: `(под, контейнер) → значения`. `oom` — ещё и
/// эпизоды OOM (kube-state-metrics).
pub async fn pod_container_use(
    src: &dyn MetricsSource,
    namespace: &str,
    days: u32,
    now: i64,
    oom: bool,
) -> Result<BTreeMap<(String, String), Use>, Failure> {
    let q = [
        promql::project_cpu_p95(namespace, days),
        promql::project_memory_max(namespace, days),
        promql::project_throttling_mean(namespace, days),
        promql::project_oom_count(namespace, days),
    ];
    let (cpu, memory, throttling, ooms) = tokio::try_join!(
        query(src, namespace, &q[0], now),
        query(src, namespace, &q[1], now),
        query(src, namespace, &q[2], now),
        async {
            if oom {
                query(src, namespace, &q[3], now).await
            } else {
                Ok(Vec::new())
            }
        },
    )?;
    let mut out: BTreeMap<(String, String), Use> = BTreeMap::new();
    let key = |s: &crate::metrics::prometheus::Sample| {
        (s.label("pod").to_string(), s.label("container").to_string())
    };
    for s in &cpu {
        out.entry(key(s)).or_default().cpu_p95 = Some(s.value);
    }
    for s in &memory {
        out.entry(key(s)).or_default().memory_max = Some(s.value);
    }
    for s in &throttling {
        out.entry(key(s)).or_default().throttling_mean = Some(s.value);
    }
    for s in &ooms {
        out.entry(key(s)).or_default().oom_count = Some(s.value.round() as u32);
    }
    Ok(out)
}

/// Риск контейнера workload'а.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "code", rename_all = "camelCase")]
pub enum Risk {
    /// Средний троттлинг контейнера с CPU limit выше 25 %.
    Throttled { container: String, mean: f64 },
    /// Эпизоды OOM за окно.
    Oom { container: String, count: u32 },
    /// Максимум памяти — 90 % memory-limit и больше.
    MemoryNearLimit {
        container: String,
        max: f64,
        limit: f64,
    },
}

/// Избыток: request намного больше потребления (на под).
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Excess {
    pub container: String,
    /// `cpu` или `memory`.
    pub resource: String,
    pub request: f64,
    /// p95 CPU или максимум памяти.
    pub usage: f64,
}

/// `a ≥ b` с запасом на погрешность дробей: граница входит (`≥ 90 %`, `≥ 0,5 ядра`).
fn at_least(a: f64, b: f64) -> bool {
    a >= b - 1e-9 * b.abs().max(1.0)
}

/// Риски и избыток контейнера по его request и limit на под (спец. §8.2, правила
/// `resources.*`).
pub fn assess(
    container: &str,
    u: &Use,
    request: Amounts,
    limit: Amounts,
) -> (Vec<Risk>, Vec<Excess>) {
    let mut risks = Vec::new();
    let mut excess = Vec::new();
    if let (Some(mean), Some(_)) = (u.throttling_mean, limit.cpu)
        && mean > THROTTLED_MEAN
    {
        risks.push(Risk::Throttled {
            container: container.into(),
            mean,
        });
    }
    if let Some(n) = u.oom_count.filter(|n| *n > 0) {
        risks.push(Risk::Oom {
            container: container.into(),
            count: n,
        });
    }
    if let (Some(max), Some(l)) = (u.memory_max, limit.memory)
        && l > 0.0
        && at_least(max, MEMORY_NEAR_LIMIT * l)
    {
        risks.push(Risk::MemoryNearLimit {
            container: container.into(),
            max,
            limit: l,
        });
    }
    if let (Some(r), Some(p95)) = (request.cpu, u.cpu_p95)
        && at_least(r, CPU_OVER_FACTOR * p95)
        && at_least(r - p95, CPU_OVER_MIN)
    {
        excess.push(Excess {
            container: container.into(),
            resource: "cpu".into(),
            request: r,
            usage: p95,
        });
    }
    if let (Some(r), Some(max)) = (request.memory, u.memory_max)
        && at_least(r, MEMORY_OVER_FACTOR * max)
        && at_least(r - max, MEMORY_OVER_MIN)
    {
        excess.push(Excess {
            container: container.into(),
            resource: "memory".into(),
            request: r,
            usage: max,
        });
    }
    (risks, excess)
}

/// Workload, которому принадлежит под: работающий — по владельцу (поды Job из CronJob — к
/// CronJob), остальные — по форме имени; из подходящих — с самым длинным именем.
pub fn workload_of(
    pod: &str,
    owner: Option<(WorkloadKind, String)>,
    workloads: &[(WorkloadKind, String)],
) -> Option<(WorkloadKind, String)> {
    if owner.is_some() {
        return owner;
    }
    workloads
        .iter()
        .filter(|(k, n)| crate::k8s::events::pod_of(&format!("{k:?}"), n, pod))
        .max_by_key(|(_, n)| n.len())
        .cloned()
}

/// Вид workload'а по имени вида Kubernetes (`Deployment`).
pub fn kind_of(s: &str) -> Option<WorkloadKind> {
    WorkloadKind::ALL
        .into_iter()
        .find(|k| format!("{k:?}") == s)
}

/// Workload, которому принадлежит под с владельцем `o` (`owner_of`): под Job из CronJob
/// (`cron` — имя его CronJob) относится к CronJob, чтобы временное имя Job не попадало в ключ
/// workload'а; остальные — к своему владельцу. `None` — владелец не workload.
pub(crate) fn workload_owner(o: &OwnerRef, cron: Option<&str>) -> Option<(WorkloadKind, String)> {
    match (o.kind.as_str(), cron) {
        ("Job", Some(c)) => Some((WorkloadKind::CronJob, c.to_string())),
        (k, _) => Some((kind_of(k)?, o.name.clone())),
    }
}

/// Владелец-workload работающих подов: `pod → (вид, имя)`.
pub fn owners(pods: &[Pod], jobs: &[Job]) -> HashMap<String, (WorkloadKind, String)> {
    let cron: HashMap<&str, &str> = jobs
        .iter()
        .filter_map(|j| Some((j.metadata.name.as_deref()?, cron_owner(&j.metadata)?)))
        .collect();
    pods.iter()
        .filter_map(|p| {
            let name = p.metadata.name.clone()?;
            let o = owner_of(p)?;
            let w = workload_owner(&o, cron.get(o.name.as_str()).copied())?;
            Some((name, w))
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadResources {
    pub kind: WorkloadKind,
    pub name: String,
    /// Работающих подов сейчас.
    pub pods: u32,
    /// Суммы по работающим подам.
    pub requests: Amounts,
    pub limits: Amounts,
    /// Потребление: p95 CPU и максимум памяти «худшего пода» по контейнерам × поды; без работающих
    /// подов — неизвестно (`None`).
    pub usage: Amounts,
    /// Requests минус потребление.
    pub reserve: Amounts,
    pub risks: Vec<Risk>,
    pub excess: Vec<Excess>,
}

impl WorkloadResources {
    /// Есть что поменять: риск или избыток (фильтр «только с рекомендациями»).
    pub fn actionable(&self) -> bool {
        !self.risks.is_empty() || !self.excess.is_empty()
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectUsage {
    pub namespace: String,
    pub window_days: u32,
    pub source: SourceInfo,
    pub source_status: SourceStatus,
    pub workloads: Vec<WorkloadResources>,
    /// Виды workloads, которые не прочитались.
    pub errors: Vec<KindError>,
    /// Список workloads или подов неполный.
    pub truncated: bool,
    pub quotas: Vec<QuotaRoom>,
    pub collected_at_ms: i64,
}

/// Под считается работающим, пока он не завершился: любая фаза, кроме `Succeeded` и `Failed`
/// (`Pending` и `Unknown` — тоже), и фаза без значения.
pub(crate) fn is_running(phase: Option<&str>) -> bool {
    !matches!(phase, Some("Succeeded" | "Failed"))
}

fn running(p: &Pod) -> bool {
    is_running(p.status.as_ref().and_then(|s| s.phase.as_deref()))
}

fn pod_amounts(p: &Pod, limits: bool) -> BTreeMap<String, Amounts> {
    let Some(spec) = p.spec.as_ref() else {
        return BTreeMap::new();
    };
    let init = spec
        .init_containers
        .iter()
        .flatten()
        .filter(|c| c.restart_policy.as_deref() == Some("Always"));
    spec.containers
        .iter()
        .chain(init)
        .map(|c| {
            let r = c.resources.as_ref();
            let m = if limits {
                r.and_then(|r| r.limits.as_ref())
            } else {
                r.and_then(|r| r.requests.as_ref())
            };
            let q = |k: &str| m.and_then(|m| m.get(k)).and_then(|v| parse_quantity(&v.0));
            (
                c.name.clone(),
                Amounts {
                    cpu: q("cpu"),
                    memory: q("memory"),
                },
            )
        })
        .collect()
}

pub(crate) fn larger(a: Option<f64>, b: Option<f64>) -> Option<f64> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (a, b) => a.or(b),
    }
}

/// Покомпонентно наибольшее из двух значений: request или limit контейнера на под берётся
/// наибольшим среди подов.
pub(crate) fn larger_amounts(a: Amounts, b: Amounts) -> Amounts {
    Amounts {
        cpu: larger(a.cpu, b.cpu),
        memory: larger(a.memory, b.memory),
    }
}

fn add(a: Option<f64>, b: Option<f64>) -> Option<f64> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a + b),
        (a, b) => a.or(b),
    }
}

/// Сводка по workload'ам из подов и потребления по поду и контейнеру.
pub fn summarize(
    workloads: &[(WorkloadKind, String)],
    pods: &[Pod],
    owner: &HashMap<String, (WorkloadKind, String)>,
    usage: &BTreeMap<(String, String), Use>,
) -> Vec<WorkloadResources> {
    // Худшее потребление по контейнерам workload'а: (вид, имя) → контейнер → значения.
    let mut worst: HashMap<(WorkloadKind, String), BTreeMap<String, Use>> = HashMap::new();
    for ((pod, container), u) in usage {
        if let Some(w) = workload_of(pod, owner.get(pod).cloned(), workloads) {
            let e = worst
                .entry(w)
                .or_default()
                .entry(container.clone())
                .or_default();
            *e = e.worst(*u);
        }
    }
    workloads
        .iter()
        .map(|(kind, name)| {
            let mine: Vec<&Pod> = pods
                .iter()
                .filter(|p| {
                    running(p)
                        && p.metadata
                            .name
                            .as_ref()
                            .and_then(|n| owner.get(n))
                            .is_some_and(|(k, n)| k == kind && n == name)
                })
                .collect();
            let mut requests = Amounts::default();
            let mut limits = Amounts::default();
            // Request и limit контейнера на под — наибольшие среди подов.
            let mut per_pod: BTreeMap<String, (Amounts, Amounts)> = BTreeMap::new();
            for p in &mine {
                let (req, lim) = (pod_amounts(p, false), pod_amounts(p, true));
                for (c, a) in &req {
                    requests.cpu = add(requests.cpu, a.cpu);
                    requests.memory = add(requests.memory, a.memory);
                    let e = per_pod.entry(c.clone()).or_default();
                    e.0 = larger_amounts(e.0, *a);
                }
                for (c, a) in &lim {
                    limits.cpu = add(limits.cpu, a.cpu);
                    limits.memory = add(limits.memory, a.memory);
                    let e = per_pod.entry(c.clone()).or_default();
                    e.1 = larger_amounts(e.1, *a);
                }
            }
            let n = mine.len() as f64;
            let containers = worst.get(&(*kind, name.clone()));
            let mut usage_sum = Amounts::default();
            let mut risks = Vec::new();
            let mut excess = Vec::new();
            for (c, u) in containers.into_iter().flatten() {
                // Без работающих подов умножать потребление худшего пода не на что: сумма неизвестна
                // (`None`), а не 0 — иначе «0 / 0Mi» выглядело бы как замер.
                if !mine.is_empty() {
                    usage_sum.cpu = add(usage_sum.cpu, u.cpu_p95.map(|v| v * n));
                    usage_sum.memory = add(usage_sum.memory, u.memory_max.map(|v| v * n));
                }
                let (req, lim) = per_pod.get(c).copied().unwrap_or_default();
                let (r, e) = assess(c, u, req, lim);
                risks.extend(r);
                excess.extend(e);
            }
            // Нет потребления (метрик по подам не пришло) — резерв неизвестен, а не весь request.
            let minus = |a: Option<f64>, b: Option<f64>| Some(a? - b?);
            WorkloadResources {
                kind: *kind,
                name: name.clone(),
                pods: mine.len() as u32,
                reserve: Amounts {
                    cpu: minus(requests.cpu, usage_sum.cpu),
                    memory: minus(requests.memory, usage_sum.memory),
                },
                requests,
                limits,
                usage: usage_sum,
                risks,
                excess,
            }
        })
        .collect()
}

/// Jobs проекта — по ним поды Job находят свой CronJob. Сбой чтения не прячется: без Jobs
/// CronJob выглядел бы пустым (0 подов) без объяснения, поэтому он идёт в `errors` видом `Job`,
/// как и другие виды, которые не прочитались. Если `list_workloads` уже назвал `Job`, второй
/// записи нет.
fn jobs_or_error(
    jobs: AppResult<crate::k8s::read::Listed<Job>>,
    errors: &mut Vec<KindError>,
) -> Vec<Job> {
    match jobs {
        Ok(l) => l.items,
        Err(error) => {
            if !errors.iter().any(|e| e.kind == WorkloadKind::Job) {
                errors.push(KindError {
                    kind: WorkloadKind::Job,
                    error,
                });
            }
            Vec::new()
        }
    }
}

/// Ресурсы проекта по выбранному источнику: история за окно или текущие значения
/// `metrics.k8s.io`.
pub async fn project_usage(
    r: &ReadClient,
    sel: &Selected,
    groups: &ApiGroups,
    namespace: &str,
    days: u32,
    now: i64,
) -> AppResult<ProjectUsage> {
    let days = super::usage::check_window(days)?;
    let ns = Some(namespace);
    let all = ListParams::default();
    let (list, pods, jobs, quotas) = tokio::join!(
        list_workloads(r, namespace, groups),
        r.list::<Pod>(ns, &all),
        r.list::<Job>(ns, &all),
        r.list::<ResourceQuota>(ns, &all),
    );
    let pods = pods?;
    let mut errors = list.errors;
    let jobs = jobs_or_error(jobs, &mut errors);
    let workloads: Vec<(WorkloadKind, String)> = list
        .items
        .iter()
        .map(|w| (w.kind, w.name.clone()))
        .collect();
    let owner = owners(&pods.items, &jobs);
    let (source, usage) = if let Some(src) = sel.history.as_deref() {
        let u = pod_container_use(src, namespace, days, now, true)
            .await
            .map_err(|f| f.error)?;
        (src.info().clone(), u)
    } else if sel.status.pod_metrics {
        let now_list = crate::metrics::pods::pods_now(r, namespace, None)
            .await
            .map_err(|f| f.error)?;
        let mut u = BTreeMap::new();
        for p in now_list {
            for c in p.containers {
                u.insert(
                    (p.pod.clone(), c.name),
                    Use {
                        cpu_p95: c.cpu,
                        memory_max: c.memory,
                        ..Default::default()
                    },
                );
            }
        }
        (crate::metrics::pods::info(), u)
    } else {
        return Err(sel.status.unavailable());
    };
    Ok(ProjectUsage {
        namespace: namespace.to_string(),
        window_days: days,
        source,
        source_status: sel.status.clone(),
        workloads: summarize(&workloads, &pods.items, &owner, &usage),
        errors,
        truncated: list.truncated || pods.truncated,
        quotas: quotas
            .map(|l| super::constraints::quota_rooms(&l.items))
            .unwrap_or_default(),
        collected_at_ms: now * 1000,
    })
}

/// Ключ собранного по проекту: подключение, проект, окно.
type ProjectKey = (String, String, u32);

/// Собранное по проектам: ключ → (когда, результат), живёт `PROJECT_TTL`. Сброс подключения
/// (`forget_connection`) меняет его эпоху: результат, сбор которого начался до сброса, в кэш не
/// попадает (`put`) — как у кэша прав (`AccessCache`).
#[derive(Default)]
pub struct ProjectCache {
    inner: std::sync::Mutex<ProjectInner>,
}

#[derive(Default)]
struct ProjectInner {
    results: HashMap<ProjectKey, (Instant, ProjectUsage)>,
    /// Растёт при каждом `forget_connection`.
    epochs: HashMap<String, u64>,
}

impl ProjectCache {
    pub fn get(&self, connection_id: &str, namespace: &str, days: u32) -> Option<ProjectUsage> {
        let inner = self.inner.lock_safe();
        let (at, u) = inner
            .results
            .get(&(connection_id.into(), namespace.into(), days))?;
        (at.elapsed() < PROJECT_TTL).then(|| u.clone())
    }

    /// Эпоха подключения: читается до сбора и передаётся в `put`.
    pub fn epoch(&self, connection_id: &str) -> u64 {
        let inner = self.inner.lock_safe();
        inner.epochs.get(connection_id).copied().unwrap_or(0)
    }

    /// Кладёт результат, если подключение не сбрасывали с эпохи `epoch`. `false` — сбрасывали
    /// (профиль, токен, отключение, 401), и числа, собранные прежним клиентом, не кэшируются.
    pub fn put(&self, connection_id: &str, epoch: u64, u: &ProjectUsage) -> bool {
        let mut inner = self.inner.lock_safe();
        if inner.epochs.get(connection_id).copied().unwrap_or(0) != epoch {
            return false;
        }
        inner
            .results
            .retain(|_, (at, _)| at.elapsed() < PROJECT_TTL);
        inner.results.insert(
            (connection_id.into(), u.namespace.clone(), u.window_days),
            (Instant::now(), u.clone()),
        );
        true
    }

    /// Забывает всё собранное по подключению: после смены профиля или токена, отключения и
    /// удаления прежние числа относятся к другому кластеру или другой учётной записи.
    pub fn forget_connection(&self, connection_id: &str) {
        let mut inner = self.inner.lock_safe();
        inner.results.retain(|(c, _, _), _| c != connection_id);
        *inner.epochs.entry(connection_id.into()).or_default() += 1;
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration as StdDuration;

    use super::*;
    use crate::k8s::testing::{mock_client, status};
    use crate::metrics::prometheus::fake::{FakeSource, vector};
    use crate::resources::usage::tests::source_status;

    const MI: f64 = 1024.0 * 1024.0;

    fn pod(
        name: &str,
        owner: (&str, &str),
        labels: serde_json::Value,
        phase: &str,
        req: (&str, &str),
        lim: Option<(&str, &str)>,
    ) -> serde_json::Value {
        let mut res = serde_json::json!({"requests": {"cpu": req.0, "memory": req.1}});
        if let Some((c, m)) = lim {
            res["limits"] = serde_json::json!({"cpu": c, "memory": m});
        }
        serde_json::json!({"metadata": {"name": name, "namespace": "team-a", "labels": labels,
            "ownerReferences": [{"apiVersion": "apps/v1", "kind": owner.0, "name": owner.1,
                "uid": "u", "controller": true}]},
            "spec": {"containers": [{"name": "app", "image": "a", "resources": res}]},
            "status": {"phase": phase}})
    }

    fn cluster(s: &crate::k8s::testing::Seen) -> (u16, serde_json::Value) {
        let list = |kind: &str, items: serde_json::Value| {
            (
                200,
                serde_json::json!({"apiVersion": "v1", "kind": kind, "metadata": {}, "items": items}),
            )
        };
        let hash = serde_json::json!({"app": "web", "pod-template-hash": "7d9f8c6b5d"});
        match s.path() {
            "/apis/apps/v1/namespaces/team-a/deployments" => list(
                "DeploymentList",
                serde_json::json!([{"metadata": {"name": "web"}, "spec": {"replicas": 2,
                    "selector": {"matchLabels": {"app": "web"}},
                    "template": crate::k8s::workloads::fixtures::template("web", "web:1")}}]),
            ),
            "/apis/batch/v1/namespaces/team-a/cronjobs" => list(
                "CronJobList",
                serde_json::json!([{"metadata": {"name": "report"}, "spec": {"schedule": "* * * * *",
                    "jobTemplate": {"spec": {"template": crate::k8s::workloads::fixtures::template("report", "r:1")}}}}]),
            ),
            "/apis/batch/v1/namespaces/team-a/jobs" => list(
                "JobList",
                serde_json::json!([{"metadata": {"name": "report-28000000", "ownerReferences": [
                    {"apiVersion": "batch/v1", "kind": "CronJob", "name": "report", "uid": "c", "controller": true}]},
                    "spec": {"template": crate::k8s::workloads::fixtures::template("report", "r:1")}}]),
            ),
            "/api/v1/namespaces/team-a/pods" => list(
                "PodList",
                serde_json::json!([
                    pod(
                        "web-7d9f8c6b5d-x2x4z",
                        ("ReplicaSet", "web-7d9f8c6b5d"),
                        hash.clone(),
                        "Running",
                        ("2", "4Gi"),
                        Some(("2", "1Gi"))
                    ),
                    pod(
                        "web-7d9f8c6b5d-b2c4d",
                        ("ReplicaSet", "web-7d9f8c6b5d"),
                        hash,
                        "Running",
                        ("2", "4Gi"),
                        Some(("2", "1Gi"))
                    ),
                    pod(
                        "report-28000000-k7m9q",
                        ("Job", "report-28000000"),
                        serde_json::json!({}),
                        "Succeeded",
                        ("100m", "64Mi"),
                        None
                    ),
                ]),
            ),
            "/api/v1/namespaces/team-a/resourcequotas" => list(
                "ResourceQuotaList",
                serde_json::json!([{"metadata": {"name": "q"}, "status": {"hard": {"requests.cpu": "10"},
                    "used": {"requests.cpu": "4100m"}}}]),
            ),
            _ => status(404, "NotFound", "no"),
        }
    }

    fn prometheus() -> FakeSource {
        FakeSource::new(|_, q| {
            let row = |pod: &'static str, v: f64| (vec![("pod", pod), ("container", "app")], v);
            let rows: Vec<(Vec<(&str, &str)>, f64)> =
                if q.contains("container_cpu_usage_seconds_total") {
                    // Удалённый за окно под web — по форме имени.
                    vec![
                        row("web-7d9f8c6b5d-x2x4z", 0.2),
                        row("web-5f6d7c8b9d-q2w4x", 0.3),
                        row("report-28000000-k7m9q", 0.05),
                    ]
                } else if q.contains("container_memory_working_set_bytes") {
                    vec![row("web-7d9f8c6b5d-x2x4z", 950.0 * MI)]
                } else if q.contains("cfs_throttled") {
                    vec![row("web-7d9f8c6b5d-b2c4d", 0.4)]
                } else {
                    vec![row("web-7d9f8c6b5d-b2c4d", 1.0)]
                };
            let refs: Vec<(&[(&str, &str)], f64)> =
                rows.iter().map(|(l, v)| (&l[..], *v)).collect();
            Ok(vector(&refs))
        })
    }

    #[tokio::test]
    async fn workloads_get_totals_usage_reserve_risks_and_excess() {
        let r = ReadClient::new(mock_client(cluster).0, StdDuration::from_secs(5));
        let sel = Selected {
            history: Some(Arc::new(prometheus())),
            status: source_status(false),
        };
        let u = project_usage(&r, &sel, &ApiGroups::default(), "team-a", 7, 1_700_000_000)
            .await
            .unwrap();
        let web = u.workloads.iter().find(|w| w.name == "web").unwrap();
        assert_eq!(web.pods, 2);
        assert_eq!(
            web.requests,
            Amounts {
                cpu: Some(4.0),
                memory: Some(8192.0 * MI)
            }
        );
        // Худший под — удалённый (0,3), × 2 работающих пода.
        assert!(
            (web.usage.cpu.unwrap() - 0.6).abs() < 1e-9,
            "{:?}",
            web.usage
        );
        assert!((web.reserve.cpu.unwrap() - 3.4).abs() < 1e-9);
        assert_eq!(web.risks.len(), 3, "{:?}", web.risks);
        assert!(
            matches!(&web.risks[0], Risk::Throttled { mean, .. } if (*mean - 0.4).abs() < 1e-9)
        );
        assert!(matches!(&web.risks[1], Risk::Oom { count: 1, .. }));
        assert!(matches!(&web.risks[2], Risk::MemoryNearLimit { .. }));
        assert_eq!(
            web.excess
                .iter()
                .map(|e| e.resource.as_str())
                .collect::<Vec<_>>(),
            ["cpu", "memory"]
        );
        assert!(web.actionable());
        let report = u.workloads.iter().find(|w| w.name == "report").unwrap();
        assert_eq!(
            report.pods, 0,
            "завершённый под Job не считается работающим"
        );
        assert_eq!(
            report.usage,
            Amounts::default(),
            "нет работающих подов — потребление неизвестно, а не 0"
        );
        assert!(!report.actionable());
        assert_eq!(u.quotas[0].resource, "requests.cpu");
        assert!(!u.truncated);
    }

    /// Метрик по подам нет вовсе или нет по ресурсу: резерв неизвестен (`None`), а не весь
    /// request — иначе такой workload встал бы первым по резерву.
    #[tokio::test]
    async fn reserve_is_unknown_where_the_usage_is_unknown() {
        let r = ReadClient::new(mock_client(cluster).0, StdDuration::from_secs(5));
        let run = |src: FakeSource| {
            let r = r.clone();
            async move {
                let sel = Selected {
                    history: Some(Arc::new(src)),
                    status: source_status(false),
                };
                let u = project_usage(&r, &sel, &ApiGroups::default(), "team-a", 7, 1_700_000_000)
                    .await
                    .unwrap();
                u.workloads.into_iter().find(|w| w.name == "web").unwrap()
            }
        };
        let web = run(FakeSource::new(|_, _| Ok(vector(&[])))).await;
        assert_eq!(web.pods, 2);
        assert_eq!(web.requests.cpu, Some(4.0));
        assert_eq!(web.usage, Amounts::default());
        assert_eq!(web.reserve, Amounts::default(), "{:?}", web.reserve);
        // Только CPU: резерв по CPU известен, по памяти — нет.
        let web = run(FakeSource::new(|_, q| {
            let rows: Vec<(&[(&str, &str)], f64)> =
                if q.contains("container_cpu_usage_seconds_total") {
                    vec![(
                        &[("pod", "web-7d9f8c6b5d-x2x4z"), ("container", "app")],
                        0.2,
                    )]
                } else {
                    vec![]
                };
            Ok(vector(&rows))
        }))
        .await;
        assert!(
            (web.reserve.cpu.unwrap() - 3.6).abs() < 1e-9,
            "{:?}",
            web.reserve
        );
        assert_eq!(web.reserve.memory, None);
    }

    /// Jobs не прочитались, а виды workload'ов — да: сбой не прячется, остальное остаётся.
    /// Список Jobs читают и `list_workloads`, и сам сбор (поды CronJob): второй запрос к Jobs
    /// отвечает 500.
    #[tokio::test]
    async fn failed_jobs_list_is_reported_and_the_rest_stays() {
        let requests = Arc::new(AtomicUsize::new(0));
        let seen = requests.clone();
        let handler = move |s: &crate::k8s::testing::Seen| {
            if s.path() == "/apis/batch/v1/namespaces/team-a/jobs"
                && seen.fetch_add(1, Ordering::SeqCst) == 1
            {
                return status(500, "InternalError", "boom");
            }
            cluster(s)
        };
        let r = ReadClient::new(mock_client(handler).0, StdDuration::from_secs(5));
        let sel = Selected {
            history: Some(Arc::new(prometheus())),
            status: source_status(false),
        };
        let u = project_usage(&r, &sel, &ApiGroups::default(), "team-a", 7, 1_700_000_000)
            .await
            .unwrap();
        assert_eq!(requests.load(Ordering::SeqCst), 2, "Jobs читают дважды");
        let jobs: Vec<_> = u
            .errors
            .iter()
            .filter(|e| e.kind == WorkloadKind::Job)
            .collect();
        assert_eq!(jobs.len(), 1, "{:?}", u.errors);
        assert_eq!(
            u.workloads.iter().find(|w| w.name == "web").unwrap().pods,
            2
        );
        assert!(u.workloads.iter().any(|w| w.name == "report"));
    }

    #[test]
    fn jobs_failure_is_listed_once_per_kind() {
        use crate::k8s::read::Listed;
        let fail = || Err(crate::k8s::errors::timeout());
        let mut errors = Vec::new();
        assert!(jobs_or_error(fail(), &mut errors).is_empty());
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].kind, WorkloadKind::Job);
        // Вид уже назван в `list_workloads`: второй записи нет.
        assert!(jobs_or_error(fail(), &mut errors).is_empty());
        assert_eq!(errors.len(), 1);
        // Другой вид не мешает.
        let mut errors = vec![KindError {
            kind: WorkloadKind::CronJob,
            error: crate::k8s::errors::timeout(),
        }];
        jobs_or_error(fail(), &mut errors);
        assert_eq!(errors.len(), 2);
        // Прочитались — ошибок нет, Jobs возвращаются.
        let job: Job =
            serde_json::from_value(serde_json::json!({"metadata": {"name": "j"}})).unwrap();
        let ok = Ok(Listed {
            items: vec![job],
            truncated: false,
        });
        let mut errors = Vec::new();
        assert_eq!(jobs_or_error(ok, &mut errors).len(), 1);
        assert!(errors.is_empty());
    }

    #[test]
    fn pods_map_to_workloads_by_owner_or_by_name() {
        let w = [
            (WorkloadKind::Deployment, "web".to_string()),
            (WorkloadKind::Deployment, "web-api".to_string()),
            (WorkloadKind::StatefulSet, "db".to_string()),
        ];
        assert_eq!(
            workload_of("web-api-7d9f8c6b5d-x2x4z", None, &w),
            Some((WorkloadKind::Deployment, "web-api".into()))
        );
        assert_eq!(
            workload_of("db-0", None, &w),
            Some((WorkloadKind::StatefulSet, "db".into()))
        );
        assert_eq!(workload_of("cache-0", None, &w), None);
        let owner = Some((WorkloadKind::DaemonSet, "agent".to_string()));
        assert_eq!(workload_of("anything", owner.clone(), &w), owner);
    }

    #[test]
    fn thresholds_of_the_rules() {
        let u = Use {
            cpu_p95: Some(0.2),
            memory_max: Some(900.0 * MI),
            throttling_mean: Some(0.25),
            oom_count: Some(0),
        };
        let one = |cpu, mem| Amounts {
            cpu: Some(cpu),
            memory: Some(mem),
        };
        let (risks, excess) = assess("app", &u, one(0.6, 1024.0 * MI), one(1.0, 1000.0 * MI));
        assert_eq!(risks.len(), 1, "троттлинг ровно 25 % — не риск: {risks:?}");
        assert!(
            matches!(risks[0], Risk::MemoryNearLimit { .. }),
            "900 из 1000 — 90 %"
        );
        assert!(excess.is_empty(), "0,6 ≥ 3 × 0,2, но разница 0,4 < 0,5");
        let (_, excess) = assess("app", &u, one(0.7, 2048.0 * MI), one(1.0, 4096.0 * MI));
        assert_eq!(excess.len(), 2);
        let (risks, _) = assess(
            "app",
            &Use {
                throttling_mean: Some(0.9),
                ..u
            },
            one(0.1, MI),
            Amounts::default(),
        );
        assert!(risks.is_empty(), "без CPU limit троттлинга нет");
    }

    /// Собранное по проекту без метрик и workload'ов — для проверки кэша.
    pub(crate) fn sample_usage(namespace: &str, window_days: u32) -> ProjectUsage {
        ProjectUsage {
            namespace: namespace.into(),
            window_days,
            source: crate::metrics::pods::info(),
            source_status: source_status(true),
            workloads: vec![],
            errors: vec![],
            truncated: false,
            quotas: vec![],
            collected_at_ms: 1,
        }
    }

    #[test]
    fn cache_keeps_results_for_five_minutes() {
        let c = ProjectCache::default();
        assert!(c.put("c1", c.epoch("c1"), &sample_usage("team-a", 7)));
        assert!(c.get("c1", "team-a", 7).is_some());
        assert!(c.get("c1", "team-a", 1).is_none());
        assert!(c.get("c2", "team-a", 7).is_none());
    }

    #[test]
    fn forgetting_a_connection_drops_all_its_results_and_only_them() {
        let c = ProjectCache::default();
        for (id, ns, days) in [
            ("c1", "team-a", 7),
            ("c1", "team-a", 1),
            ("c1", "team-b", 7),
            ("c2", "team-a", 7),
        ] {
            assert!(c.put(id, c.epoch(id), &sample_usage(ns, days)));
        }
        c.forget_connection("c1");
        for (ns, days) in [("team-a", 7), ("team-a", 1), ("team-b", 7)] {
            assert!(c.get("c1", ns, days).is_none(), "{ns}, {days}");
        }
        assert!(c.get("c2", "team-a", 7).is_some());
    }

    /// Сбор начался до сброса подключения: его результат старого клиента в кэш не идёт, а
    /// сбор после сброса — идёт.
    #[test]
    fn a_result_collected_before_the_connection_was_forgotten_is_not_cached() {
        let c = ProjectCache::default();
        let epoch = c.epoch("c1");
        c.forget_connection("c1");
        assert!(!c.put("c1", epoch, &sample_usage("team-a", 7)));
        assert!(c.get("c1", "team-a", 7).is_none());
        assert!(
            c.put("c1", c.epoch("c1"), &sample_usage("team-a", 7)),
            "после сброса — с новой эпохи"
        );
        assert!(c.get("c1", "team-a", 7).is_some());
        // Эпоха одного подключения не мешает другому.
        let other = c.epoch("c2");
        c.forget_connection("c1");
        assert!(c.put("c2", other, &sample_usage("team-a", 7)));
    }
}
