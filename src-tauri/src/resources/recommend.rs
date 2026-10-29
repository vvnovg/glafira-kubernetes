//! Движок рекомендаций `requests` / `limits` (спец. §7.3) — чистая функция над статистикой
//! workload'а и ограничениями проекта; коэффициенты — константы модуля, в MVP не
//! настраиваются. Нарушение ограничения рекомендацию не скрывает, а показывается рядом.

use serde::Serialize;

use super::current::Amounts;
use super::usage::{ContainerUsage, WorkloadUsage};
use crate::k8s::workloads::WorkloadKind;
use crate::metrics::prometheus::{SourceInfo, SourceKind};

/// CPU request — p95 с запасом.
pub const CPU_REQUEST_FACTOR: f64 = 1.15;
/// CPU limit — p99 с запасом.
pub const CPU_LIMIT_FACTOR: f64 = 1.3;
/// Memory request — p99 с запасом.
pub const MEMORY_REQUEST_FACTOR: f64 = 1.15;
/// Memory limit — максимум с запасом.
pub const MEMORY_LIMIT_FACTOR: f64 = 1.3;
/// Лимит, который обрезал потребление (троттлинг, OOM), растёт не меньше чем в полтора раза.
pub const CLIPPED_LIMIT_FACTOR: f64 = 1.5;
/// Троттлинг p95, выше которого потребление CPU считается ограниченным лимитом.
pub const THROTTLED_P95: f64 = 0.10;
/// Шаг CPU — 10m, не меньше 10m.
pub const CPU_STEP: f64 = 0.01;
/// Шаг памяти — 16Mi, request не меньше 32Mi.
pub const MEMORY_STEP: f64 = 16.0 * 1024.0 * 1024.0;
pub const MEMORY_MIN: f64 = 32.0 * 1024.0 * 1024.0;

/// Переменные окружения, по которым видна JVM.
pub const JAVA_ENV: [&str; 3] = ["JAVA_OPTS", "JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS"];

/// Уверенность рекомендации (спец. §7.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum Confidence {
    Low,
    Medium,
    High,
}

/// Поле ресурсов контейнера.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum Field {
    CpuRequest,
    CpuLimit,
    MemoryRequest,
    MemoryLimit,
}

/// Граница `LimitRange`, к которой подтянуто значение.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum Bound {
    Min,
    Max,
    /// `maxLimitRequestRatio`: request поднят, чтобы limit / request не превышал предел.
    Ratio,
}

/// Пометка к рекомендации контейнера.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "code", rename_all = "camelCase")]
pub enum Note {
    /// CPU limit не задан, и `LimitRange` его не требует: троттлинга нет, limit не добавляется.
    CpuLimitNotSet,
    /// Троттлинг p95 выше 10 %: наблюдаемое потребление CPU ограничено лимитом.
    CpuThrottled { p95: f64 },
    /// OOMKilled в окне: наблюдаемое потребление памяти обрезано лимитом.
    OomKilled { count: u32 },
    /// Данных о контейнере за окно нет: рекомендации нет.
    NoData,
    /// Значение подтянуто к границе `LimitRange`.
    LimitRange { field: Field, bound: Bound },
    /// Текущее значение пришло от `LimitRange` по умолчанию, в шаблоне его нет.
    LimitRangeDefault { field: Field },
    /// JVM: проверить `-Xmx` / `MaxRAMPercentage` вместе с лимитом памяти.
    Java,
    /// Go: проверить `GOMEMLIMIT` вместе с лимитом памяти.
    Go,
}

/// Нарушение ограничения проекта или кластера.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "code", rename_all = "camelCase")]
pub enum Violation {
    /// Запаса квоты не хватит: `needed` сверх занятого с учётом реплик и выкладки, `free` —
    /// свободно (в ядрах или байтах).
    Quota {
        quota: String,
        resource: String,
        needed: f64,
        free: f64,
    },
    /// Request больше allocatable самой крупной ноды.
    NodeAllocatable {
        container: String,
        resource: String,
        request: f64,
        allocatable: f64,
    },
}

/// HPA по utilization: смена request сдвигает порог масштабирования (спец. §7.3).
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HpaShift {
    pub hpa: String,
    pub resource: String,
    /// Текущий target utilization, %.
    pub target: i32,
    /// Target при рекомендованных requests, при котором HPA масштабирует как сейчас, %.
    pub equivalent: i32,
}

/// Границы `LimitRange` для контейнера (`type: Container`), сведённые по всем объектам проекта.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LimitRangeBounds {
    pub min: Amounts,
    pub max: Amounts,
    pub default_limit: Amounts,
    pub default_request: Amounts,
    pub max_ratio: Amounts,
}

impl LimitRangeBounds {
    /// `LimitRange` требует CPU limit: задан максимум, limit по умолчанию или предел отношения.
    fn requires_cpu_limit(&self) -> bool {
        self.max.cpu.is_some() || self.default_limit.cpu.is_some() || self.max_ratio.cpu.is_some()
    }
}

/// Запас `ResourceQuota`: ресурс `requests.cpu`, `limits.memory`… — лимит и занято.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QuotaRoom {
    pub quota: String,
    pub resource: String,
    pub hard: f64,
    pub used: f64,
}

/// HPA workload'а по utilization ресурса.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HpaTarget {
    pub hpa: String,
    pub resource: String,
    pub target: i32,
}

/// Ограничения проекта и кластера (спец. §7.3).
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Constraints {
    pub limit_range: Option<LimitRangeBounds>,
    pub quotas: Vec<QuotaRoom>,
    /// Allocatable самой крупной ноды (по CPU и по памяти отдельно); `None` — ноды не читаются.
    pub largest_node: Option<Amounts>,
    pub hpa: Vec<HpaTarget>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Values {
    pub requests: Amounts,
    pub limits: Amounts,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerRecommendation {
    pub name: String,
    /// Действующие сейчас значения: шаблон, а без значения в нём — `LimitRange` по умолчанию.
    pub current: Values,
    /// Рекомендация; `None` — данных нет.
    pub recommended: Option<Values>,
    pub notes: Vec<Note>,
}

/// Разница суммарных requests и limits по workload'у с учётом реплик.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Totals {
    pub requests: Amounts,
    pub limits: Amounts,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Recommendation {
    pub namespace: String,
    pub kind: WorkloadKind,
    pub name: String,
    pub window_days: u32,
    pub source: SourceInfo,
    pub confidence: Confidence,
    pub coverage: Option<f64>,
    pub data_seconds: Option<i64>,
    pub replicas: Option<i32>,
    pub containers: Vec<ContainerRecommendation>,
    pub violations: Vec<Violation>,
    pub hpa: Vec<HpaShift>,
    /// Рекомендовано минус сейчас, × реплик: «−1,2 ядра, +512 Mi».
    pub totals: Totals,
}

/// Вверх до шага.
fn round_up(v: f64, step: f64) -> f64 {
    ((v / step) - 1e-9).ceil().max(0.0) * step
}

fn cpu(v: f64) -> f64 {
    // Округление до 10m считается в миллиядрах: доли ядра в двоичной записи неточны.
    (round_up(v, CPU_STEP).max(CPU_STEP) * 1000.0).round() / 1000.0
}

fn memory(v: f64) -> f64 {
    round_up(v, MEMORY_STEP)
}

/// Уверенность по окну, данным и покрытию (спец. §7.3).
pub fn confidence(
    source: &SourceInfo,
    days: u32,
    data_seconds: Option<i64>,
    coverage: Option<f64>,
) -> Confidence {
    if source.kind == SourceKind::MetricsServer {
        return Confidence::Low;
    }
    let data = data_seconds.unwrap_or(0);
    let cov = coverage.unwrap_or(0.0);
    if days >= 7 && data >= 7 * 86_400 && cov >= 0.8 {
        Confidence::High
    } else if data >= 86_400 && cov >= 0.5 {
        Confidence::Medium
    } else {
        Confidence::Low
    }
}

/// Подтягивает значение к границам `LimitRange`, с пометкой.
fn clamp(v: f64, min: Option<f64>, max: Option<f64>, field: Field, notes: &mut Vec<Note>) -> f64 {
    if let Some(m) = min
        && v < m
    {
        notes.push(Note::LimitRange {
            field,
            bound: Bound::Min,
        });
        return m;
    }
    if let Some(m) = max
        && v > m
    {
        notes.push(Note::LimitRange {
            field,
            bound: Bound::Max,
        });
        return m;
    }
    v
}

fn container(u: &ContainerUsage, lr: Option<&LimitRangeBounds>) -> ContainerRecommendation {
    let c = &u.current;
    let s = &u.stats;
    let current = Values {
        requests: c.effective_requests(),
        limits: c.effective_limits(),
    };
    let mut notes = Vec::new();
    for (field, from_lr) in [
        (Field::CpuRequest, c.limit_range_requests.cpu),
        (Field::CpuLimit, c.limit_range_limits.cpu),
        (Field::MemoryRequest, c.limit_range_requests.memory),
        (Field::MemoryLimit, c.limit_range_limits.memory),
    ] {
        if from_lr.is_some() {
            notes.push(Note::LimitRangeDefault { field });
        }
    }
    // Без перцентилей (только metrics.k8s.io) — по текущему значению.
    let cpu_p95 = s.cpu.p95.or(s.cpu.max);
    let cpu_p99 = s.cpu.p99.or(s.cpu.max);
    let mem_p99 = s.memory.p99.or(s.memory.max);
    let mem_max = s.memory.max;
    let (Some(cpu_p95), Some(cpu_p99), Some(mem_p99), Some(mem_max)) =
        (cpu_p95, cpu_p99, mem_p99, mem_max)
    else {
        notes.push(Note::NoData);
        return ContainerRecommendation {
            name: c.name.clone(),
            current,
            recommended: None,
            notes,
        };
    };
    let min = lr.map(|l| l.min).unwrap_or_default();
    let max = lr.map(|l| l.max).unwrap_or_default();
    let mut cpu_request = clamp(
        cpu(cpu_p95 * CPU_REQUEST_FACTOR),
        min.cpu,
        max.cpu,
        Field::CpuRequest,
        &mut notes,
    );
    let requires_limit = lr.is_some_and(LimitRangeBounds::requires_cpu_limit);
    let cpu_limit = match current.limits.cpu {
        None if !requires_limit => {
            notes.push(Note::CpuLimitNotSet);
            None
        }
        limit => {
            let mut v = cpu((cpu_p99 * CPU_LIMIT_FACTOR).max(cpu_request));
            if let (Some(p95), Some(l)) = (s.throttling.p95, limit)
                && p95 > THROTTLED_P95
            {
                notes.push(Note::CpuThrottled { p95 });
                v = v.max(cpu(l * CLIPPED_LIMIT_FACTOR));
            }
            Some(clamp(v, min.cpu, max.cpu, Field::CpuLimit, &mut notes))
        }
    };
    let mut memory_request = clamp(
        memory(mem_p99 * MEMORY_REQUEST_FACTOR).max(MEMORY_MIN),
        min.memory,
        max.memory,
        Field::MemoryRequest,
        &mut notes,
    );
    let mut memory_limit = memory((mem_max * MEMORY_LIMIT_FACTOR).max(memory_request));
    if let Some(n) = s.oom_count.filter(|n| *n > 0) {
        notes.push(Note::OomKilled { count: n });
        if let Some(l) = current.limits.memory {
            memory_limit = memory_limit.max(memory(l * CLIPPED_LIMIT_FACTOR));
        }
    }
    let memory_limit = clamp(
        memory_limit,
        min.memory,
        max.memory,
        Field::MemoryLimit,
        &mut notes,
    );
    // Предел отношения limit / request: request поднимается.
    if let Some(ratio) = lr.and_then(|l| l.max_ratio.cpu)
        && let Some(l) = cpu_limit
        && l / cpu_request > ratio
    {
        cpu_request = cpu(l / ratio);
        notes.push(Note::LimitRange {
            field: Field::CpuRequest,
            bound: Bound::Ratio,
        });
    }
    if let Some(ratio) = lr.and_then(|l| l.max_ratio.memory)
        && memory_limit / memory_request > ratio
    {
        memory_request = memory(memory_limit / ratio);
        notes.push(Note::LimitRange {
            field: Field::MemoryRequest,
            bound: Bound::Ratio,
        });
    }
    if c.env_names.iter().any(|n| JAVA_ENV.contains(&n.as_str())) {
        notes.push(Note::Java);
    }
    if c.env_names.iter().any(|n| n == "GOMEMLIMIT") {
        notes.push(Note::Go);
    }
    ContainerRecommendation {
        name: c.name.clone(),
        current,
        recommended: Some(Values {
            requests: Amounts {
                cpu: Some(cpu_request),
                memory: Some(memory_request),
            },
            limits: Amounts {
                cpu: cpu_limit,
                memory: Some(memory_limit),
            },
        }),
        notes,
    }
}

fn sum(list: &[ContainerRecommendation], f: impl Fn(&Values) -> Option<f64>, new: bool) -> f64 {
    list.iter()
        .filter_map(|c| {
            let v = if new {
                c.recommended.as_ref()
            } else {
                Some(&c.current)
            }?;
            f(v)
        })
        .sum()
}

/// Значение после рекомендации: рекомендованное, а без неё — текущее.
fn after(c: &ContainerRecommendation, f: impl Fn(&Values) -> Option<f64>) -> f64 {
    c.recommended
        .as_ref()
        .and_then(&f)
        .or(f(&c.current))
        .unwrap_or(0.0)
}

/// Рекомендация по статистике workload'а и ограничениям.
pub fn recommend(u: &WorkloadUsage, c: &Constraints) -> Recommendation {
    let containers: Vec<ContainerRecommendation> = u
        .containers
        .iter()
        .filter(|x| x.current.recommended())
        .map(|x| container(x, c.limit_range.as_ref()))
        .collect();
    let replicas = f64::from(u.replicas.unwrap_or(1).max(0));
    let surge = f64::from(u.surge.max(0));
    let with: Vec<&ContainerRecommendation> = containers
        .iter()
        .filter(|x| x.recommended.is_some())
        .collect();
    let total = |f: &dyn Fn(&Values) -> Option<f64>| -> Option<f64> {
        let new: f64 = with
            .iter()
            .map(|x| x.recommended.as_ref().and_then(f).unwrap_or(0.0))
            .sum();
        let old: f64 = with.iter().map(|x| f(&x.current).unwrap_or(0.0)).sum();
        (!with.is_empty()).then_some((new - old) * replicas)
    };
    let totals = Totals {
        requests: Amounts {
            cpu: total(&|v| v.requests.cpu),
            memory: total(&|v| v.requests.memory),
        },
        limits: Amounts {
            cpu: total(&|v| v.limits.cpu),
            memory: total(&|v| v.limits.memory),
        },
    };
    let mut violations = Vec::new();
    // Квота: прирост за все реплики плюс поды выкладки с новыми значениями. Уменьшение
    // квоту не освобождает, пока старые поды живы: поды выкладки занимают её сверх занятого.
    for q in &c.quotas {
        let pick: &dyn Fn(&Values) -> Option<f64> = match q.resource.as_str() {
            "requests.cpu" | "cpu" => &|v| v.requests.cpu,
            "requests.memory" | "memory" => &|v| v.requests.memory,
            "limits.cpu" => &|v| v.limits.cpu,
            "limits.memory" => &|v| v.limits.memory,
            _ => continue,
        };
        let new: f64 = containers.iter().map(|x| after(x, pick)).sum();
        let old: f64 = containers
            .iter()
            .map(|x| pick(&x.current).unwrap_or(0.0))
            .sum();
        let needed = (new - old).max(0.0) * replicas + new * surge;
        let free = q.hard - q.used;
        if needed > free + 1e-9 {
            violations.push(Violation::Quota {
                quota: q.quota.clone(),
                resource: q.resource.clone(),
                needed,
                free,
            });
        }
    }
    if let Some(node) = c.largest_node {
        for x in &with {
            let r = x
                .recommended
                .as_ref()
                .map(|v| v.requests)
                .unwrap_or_default();
            for (resource, req, alloc) in
                [("cpu", r.cpu, node.cpu), ("memory", r.memory, node.memory)]
            {
                if let (Some(req), Some(alloc)) = (req, alloc)
                    && req > alloc
                {
                    violations.push(Violation::NodeAllocatable {
                        container: x.name.clone(),
                        resource: resource.into(),
                        request: req,
                        allocatable: alloc,
                    });
                }
            }
        }
    }
    let hpa = c
        .hpa
        .iter()
        .filter_map(|h| {
            let pick: &dyn Fn(&Values) -> Option<f64> = match h.resource.as_str() {
                "cpu" => &|v| v.requests.cpu,
                "memory" => &|v| v.requests.memory,
                _ => return None,
            };
            let old = sum(&containers, pick, false);
            let new: f64 = containers.iter().map(|x| after(x, pick)).sum();
            (old > 0.0 && new > 0.0 && (new - old).abs() > 1e-9).then(|| HpaShift {
                hpa: h.hpa.clone(),
                resource: h.resource.clone(),
                target: h.target,
                // Target utilization у HPA не меньше 1 %.
                equivalent: ((f64::from(h.target) * old / new).round() as i32).max(1),
            })
        })
        .collect();
    Recommendation {
        namespace: u.namespace.clone(),
        kind: u.kind,
        name: u.name.clone(),
        window_days: u.window_days,
        confidence: confidence(&u.source, u.window_days, u.data_seconds, u.coverage),
        source: u.source.clone(),
        coverage: u.coverage,
        data_seconds: u.data_seconds,
        replicas: u.replicas,
        containers,
        violations,
        hpa,
        totals,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::prometheus::SourceOrigin;
    use crate::resources::current::ContainerCurrent;
    use crate::resources::usage::{ContainerStats, CpuStats, MemoryStats, ThrottlingStats};

    const MI: f64 = 1024.0 * 1024.0;

    fn src(kind: SourceKind) -> SourceInfo {
        SourceInfo {
            kind,
            origin: SourceOrigin::Auto,
            location: "x".into(),
            history: kind != SourceKind::MetricsServer,
        }
    }

    fn app(requests: Amounts, limits: Amounts, stats: ContainerStats) -> ContainerUsage {
        ContainerUsage {
            current: ContainerCurrent {
                name: "app".into(),
                init: false,
                sidecar: false,
                requests,
                limits,
                limit_range_requests: Amounts::default(),
                limit_range_limits: Amounts::default(),
                env_names: vec![],
            },
            stats,
        }
    }

    fn stats(cpu_p95: f64, cpu_p99: f64, mem_p99: f64, mem_max: f64) -> ContainerStats {
        ContainerStats {
            cpu: CpuStats {
                p50: Some(cpu_p95 / 2.0),
                p95: Some(cpu_p95),
                p99: Some(cpu_p99),
                max: Some(cpu_p99),
            },
            memory: MemoryStats {
                p95: Some(mem_p99),
                p99: Some(mem_p99),
                max: Some(mem_max),
            },
            throttling: ThrottlingStats::default(),
            oom_count: Some(0),
            oom_last: None,
            restarts: Some(0),
        }
    }

    fn usage(containers: Vec<ContainerUsage>) -> WorkloadUsage {
        WorkloadUsage {
            namespace: "team-a".into(),
            kind: WorkloadKind::Deployment,
            name: "web".into(),
            window_days: 7,
            source: src(SourceKind::Prometheus),
            source_status: crate::resources::usage::tests::source_status(false),
            coverage: Some(0.95),
            data_seconds: Some(7 * 86_400),
            owner_metrics: true,
            replicas: Some(3),
            surge: 1,
            containers,
            charts: None,
            pods_now: vec![],
        }
    }

    fn amounts(cpu: Option<f64>, memory: Option<f64>) -> Amounts {
        Amounts { cpu, memory }
    }

    fn rec(u: &WorkloadUsage) -> ContainerRecommendation {
        recommend(u, &Constraints::default()).containers.remove(0)
    }

    #[test]
    fn requests_and_limits_follow_the_percentiles_and_round_up() {
        let u = usage(vec![app(
            amounts(Some(1.0), Some(1024.0 * MI)),
            amounts(Some(2.0), Some(2048.0 * MI)),
            stats(0.2, 0.3, 300.0 * MI, 400.0 * MI),
        )]);
        let r = rec(&u);
        let v = r.recommended.unwrap();
        // 0,2 × 1,15 = 0,23; 0,3 × 1,3 = 0,39.
        assert_eq!(v.requests.cpu, Some(0.23));
        assert_eq!(v.limits.cpu, Some(0.39));
        // 300Mi × 1,15 = 345Mi → 352Mi; 400Mi × 1,3 = 520Mi → 528Mi.
        assert_eq!(v.requests.memory, Some(352.0 * MI));
        assert_eq!(v.limits.memory, Some(528.0 * MI));
        assert!(r.notes.is_empty(), "{:?}", r.notes);
        let all = recommend(&u, &Constraints::default());
        assert_eq!(all.confidence, Confidence::High);
        // (0,23 − 1) × 3 реплики.
        assert!(
            (all.totals.requests.cpu.unwrap() + 2.31).abs() < 1e-9,
            "{:?}",
            all.totals
        );
        assert_eq!(all.totals.limits.memory, Some((528.0 - 2048.0) * MI * 3.0));
    }

    #[test]
    fn small_usage_gets_the_floors() {
        let r = rec(&usage(vec![app(
            Amounts::default(),
            Amounts::default(),
            stats(0.001, 0.002, 5.0 * MI, 6.0 * MI),
        )]));
        let v = r.recommended.unwrap();
        assert_eq!(v.requests.cpu, Some(0.01), "не меньше 10m");
        assert_eq!(v.requests.memory, Some(32.0 * MI), "не меньше 32Mi");
        assert_eq!(v.limits.memory, Some(32.0 * MI), "не меньше request");
        assert_eq!(v.limits.cpu, None);
        assert_eq!(r.notes, [Note::CpuLimitNotSet]);
    }

    #[test]
    fn throttling_and_oom_raise_clipped_limits() {
        let mut s = stats(0.4, 0.5, 400.0 * MI, 500.0 * MI);
        s.throttling.p95 = Some(0.25);
        s.oom_count = Some(2);
        let r = rec(&usage(vec![app(
            amounts(Some(0.5), Some(512.0 * MI)),
            amounts(Some(0.5), Some(512.0 * MI)),
            s,
        )]));
        let v = r.recommended.unwrap();
        // max(0,5 × 1,3 = 0,65; 0,5 × 1,5 = 0,75).
        assert_eq!(v.limits.cpu, Some(0.75));
        // max(500Mi × 1,3 = 650 → 656Mi; 512Mi × 1,5 = 768Mi).
        assert_eq!(v.limits.memory, Some(768.0 * MI));
        assert_eq!(
            r.notes,
            [
                Note::CpuThrottled { p95: 0.25 },
                Note::OomKilled { count: 2 }
            ]
        );
    }

    #[test]
    fn limit_range_bounds_and_defaults() {
        let mut u = usage(vec![app(
            Amounts::default(),
            Amounts::default(),
            stats(0.001, 0.002, 5.0 * MI, 2000.0 * MI),
        )]);
        u.containers[0].current.limit_range_limits.cpu = Some(0.5);
        let lr = LimitRangeBounds {
            min: amounts(Some(0.05), None),
            max: amounts(None, Some(1024.0 * MI)),
            max_ratio: amounts(Some(4.0), None),
            default_limit: amounts(Some(0.5), None),
            ..Default::default()
        };
        let r = recommend(
            &u,
            &Constraints {
                limit_range: Some(lr),
                ..Default::default()
            },
        )
        .containers
        .remove(0);
        let v = r.recommended.unwrap();
        assert_eq!(v.requests.cpu, Some(0.05), "к min");
        assert_eq!(
            v.limits.cpu,
            Some(0.05),
            "limit задан LimitRange — считается"
        );
        assert_eq!(v.limits.memory, Some(1024.0 * MI), "к max");
        assert!(r.notes.contains(&Note::LimitRangeDefault {
            field: Field::CpuLimit
        }));
        assert!(r.notes.contains(&Note::LimitRange {
            field: Field::CpuRequest,
            bound: Bound::Min
        }));
        assert!(r.notes.contains(&Note::LimitRange {
            field: Field::MemoryLimit,
            bound: Bound::Max
        }));
        assert!(!r.notes.contains(&Note::CpuLimitNotSet));
    }

    #[test]
    fn ratio_raises_the_request() {
        let r = recommend(
            &usage(vec![app(
                amounts(Some(0.1), None),
                amounts(Some(2.0), None),
                stats(0.1, 1.5, 100.0 * MI, 100.0 * MI),
            )]),
            &Constraints {
                limit_range: Some(LimitRangeBounds {
                    max_ratio: amounts(Some(2.0), Some(1.5)),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .containers
        .remove(0);
        let v = r.recommended.unwrap();
        // limit 1,5 × 1,3 = 1,95 / 2 → request 0,98.
        assert_eq!(v.limits.cpu, Some(1.95));
        assert_eq!(v.requests.cpu, Some(0.98));
        // limit 130Mi → 144Mi; request 115 → 128Mi; 144/128 < 1,5 — без подъёма.
        assert!(r.notes.contains(&Note::LimitRange {
            field: Field::CpuRequest,
            bound: Bound::Ratio
        }));
        assert!(!r.notes.contains(&Note::LimitRange {
            field: Field::MemoryRequest,
            bound: Bound::Ratio
        }));
    }

    #[test]
    fn ratio_raises_the_memory_request() {
        let r = recommend(
            &usage(vec![app(
                amounts(None, Some(64.0 * MI)),
                amounts(None, Some(1024.0 * MI)),
                stats(0.1, 0.1, 100.0 * MI, 400.0 * MI),
            )]),
            &Constraints {
                limit_range: Some(LimitRangeBounds {
                    max_ratio: amounts(None, Some(2.0)),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .containers
        .remove(0);
        let v = r.recommended.unwrap();
        // limit 400Mi × 1,3 = 520Mi → 528Mi; request 115Mi → 128Mi; 528 / 128 > 2.
        assert_eq!(v.limits.memory, Some(528.0 * MI));
        // Request поднимается до limit / 2 = 264Mi → 272Mi.
        assert_eq!(v.requests.memory, Some(272.0 * MI));
        assert!(r.notes.contains(&Note::LimitRange {
            field: Field::MemoryRequest,
            bound: Bound::Ratio
        }));
    }

    #[test]
    fn limit_range_that_requires_a_cpu_limit_gets_one() {
        let new_usage = || {
            usage(vec![app(
                amounts(Some(0.5), Some(256.0 * MI)),
                Amounts::default(),
                stats(0.2, 0.3, 100.0 * MI, 120.0 * MI),
            )])
        };
        // Без LimitRange limit не ставится — это совет, а не требование.
        let free = rec(&new_usage());
        assert_eq!(free.recommended.unwrap().limits.cpu, None);
        assert_eq!(free.notes, [Note::CpuLimitNotSet]);
        // Максимум, limit по умолчанию или предел отношения требуют limit.
        for lr in [
            LimitRangeBounds {
                default_limit: amounts(Some(1.0), None),
                ..Default::default()
            },
            LimitRangeBounds {
                max_ratio: amounts(Some(4.0), None),
                ..Default::default()
            },
            LimitRangeBounds {
                max: amounts(Some(2.0), None),
                ..Default::default()
            },
        ] {
            let r = recommend(
                &new_usage(),
                &Constraints {
                    limit_range: Some(lr.clone()),
                    ..Default::default()
                },
            )
            .containers
            .remove(0);
            // 0,3 × 1,3 = 0,39.
            assert_eq!(r.recommended.unwrap().limits.cpu, Some(0.39), "{lr:?}");
            assert!(!r.notes.contains(&Note::CpuLimitNotSet), "{lr:?}");
        }
    }

    #[test]
    fn quota_nodes_and_hpa_are_checked_beside_the_recommendation() {
        let u = usage(vec![app(
            amounts(Some(0.1), Some(128.0 * MI)),
            amounts(None, Some(256.0 * MI)),
            stats(0.9, 1.0, 200.0 * MI, 250.0 * MI),
        )]);
        let c = Constraints {
            quotas: vec![
                QuotaRoom {
                    quota: "compute".into(),
                    resource: "requests.cpu".into(),
                    hard: 4.0,
                    used: 3.5,
                },
                QuotaRoom {
                    quota: "compute".into(),
                    resource: "pods".into(),
                    hard: 10.0,
                    used: 10.0,
                },
            ],
            largest_node: Some(amounts(Some(0.5), Some(1024.0 * MI * 1024.0))),
            hpa: vec![HpaTarget {
                hpa: "web".into(),
                resource: "cpu".into(),
                target: 80,
            }],
            ..Default::default()
        };
        let r = recommend(&u, &c);
        let v = r.containers[0].recommended.as_ref().unwrap();
        assert_eq!(v.requests.cpu, Some(1.04));
        // (1,04 − 0,1) × 3 + 1,04 × 1 = 3,86 > 0,5.
        let Violation::Quota { needed, free, .. } = &r.violations[0] else {
            panic!("{:?}", r.violations)
        };
        assert!((needed - 3.86).abs() < 1e-9 && (free - 0.5).abs() < 1e-9);
        assert!(
            matches!(&r.violations[1], Violation::NodeAllocatable { resource, .. } if resource == "cpu")
        );
        assert_eq!(
            r.violations.len(),
            2,
            "квота подов не про ресурсы контейнеров"
        );
        assert_eq!(
            r.hpa,
            [HpaShift {
                hpa: "web".into(),
                resource: "cpu".into(),
                target: 80,
                equivalent: 8
            }]
        );
    }

    /// Request CPU падает с 1,0 до 0,12 (0,1 × 1,15 = 0,115 → 0,12); реплик 3, выкладка +1.
    fn shrinking_usage() -> WorkloadUsage {
        usage(vec![app(
            amounts(Some(1.0), Some(256.0 * MI)),
            amounts(None, Some(512.0 * MI)),
            stats(0.1, 0.1, 100.0 * MI, 120.0 * MI),
        )])
    }

    fn cpu_quota(hard: f64, used: f64) -> Constraints {
        Constraints {
            quotas: vec![QuotaRoom {
                quota: "compute".into(),
                resource: "requests.cpu".into(),
                hard,
                used,
            }],
            ..Default::default()
        }
    }

    #[test]
    fn a_full_quota_still_fails_the_rollout_surge_when_requests_go_down() {
        let r = recommend(&shrinking_usage(), &cpu_quota(6.0, 6.0));
        assert_eq!(
            r.containers[0].recommended.as_ref().unwrap().requests.cpu,
            Some(0.12)
        );
        // Старые поды держат квоту, пока новый под выкладки не готов: нужно 1 × 0,12.
        let [Violation::Quota { needed, free, .. }] = r.violations.as_slice() else {
            panic!("{:?}", r.violations)
        };
        assert!((needed - 0.12).abs() < 1e-9 && free.abs() < 1e-9);
    }

    #[test]
    fn decreased_requests_with_room_for_the_surge_do_not_violate_the_quota() {
        let r = recommend(&shrinking_usage(), &cpu_quota(6.0, 5.0));
        assert!(r.violations.is_empty(), "{:?}", r.violations);
        // Ровно под выкладку: свободно 0,12 при нужных 0,12.
        let r = recommend(&shrinking_usage(), &cpu_quota(6.12, 6.0));
        assert!(r.violations.is_empty(), "{:?}", r.violations);
    }

    #[test]
    fn hpa_equivalent_target_is_at_least_one_percent() {
        let u = usage(vec![app(
            amounts(Some(0.1), Some(128.0 * MI)),
            amounts(None, Some(256.0 * MI)),
            stats(0.9, 1.0, 200.0 * MI, 250.0 * MI),
        )]);
        let r = recommend(
            &u,
            &Constraints {
                hpa: vec![HpaTarget {
                    hpa: "web".into(),
                    resource: "cpu".into(),
                    target: 5,
                }],
                ..Default::default()
            },
        );
        // Request растёт с 0,1 до 1,04: 5 × 0,1 / 1,04 ≈ 0,48 → 0 без нижней границы.
        assert_eq!(r.hpa.len(), 1);
        assert_eq!(r.hpa[0].target, 5);
        assert_eq!(r.hpa[0].equivalent, 1);
    }

    #[test]
    fn confidence_by_window_data_and_coverage() {
        let p = src(SourceKind::Prometheus);
        assert_eq!(
            confidence(&p, 7, Some(7 * 86_400), Some(0.8)),
            Confidence::High
        );
        assert_eq!(
            confidence(&p, 7, Some(7 * 86_400 - 1), Some(0.9)),
            Confidence::Medium
        );
        assert_eq!(
            confidence(&p, 1, Some(86_400), Some(0.5)),
            Confidence::Medium
        );
        assert_eq!(confidence(&p, 7, Some(86_399), Some(0.9)), Confidence::Low);
        assert_eq!(
            confidence(&p, 7, Some(7 * 86_400), Some(0.49)),
            Confidence::Low
        );
        assert_eq!(
            confidence(&src(SourceKind::MetricsServer), 7, None, None),
            Confidence::Low
        );
    }

    #[test]
    fn pod_metrics_alone_recommend_from_the_current_value() {
        let mut s = ContainerStats::default();
        s.cpu.max = Some(0.2);
        s.memory.max = Some(100.0 * MI);
        let mut u = usage(vec![app(Amounts::default(), Amounts::default(), s)]);
        u.source = src(SourceKind::MetricsServer);
        u.coverage = None;
        u.data_seconds = None;
        let r = recommend(&u, &Constraints::default());
        assert_eq!(r.confidence, Confidence::Low);
        let v = r.containers[0].recommended.as_ref().unwrap();
        assert_eq!(v.requests.cpu, Some(0.23));
        assert_eq!(v.requests.memory, Some(128.0 * MI));
    }

    #[test]
    fn no_data_init_containers_and_runtime_hints() {
        let mut java = app(
            Amounts::default(),
            Amounts::default(),
            stats(0.1, 0.1, 64.0 * MI, 64.0 * MI),
        );
        java.current.env_names = vec!["JAVA_TOOL_OPTIONS".into(), "GOMEMLIMIT".into()];
        let mut empty = app(
            Amounts::default(),
            Amounts::default(),
            ContainerStats::default(),
        );
        empty.current.name = "idle".into();
        let mut init = app(
            Amounts::default(),
            Amounts::default(),
            stats(1.0, 1.0, MI, MI),
        );
        init.current.name = "migrate".into();
        init.current.init = true;
        let r = recommend(&usage(vec![init, java, empty]), &Constraints::default());
        assert_eq!(r.containers.len(), 2, "init-контейнер без рекомендации");
        assert!(r.containers[0].notes.contains(&Note::Java));
        assert!(r.containers[0].notes.contains(&Note::Go));
        assert_eq!(r.containers[1].recommended, None);
        assert_eq!(r.containers[1].notes, [Note::NoData]);
        assert!(
            r.totals.requests.cpu.is_some(),
            "итог — по контейнерам с рекомендацией"
        );
    }
}
