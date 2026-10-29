//! Анализ ресурсов для LLM (спец. §7, §10.4): статистика и рекомендация приложения → ответы
//! крейта. Значения — строками quantity, пометки и ограничения — английскими фразами. Адрес
//! своего источника метрик из профиля модель не получает: у источника из профиля нет `location`,
//! у ошибки `metrics_unavailable` — только виды источников и коды причин.

use glafira_mcp_core::BackendError;
use glafira_mcp_kubernetes::model::Resources;
use glafira_mcp_kubernetes::resources as mr;

use crate::change::facts::{Constraint, Warning};
use crate::k8s::quantity::{format_cpu, format_memory};
use crate::metrics::prometheus::{SourceInfo, SourceOrigin};
use crate::metrics::select::SourceStatus;
use crate::resources::current::Amounts;
use crate::resources::recommend::{Bound, Field, Note, Recommendation, Violation};
use crate::resources::usage::WorkloadUsage;

const MIB: f64 = 1024.0 * 1024.0;

fn code<T: serde::Serialize>(v: &T) -> String {
    serde_json::to_value(v)
        .ok()
        .and_then(|v| v.as_str().map(String::from))
        .unwrap_or_default()
}

fn round(v: f64, digits: i32) -> f64 {
    let k = 10f64.powi(digits);
    (v * k).round() / k
}

fn quantities(a: Amounts) -> Resources {
    Resources {
        cpu: a.cpu.map(format_cpu),
        memory: a.memory.map(format_memory),
    }
}

fn source(s: &SourceInfo) -> mr::Source {
    mr::Source {
        kind: code(&s.kind),
        origin: code(&s.origin),
        location: (s.origin == SourceOrigin::Auto).then(|| s.location.clone()),
        history: s.history,
    }
}

fn rejected(status: &SourceStatus) -> Vec<mr::RejectedSource> {
    status
        .rejected
        .iter()
        .map(|r| mr::RejectedSource {
            kind: code(&r.source.kind),
            location: (r.source.origin == SourceOrigin::Auto).then(|| r.source.location.clone()),
            reason: code(&r.reason),
        })
        .collect()
}

/// Ни одного источника: виды источников и коды причин, без адресов из профиля.
pub fn metrics_unavailable(status: &SourceStatus) -> BackendError {
    let reasons: Vec<String> = rejected(status)
        .into_iter()
        .map(|r| match r.location {
            Some(l) => format!("{} {l}: {}", r.kind, r.reason),
            None => format!(
                "{} (own source from the connection settings): {}",
                r.kind, r.reason
            ),
        })
        .collect();
    let tail = match reasons.is_empty() {
        true => String::new(),
        false => format!(" — {}", reasons.join("; ")),
    };
    BackendError::new(
        "metrics_unavailable",
        format!(
            "No metrics source is available{tail}. Ask the user to set a metrics source for the connection in Glafira."
        ),
    )
}

/// `get_resource_usage`: время рядов — секунды до момента ответа.
pub fn usage(u: &WorkloadUsage, now: i64) -> mr::ResourceUsage {
    let fields = |c: &crate::resources::current::ContainerCurrent| {
        let mut out = Vec::new();
        for (name, v) in [
            ("cpuRequest", c.limit_range_requests.cpu),
            ("cpuLimit", c.limit_range_limits.cpu),
            ("memoryRequest", c.limit_range_requests.memory),
            ("memoryLimit", c.limit_range_limits.memory),
        ] {
            if v.is_some() {
                out.push(name.to_string());
            }
        }
        out
    };
    let percent = |v: Option<f64>| v.map(|x| round(x * 100.0, 1));
    mr::ResourceUsage {
        window_days: u.window_days,
        source: source(&u.source),
        rejected: rejected(&u.source_status),
        coverage_percent: percent(u.coverage),
        data_days: u.data_seconds.map(|s| round(s as f64 / 86_400.0, 1)),
        kube_state_metrics: u.owner_metrics,
        replicas: u.replicas,
        containers: u
            .containers
            .iter()
            .map(|c| {
                let s = &c.stats;
                mr::ContainerUsage {
                    name: c.current.name.clone(),
                    init: c.current.init,
                    requests: quantities(c.current.requests),
                    limits: quantities(c.current.limits),
                    limit_range_defaults: fields(&c.current),
                    stats: mr::UsageStats {
                        cpu_p50: s.cpu.p50.map(format_cpu),
                        cpu_p95: s.cpu.p95.map(format_cpu),
                        cpu_p99: s.cpu.p99.map(format_cpu),
                        cpu_max: s.cpu.max.map(format_cpu),
                        memory_p95: s.memory.p95.map(format_memory),
                        memory_p99: s.memory.p99.map(format_memory),
                        memory_max: s.memory.max.map(format_memory),
                        throttling_mean_percent: percent(s.throttling.mean),
                        throttling_p95_percent: percent(s.throttling.p95),
                        oom_count: s.oom_count,
                        last_oom_seconds_ago: s.oom_last.map(|t| now - t),
                        restarts: s.restarts,
                    },
                }
            })
            .collect(),
        series: u.charts.as_ref().map(|ch| {
            let series =
                |list: &[crate::resources::usage::ChartSeries], scale: f64, digits: i32| {
                    list.iter()
                        .map(|s| mr::ContainerSeries {
                            container: s.container.clone(),
                            points: s
                                .points
                                .iter()
                                .map(|(t, v)| (now - t, round(v / scale, digits)))
                                .collect(),
                        })
                        .collect()
                };
            let markers = |list: &[crate::resources::usage::Marker]| {
                list.iter()
                    .map(|m| mr::Marker {
                        container: m.container.clone(),
                        seconds_ago: now - m.time,
                        count: m.count,
                    })
                    .collect()
            };
            mr::UsageSeries {
                step_seconds: ch.step_seconds,
                cpu_cores: series(&ch.cpu, 1.0, 3),
                memory_mib: series(&ch.memory, MIB, 1),
                restarts: markers(&ch.restarts),
                ooms: markers(&ch.ooms),
            }
        }),
    }
}

fn field(f: Field) -> &'static str {
    match f {
        Field::CpuRequest => "CPU request",
        Field::CpuLimit => "CPU limit",
        Field::MemoryRequest => "memory request",
        Field::MemoryLimit => "memory limit",
    }
}

fn value(f: Field, v: f64) -> String {
    match f {
        Field::CpuRequest | Field::CpuLimit => format_cpu(v),
        Field::MemoryRequest | Field::MemoryLimit => format_memory(v),
    }
}

/// Количество ресурса словами: ядра или память.
fn amount(resource: &str, v: f64) -> String {
    if resource.ends_with("cpu") {
        format_cpu(v)
    } else {
        format_memory(v)
    }
}

pub fn note(n: &Note) -> String {
    match n {
        Note::CpuLimitNotSet => {
            "No CPU limit is set and LimitRange does not require one: there is no throttling, so no limit is added".into()
        }
        Note::CpuThrottled { p95 } => format!(
            "Throttled in {:.0}% of CPU periods (p95): the observed CPU is capped by the limit, so the limit is raised at least 1.5 times",
            p95 * 100.0
        ),
        Note::OomKilled { count } => format!(
            "OOMKilled {count} times in the window: the observed memory is cut by the limit, so the limit is raised at least 1.5 times"
        ),
        Note::NoData => "No usage data for this container in the window: no recommendation".into(),
        Note::LimitRange { field: f, bound } => format!(
            "The {} was pulled to the LimitRange {}",
            field(*f),
            match bound {
                Bound::Min => "minimum",
                Bound::Max => "maximum",
                Bound::Ratio => "maxLimitRequestRatio",
            }
        ),
        Note::LimitRangeDefault { field: f } => format!(
            "The current {} comes from a LimitRange default, not from the pod template",
            field(*f)
        ),
        Note::Java => {
            "JVM settings in the environment (JAVA_OPTS, JAVA_TOOL_OPTIONS or JDK_JAVA_OPTIONS): check -Xmx or MaxRAMPercentage together with the memory limit".into()
        }
        Note::Go => "GOMEMLIMIT in the environment: check it together with the memory limit".into(),
    }
}

pub fn violation(v: &Violation) -> String {
    match v {
        Violation::Quota {
            quota,
            resource,
            needed,
            free,
        } => format!(
            "ResourceQuota {quota}: {resource} needs {} more with all replicas and the rollout surge, only {} is free",
            amount(resource, *needed),
            amount(resource, free.max(0.0))
        ),
        Violation::NodeAllocatable {
            container,
            resource,
            request,
            allocatable,
        } => format!(
            "Container {container}: the {resource} request {} is above the allocatable {} of the largest node",
            amount(resource, *request),
            amount(resource, *allocatable)
        ),
    }
}

/// Предупреждение к предложению.
pub fn warning(w: &Warning) -> String {
    match w {
        Warning::HpaManaged { hpa } => {
            format!("HPA {hpa} scales this workload and will overwrite the number of replicas")
        }
        Warning::ScaleToZero => "Scaling to 0 removes all pods of the workload".into(),
        Warning::LimitRange {
            container,
            field: f,
            bound,
            limit,
        } => format!(
            "Container {container}: the {} is {} the LimitRange {} {}: new pods will be rejected",
            field(*f),
            match bound {
                Bound::Max => "above",
                _ => "below",
            },
            match bound {
                Bound::Max => "maximum",
                _ => "minimum",
            },
            value(*f, *limit)
        ),
        Warning::Quota {
            quota,
            resource,
            needed,
            free,
        } => format!(
            "ResourceQuota {quota}: {resource} needs {} more with all replicas and the rollout surge, only {} is free: the pods of the new revision would not be created and the rollout would stall",
            amount(resource, *needed),
            amount(resource, free.max(0.0))
        ),
        Warning::Orphan => "The pod has no controller: nothing will recreate it".into(),
        Warning::Unverified { what } => format!(
            "The {} of the project could not be read, so the change was not checked against them",
            match what {
                Constraint::Hpa => "HorizontalPodAutoscalers",
                Constraint::LimitRange => "LimitRanges",
                Constraint::Quota => "ResourceQuotas",
            }
        ),
    }
}

/// `recommend_resources`.
pub fn recommendation(r: &Recommendation, status: &SourceStatus) -> mr::Recommendation {
    mr::Recommendation {
        window_days: r.window_days,
        source: source(&r.source),
        rejected: rejected(status),
        confidence: code(&r.confidence),
        coverage_percent: r.coverage.map(|c| round(c * 100.0, 1)),
        data_days: r.data_seconds.map(|s| round(s as f64 / 86_400.0, 1)),
        replicas: r.replicas,
        containers: r
            .containers
            .iter()
            .map(|c| mr::ContainerRecommendation {
                name: c.name.clone(),
                current_requests: quantities(c.current.requests),
                current_limits: quantities(c.current.limits),
                recommended_requests: c.recommended.as_ref().map(|v| quantities(v.requests)),
                recommended_limits: c.recommended.as_ref().map(|v| quantities(v.limits)),
                notes: c.notes.iter().map(note).collect(),
            })
            .collect(),
        violations: r.violations.iter().map(violation).collect(),
        hpa: r
            .hpa
            .iter()
            .map(|h| {
                format!(
                    "HPA {} scales on {} utilization of {}%; with the recommended requests the same scaling needs a target of {}%",
                    h.hpa, h.resource, h.target, h.equivalent
                )
            })
            .collect(),
        totals: mr::Totals {
            requests_cpu_cores: r.totals.requests.cpu.map(|v| round(v, 3)),
            requests_memory_mib: r.totals.requests.memory.map(|v| round(v / MIB, 1)),
            limits_cpu_cores: r.totals.limits.cpu.map(|v| round(v, 3)),
            limits_memory_mib: r.totals.limits.memory.map(|v| round(v / MIB, 1)),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::prometheus::{Reason, SourceKind};
    use crate::metrics::select::{Mode, Rejected};

    fn status() -> SourceStatus {
        let own = SourceInfo {
            kind: SourceKind::Prometheus,
            origin: SourceOrigin::Profile,
            location: "https://secret-prometheus.corp.example".into(),
            history: true,
        };
        let auto = SourceInfo {
            kind: SourceKind::MetricsServer,
            origin: SourceOrigin::Auto,
            location: "metrics.k8s.io".into(),
            history: false,
        };
        let rejected = |source: SourceInfo, reason| Rejected {
            message: crate::metrics::prometheus::reason_text(reason, &source),
            error: crate::error::AppError::internal_with(source.location.clone()),
            source,
            reason,
        };
        SourceStatus {
            mode: Mode::Prometheus,
            selected: None,
            rejected: vec![
                rejected(own, Reason::Forbidden),
                rejected(auto, Reason::NotFound),
            ],
            pod_metrics: false,
            checked_at_ms: 0,
        }
    }

    #[test]
    fn the_model_never_gets_the_own_source_address() {
        let e = metrics_unavailable(&status());
        assert_eq!(e.code, "metrics_unavailable");
        assert!(!e.message.contains("secret-prometheus"), "{}", e.message);
        assert!(
            e.message
                .contains("prometheus (own source from the connection settings): forbidden"),
            "{}",
            e.message
        );
        assert!(
            e.message.contains("metricsServer metrics.k8s.io: notFound"),
            "{}",
            e.message
        );
        let r = rejected(&status());
        assert_eq!(r[0].location, None);
    }

    #[test]
    fn notes_and_warnings_read_as_english_sentences() {
        assert!(
            note(&Note::CpuThrottled { p95: 0.25 }).starts_with("Throttled in 25% of CPU periods")
        );
        assert_eq!(
            note(&Note::LimitRange {
                field: Field::MemoryLimit,
                bound: Bound::Max
            }),
            "The memory limit was pulled to the LimitRange maximum"
        );
        assert_eq!(
            violation(&Violation::Quota {
                quota: "compute".into(),
                resource: "requests.memory".into(),
                needed: 2048.0 * MIB,
                free: 1024.0 * MIB,
            }),
            "ResourceQuota compute: requests.memory needs 2Gi more with all replicas and the rollout surge, only 1Gi is free"
        );
        assert_eq!(
            warning(&Warning::LimitRange {
                container: "app".into(),
                field: Field::CpuRequest,
                bound: Bound::Min,
                limit: 0.1,
            }),
            "Container app: the CPU request is below the LimitRange minimum 100m: new pods will be rejected"
        );
        assert_eq!(
            warning(&Warning::Unverified {
                what: Constraint::LimitRange
            }),
            "The LimitRanges of the project could not be read, so the change was not checked against them"
        );
    }
}
