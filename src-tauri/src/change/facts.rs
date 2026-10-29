//! Изменение против прочитанных объектов (спец. §9, §10.5): что в кластере сейчас (`Facts`,
//! только чтение), проверка (объект и контейнеры есть, `limits ≥ requests` с текущими
//! значениями, предупреждения `LimitRange`, квоты и HPA) и «было → станет» по полям.

use std::collections::BTreeMap;

use k8s_openapi::api::autoscaling::v2::HorizontalPodAutoscaler;
use k8s_openapi::api::core::v1::{Container, LimitRange, Pod, ResourceQuota};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use kube::api::ListParams;
use serde::Serialize;

use super::{Change, ContainerResources, check_bounds, check_limits};
use crate::error::AppResult;
use crate::k8s::quantity::parse_quantity;
use crate::k8s::read::ReadClient;
use crate::k8s::workloads::{WorkloadSpec, get_spec};
use crate::resources::constraints::{limit_range_bounds, quota_rooms};
use crate::resources::current::Amounts;
use crate::resources::recommend::{Bound, Field, LimitRangeBounds, QuotaRoom};

/// Аннотация шаблона, которой перезапускает `kubectl rollout restart`.
pub const RESTARTED_AT: &str = "kubectl.kubernetes.io/restartedAt";

/// Значения quantity строками, как в объекте.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Texts {
    pub cpu: Option<String>,
    pub memory: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerNow {
    pub name: String,
    pub init: bool,
    pub requests: Texts,
    pub limits: Texts,
}

impl ContainerNow {
    fn amounts(t: &Texts) -> Amounts {
        let q = |v: &Option<String>| v.as_deref().and_then(parse_quantity);
        Amounts {
            cpu: q(&t.cpu),
            memory: q(&t.memory),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HpaNow {
    pub name: String,
    pub min_replicas: Option<i32>,
    pub max_replicas: i32,
    pub resource_version: Option<String>,
    pub generation: Option<i64>,
}

/// Workload изменения сейчас: шаблон, HPA и ограничения проекта.
#[derive(Debug, Clone)]
pub struct WorkloadFacts {
    pub spec: WorkloadSpec,
    pub containers: Vec<ContainerNow>,
    /// HPA, который масштабирует workload.
    pub hpa: Option<HpaNow>,
    pub restarted_at: Option<String>,
    pub limit_range: Option<LimitRangeBounds>,
    pub quotas: Vec<QuotaRoom>,
}

/// Объект изменения в кластере сейчас.
#[derive(Debug, Clone)]
pub enum Facts {
    Workload(Box<WorkloadFacts>),
    Hpa(HpaNow),
    Pod {
        name: String,
        /// Под создан контроллером (его пересоздадут после удаления).
        controlled: bool,
        resource_version: Option<String>,
        /// Тот же объект, а не пересозданный под с тем же именем.
        uid: Option<String>,
    },
}

impl Facts {
    /// Версия объекта для формы: у workload'а и HPA — `metadata.generation` (растёт при изменении
    /// spec, а не статуса: идущая выкладка не делает предпросмотр устаревшим), у пода — `uid`.
    pub fn version(&self) -> Option<String> {
        match self {
            Facts::Workload(w) => w.spec.generation.map(|g| g.to_string()),
            Facts::Hpa(h) => h.generation.map(|g| g.to_string()),
            Facts::Pod { uid, .. } => uid.clone(),
        }
    }

    /// `resourceVersion` объекта при чтении: с ним идёт запись — объект, изменённый между чтением
    /// и записью, даёт ответ 409.
    pub fn resource_version(&self) -> Option<&str> {
        match self {
            Facts::Workload(w) => w.spec.resource_version.as_deref(),
            Facts::Hpa(h) => h.resource_version.as_deref(),
            Facts::Pod {
                resource_version, ..
            } => resource_version.as_deref(),
        }
    }
}

fn texts(m: Option<&BTreeMap<String, Quantity>>) -> Texts {
    let v = |k: &str| m.and_then(|m| m.get(k)).map(|q| q.0.clone());
    Texts {
        cpu: v("cpu"),
        memory: v("memory"),
    }
}

fn container_now(c: &Container, init: bool) -> ContainerNow {
    let r = c.resources.as_ref();
    ContainerNow {
        name: c.name.clone(),
        init,
        requests: texts(r.and_then(|r| r.requests.as_ref())),
        limits: texts(r.and_then(|r| r.limits.as_ref())),
    }
}

fn hpa_now(h: &HorizontalPodAutoscaler) -> Option<HpaNow> {
    let s = h.spec.as_ref()?;
    Some(HpaNow {
        name: h.metadata.name.clone()?,
        min_replicas: s.min_replicas,
        max_replicas: s.max_replicas,
        resource_version: h.metadata.resource_version.clone(),
        generation: h.metadata.generation,
    })
}

/// Читает объект изменения и то, против чего его проверяют. Объекта нет — `NOT_FOUND`.
pub async fn read_facts(r: &ReadClient, change: &Change) -> AppResult<Facts> {
    let ns = change.namespace();
    let all = ListParams::default();
    match change {
        Change::SetResources { workload, .. }
        | Change::Scale { workload, .. }
        | Change::RolloutRestart { workload, .. } => {
            let spec = get_spec(r, ns, workload.kind, &workload.name).await?;
            let (hpas, lrs, quotas) = tokio::join!(
                r.list::<HorizontalPodAutoscaler>(Some(ns), &all),
                r.list::<LimitRange>(Some(ns), &all),
                r.list::<ResourceQuota>(Some(ns), &all),
            );
            let kind = format!("{:?}", workload.kind);
            let hpa = hpas.ok().and_then(|l| {
                l.items
                    .iter()
                    .find(|h| {
                        h.spec.as_ref().is_some_and(|s| {
                            s.scale_target_ref.kind == kind
                                && s.scale_target_ref.name == workload.name
                        })
                    })
                    .and_then(hpa_now)
            });
            let template = spec.template.as_ref();
            let pod_spec = template.and_then(|t| t.spec.as_ref());
            let containers = pod_spec
                .map(|s| {
                    let init = s
                        .init_containers
                        .iter()
                        .flatten()
                        .map(|c| container_now(c, true));
                    init.chain(s.containers.iter().map(|c| container_now(c, false)))
                        .collect()
                })
                .unwrap_or_default();
            let restarted_at = template
                .and_then(|t| t.metadata.as_ref())
                .and_then(|m| m.annotations.as_ref())
                .and_then(|a| a.get(RESTARTED_AT).cloned());
            Ok(Facts::Workload(Box::new(WorkloadFacts {
                spec,
                containers,
                hpa,
                restarted_at,
                limit_range: lrs.ok().and_then(|l| limit_range_bounds(&l.items)),
                quotas: quotas.map(|l| quota_rooms(&l.items)).unwrap_or_default(),
            })))
        }
        Change::SetHpaBounds { hpa, .. } => {
            let h: HorizontalPodAutoscaler = r.get(ns, hpa).await?;
            hpa_now(&h)
                .map(Facts::Hpa)
                .ok_or_else(|| crate::k8s::errors::malformed("HorizontalPodAutoscaler", hpa))
        }
        Change::DeletePod { pod, .. } => {
            let p: Pod = r.get(ns, pod).await?;
            Ok(Facts::Pod {
                name: pod.clone(),
                controlled: p
                    .metadata
                    .owner_references
                    .iter()
                    .flatten()
                    .any(|o| o.controller == Some(true)),
                resource_version: p.metadata.resource_version.clone(),
                uid: p.metadata.uid.clone(),
            })
        }
    }
}

/// Предупреждение к изменению: его видно до подтверждения, применить можно.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "code", rename_all = "camelCase")]
pub enum Warning {
    /// Workload масштабирует HPA: число реплик он перезапишет.
    HpaManaged { hpa: String },
    /// Масштаб в 0: подов не останется.
    ScaleToZero,
    /// Значение вне границы `LimitRange`: API-сервер его отклонит (это покажет dry-run).
    LimitRange {
        container: String,
        field: Field,
        bound: Bound,
        limit: f64,
    },
    /// Запаса квоты не хватит с учётом реплик и подов выкладки.
    Quota {
        quota: String,
        resource: String,
        needed: f64,
        free: f64,
    },
    /// Под без контроллера: после удаления его никто не создаст заново.
    Orphan,
}

fn not_in_template(container: &str) -> crate::error::AppError {
    crate::app_err!(
        crate::error::ErrorCode::InvalidInput,
        "errors.change.noContainer",
        "There is no container {{container}} in the pod template",
        container = container
    )
}

/// Значения контейнера после изменения: requests и limits.
fn after(c: &ContainerResources, now: &ContainerNow) -> (Amounts, Amounts) {
    let v = |change: &Option<Option<String>>, current: Option<f64>| match change {
        None => current,
        Some(None) => None,
        Some(Some(q)) => parse_quantity(q),
    };
    let (r, l) = (
        ContainerNow::amounts(&now.requests),
        ContainerNow::amounts(&now.limits),
    );
    (
        Amounts {
            cpu: v(&c.requests.cpu, r.cpu),
            memory: v(&c.requests.memory, r.memory),
        },
        Amounts {
            cpu: v(&c.limits.cpu, l.cpu),
            memory: v(&c.limits.memory, l.memory),
        },
    )
}

/// Проверка изменения против кластера: ошибка — изменение неверно (нет контейнера, limit
/// меньше request с текущими значениями, границы HPA), предупреждения — применить можно.
pub fn check(change: &Change, facts: &Facts) -> AppResult<Vec<Warning>> {
    let mut out = Vec::new();
    match (change, facts) {
        (Change::SetResources { containers, .. }, Facts::Workload(w)) => {
            let WorkloadFacts {
                spec,
                containers: now,
                limit_range,
                quotas,
                ..
            } = w.as_ref();
            let mut delta = (
                Amounts::default(),
                Amounts::default(),
                Amounts::default(),
                Amounts::default(),
            );
            for c in containers {
                let n = now
                    .iter()
                    .find(|x| x.name == c.name)
                    .ok_or_else(|| not_in_template(&c.name))?;
                let (req, lim) = after(c, n);
                check_limits(&c.name, req.cpu, lim.cpu, "CPU")?;
                check_limits(&c.name, req.memory, lim.memory, "memory")?;
                if let Some(lr) = limit_range {
                    for (field, v, cpu) in [
                        (Field::CpuRequest, req.cpu, true),
                        (Field::CpuLimit, lim.cpu, true),
                        (Field::MemoryRequest, req.memory, false),
                        (Field::MemoryLimit, lim.memory, false),
                    ] {
                        let pick = |a: Amounts| if cpu { a.cpu } else { a.memory };
                        let Some(v) = v else { continue };
                        if let Some(m) = pick(lr.min).filter(|m| v < *m) {
                            out.push(Warning::LimitRange {
                                container: c.name.clone(),
                                field,
                                bound: Bound::Min,
                                limit: m,
                            });
                        }
                        if let Some(m) = pick(lr.max).filter(|m| v > *m) {
                            out.push(Warning::LimitRange {
                                container: c.name.clone(),
                                field,
                                bound: Bound::Max,
                                limit: m,
                            });
                        }
                    }
                }
                if !n.init {
                    let (r0, l0) = (
                        ContainerNow::amounts(&n.requests),
                        ContainerNow::amounts(&n.limits),
                    );
                    let add = |a: &mut Amounts, new: Amounts, old: Amounts| {
                        a.cpu = Some(
                            a.cpu.unwrap_or(0.0) + new.cpu.unwrap_or(0.0) - old.cpu.unwrap_or(0.0),
                        );
                        a.memory = Some(
                            a.memory.unwrap_or(0.0) + new.memory.unwrap_or(0.0)
                                - old.memory.unwrap_or(0.0),
                        );
                    };
                    add(&mut delta.0, req, r0);
                    add(&mut delta.1, lim, l0);
                    add(&mut delta.2, req, Amounts::default());
                    add(&mut delta.3, lim, Amounts::default());
                }
            }
            // Квота: прирост за все реплики плюс поды выкладки с новыми значениями.
            let replicas = f64::from(spec.row.desired.unwrap_or(1).max(0));
            let surge = f64::from(spec.surge.max(0));
            for q in quotas {
                let (d, new) = match q.resource.as_str() {
                    "requests.cpu" | "cpu" => (delta.0.cpu, delta.2.cpu),
                    "requests.memory" | "memory" => (delta.0.memory, delta.2.memory),
                    "limits.cpu" => (delta.1.cpu, delta.3.cpu),
                    "limits.memory" => (delta.1.memory, delta.3.memory),
                    _ => continue,
                };
                let needed = d.unwrap_or(0.0) * replicas + new.unwrap_or(0.0) * surge;
                let free = q.hard - q.used;
                if needed > free + 1e-9 && needed > 0.0 {
                    out.push(Warning::Quota {
                        quota: q.quota.clone(),
                        resource: q.resource.clone(),
                        needed,
                        free,
                    });
                }
            }
        }
        (Change::Scale { replicas, .. }, Facts::Workload(w)) => {
            if let Some(h) = &w.hpa {
                out.push(Warning::HpaManaged {
                    hpa: h.name.clone(),
                });
            }
            if *replicas == 0 {
                out.push(Warning::ScaleToZero);
            }
        }
        (Change::RolloutRestart { .. }, Facts::Workload(_)) => {}
        (
            Change::SetHpaBounds {
                min_replicas,
                max_replicas,
                ..
            },
            Facts::Hpa(h),
        ) => check_bounds(min_replicas.or(h.min_replicas), *max_replicas)?,
        (Change::DeletePod { .. }, Facts::Pod { controlled, .. }) => {
            if !controlled {
                out.push(Warning::Orphan);
            }
        }
        _ => {
            return Err(crate::error::AppError::internal_with(
                "facts do not match the change",
            ));
        }
    }
    Ok(out)
}

/// Поле «было → станет».
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FieldChange {
    /// Контейнер у ресурсов, иначе `None`.
    pub container: Option<String>,
    /// `requests.cpu`, `limits.memory`, `replicas`, `minReplicas`, `maxReplicas`, `restartedAt`,
    /// `pod`.
    pub field: String,
    pub before: Option<String>,
    pub after: Option<String>,
}

fn field(
    container: Option<&str>,
    name: &str,
    before: Option<String>,
    after: Option<String>,
) -> FieldChange {
    FieldChange {
        container: container.map(String::from),
        field: name.into(),
        before,
        after,
    }
}

/// «Было → станет»: только поля, которые меняются; у перезапуска — время в аннотации
/// шаблона, у удаления пода — сам под.
pub fn diff(change: &Change, facts: &Facts, now_rfc3339: &str) -> Vec<FieldChange> {
    let mut out = Vec::new();
    match (change, facts) {
        (Change::SetResources { containers, .. }, Facts::Workload(w)) => {
            let now = &w.containers;
            for c in containers {
                let Some(n) = now.iter().find(|x| x.name == c.name) else {
                    continue;
                };
                for (name, change, current) in [
                    ("requests.cpu", &c.requests.cpu, &n.requests.cpu),
                    ("requests.memory", &c.requests.memory, &n.requests.memory),
                    ("limits.cpu", &c.limits.cpu, &n.limits.cpu),
                    ("limits.memory", &c.limits.memory, &n.limits.memory),
                ] {
                    let Some(new) = change else { continue };
                    let same = match (new, current) {
                        (Some(a), Some(b)) => parse_quantity(a) == parse_quantity(b),
                        (None, None) => true,
                        _ => false,
                    };
                    if !same {
                        out.push(field(Some(&c.name), name, current.clone(), new.clone()));
                    }
                }
            }
        }
        (Change::Scale { replicas, .. }, Facts::Workload(w)) => {
            let before = w.spec.row.desired;
            if before != Some(*replicas) {
                out.push(field(
                    None,
                    "replicas",
                    before.map(|v| v.to_string()),
                    Some(replicas.to_string()),
                ));
            }
        }
        (Change::RolloutRestart { .. }, Facts::Workload(w)) => {
            out.push(field(
                None,
                "restartedAt",
                w.restarted_at.clone(),
                Some(now_rfc3339.into()),
            ));
        }
        (
            Change::SetHpaBounds {
                min_replicas,
                max_replicas,
                ..
            },
            Facts::Hpa(h),
        ) => {
            if let Some(m) = min_replicas
                && h.min_replicas != Some(*m)
            {
                out.push(field(
                    None,
                    "minReplicas",
                    h.min_replicas.map(|v| v.to_string()),
                    Some(m.to_string()),
                ));
            }
            if h.max_replicas != *max_replicas {
                out.push(field(
                    None,
                    "maxReplicas",
                    Some(h.max_replicas.to_string()),
                    Some(max_replicas.to_string()),
                ));
            }
        }
        (Change::DeletePod { .. }, Facts::Pod { name, .. }) => {
            out.push(field(None, "pod", Some(name.clone()), None));
        }
        _ => {}
    }
    out
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde_json::json;

    use super::*;
    use crate::k8s::pods::WorkloadRef;
    use crate::k8s::testing::{mock_client, status};
    use crate::k8s::workloads::WorkloadKind;

    const MI: f64 = 1024.0 * 1024.0;

    fn cluster(s: &crate::k8s::testing::Seen) -> (u16, serde_json::Value) {
        let list = |kind: &str, items: serde_json::Value| {
            (
                200,
                json!({"apiVersion": "v1", "kind": kind, "metadata": {}, "items": items}),
            )
        };
        match s.path() {
            "/apis/apps/v1/namespaces/team-a/deployments/web" => (
                200,
                json!({"apiVersion": "apps/v1", "kind": "Deployment",
                    "metadata": {"name": "web", "namespace": "team-a", "resourceVersion": "11", "generation": 2},
                    "spec": {"replicas": 2, "selector": {"matchLabels": {"app": "web"}},
                        "template": {"metadata": {"labels": {"app": "web"},
                            "annotations": {"kubectl.kubernetes.io/restartedAt": "2026-10-01T00:00:00Z"}},
                        "spec": {"initContainers": [{"name": "migrate", "image": "m"}],
                            "containers": [{"name": "app", "image": "web:1",
                            "resources": {"requests": {"cpu": "500m", "memory": "512Mi"},
                                          "limits": {"memory": "1Gi"}}}]}}}}),
            ),
            "/apis/autoscaling/v2/namespaces/team-a/horizontalpodautoscalers" => list(
                "HorizontalPodAutoscalerList",
                json!([{"metadata": {"name": "web-hpa"}, "spec": {"minReplicas": 2, "maxReplicas": 6,
                    "scaleTargetRef": {"kind": "Deployment", "name": "web", "apiVersion": "apps/v1"}}}]),
            ),
            "/apis/autoscaling/v2/namespaces/team-a/horizontalpodautoscalers/web-hpa" => (
                200,
                json!({"apiVersion": "autoscaling/v2", "kind": "HorizontalPodAutoscaler",
                    "metadata": {"name": "web-hpa", "resourceVersion": "5", "generation": 1},
                    "spec": {"minReplicas": 2, "maxReplicas": 6,
                        "scaleTargetRef": {"kind": "Deployment", "name": "web"}}}),
            ),
            "/api/v1/namespaces/team-a/limitranges" => list(
                "LimitRangeList",
                json!([{"metadata": {"name": "lr"}, "spec": {"limits": [
                    {"type": "Container", "max": {"memory": "2Gi"}, "min": {"cpu": "100m"}}]}}]),
            ),
            "/api/v1/namespaces/team-a/resourcequotas" => list(
                "ResourceQuotaList",
                json!([{"metadata": {"name": "compute"}, "status": {
                    "hard": {"requests.memory": "4Gi"}, "used": {"requests.memory": "3Gi"}}}]),
            ),
            "/api/v1/namespaces/team-a/pods/solo" => (
                200,
                json!({"apiVersion": "v1", "kind": "Pod",
                    "metadata": {"name": "solo", "resourceVersion": "3", "uid": "u-solo"}, "spec": {"containers": []}}),
            ),
            _ => status(404, "NotFound", "not found"),
        }
    }

    fn reader() -> ReadClient {
        ReadClient::new(mock_client(cluster).0, Duration::from_secs(5))
    }

    fn web() -> WorkloadRef {
        WorkloadRef {
            kind: WorkloadKind::Deployment,
            name: "web".into(),
        }
    }

    fn set(containers: serde_json::Value) -> Change {
        serde_json::from_value(json!({
            "kind": "setResources", "namespace": "team-a",
            "workload": {"kind": "Deployment", "name": "web"}, "containers": containers
        }))
        .unwrap()
    }

    #[tokio::test]
    async fn resources_are_checked_against_the_template_limit_range_and_quota() {
        let r = reader();
        let c = set(
            json!([{"name": "app", "requests": {"cpu": "50m", "memory": "1Gi"}, "limits": {"memory": "3Gi"}}]),
        );
        let f = read_facts(&r, &c).await.unwrap();
        assert_eq!(
            (f.resource_version(), f.version().as_deref()),
            (Some("11"), Some("2"))
        );
        let w = check(&c, &f).unwrap();
        assert!(w.contains(&Warning::LimitRange {
            container: "app".into(),
            field: Field::CpuRequest,
            bound: Bound::Min,
            limit: 0.1
        }));
        assert!(w.contains(&Warning::LimitRange {
            container: "app".into(),
            field: Field::MemoryLimit,
            bound: Bound::Max,
            limit: 2048.0 * MI
        }));
        // (1Gi − 512Mi) × 2 реплики + 1Gi × 1 под выкладки = 2Gi > 1Gi свободно.
        assert!(
            w.contains(&Warning::Quota {
                quota: "compute".into(),
                resource: "requests.memory".into(),
                needed: 2048.0 * MI,
                free: 1024.0 * MI
            }),
            "{w:?}"
        );
        assert_eq!(
            diff(&c, &f, "now"),
            [
                FieldChange {
                    container: Some("app".into()),
                    field: "requests.cpu".into(),
                    before: Some("500m".into()),
                    after: Some("50m".into())
                },
                FieldChange {
                    container: Some("app".into()),
                    field: "requests.memory".into(),
                    before: Some("512Mi".into()),
                    after: Some("1Gi".into())
                },
                FieldChange {
                    container: Some("app".into()),
                    field: "limits.memory".into(),
                    before: Some("1Gi".into()),
                    after: Some("3Gi".into())
                },
            ]
        );
        // Request больше текущего limit — ошибка, а не предупреждение.
        let e = check(
            &set(json!([{"name": "app", "requests": {"memory": "2Gi"}}])),
            &f,
        )
        .unwrap_err();
        assert_eq!(e.key, "errors.change.limitBelowRequest");
        let e = check(
            &set(json!([{"name": "ghost", "requests": {"cpu": "1"}}])),
            &f,
        )
        .unwrap_err();
        assert_eq!(e.key, "errors.change.noContainer");
        // Init-контейнер есть в шаблоне; «то же значение» в другой записи — не изменение.
        let same = set(
            json!([{"name": "app", "requests": {"cpu": "0.5"}}, {"name": "migrate", "limits": {"cpu": "1"}}]),
        );
        assert!(check(&same, &f).unwrap().is_empty());
        assert_eq!(diff(&same, &f, "now").len(), 1);
    }

    #[tokio::test]
    async fn scale_restart_hpa_and_pod() {
        let r = reader();
        let scale = Change::Scale {
            namespace: "team-a".into(),
            workload: web(),
            replicas: 0,
        };
        let f = read_facts(&r, &scale).await.unwrap();
        assert_eq!(
            check(&scale, &f).unwrap(),
            [
                Warning::HpaManaged {
                    hpa: "web-hpa".into()
                },
                Warning::ScaleToZero
            ]
        );
        assert_eq!(diff(&scale, &f, "now")[0].before.as_deref(), Some("2"));
        let restart = Change::RolloutRestart {
            namespace: "team-a".into(),
            workload: web(),
        };
        let d = diff(
            &restart,
            &read_facts(&r, &restart).await.unwrap(),
            "2026-10-28T12:00:00Z",
        );
        assert_eq!(d[0].before.as_deref(), Some("2026-10-01T00:00:00Z"));
        assert_eq!(d[0].after.as_deref(), Some("2026-10-28T12:00:00Z"));
        let hpa = |min| Change::SetHpaBounds {
            namespace: "team-a".into(),
            hpa: "web-hpa".into(),
            min_replicas: min,
            max_replicas: 1,
        };
        let f = read_facts(&r, &hpa(None)).await.unwrap();
        assert_eq!(
            (f.resource_version(), f.version().as_deref()),
            (Some("5"), Some("1"))
        );
        assert_eq!(
            check(&hpa(None), &f).unwrap_err().key,
            "errors.change.hpaBounds",
            "текущий min 2 > max 1"
        );
        assert!(check(&hpa(Some(1)), &f).is_ok());
        assert_eq!(diff(&hpa(Some(1)), &f, "now").len(), 2);
        let pod = Change::DeletePod {
            namespace: "team-a".into(),
            pod: "solo".into(),
        };
        let f = read_facts(&r, &pod).await.unwrap();
        assert_eq!(check(&pod, &f).unwrap(), [Warning::Orphan]);
        assert_eq!(f.version().as_deref(), Some("u-solo"));
        let missing = Change::DeletePod {
            namespace: "team-a".into(),
            pod: "ghost".into(),
        };
        assert_eq!(
            read_facts(&r, &missing).await.unwrap_err().code,
            crate::error::ErrorCode::NotFound
        );
    }
}
