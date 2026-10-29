//! Изменение против прочитанных объектов (спец. §9, §10.5): что в кластере сейчас (`Facts`,
//! только чтение), проверка (объект и контейнеры есть, `limits ≥ requests` с текущими
//! значениями, предупреждения `LimitRange`, квоты и HPA) и «было → станет» по полям.

use std::collections::BTreeMap;

use k8s_openapi::api::autoscaling::v2::HorizontalPodAutoscaler;
use k8s_openapi::api::core::v1::{Container, LimitRange, Pod, ResourceQuota};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use kube::api::ListParams;
use serde::Serialize;

use super::{
    Change, ContainerResources, check_bounds, check_limits, quantity_lt, same_quantity_text,
};
use crate::error::{AppResult, ErrorCode};
use crate::k8s::quantity::{format_cpu, format_memory, parse_quantity};
use crate::k8s::read::{Listed, ReadClient};
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

/// Что при проверке изменения не удалось прочитать (ответ не 404): HPA, `LimitRange` или квоты
/// проекта. Пустой результат вместо них выглядел бы как «нет ограничений».
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum Constraint {
    Hpa,
    LimitRange,
    Quota,
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
    /// Списки, которые не прочитались: «нет HPA / LimitRange / квоты» про них сказать нельзя.
    pub unverified: Vec<Constraint>,
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

/// Объекты списка. 404 (такого API в кластере нет) — «таких объектов нет»; любая другая
/// ошибка (403, таймаут) — «не проверено»: `what` попадает в `unverified`, а не в «нет».
fn listed<K>(
    list: AppResult<Listed<K>>,
    what: Constraint,
    unverified: &mut Vec<Constraint>,
) -> Vec<K> {
    match list {
        Ok(l) => l.items,
        Err(e) if e.code == ErrorCode::NotFound => Vec::new(),
        Err(_) => {
            unverified.push(what);
            Vec::new()
        }
    }
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
            let mut unverified = Vec::new();
            let hpa = listed(hpas, Constraint::Hpa, &mut unverified)
                .iter()
                .find(|h| {
                    h.spec.as_ref().is_some_and(|s| {
                        s.scale_target_ref.kind == kind && s.scale_target_ref.name == workload.name
                    })
                })
                .and_then(hpa_now);
            let limit_range =
                limit_range_bounds(&listed(lrs, Constraint::LimitRange, &mut unverified));
            let quotas = quota_rooms(&listed(quotas, Constraint::Quota, &mut unverified));
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
                limit_range,
                quotas,
                unverified,
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
    /// Значение вне границы `LimitRange`. `check` его больше не выдаёт: API-сервер шаблон
    /// workload'а по `LimitRange` не проверяет (LimitRanger пропускает только Pod и PVC), поды
    /// новой ревизии отклонят позже, и выкладка встанет, поэтому нарушение — ошибка
    /// `errors.change.limitRangeMin` / `errors.change.limitRangeMax`, а не предупреждение.
    /// Вариант оставлен для кода, который его разбирает.
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
    /// Не удалось прочитать HPA, `LimitRange` или квоты проекта (не 404): изменение против них
    /// не проверено, а не проверено и чисто.
    Unverified { what: Constraint },
}

fn not_in_template(container: &str) -> crate::error::AppError {
    crate::app_err!(
        crate::error::ErrorCode::InvalidInput,
        "errors.change.noContainer",
        "There is no container {{container}} in the pod template",
        container = container
    )
}

/// Значение вне границы `LimitRange`: ошибка, а не предупреждение. API-сервер не проверяет
/// шаблон workload'а по `LimitRange` (LimitRanger пропускает только Pod и PVC): поды новой
/// ревизии отклонят при создании, и выкладка встанет.
fn limit_range_violation(
    container: &str,
    field: Field,
    above: bool,
    limit: f64,
) -> crate::error::AppError {
    let (label, cpu) = match field {
        Field::CpuRequest => ("CPU request", true),
        Field::CpuLimit => ("CPU limit", true),
        Field::MemoryRequest => ("memory request", false),
        Field::MemoryLimit => ("memory limit", false),
    };
    let limit = if cpu {
        format_cpu(limit)
    } else {
        format_memory(limit)
    };
    let text = if above {
        crate::ltext!(
            "errors.change.limitRangeMax",
            "Container {{container}}: the {{field}} is above the LimitRange maximum {{limit}}; the API server does not check the template, so the pods of the new revision would be rejected",
            container = container,
            field = label,
            limit = limit
        )
    } else {
        crate::ltext!(
            "errors.change.limitRangeMin",
            "Container {{container}}: the {{field}} is below the LimitRange minimum {{limit}}; the API server does not check the template, so the pods of the new revision would be rejected",
            container = container,
            field = label,
            limit = limit
        )
    };
    crate::error::AppError::localized(crate::error::ErrorCode::InvalidInput, text)
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
/// меньше request с текущими значениями, значение вне границ `LimitRange`, границы HPA),
/// предупреждения — применить можно.
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
                        if let Some(m) = pick(lr.min).filter(|m| quantity_lt(v, *m)) {
                            return Err(limit_range_violation(&c.name, field, false, m));
                        }
                        if let Some(m) = pick(lr.max).filter(|m| quantity_lt(*m, v)) {
                            return Err(limit_range_violation(&c.name, field, true, m));
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
            // Квота: прирост за все реплики плюс поды выкладки с новыми значениями. Уменьшение
            // квоту не освобождает, пока старые поды живы: поды выкладки занимают её сверх занятого.
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
                let needed = d.unwrap_or(0.0).max(0.0) * replicas + new.unwrap_or(0.0) * surge;
                let free = q.hard - q.used;
                if quantity_lt(free, needed) && needed > 0.0 {
                    out.push(Warning::Quota {
                        quota: q.quota.clone(),
                        resource: q.resource.clone(),
                        needed,
                        free,
                    });
                }
            }
            // HPA на проверку ресурсов не влияет; LimitRange и квоты, которые не прочитались, —
            // предупреждение, а не молчание.
            out.extend(
                w.unverified
                    .iter()
                    .filter(|c| **c != Constraint::Hpa)
                    .map(|&what| Warning::Unverified { what }),
            );
        }
        (Change::Scale { replicas, .. }, Facts::Workload(w)) => {
            if let Some(h) = &w.hpa {
                out.push(Warning::HpaManaged {
                    hpa: h.name.clone(),
                });
            }
            if w.unverified.contains(&Constraint::Hpa) {
                out.push(Warning::Unverified {
                    what: Constraint::Hpa,
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
                        (Some(a), Some(b)) => same_quantity_text(a, b),
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
        // Значение вне границы LimitRange (min CPU 100m, max памяти 2Gi) — ошибка, а не
        // предупреждение: шаблон API-сервер по LimitRange не проверяет, поды новой ревизии
        // отклонят позже. Первым в `c` нарушен request CPU 50m.
        let e = check(&c, &f).unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidInput);
        assert_eq!(e.key, "errors.change.limitRangeMin");
        assert_eq!(
            (
                e.params["container"].as_str(),
                e.params["field"].as_str(),
                e.params["limit"].as_str()
            ),
            ("app", "CPU request", "100m")
        );
        let e = check(
            &set(json!([{"name": "app", "limits": {"memory": "3Gi"}}])),
            &f,
        )
        .unwrap_err();
        assert_eq!(e.key, "errors.change.limitRangeMax");
        assert_eq!(
            (
                e.params["container"].as_str(),
                e.params["field"].as_str(),
                e.params["limit"].as_str()
            ),
            ("app", "memory limit", "2Gi")
        );
        // Ровно на границах и внутри них изменение проходит.
        let inside =
            set(json!([{"name": "app", "requests": {"cpu": "100m"}, "limits": {"memory": "2Gi"}}]));
        assert!(check(&inside, &f).unwrap().is_empty());
        // (1Gi − 512Mi) × 2 реплики + 1Gi × 1 под выкладки = 2Gi > 1Gi свободно.
        let grow = set(json!([{"name": "app", "requests": {"memory": "1Gi"}}]));
        assert_eq!(
            check(&grow, &f).unwrap(),
            [Warning::Quota {
                quota: "compute".into(),
                resource: "requests.memory".into(),
                needed: 2048.0 * MI,
                free: 1024.0 * MI
            }]
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

    const HPAS: &str = "/apis/autoscaling/v2/namespaces/team-a/horizontalpodautoscalers";
    const LIMIT_RANGES: &str = "/api/v1/namespaces/team-a/limitranges";
    const QUOTAS: &str = "/api/v1/namespaces/team-a/resourcequotas";

    /// Клиент с ответами `cluster`, где пути из `over` подменены.
    fn reader_over(
        over: impl Fn(&str) -> Option<(u16, serde_json::Value)> + Send + 'static,
    ) -> ReadClient {
        let (client, _) = mock_client(move |s| over(s.path()).unwrap_or_else(|| cluster(s)));
        ReadClient::new(client, Duration::from_secs(5))
    }

    fn items_of(kind: &str, items: serde_json::Value) -> (u16, serde_json::Value) {
        (
            200,
            json!({"apiVersion": "v1", "kind": kind, "metadata": {}, "items": items}),
        )
    }

    fn deployment(containers: serde_json::Value) -> (u16, serde_json::Value) {
        (
            200,
            json!({"apiVersion": "apps/v1", "kind": "Deployment",
                "metadata": {"name": "web", "namespace": "team-a", "resourceVersion": "11", "generation": 2},
                "spec": {"replicas": 2, "selector": {"matchLabels": {"app": "web"}},
                    "template": {"metadata": {"labels": {"app": "web"}},
                        "spec": {"containers": containers}}}}),
        )
    }

    #[tokio::test]
    async fn quantities_in_another_notation_are_equal_in_the_check_and_the_diff() {
        // `700m` как f64 — 0.7000000000000001, а `0.7` — 0.7: точное сравнение их различает.
        let r = reader_over(|p| match p {
            "/apis/apps/v1/namespaces/team-a/deployments/web" => Some(deployment(json!([
                {"name": "app", "image": "web:1",
                 "resources": {"requests": {"cpu": "700m"}, "limits": {"cpu": "0.7"}}}
            ]))),
            LIMIT_RANGES => Some(items_of(
                "LimitRangeList",
                json!([{"metadata": {"name": "lr"}, "spec": {"limits": [
                    {"type": "Container", "min": {"cpu": "700m"}, "max": {"cpu": "0.7"}}]}}]),
            )),
            _ => None,
        });
        // В кластере request 700m и limit 0.7 равны: изменение памяти не упирается в CPU.
        let memory = set(json!([{"name": "app", "requests": {"memory": "64Mi"}}]));
        let f = read_facts(&r, &memory).await.unwrap();
        assert!(check(&memory, &f).unwrap().is_empty());
        // Новый request «700m» при limit «0.7» не выше limit.
        let same = set(json!([{"name": "app", "requests": {"cpu": "700m"}}]));
        assert!(check(&same, &f).unwrap().is_empty());
        // Настоящий limit ниже request по-прежнему ошибка.
        let above = set(json!([{"name": "app", "requests": {"cpu": "701m"}}]));
        assert_eq!(
            check(&above, &f).unwrap_err().key,
            "errors.change.limitBelowRequest"
        );
        // Те же значения в другой записи — не изменение: ни в «было → станет», ни в границах
        // LimitRange (min 700m, max 0.7).
        let reworded =
            set(json!([{"name": "app", "requests": {"cpu": "0.7"}, "limits": {"cpu": "700m"}}]));
        assert!(diff(&reworded, &f, "now").is_empty());
        assert!(check(&reworded, &f).unwrap().is_empty());
        assert_eq!(
            diff(
                &set(json!([{"name": "app", "limits": {"cpu": "701m"}}])),
                &f,
                "now"
            )
            .len(),
            1
        );
    }

    async fn warnings_when(path: &'static str, code: u16, change: &Change) -> Vec<Warning> {
        let r = reader_over(move |p| {
            (p == path).then(|| status(code, "Failure", "the list cannot be read"))
        });
        let f = read_facts(&r, change).await.unwrap();
        check(change, &f).unwrap()
    }

    #[tokio::test]
    async fn lists_that_cannot_be_read_are_unverified_and_a_missing_api_is_not() {
        let scale = Change::Scale {
            namespace: "team-a".into(),
            workload: web(),
            replicas: 3,
        };
        // Request ниже min LimitRange из `cluster`: с прочитанным LimitRange это была бы ошибка,
        // а без него (не прочитался или нет API) — только «не проверено» или ничего.
        let cpu = set(json!([{"name": "app", "requests": {"cpu": "50m"}}]));
        let calm = set(json!([{"name": "app", "requests": {"cpu": "200m"}}]));
        // Request памяти растёт и не помещается в квоту из `cluster`: с квотой был бы Quota.
        let memory = set(json!([{"name": "app", "requests": {"memory": "1Gi"}}]));
        let restart = Change::RolloutRestart {
            namespace: "team-a".into(),
            workload: web(),
        };
        for (path, change, what) in [
            (HPAS, &scale, Constraint::Hpa),
            (LIMIT_RANGES, &cpu, Constraint::LimitRange),
            (QUOTAS, &memory, Constraint::Quota),
        ] {
            // 403 и сбой сервера: не проверено; HpaManaged и прочее из прочитанного не появляется.
            for code in [403, 500] {
                assert_eq!(
                    warnings_when(path, code, change).await,
                    [Warning::Unverified { what }],
                    "{path} {code}"
                );
            }
            // 404: API в кластере нет — таких объектов нет, предупреждения тоже.
            assert!(
                warnings_when(path, 404, change).await.is_empty(),
                "{path} 404"
            );
        }
        // Что не прочиталось, но изменением не используется, о предупреждении не просит.
        let no_unverified =
            |w: Vec<Warning>| !w.iter().any(|w| matches!(w, Warning::Unverified { .. }));
        assert!(no_unverified(warnings_when(HPAS, 403, &calm).await));
        assert!(no_unverified(warnings_when(QUOTAS, 403, &scale).await));
        assert!(no_unverified(
            warnings_when(LIMIT_RANGES, 403, &scale).await
        ));
        assert!(no_unverified(warnings_when(HPAS, 403, &restart).await));
        // Прочитанное остаётся: HPA есть — HpaManaged, а LimitRange не прочитался отдельно.
        assert_eq!(
            warnings_when(LIMIT_RANGES, 403, &scale).await,
            [Warning::HpaManaged {
                hpa: "web-hpa".into()
            }]
        );
        assert_eq!(
            serde_json::to_value(Warning::Unverified {
                what: Constraint::LimitRange
            })
            .unwrap(),
            json!({"code": "unverified", "what": "limitRange"})
        );
    }

    fn reader_with_quota_used(used: &'static str) -> ReadClient {
        reader_over(move |p| {
            (p == QUOTAS).then(|| {
                items_of(
                    "ResourceQuotaList",
                    json!([{"metadata": {"name": "compute"}, "status": {
                        "hard": {"requests.memory": "4Gi"}, "used": {"requests.memory": used}}}]),
                )
            })
        })
    }

    #[tokio::test]
    async fn a_decrease_still_needs_quota_room_for_the_rollout_surge() {
        // Request памяти 512Mi → 256Mi у 2 реплик с одним подом выкладки.
        let shrink = set(json!([{"name": "app", "requests": {"memory": "256Mi"}}]));
        // Квота занята полностью: старые поды держат её, пока под выкладки не готов, — нужно
        // 1 × 256Mi, а не (256Mi − 512Mi) × 2 + 256Mi < 0.
        let r = reader_with_quota_used("4Gi");
        let f = read_facts(&r, &shrink).await.unwrap();
        assert_eq!(
            check(&shrink, &f).unwrap(),
            [Warning::Quota {
                quota: "compute".into(),
                resource: "requests.memory".into(),
                needed: 256.0 * MI,
                free: 0.0
            }]
        );
        // Свободно ровно под выкладку (256Mi) и с запасом: предупреждения нет.
        for used in ["3840Mi", "3Gi"] {
            let r = reader_with_quota_used(used);
            let f = read_facts(&r, &shrink).await.unwrap();
            assert!(check(&shrink, &f).unwrap().is_empty(), "{used}");
        }
    }
}
