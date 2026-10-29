//! Вид изменения `Change` (спец. §9): одна и та же форма в командах интерфейса, в файле
//! предложений и у LLM. Здесь — модель, проверка формы, объект изменения и сравнение
//! применённого с предложенным (чистые функции), проверка против прочитанных объектов и
//! «было → станет» (`facts`). Запросы записи в кластер — только в `ops`: этот модуль кластер
//! только читает, им пользуется и MCP-шлюз.

pub mod facts;

use serde::{Deserialize, Deserializer, Serialize};

use crate::error::{AppError, AppResult, ErrorCode};
use crate::health::model::{Target, TargetKind};
use crate::k8s::pods::WorkloadRef;
use crate::k8s::quantity::parse_quantity;
use crate::k8s::workloads::WorkloadKind;

/// Контейнеров в одном `setResources`.
pub const MAX_CONTAINERS: usize = 50;
/// Реплик в `scale` и границ HPA.
pub const MAX_REPLICAS: i32 = 1000;

/// Нет поля — `None` (не менять), `null` — `Some(None)` (убрать), строка — `Some(Some(…))`.
fn present<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Option<String>>, D::Error> {
    Option::<String>::deserialize(d).map(Some)
}

/// requests или limits контейнера: quantity строкой, `null` — убрать, нет поля — не менять.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceValues {
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub cpu: Option<Option<String>>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub memory: Option<Option<String>>,
}

impl ResourceValues {
    fn is_empty(&self) -> bool {
        self.cpu.is_none() && self.memory.is_none()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerResources {
    pub name: String,
    #[serde(default)]
    pub requests: ResourceValues,
    #[serde(default)]
    pub limits: ResourceValues,
}

/// Изменение кластера (спец. §9). `deletePod` предложить нельзя (§10.5).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum Change {
    SetResources {
        namespace: String,
        workload: WorkloadRef,
        containers: Vec<ContainerResources>,
    },
    Scale {
        namespace: String,
        workload: WorkloadRef,
        replicas: i32,
    },
    RolloutRestart {
        namespace: String,
        workload: WorkloadRef,
    },
    #[serde(rename_all = "camelCase")]
    SetHpaBounds {
        namespace: String,
        hpa: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        min_replicas: Option<i32>,
        max_replicas: i32,
    },
    DeletePod {
        namespace: String,
        pod: String,
    },
}

impl Change {
    /// Имя вида — как `kind` в JSON (журнал, ключ повтора предложения).
    pub fn kind_name(&self) -> &'static str {
        match self {
            Change::SetResources { .. } => "setResources",
            Change::Scale { .. } => "scale",
            Change::RolloutRestart { .. } => "rolloutRestart",
            Change::SetHpaBounds { .. } => "setHpaBounds",
            Change::DeletePod { .. } => "deletePod",
        }
    }

    pub fn namespace(&self) -> &str {
        match self {
            Change::SetResources { namespace, .. }
            | Change::Scale { namespace, .. }
            | Change::RolloutRestart { namespace, .. }
            | Change::SetHpaBounds { namespace, .. }
            | Change::DeletePod { namespace, .. } => namespace,
        }
    }

    pub fn workload(&self) -> Option<&WorkloadRef> {
        match self {
            Change::SetResources { workload, .. }
            | Change::Scale { workload, .. }
            | Change::RolloutRestart { workload, .. } => Some(workload),
            _ => None,
        }
    }

    /// Объект изменения: workload, HPA или под.
    pub fn target(&self) -> Target {
        let ns = self.namespace();
        match self {
            Change::SetResources { workload, .. }
            | Change::Scale { workload, .. }
            | Change::RolloutRestart { workload, .. } => {
                Target::workload(ns, workload.kind, &workload.name)
            }
            Change::SetHpaBounds { hpa, .. } => Target::object(TargetKind::Hpa, ns, hpa),
            Change::DeletePod { pod, .. } => Target::object(TargetKind::Pod, ns, pod),
        }
    }

    /// Имена без пробелов по краям: так их проверяют и хранят.
    pub fn trimmed(mut self) -> Self {
        let t = |s: &mut String| *s = s.trim().to_string();
        match &mut self {
            Change::SetResources {
                namespace,
                workload,
                containers,
            } => {
                t(namespace);
                t(&mut workload.name);
                for c in containers {
                    t(&mut c.name);
                    for v in [
                        &mut c.requests.cpu,
                        &mut c.requests.memory,
                        &mut c.limits.cpu,
                        &mut c.limits.memory,
                    ] {
                        if let Some(Some(q)) = v {
                            t(q);
                        }
                    }
                }
            }
            Change::Scale {
                namespace,
                workload,
                ..
            }
            | Change::RolloutRestart {
                namespace,
                workload,
            } => {
                t(namespace);
                t(&mut workload.name);
            }
            Change::SetHpaBounds { namespace, hpa, .. } => {
                t(namespace);
                t(hpa);
            }
            Change::DeletePod { namespace, pod } => {
                t(namespace);
                t(pod);
            }
        }
        self
    }
}

/// Неверное изменение: текст для человека (ключ словаря) и, у LLM, `invalid_argument` с
/// английским текстом.
fn invalid(text: crate::i18n::LocalizedText) -> AppError {
    AppError::localized(ErrorCode::InvalidInput, text)
}

/// Quantity поля `field` разбирается и не отрицательна.
fn quantity(v: &str, container: &str, field: &str) -> AppResult<f64> {
    match parse_quantity(v) {
        Some(q) if q >= 0.0 && q.is_finite() && !v.is_empty() => Ok(q),
        _ => Err(invalid(crate::ltext!(
            "errors.change.badQuantity",
            "Container {{container}}: “{{value}}” is not a valid {{field}} quantity",
            container = container,
            value = v.escape_debug(),
            field = field
        ))),
    }
}

/// Форма изменения без кластера: имена, числа, quantity, `limits ≥ requests` внутри изменения
/// (с текущими значениями — `facts::check`).
pub fn validate_form(c: &Change) -> AppResult<()> {
    crate::k8s::read::check_namespace(c.namespace())?;
    if let Some(w) = c.workload() {
        crate::k8s::read::check_name(&w.name)?;
    }
    match c {
        Change::SetResources { containers, .. } => {
            if containers.is_empty() || containers.len() > MAX_CONTAINERS {
                return Err(invalid(crate::ltext!(
                    "errors.change.containersCount",
                    "List 1 to {{max}} containers",
                    max = MAX_CONTAINERS
                )));
            }
            let mut seen = std::collections::BTreeSet::new();
            for ct in containers {
                crate::k8s::read::check_name(&ct.name)?;
                if !seen.insert(ct.name.as_str()) {
                    return Err(invalid(crate::ltext!(
                        "errors.change.duplicateContainer",
                        "Container {{container}} is listed twice",
                        container = ct.name
                    )));
                }
                if ct.requests.is_empty() && ct.limits.is_empty() {
                    return Err(invalid(crate::ltext!(
                        "errors.change.nothingToChange",
                        "Container {{container}}: nothing to change",
                        container = ct.name
                    )));
                }
                let mut values = [None; 4];
                for (i, (v, field)) in [
                    (&ct.requests.cpu, "CPU request"),
                    (&ct.requests.memory, "memory request"),
                    (&ct.limits.cpu, "CPU limit"),
                    (&ct.limits.memory, "memory limit"),
                ]
                .into_iter()
                .enumerate()
                {
                    if let Some(Some(q)) = v {
                        values[i] = Some(quantity(q, &ct.name, field)?);
                    }
                }
                check_limits(&ct.name, values[0], values[2], "CPU")?;
                check_limits(&ct.name, values[1], values[3], "memory")?;
            }
        }
        Change::Scale {
            workload, replicas, ..
        } => {
            if !matches!(
                workload.kind,
                WorkloadKind::Deployment
                    | WorkloadKind::StatefulSet
                    | WorkloadKind::DeploymentConfig
            ) {
                return Err(invalid(crate::ltext!(
                    "errors.change.scaleKind",
                    "Only a Deployment, StatefulSet or DeploymentConfig can be scaled"
                )));
            }
            if !(0..=MAX_REPLICAS).contains(replicas) {
                return Err(invalid(crate::ltext!(
                    "errors.change.replicas",
                    "Replicas must be from 0 to {{max}}",
                    max = MAX_REPLICAS
                )));
            }
        }
        Change::RolloutRestart { workload, .. } => {
            if matches!(workload.kind, WorkloadKind::Job | WorkloadKind::CronJob) {
                return Err(invalid(crate::ltext!(
                    "errors.change.restartKind",
                    "A Job or CronJob cannot be restarted"
                )));
            }
        }
        Change::SetHpaBounds {
            hpa,
            min_replicas,
            max_replicas,
            ..
        } => {
            crate::k8s::read::check_name(hpa)?;
            check_bounds(*min_replicas, *max_replicas)?;
        }
        Change::DeletePod { pod, .. } => crate::k8s::read::check_name(pod)?,
    }
    Ok(())
}

/// `limits ≥ requests`, если заданы оба.
pub fn check_limits(
    container: &str,
    request: Option<f64>,
    limit: Option<f64>,
    what: &str,
) -> AppResult<()> {
    if let (Some(r), Some(l)) = (request, limit)
        && l < r
    {
        return Err(invalid(crate::ltext!(
            "errors.change.limitBelowRequest",
            "Container {{container}}: the {{resource}} limit is below the request",
            container = container,
            resource = what
        )));
    }
    Ok(())
}

/// `1 ≤ min ≤ max ≤ 1000`.
pub fn check_bounds(min: Option<i32>, max: i32) -> AppResult<()> {
    let ok = (1..=MAX_REPLICAS).contains(&max) && min.is_none_or(|m| (1..=max).contains(&m));
    if ok {
        return Ok(());
    }
    Err(invalid(crate::ltext!(
        "errors.change.hpaBounds",
        "HPA bounds must be 1 ≤ minReplicas ≤ maxReplicas ≤ {{max}}",
        max = MAX_REPLICAS
    )))
}

/// Одинаковые quantity: `500m` и `0.5` — одно и то же.
fn same_quantity(a: &Option<Option<String>>, b: &Option<Option<String>>) -> bool {
    match (a, b) {
        (None, None) | (Some(None), Some(None)) => true,
        (Some(Some(x)), Some(Some(y))) => match (parse_quantity(x), parse_quantity(y)) {
            (Some(x), Some(y)) => (x - y).abs() <= 1e-9 * x.abs().max(1.0),
            _ => x == y,
        },
        _ => false,
    }
}

/// Применено ровно то, что предлагалось (спец. §10.5): тот же вид и объект, ресурсы — по
/// контейнерам без учёта порядка и записи quantity, реплики и границы HPA — числами.
pub fn applied_as_proposed(proposed: &Change, applied: &Change) -> bool {
    if proposed.kind_name() != applied.kind_name() || proposed.target() != applied.target() {
        return false;
    }
    match (proposed, applied) {
        (
            Change::SetResources { containers: a, .. },
            Change::SetResources { containers: b, .. },
        ) => {
            a.len() == b.len()
                && a.iter().all(|x| {
                    b.iter().any(|y| {
                        x.name == y.name
                            && same_quantity(&x.requests.cpu, &y.requests.cpu)
                            && same_quantity(&x.requests.memory, &y.requests.memory)
                            && same_quantity(&x.limits.cpu, &y.limits.cpu)
                            && same_quantity(&x.limits.memory, &y.limits.memory)
                    })
                })
        }
        (Change::Scale { replicas: a, .. }, Change::Scale { replicas: b, .. }) => a == b,
        (
            Change::SetHpaBounds {
                min_replicas: a1,
                max_replicas: a2,
                ..
            },
            Change::SetHpaBounds {
                min_replicas: b1,
                max_replicas: b2,
                ..
            },
        ) => a1 == b1 && a2 == b2,
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

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

    #[test]
    fn json_shape_keeps_absent_null_and_values_apart() {
        let c = set(json!([{"name": "app", "requests": {"cpu": "250m", "memory": null}}]));
        let Change::SetResources { containers, .. } = &c else {
            panic!()
        };
        assert_eq!(containers[0].requests.cpu, Some(Some("250m".into())));
        assert_eq!(containers[0].requests.memory, Some(None), "null — убрать");
        assert_eq!(containers[0].limits.cpu, None, "нет поля — не менять");
        assert_eq!(
            serde_json::to_value(&c).unwrap()["containers"][0],
            json!({"name": "app", "requests": {"cpu": "250m", "memory": null}, "limits": {}})
        );
        let h: Change = serde_json::from_value(json!({
            "kind": "setHpaBounds", "namespace": "team-a", "hpa": "web", "maxReplicas": 5
        }))
        .unwrap();
        assert_eq!(
            serde_json::to_value(&h).unwrap(),
            json!({
                "kind": "setHpaBounds", "namespace": "team-a", "hpa": "web", "maxReplicas": 5
            })
        );
        assert!(
            serde_json::from_value::<Change>(json!({"kind": "deleteNamespace", "namespace": "x"}))
                .is_err()
        );
    }

    #[test]
    fn targets_and_names() {
        let s = Change::Scale {
            namespace: "team-a".into(),
            workload: web(),
            replicas: 3,
        };
        assert_eq!(s.kind_name(), "scale");
        assert_eq!(
            s.target(),
            Target::workload("team-a", WorkloadKind::Deployment, "web")
        );
        let h = Change::SetHpaBounds {
            namespace: " team-a ".into(),
            hpa: " web ".into(),
            min_replicas: None,
            max_replicas: 3,
        }
        .trimmed();
        assert_eq!(h.target(), Target::object(TargetKind::Hpa, "team-a", "web"));
        let p = Change::DeletePod {
            namespace: "team-a".into(),
            pod: "web-1".into(),
        };
        assert_eq!(
            p.target(),
            Target::object(TargetKind::Pod, "team-a", "web-1")
        );
        let c = set(json!([{"name": " app ", "requests": {"cpu": " 1 "}}])).trimmed();
        let Change::SetResources { containers, .. } = c else {
            panic!()
        };
        assert_eq!(
            (
                containers[0].name.as_str(),
                containers[0].requests.cpu.clone()
            ),
            ("app", Some(Some("1".into())))
        );
    }

    #[test]
    fn form_rules() {
        let key = |c: Change| validate_form(&c).err().map(|e| e.key);
        assert_eq!(
            key(set(
                json!([{"name": "app", "requests": {"cpu": "250m"}, "limits": {"cpu": "1"}}])
            )),
            None
        );
        assert_eq!(
            key(set(json!([]))).as_deref(),
            Some("errors.change.containersCount")
        );
        assert_eq!(
            key(set(json!([{"name": "app", "requests": {"cpu": "1"}}, {"name": "app", "limits": {"cpu": "2"}}]))).as_deref(),
            Some("errors.change.duplicateContainer")
        );
        assert_eq!(
            key(set(json!([{"name": "app"}]))).as_deref(),
            Some("errors.change.nothingToChange")
        );
        assert_eq!(
            key(set(json!([{"name": "app", "requests": {"cpu": "lots"}}]))).as_deref(),
            Some("errors.change.badQuantity")
        );
        assert_eq!(
            key(set(
                json!([{"name": "app", "requests": {"memory": "-1Mi"}}])
            ))
            .as_deref(),
            Some("errors.change.badQuantity")
        );
        assert_eq!(
            key(set(json!([{"name": "app", "requests": {"memory": "1Gi"}, "limits": {"memory": "512Mi"}}]))).as_deref(),
            Some("errors.change.limitBelowRequest")
        );
        assert_eq!(
            key(set(json!([{"name": "a/b", "requests": {"cpu": "1"}}]))).as_deref(),
            Some("errors.k8s.badName")
        );
        let scale = |kind: WorkloadKind, replicas| Change::Scale {
            namespace: "team-a".into(),
            workload: WorkloadRef {
                kind,
                name: "web".into(),
            },
            replicas,
        };
        assert_eq!(key(scale(WorkloadKind::StatefulSet, 0)), None);
        assert_eq!(
            key(scale(WorkloadKind::Deployment, 1001)).as_deref(),
            Some("errors.change.replicas")
        );
        assert_eq!(
            key(scale(WorkloadKind::DaemonSet, 1)).as_deref(),
            Some("errors.change.scaleKind")
        );
        let restart = |kind| Change::RolloutRestart {
            namespace: "team-a".into(),
            workload: WorkloadRef {
                kind,
                name: "web".into(),
            },
        };
        assert_eq!(key(restart(WorkloadKind::DaemonSet)), None);
        assert_eq!(
            key(restart(WorkloadKind::CronJob)).as_deref(),
            Some("errors.change.restartKind")
        );
        let hpa = |min, max| Change::SetHpaBounds {
            namespace: "team-a".into(),
            hpa: "web".into(),
            min_replicas: min,
            max_replicas: max,
        };
        assert_eq!(key(hpa(Some(2), 5)), None);
        assert_eq!(key(hpa(None, 1)), None);
        for (min, max) in [(Some(0), 5), (Some(6), 5), (None, 0), (None, 1001)] {
            assert_eq!(
                key(hpa(min, max)).as_deref(),
                Some("errors.change.hpaBounds"),
                "{min:?} {max}"
            );
        }
        let bad_ns = Change::DeletePod {
            namespace: "Team_A".into(),
            pod: "x".into(),
        };
        assert_eq!(
            key(bad_ns).as_deref(),
            Some("errors.connections.badNamespace")
        );
    }

    #[test]
    fn applied_is_compared_by_values_not_by_spelling() {
        let a = set(
            json!([{"name": "app", "requests": {"cpu": "500m"}}, {"name": "side", "limits": {"memory": "1Gi"}}]),
        );
        let b = set(
            json!([{"name": "side", "limits": {"memory": "1024Mi"}}, {"name": "app", "requests": {"cpu": "0.5"}}]),
        );
        assert!(applied_as_proposed(&a, &b));
        let edited = set(
            json!([{"name": "app", "requests": {"cpu": "600m"}}, {"name": "side", "limits": {"memory": "1Gi"}}]),
        );
        assert!(!applied_as_proposed(&a, &edited));
        let dropped = set(json!([{"name": "app", "requests": {"cpu": "500m"}}]));
        assert!(!applied_as_proposed(&a, &dropped));
        let scale = |n| Change::Scale {
            namespace: "team-a".into(),
            workload: web(),
            replicas: n,
        };
        assert!(applied_as_proposed(&scale(3), &scale(3)));
        assert!(!applied_as_proposed(&scale(3), &scale(4)));
        assert!(!applied_as_proposed(&scale(3), &a), "другой вид");
        let restart = Change::RolloutRestart {
            namespace: "team-a".into(),
            workload: web(),
        };
        assert!(applied_as_proposed(&restart, &restart.clone()));
    }
}
