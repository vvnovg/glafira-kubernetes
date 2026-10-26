//! Правила квот, PVC и HPA проекта (спец. §8.2).

use serde_json::json;

use super::{Findings, GRACE_S, listed};
use crate::health::model::{Target, TargetKind};
use crate::health::snapshot::ClusterSnapshot;

/// Квота почти исчерпана: использовано не меньше этой доли (тот же порог, что в интерфейсе).
pub const QUOTA_NEAR: f64 = 0.9;

pub fn check(s: &ClusterSnapshot, out: &mut Findings) {
    for p in &s.projects {
        let ns = p.namespace.as_str();
        for q in p.quotas.iter().flatten() {
            let near: Vec<String> = q
                .entries
                .iter()
                .filter(|e| e.ratio >= QUOTA_NEAR)
                .map(|e| {
                    format!(
                        "{}: {}/{} ({}%)",
                        e.resource,
                        e.used,
                        e.hard,
                        (e.ratio * 100.0).round() as i64
                    )
                })
                .collect();
            if !near.is_empty() {
                out.emit(
                    "quota.nearLimit",
                    Target::object(TargetKind::Quota, ns, &q.name),
                    None,
                    json!({ "resources": listed(&near) }),
                );
            }
        }
        for c in p.pvcs.iter().flatten() {
            let target = || Target::object(TargetKind::Pvc, ns, &c.name);
            match c.phase.as_deref() {
                Some("Lost") => out.emit(
                    "pvc.pending",
                    target(),
                    None,
                    json!({ "phase": "Lost", "class": c.class }),
                ),
                // Без пода-потребителя ожидание — норма для класса `WaitForFirstConsumer`.
                Some("Pending")
                    if !c.mounted_by.is_empty()
                        && c.created.is_some_and(|t| s.now - t >= GRACE_S) =>
                {
                    out.emit(
                        "pvc.pending",
                        target(),
                        None,
                        json!({
                            "phase": "Pending",
                            "class": c.class,
                            "pods": listed(&c.mounted_by),
                            "minutes": c.created.map(|t| (s.now - t) / 60),
                        }),
                    )
                }
                _ => {}
            }
        }
        for h in p.hpas.iter().flatten() {
            let target = || Target::object(TargetKind::Hpa, ns, &h.name);
            let scaled = format!("{}/{}", h.target_kind, h.target_name);
            if h.conditions
                .get("ScalingLimited")
                .is_some_and(|c| c.is("True") && c.reason.as_deref() == Some("TooManyReplicas"))
            {
                out.emit(
                    "hpa.atMax",
                    target(),
                    None,
                    json!({ "maxReplicas": h.max_replicas, "current": h.current_replicas,
                        "target": scaled }),
                );
            }
            let stuck = ["AbleToScale", "ScalingActive"]
                .into_iter()
                .find_map(|name| {
                    h.conditions
                        .get(name)
                        .filter(|c| c.is("False"))
                        .map(|c| (name, c))
                });
            if let Some((name, c)) = stuck {
                out.emit(
                    "hpa.unableToScale",
                    target(),
                    None,
                    json!({ "condition": name, "reason": c.reason, "message": c.message,
                        "target": scaled }),
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::super::fixtures::NOW;
    use super::*;
    use crate::health::model::{Finding, Severity};
    use crate::health::snapshot::*;
    use crate::platform::PlatformKind;
    use crate::storage::profile::Environment;

    fn run(p: ProjectSnap, env: Environment) -> Vec<Finding> {
        let mut s = ClusterSnapshot::empty(NOW, PlatformKind::Kubernetes);
        s.projects = vec![p];
        crate::health::rules::evaluate(&s, env)
    }

    fn ids(v: &[Finding]) -> Vec<String> {
        v.iter()
            .map(|f| format!("{} {}", f.rule_id, f.target.name.as_deref().unwrap_or("-")))
            .collect()
    }

    fn uses(resource: &str, used: &str, hard: &str, ratio: f64) -> QuotaUse {
        QuotaUse {
            resource: resource.into(),
            used: used.into(),
            hard: hard.into(),
            ratio,
        }
    }

    fn cond(status: &str, reason: &str) -> Cond {
        Cond {
            status: status.into(),
            reason: Some(reason.into()),
            message: Some(format!("{reason} message")),
            since: None,
        }
    }

    #[test]
    fn quota_at_ninety_percent() {
        let p = ProjectSnap {
            namespace: "team-a".into(),
            quotas: Some(vec![
                QuotaSnap {
                    name: "compute".into(),
                    entries: vec![
                        uses("pods", "18", "20", 0.9),
                        uses("requests.cpu", "3", "4", 0.75),
                        uses("requests.memory", "9Gi", "8Gi", 1.125),
                    ],
                },
                QuotaSnap {
                    name: "objects".into(),
                    entries: vec![uses("count/configmaps", "8", "10", 0.8999)],
                },
            ]),
            ..Default::default()
        };
        let v = run(p.clone(), Environment::Prod);
        assert_eq!(ids(&v), ["quota.nearLimit compute"]);
        assert_eq!(
            v[0].params["resources"],
            json!(["pods: 18/20 (90%)", "requests.memory: 9Gi/8Gi (113%)"])
        );
        assert_eq!(run(p, Environment::Local)[0].severity, Severity::Info);
    }

    #[test]
    fn lost_or_long_pending_claims_that_a_pod_waits_for() {
        let pvc = |name: &str, phase: &str, age: i64, pods: &[&str]| PvcSnap {
            name: name.into(),
            phase: Some(phase.into()),
            created: Some(NOW - age),
            class: Some("standard".into()),
            mounted_by: pods.iter().map(|p| p.to_string()).collect(),
        };
        let p = ProjectSnap {
            namespace: "team-a".into(),
            pvcs: Some(vec![
                pvc("lost", "Lost", 10, &[]),
                pvc("waiting", "Pending", GRACE_S, &["db-0"]),
                pvc("fresh", "Pending", 60, &["db-1"]),
                pvc("unused", "Pending", 86_400, &[]),
                pvc("bound", "Bound", 86_400, &["web-1"]),
            ]),
            ..Default::default()
        };
        let v = run(p, Environment::Prod);
        assert_eq!(ids(&v), ["pvc.pending lost", "pvc.pending waiting"]);
        assert_eq!(v[0].params["phase"], "Lost");
        assert_eq!(v[1].params["pods"], json!(["db-0"]));
        assert_eq!(v[1].params["minutes"], 5);
    }

    #[test]
    fn hpa_at_max_and_unable_to_scale() {
        let hpa = |name: &str, conds: &[(&str, Cond)]| HpaSnap {
            name: name.into(),
            target_kind: "Deployment".into(),
            target_name: name.into(),
            min_replicas: Some(2),
            max_replicas: 5,
            current_replicas: Some(5),
            conditions: conds
                .iter()
                .map(|(n, c)| (n.to_string(), c.clone()))
                .collect::<BTreeMap<_, _>>(),
        };
        let p = ProjectSnap {
            namespace: "team-a".into(),
            hpas: Some(vec![
                hpa(
                    "web",
                    &[("ScalingLimited", cond("True", "TooManyReplicas"))],
                ),
                hpa(
                    "api",
                    &[
                        ("AbleToScale", cond("True", "SucceededGetScale")),
                        ("ScalingActive", cond("False", "FailedGetResourceMetric")),
                    ],
                ),
                hpa("ok", &[("ScalingLimited", cond("True", "TooFewReplicas"))]),
            ]),
            ..Default::default()
        };
        let v = run(p, Environment::Prod);
        assert_eq!(ids(&v), ["hpa.atMax web", "hpa.unableToScale api"]);
        assert_eq!(v[0].params["target"], "Deployment/web");
        assert_eq!(v[0].params["maxReplicas"], 5);
        assert_eq!(
            (
                v[1].params["condition"].as_str(),
                v[1].params["reason"].as_str()
            ),
            (Some("ScalingActive"), Some("FailedGetResourceMetric"))
        );
    }
}
