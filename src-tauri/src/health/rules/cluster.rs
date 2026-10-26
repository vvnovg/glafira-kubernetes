//! Правила уровня кластера (спец. §8.2): API-сервер, OpenShift (операторы, обновление, пулы
//! машин), ноды; сводка непроверенного `check.partial`.

use serde_json::json;

use super::{Findings, listed};
use crate::health::model::Target;
use crate::health::snapshot::ClusterSnapshot;

/// Доля requests от allocatable, выше которой нода перегружена заявками, %.
pub const REQUESTS_HIGH_PERCENT: i64 = 90;

pub fn check(s: &ClusterSnapshot, out: &mut Findings) {
    if let Some(r) = &s.readyz
        && !r.ok
    {
        out.emit(
            "cluster.apiNotReady",
            Target::cluster(),
            None,
            json!({ "failed": listed(&r.failed), "count": r.failed.len() }),
        );
    }
    if let Some(o) = &s.openshift {
        if let Some(ops) = &o.operators {
            let bad: Vec<_> = ops
                .iter()
                .filter(|op| op.degraded || op.available == Some(false))
                .collect();
            if !bad.is_empty() {
                let operators: Vec<_> = bad
                    .iter()
                    .map(|op| {
                        json!({ "name": op.name, "degraded": op.degraded,
                            "available": op.available, "message": op.message })
                    })
                    .collect();
                out.emit(
                    "cluster.operatorDegraded",
                    Target::cluster(),
                    None,
                    json!({ "operators": listed(&operators), "count": bad.len(),
                        "names": listed(&bad.iter().map(|op| op.name.clone()).collect::<Vec<_>>()) }),
                );
            }
        }
        if let Some(Some(failing)) = &o.failing
            && failing.is("True")
        {
            out.emit(
                "cluster.updateFailing",
                Target::cluster(),
                None,
                json!({ "reason": failing.reason, "message": failing.message }),
            );
        }
        if let Some(pools) = &o.degraded_pools
            && !pools.is_empty()
        {
            out.emit(
                "cluster.mcpDegraded",
                Target::cluster(),
                None,
                json!({ "pools": listed(pools), "count": pools.len() }),
            );
        }
    }
    for n in s.nodes.iter().flatten() {
        let target = || Target::node(&n.name);
        match &n.ready {
            Some(c) if c.is("True") => {}
            ready => out.emit(
                "node.notReady",
                target(),
                None,
                json!({
                    "status": ready.as_ref().map_or("missing", |c| c.status.as_str()),
                    "reason": ready.as_ref().and_then(|c| c.reason.clone()),
                    "message": ready.as_ref().and_then(|c| c.message.clone()),
                }),
            ),
        }
        if !n.pressures.is_empty() {
            out.emit(
                "node.pressure",
                target(),
                None,
                json!({ "conditions": n.pressures }),
            );
        }
        if let Some(r) = s.node_requests.as_ref().and_then(|m| m.get(&n.name)) {
            let percent =
                |used: i64, total: Option<i64>| total.filter(|t| *t > 0).map(|t| used * 100 / t);
            let cpu = percent(r.cpu_milli, n.allocatable_cpu_milli);
            let memory = percent(r.memory, n.allocatable_memory);
            if [cpu, memory]
                .iter()
                .flatten()
                .any(|p| *p > REQUESTS_HIGH_PERCENT)
            {
                out.emit(
                    "node.requestsHigh",
                    target(),
                    None,
                    json!({ "cpuPercent": cpu, "memoryPercent": memory }),
                );
            }
        }
        if n.unschedulable {
            out.emit("node.unschedulable", target(), None, json!({}));
        }
    }
}

/// `check.partial`: что не проверено и почему. `gaps` — список пробелов целиком (проектов не
/// больше 50, частей на проект около дюжины): шлюз MCP сужает его по области LLM до обрезки, иначе
/// пробелы вне области, попавшие в первые 20, вытеснили бы пробелы области. Двадцатью обрезают
/// только то, что показывают: тексты находки в приложении и ответ модели.
pub fn partial(s: &ClusterSnapshot, out: &mut Findings) {
    let c = &s.coverage;
    if !c.partial() {
        return;
    }
    let gaps: Vec<String> = c
        .gaps
        .iter()
        .map(|g| match &g.namespace {
            Some(ns) => format!("{ns}/{}: {}", g.part, g.code),
            None => format!("{}: {}", g.part, g.code),
        })
        .collect();
    out.emit(
        "check.partial",
        Target::cluster(),
        None,
        json!({
            "projectsChecked": c.projects_checked,
            "projectsTotal": c.projects_total,
            "projectLimit": c.projects_checked < c.projects_total,
            "podLimit": c.pod_limit,
            "timedOut": c.timed_out,
            "gaps": gaps,
            "gapCount": gaps.len(),
        }),
    );
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::super::fixtures::{ENVS, NOW};
    use super::*;
    use crate::health::model::{Finding, Severity};
    use crate::health::snapshot::*;
    use crate::platform::PlatformKind;
    use crate::storage::profile::Environment;

    fn run(s: &ClusterSnapshot, env: Environment) -> Vec<Finding> {
        crate::health::rules::evaluate(s, env)
    }

    fn ids(v: &[Finding]) -> Vec<String> {
        v.iter()
            .map(|f| format!("{} {}", f.rule_id, f.target.name.as_deref().unwrap_or("-")))
            .collect()
    }

    fn cond(status: &str) -> Cond {
        Cond {
            status: status.into(),
            ..Default::default()
        }
    }

    fn node(name: &str) -> NodeSnap {
        NodeSnap {
            name: name.into(),
            ready: Some(cond("True")),
            allocatable_cpu_milli: Some(4000),
            allocatable_memory: Some(8 << 30),
            ..Default::default()
        }
    }

    #[test]
    fn api_server_that_is_not_ready_is_critical_on_prod() {
        let mut s = ClusterSnapshot::empty(NOW, PlatformKind::Kubernetes);
        s.readyz = Some(Readyz {
            ok: false,
            failed: vec!["etcd".into(), "informer-sync".into()],
        });
        let v = run(&s, Environment::Prod);
        assert_eq!(ids(&v), ["cluster.apiNotReady -"]);
        assert_eq!(v[0].severity, Severity::Critical);
        assert_eq!(v[0].params["failed"], json!(["etcd", "informer-sync"]));
        assert_eq!(run(&s, Environment::Dev)[0].severity, Severity::Warning);
        s.readyz = Some(Readyz {
            ok: true,
            failed: vec![],
        });
        assert!(run(&s, Environment::Prod).is_empty());
        s.readyz = None;
        assert!(
            run(&s, Environment::Prod).is_empty(),
            "без ответа — не находка"
        );
    }

    #[test]
    fn openshift_operators_update_and_pools() {
        let mut s = ClusterSnapshot::empty(NOW, PlatformKind::Openshift);
        s.openshift = Some(OpenShiftSnap {
            operators: Some(vec![
                OperatorSnap {
                    name: "ingress".into(),
                    degraded: true,
                    available: Some(true),
                    message: Some("router pods crash".into()),
                },
                OperatorSnap {
                    name: "dns".into(),
                    degraded: false,
                    available: Some(false),
                    message: None,
                },
                OperatorSnap {
                    name: "console".into(),
                    degraded: false,
                    available: Some(true),
                    message: None,
                },
            ]),
            failing: Some(Some(Cond {
                status: "True".into(),
                reason: Some("ClusterOperatorDegraded".into()),
                message: Some("Cluster operator ingress is degraded".into()),
                since: Some(NOW - 600),
            })),
            degraded_pools: Some(vec!["worker".into()]),
        });
        let v = run(&s, Environment::Stage);
        assert_eq!(
            ids(&v),
            [
                "cluster.operatorDegraded -",
                "cluster.updateFailing -",
                "cluster.mcpDegraded -"
            ]
        );
        assert_eq!(v[0].params["names"], json!(["ingress", "dns"]));
        assert_eq!(v[0].params["count"], 2);
        assert_eq!(v[1].params["reason"], "ClusterOperatorDegraded");
        assert_eq!(v[2].params["pools"], json!(["worker"]));
        s.openshift = Some(OpenShiftSnap {
            failing: Some(Some(cond("False"))),
            ..Default::default()
        });
        assert!(
            run(&s, Environment::Prod).is_empty(),
            "не прочитано — не находка"
        );
    }

    #[test]
    fn nodes_not_ready_under_pressure_overcommitted_and_cordoned() {
        let mut s = ClusterSnapshot::empty(NOW, PlatformKind::Kubernetes);
        let mut down = node("w1");
        down.ready = Some(Cond {
            status: "Unknown".into(),
            reason: Some("NodeStatusUnknown".into()),
            message: Some("Kubelet stopped posting node status.".into()),
            since: None,
        });
        let mut missing = node("w2");
        missing.ready = None;
        let mut pressed = node("w3");
        pressed.pressures = vec!["MemoryPressure".into(), "DiskPressure".into()];
        let mut busy = node("w4");
        busy.unschedulable = true;
        s.nodes = Some(vec![down, missing, pressed, busy, node("w5")]);
        s.node_requests = Some(BTreeMap::from([
            (
                "w4".to_string(),
                Requests {
                    cpu_milli: 3700,
                    memory: 1 << 30,
                },
            ),
            (
                "w5".to_string(),
                Requests {
                    cpu_milli: 3600,
                    memory: 8 << 30,
                },
            ),
        ]));
        let v = run(&s, Environment::Prod);
        assert_eq!(
            ids(&v),
            [
                "node.notReady w1",
                "node.notReady w2",
                "node.pressure w3",
                "node.requestsHigh w4",
                "node.requestsHigh w5",
                "node.unschedulable w4"
            ]
        );
        assert_eq!(v[0].params["status"], "Unknown");
        assert_eq!(v[1].params["status"], "missing");
        assert_eq!(
            v[2].params["conditions"],
            json!(["MemoryPressure", "DiskPressure"])
        );
        assert_eq!(
            (
                v[3].params["cpuPercent"].as_i64(),
                v[3].params["memoryPercent"].as_i64()
            ),
            (Some(92), Some(12))
        );
        assert_eq!(
            v[4].params["cpuPercent"], 90,
            "ровно 90 % — не больше 90 %; память 100 %"
        );
        assert_eq!(v[4].params["memoryPercent"], 100);
        assert_eq!(v[2].severity, Severity::Critical);
        assert_eq!(run(&s, Environment::Stage)[2].severity, Severity::Warning);
    }

    #[test]
    fn partial_lists_what_was_not_checked() {
        let mut s = ClusterSnapshot::empty(NOW, PlatformKind::Kubernetes);
        assert!(run(&s, Environment::Prod).is_empty());
        s.coverage = Coverage {
            projects_total: 60,
            projects_checked: 50,
            pod_limit: true,
            timed_out: false,
            gaps: vec![
                Gap {
                    part: "nodes".into(),
                    namespace: None,
                    code: "FORBIDDEN".into(),
                },
                Gap {
                    part: "secrets".into(),
                    namespace: Some("team-a".into()),
                    code: "FORBIDDEN".into(),
                },
            ],
        };
        let v = run(&s, Environment::Prod);
        assert_eq!(ids(&v), ["check.partial -"]);
        for env in ENVS {
            assert_eq!(run(&s, env)[0].severity, Severity::Info, "{env:?}");
        }
        assert_eq!(
            v[0].params["gaps"],
            json!(["nodes: FORBIDDEN", "team-a/secrets: FORBIDDEN"])
        );
        assert_eq!(
            (
                v[0].params["projectLimit"].as_bool(),
                v[0].params["podLimit"].as_bool()
            ),
            (Some(true), Some(true))
        );
    }

    #[test]
    fn partial_keeps_the_full_list_of_gaps() {
        // Список целиком: шлюз MCP сужает его по области LLM, а видимыми остаются первые 20 —
        // это уже дело текстов (приложение) и шлюза (модель).
        let mut s = ClusterSnapshot::empty(NOW, PlatformKind::Kubernetes);
        s.coverage = Coverage {
            projects_total: 40,
            projects_checked: 40,
            pod_limit: false,
            timed_out: false,
            gaps: (0..40)
                .map(|i| Gap {
                    part: "secrets".into(),
                    namespace: Some(format!("team-{i:02}")),
                    code: "FORBIDDEN".into(),
                })
                .collect(),
        };
        let v = run(&s, Environment::Prod);
        let gaps = v[0].params["gaps"].as_array().unwrap();
        assert_eq!(gaps.len(), 40);
        assert_eq!(v[0].params["gapCount"], 40);
        assert_eq!(gaps[0], "team-00/secrets: FORBIDDEN");
        assert_eq!(gaps[39], "team-39/secrets: FORBIDDEN");
    }
}
