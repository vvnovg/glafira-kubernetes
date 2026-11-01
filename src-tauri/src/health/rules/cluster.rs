//! Правила уровня кластера (спец. §8.2): API-сервер, OpenShift (операторы, обновление, пулы
//! машин), DropApp (компоненты, узлы, `IngressTLS`), ноды; сводка непроверенного `check.partial`.

use serde_json::json;

use super::{Findings, listed};
use crate::health::model::Target;
use crate::health::snapshot::{ClusterSnapshot, DropAppSnap};

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
    if let Some(d) = &s.dropapp {
        dropapp(d, out);
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
            // В `params` — целый процент; порог сравнивается точно: 90,5 % — больше 90 %.
            let percent =
                |used: i64, total: Option<i64>| total.filter(|t| *t > 0).map(|t| used * 100 / t);
            let over = |used: i64, total: Option<i64>| {
                total.filter(|t| *t > 0).is_some_and(|t| {
                    used.saturating_mul(100) > t.saturating_mul(REQUESTS_HIGH_PERCENT)
                })
            };
            let cpu = percent(r.cpu_milli, n.allocatable_cpu_milli);
            let memory = percent(r.memory, n.allocatable_memory);
            if over(r.cpu_milli, n.allocatable_cpu_milli) || over(r.memory, n.allocatable_memory) {
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

/// Предел текста события в params: сообщение события пишет кто угодно, оно уходит в находку,
/// MCP и отчёт.
const MAX_EVENT_CHARS: usize = 1024;

/// Текст события не длиннее `MAX_EVENT_CHARS` символов: длиннее — обрезается по символу, с «…».
fn cap_event(event: &str) -> String {
    if event.chars().count() <= MAX_EVENT_CHARS {
        return event.to_string();
    }
    let mut cut: String = event.chars().take(MAX_EVENT_CHARS - 1).collect();
    cut.push('…');
    cut
}

/// Правила DropApp (спека O5 §7.2): списки — до `MAX_LISTED`, `count` — всех.
fn dropapp(d: &DropAppSnap, out: &mut Findings) {
    let failed: Vec<_> = d
        .components
        .iter()
        .flatten()
        .filter(|c| c.is_failed())
        .collect();
    if !failed.is_empty() {
        let components: Vec<_> = failed
            .iter()
            .map(|c| {
                json!({ "kind": c.kind.as_str(), "name": c.name, "namespace": c.namespace,
                    "status": c.status, "event": c.event.as_deref().map(cap_event) })
            })
            .collect();
        let names: Vec<_> = failed
            .iter()
            .map(|c| format!("{} {}", c.kind.as_str(), c.name))
            .collect();
        out.emit(
            "cluster.componentFailed",
            Target::cluster(),
            None,
            json!({ "components": listed(&components), "count": failed.len(),
                "names": listed(&names) }),
        );
    }
    let nodes: Vec<_> = d
        .nodes
        .iter()
        .flatten()
        .filter(|n| n.has_problem())
        .collect();
    if !nodes.is_empty() {
        let rows: Vec<_> = nodes
            .iter()
            .map(|n| json!({ "name": n.name, "status": n.status }))
            .collect();
        let names: Vec<_> = nodes.iter().map(|n| n.name.clone()).collect();
        out.emit(
            "cluster.dropappNodeNotReady",
            Target::cluster(),
            None,
            json!({ "nodes": listed(&rows), "count": nodes.len(), "names": listed(&names) }),
        );
    }
    if let Some(tls) = &d.ingress_tls
        && !tls.is_empty()
    {
        let rows: Vec<_> = tls
            .iter()
            .map(|t| {
                json!({ "name": t.name, "namespace": t.namespace, "reason": t.reason,
                    "message": t.message })
            })
            .collect();
        let names: Vec<_> = tls
            .iter()
            .map(|t| match &t.namespace {
                Some(ns) => format!("{ns}/{}", t.name),
                None => t.name.clone(),
            })
            .collect();
        out.emit(
            "cluster.ingressTlsNotReady",
            Target::cluster(),
            None,
            json!({ "ingressTls": listed(&rows), "count": tls.len(), "names": listed(&names) }),
        );
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
    use crate::k8s::dropapp::{ComponentKind, ComponentRow, DropAppNodeRow, IngressTlsRow};
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

    fn component(kind: ComponentKind, name: &str, status: Option<&str>) -> ComponentRow {
        ComponentRow {
            kind,
            name: name.into(),
            namespace: (kind == ComponentKind::Component).then(|| "dapp-system".into()),
            status: status.map(String::from),
            retry: None,
            event: None,
        }
    }

    fn dropapp_node(name: &str, status: Option<&str>) -> DropAppNodeRow {
        DropAppNodeRow {
            name: name.into(),
            status: status.map(String::from),
        }
    }

    /// Текст события в params — не длиннее 1024 символов: обрезается по символу, с «…».
    #[test]
    fn long_component_event_is_capped() {
        let mut s = ClusterSnapshot::empty(NOW, PlatformKind::Dropapp);
        let mut failed = component(ComponentKind::Tool, "grafana", Some("failed"));
        failed.event = Some("ж".repeat(2000));
        let mut short = component(ComponentKind::Tool, "loki", Some("failed"));
        short.event = Some("я".repeat(1024));
        s.dropapp = Some(DropAppSnap {
            components: Some(vec![failed, short]),
            nodes: Some(vec![]),
            ingress_tls: Some(vec![]),
        });
        let v = run(&s, Environment::Prod);
        let event = |i: usize| {
            v[0].params["components"][i]["event"]
                .as_str()
                .unwrap()
                .to_string()
        };
        let long = event(0);
        assert_eq!(long.chars().count(), 1024);
        assert!(long.ends_with("ж…"), "{long}");
        assert_eq!(event(1), "я".repeat(1024), "ровно 1024 — без обрезки");
    }

    #[test]
    fn dropapp_components_nodes_and_ingress_tls() {
        let mut s = ClusterSnapshot::empty(NOW, PlatformKind::Dropapp);
        let mut failed = component(ComponentKind::Component, "cert-manager", Some("Failed"));
        failed.event = Some("InstallFailed: helm install failed: timed out".into());
        s.dropapp = Some(DropAppSnap {
            components: Some(vec![
                failed,
                component(ComponentKind::Tool, "grafana", Some("error")),
                // Обновление — проблема обзора, но не правило.
                component(ComponentKind::Tool, "dex", Some("upgrading")),
                component(ComponentKind::Component, "ingress-nginx", Some("Success")),
                component(ComponentKind::Tool, "loki", None),
            ]),
            nodes: Some(vec![
                dropapp_node("master-1", Some("ready")),
                dropapp_node("worker-2", Some("NotReady")),
                // Статуса нет — состояние неизвестно.
                dropapp_node("worker-3", None),
            ]),
            ingress_tls: Some(vec![IngressTlsRow {
                name: "apps".into(),
                namespace: Some("dapp-system".into()),
                reason: Some("CertificateExpired".into()),
                message: Some("certificate expired on 2026-10-30".into()),
            }]),
        });
        let v = run(&s, Environment::Prod);
        assert_eq!(
            ids(&v),
            [
                "cluster.componentFailed -",
                "cluster.dropappNodeNotReady -",
                "cluster.ingressTlsNotReady -"
            ]
        );
        let c = &v[0].params;
        assert_eq!(c["count"], 2);
        assert_eq!(
            c["names"],
            json!(["Component cert-manager", "Tool grafana"])
        );
        assert_eq!(
            c["components"][0],
            json!({"kind": "Component", "name": "cert-manager", "namespace": "dapp-system",
                "status": "Failed", "event": "InstallFailed: helm install failed: timed out"})
        );
        assert_eq!(
            c["components"][1],
            json!({"kind": "Tool", "name": "grafana", "namespace": null, "status": "error",
                "event": null})
        );
        let n = &v[1].params;
        assert_eq!(n["count"], 1);
        assert_eq!(n["names"], json!(["worker-2"]));
        assert_eq!(
            n["nodes"],
            json!([{"name": "worker-2", "status": "NotReady"}])
        );
        let t = &v[2].params;
        assert_eq!(t["count"], 1);
        assert_eq!(t["names"], json!(["dapp-system/apps"]));
        assert_eq!(
            t["ingressTls"],
            json!([{"name": "apps", "namespace": "dapp-system", "reason": "CertificateExpired",
                "message": "certificate expired on 2026-10-30"}])
        );
    }

    #[test]
    fn dropapp_rules_by_environment() {
        let mut s = ClusterSnapshot::empty(NOW, PlatformKind::Dropapp);
        s.dropapp = Some(DropAppSnap {
            components: Some(vec![component(
                ComponentKind::Tool,
                "grafana",
                Some("failed"),
            )]),
            nodes: Some(vec![dropapp_node("worker-2", Some("NotReady"))]),
            ingress_tls: Some(vec![IngressTlsRow {
                name: "apps".into(),
                namespace: None,
                reason: None,
                message: None,
            }]),
        });
        let levels = |env| {
            run(&s, env)
                .iter()
                .map(|f| (f.rule_id.clone(), f.severity))
                .collect::<Vec<_>>()
        };
        use Severity::*;
        for (env, component, other) in [
            (Environment::Prod, Critical, Warning),
            (Environment::Stage, Critical, Warning),
            (Environment::Test, Warning, Info),
            (Environment::Dev, Warning, Info),
            (Environment::Local, Warning, Info),
        ] {
            assert_eq!(
                levels(env),
                [
                    ("cluster.componentFailed".to_string(), component),
                    ("cluster.dropappNodeNotReady".to_string(), other),
                    ("cluster.ingressTlsNotReady".to_string(), other),
                ],
                "{env:?}"
            );
        }
    }

    #[test]
    fn dropapp_without_problems_or_unread_parts_is_quiet() {
        let mut s = ClusterSnapshot::empty(NOW, PlatformKind::Dropapp);
        s.dropapp = Some(DropAppSnap {
            components: Some(vec![
                component(ComponentKind::Tool, "dex", Some("upgrading")),
                component(ComponentKind::Component, "ingress-nginx", Some("Success")),
            ]),
            nodes: Some(vec![
                dropapp_node("master-1", Some("Ready")),
                dropapp_node("worker-3", None),
            ]),
            ingress_tls: Some(vec![]),
        });
        assert!(run(&s, Environment::Prod).is_empty());
        s.dropapp = Some(DropAppSnap::default());
        assert!(
            run(&s, Environment::Prod).is_empty(),
            "не прочитано — не находка"
        );
    }

    #[test]
    fn dropapp_lists_at_most_twenty_but_counts_all() {
        let mut s = ClusterSnapshot::empty(NOW, PlatformKind::Dropapp);
        s.dropapp = Some(DropAppSnap {
            components: Some(
                (0..25)
                    .map(|i| component(ComponentKind::Tool, &format!("t{i:02}"), Some("failed")))
                    .collect(),
            ),
            ..Default::default()
        });
        let v = run(&s, Environment::Prod);
        assert_eq!(v[0].params["count"], 25);
        assert_eq!(v[0].params["components"].as_array().unwrap().len(), 20);
        assert_eq!(v[0].params["names"].as_array().unwrap().len(), 20);
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
    fn requests_high_compares_exactly_not_the_truncated_percent() {
        let mut s = ClusterSnapshot::empty(NOW, PlatformKind::Kubernetes);
        let mut tight = |name: &str, cpu: i64, memory: i64| {
            let mut n = node(name);
            n.allocatable_cpu_milli = Some(4000);
            n.allocatable_memory = Some(1000);
            s.nodes.get_or_insert_with(Vec::new).push(n);
            s.node_requests.get_or_insert_with(BTreeMap::new).insert(
                name.to_string(),
                Requests {
                    cpu_milli: cpu,
                    memory,
                },
            );
        };
        // Ровно 90 % по обоим ресурсам — не больше 90 %.
        tight("exact", 3600, 900);
        // 90,025 % CPU: целая часть 90, но это больше 90 %.
        tight("cpu-over", 3601, 100);
        // 90,1 % памяти.
        tight("memory-over", 100, 901);
        let v = run(&s, Environment::Prod);
        assert_eq!(
            ids(&v),
            [
                "node.requestsHigh cpu-over",
                "node.requestsHigh memory-over"
            ]
        );
        // В `params` — целые проценты, как и прежде.
        assert_eq!(v[0].params["cpuPercent"], 90);
        assert_eq!(v[1].params["memoryPercent"], 90);
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
