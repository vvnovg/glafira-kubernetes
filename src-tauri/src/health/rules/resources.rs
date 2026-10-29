//! Правила `resources.*` (спец. §8.2): потребление контейнеров за 24 ч из источника истории
//! метрик против requests и limits работающих подов. Пороги и сопоставление подов с
//! workload'ами — те же, что у вкладки «Ресурсы» проекта (`resources::project`).

use std::collections::{BTreeMap, HashMap};

use serde_json::json;

use super::{Findings, listed};
use crate::health::model::Target;
use crate::health::snapshot::{ClusterSnapshot, ContainerAmounts, ProjectSnap};
use crate::k8s::quantity::{format_cpu, format_memory};
use crate::k8s::workloads::WorkloadKind;
use crate::resources::current::Amounts;
use crate::resources::project::{Excess, Risk, Use, assess, kind_of, workload_of};

fn larger(a: Option<f64>, b: Option<f64>) -> Option<f64> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (a, b) => a.or(b),
    }
}

/// Request и limit контейнера на под — наибольшие среди работающих подов workload'а.
fn per_pod(a: &mut (Amounts, Amounts), c: &ContainerAmounts) {
    let (r, l) = (c.requests(), c.limits());
    a.0.cpu = larger(a.0.cpu, r.cpu);
    a.0.memory = larger(a.0.memory, r.memory);
    a.1.cpu = larger(a.1.cpu, l.cpu);
    a.1.memory = larger(a.1.memory, l.memory);
}

fn project(p: &ProjectSnap, out: &mut Findings) {
    let (Some(usage), Some(pods), Some(workloads)) = (&p.usage, &p.pods, &p.workloads) else {
        return;
    };
    let list: Vec<(WorkloadKind, String)> =
        workloads.iter().map(|w| (w.kind, w.name.clone())).collect();
    let owner: HashMap<&str, (WorkloadKind, String)> = pods
        .iter()
        .filter_map(|pod| {
            let o = pod.owner.as_ref()?;
            Some((pod.name.as_str(), (kind_of(&o.kind)?, o.name.clone())))
        })
        .collect();
    // Худшее потребление и request / limit на под: workload → контейнер → значения.
    let mut worst: BTreeMap<(WorkloadKind, String), BTreeMap<String, Use>> = BTreeMap::new();
    for ((pod, container), u) in usage {
        if let Some(w) = workload_of(pod, owner.get(pod.as_str()).cloned(), &list) {
            let e = worst
                .entry(w)
                .or_default()
                .entry(container.clone())
                .or_default();
            *e = Use {
                cpu_p95: larger(e.cpu_p95, u.cpu_p95),
                memory_max: larger(e.memory_max, u.memory_max),
                throttling_mean: larger(e.throttling_mean, u.throttling_mean),
                oom_count: None,
            };
        }
    }
    let mut amounts: HashMap<(WorkloadKind, String), BTreeMap<String, (Amounts, Amounts)>> =
        HashMap::new();
    for pod in pods
        .iter()
        .filter(|p| p.phase == "Running" || p.phase == "Pending")
    {
        if let Some(w) = owner.get(pod.name.as_str()) {
            let m = amounts.entry(w.clone()).or_default();
            for c in pod.containers.iter().filter(|c| !c.init || c.sidecar) {
                per_pod(m.entry(c.name.clone()).or_default(), &c.amounts);
            }
        }
    }
    for ((kind, name), containers) in &worst {
        let (mut throttled, mut near, mut cpu, mut memory) =
            (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        for (c, u) in containers {
            let Some((req, lim)) = amounts.get(&(*kind, name.clone())).and_then(|m| m.get(c))
            else {
                continue;
            };
            let (risks, excess) = assess(c, u, *req, *lim);
            for r in risks {
                match r {
                    Risk::Throttled { container, mean } => {
                        throttled.push(format!("{container} {:.0}%", mean * 100.0));
                    }
                    Risk::MemoryNearLimit {
                        container,
                        max,
                        limit,
                    } => {
                        near.push(format!(
                            "{container} {} / {}",
                            format_memory(max),
                            format_memory(limit)
                        ));
                    }
                    Risk::Oom { .. } => {}
                }
            }
            for Excess {
                container,
                resource,
                request,
                usage,
            } in excess
            {
                if resource == "cpu" {
                    cpu.push(format!(
                        "{container} {} / p95 {}",
                        format_cpu(request),
                        format_cpu(usage)
                    ));
                } else {
                    memory.push(format!(
                        "{container} {} / max {}",
                        format_memory(request),
                        format_memory(usage)
                    ));
                }
            }
        }
        let target = || Target::workload(&p.namespace, *kind, name);
        if !throttled.is_empty() {
            out.emit(
                "resources.cpuThrottled",
                target(),
                None,
                json!({ "containers": listed(&throttled) }),
            );
        }
        if !near.is_empty() {
            out.emit(
                "resources.memoryNearLimit",
                target(),
                None,
                json!({ "containers": listed(&near) }),
            );
        }
        if !cpu.is_empty() || !memory.is_empty() {
            out.emit(
                "resources.overProvisioned",
                target(),
                None,
                json!({ "cpu": listed(&cpu), "memory": listed(&memory) }),
            );
        }
    }
}

pub fn check(s: &ClusterSnapshot, out: &mut Findings) {
    for p in &s.projects {
        project(p, out);
    }
}

#[cfg(test)]
mod tests {
    use super::super::fixtures::{ENVS, NOW};
    use super::super::pods::fixtures::{pod, project as with_pods};
    use super::*;
    use crate::health::model::{Finding, Severity};
    use crate::health::snapshot::{PodSnap, WorkloadSnap};
    use crate::platform::PlatformKind;
    use crate::storage::profile::Environment;

    const MI: i64 = 1024 * 1024;

    fn web_pod(
        name: &str,
        cpu_req: i64,
        cpu_lim: Option<i64>,
        mem_req: i64,
        mem_lim: i64,
    ) -> PodSnap {
        let mut p = pod(name, Some(("Deployment", "web")));
        p.containers[0].amounts = ContainerAmounts {
            cpu_request_milli: Some(cpu_req),
            memory_request: Some(mem_req),
            cpu_limit_milli: cpu_lim,
            memory_limit: Some(mem_lim),
        };
        p
    }

    fn snapshot(pods: Vec<PodSnap>, usage: &[(&str, Use)]) -> ClusterSnapshot {
        let mut p = with_pods("team-a", pods);
        p.workloads = Some(vec![WorkloadSnap {
            kind: WorkloadKind::Deployment,
            name: "web".into(),
            created: None,
            desired: Some(1),
            available: Some(1),
            progressing: None,
            available_cond: None,
            containers: vec![],
        }]);
        p.usage = Some(
            usage
                .iter()
                .map(|(pod, u)| (((*pod).to_string(), "web".to_string()), *u))
                .collect(),
        );
        let mut s = ClusterSnapshot::empty(NOW, PlatformKind::Kubernetes);
        s.projects = vec![p];
        s
    }

    fn run(s: &ClusterSnapshot, env: Environment) -> Vec<Finding> {
        crate::health::rules::evaluate(s, env)
            .into_iter()
            .filter(|f| f.rule_id.starts_with("resources."))
            .collect()
    }

    fn u(cpu_p95: f64, memory_max: f64, throttling: f64) -> Use {
        Use {
            cpu_p95: Some(cpu_p95),
            memory_max: Some(memory_max),
            throttling_mean: Some(throttling),
            oom_count: None,
        }
    }

    #[test]
    fn throttled_near_limit_and_over_provisioned_workloads() {
        let s = snapshot(
            vec![web_pod(
                "web-7d9f8c6b5d-x2x4z",
                2000,
                Some(1000),
                4096 * MI,
                1024 * MI,
            )],
            &[
                ("web-7d9f8c6b5d-x2x4z", u(0.3, 950.0 * MI as f64, 0.4)),
                // Под, удалённый за сутки, — по форме имени.
                ("web-5f6d7c8b9d-q2w4x", u(0.2, 100.0 * MI as f64, 0.1)),
            ],
        );
        let f = run(&s, Environment::Prod);
        let ids: Vec<_> = f.iter().map(|f| f.rule_id.as_str()).collect();
        assert_eq!(
            ids,
            [
                "resources.cpuThrottled",
                "resources.memoryNearLimit",
                "resources.overProvisioned"
            ]
        );
        assert_eq!(
            f[0].key,
            "resources.cpuThrottled:workload:team-a:Deployment/web"
        );
        assert_eq!(f[0].params["containers"][0], "web 40%");
        assert_eq!(f[1].params["containers"][0], "web 950Mi / 1Gi");
        assert_eq!(f[2].params["cpu"][0], "web 2 / p95 300m");
        assert_eq!(f[2].params["memory"][0], "web 4Gi / max 950Mi");
    }

    #[test]
    fn severity_follows_the_environment() {
        let s = snapshot(
            vec![web_pod(
                "web-7d9f8c6b5d-x2x4z",
                2000,
                Some(1000),
                4096 * MI,
                1024 * MI,
            )],
            &[("web-7d9f8c6b5d-x2x4z", u(0.3, 950.0 * MI as f64, 0.4))],
        );
        for env in ENVS {
            let sev: Vec<_> = run(&s, env).iter().map(|f| f.severity).collect();
            let expected = match env {
                Environment::Prod | Environment::Stage => {
                    [Severity::Warning, Severity::Warning, Severity::Info]
                }
                _ => [Severity::Info, Severity::Info, Severity::Info],
            };
            assert_eq!(sev, expected, "{env:?}");
        }
    }

    #[test]
    fn nothing_without_usage_limits_or_running_pods() {
        // Без CPU limit троттлинга нет, память далеко от лимита, request по делу.
        let calm = snapshot(
            vec![web_pod(
                "web-7d9f8c6b5d-x2x4z",
                300,
                None,
                512 * MI,
                1024 * MI,
            )],
            &[("web-7d9f8c6b5d-x2x4z", u(0.25, 400.0 * MI as f64, 0.9))],
        );
        assert!(run(&calm, Environment::Prod).is_empty());
        let mut unread = calm.clone();
        unread.projects[0].usage = None;
        assert!(run(&unread, Environment::Prod).is_empty());
        // Подов нет — сравнивать не с чем.
        let gone = snapshot(
            vec![],
            &[("web-7d9f8c6b5d-x2x4z", u(0.3, 950.0 * MI as f64, 0.4))],
        );
        assert!(run(&gone, Environment::Prod).is_empty());
    }
}
