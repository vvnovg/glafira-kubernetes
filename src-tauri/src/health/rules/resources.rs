//! Правила `resources.*` (спец. §8.2): потребление контейнеров за 24 ч из источника истории
//! метрик против requests и limits работающих подов. Пороги и сопоставление подов с
//! workload'ами — те же, что у вкладки «Ресурсы» проекта (`resources::project`).

use std::collections::{BTreeMap, HashMap};

use serde_json::json;

use super::{Findings, listed};
use crate::health::model::Target;
use crate::health::snapshot::{ClusterSnapshot, ProjectSnap};
use crate::k8s::quantity::{format_cpu, format_memory};
use crate::k8s::workloads::WorkloadKind;
use crate::resources::current::Amounts;
use crate::resources::project::{
    Excess, Risk, Use, assess, is_running, larger_amounts, workload_of, workload_owner,
};

fn project(p: &ProjectSnap, out: &mut Findings) {
    let (Some(usage), Some(pods), Some(workloads)) = (&p.usage, &p.pods, &p.workloads) else {
        return;
    };
    let list: Vec<(WorkloadKind, String)> =
        workloads.iter().map(|w| (w.kind, w.name.clone())).collect();
    // Job → его CronJob: поды Job из CronJob относятся к CronJob, как во вкладке «Ресурсы».
    let cron: HashMap<&str, &str> = workloads
        .iter()
        .filter(|w| w.kind == WorkloadKind::Job)
        .filter_map(|w| Some((w.name.as_str(), w.cron.as_deref()?)))
        .collect();
    let owner: HashMap<&str, (WorkloadKind, String)> = pods
        .iter()
        .filter_map(|pod| {
            let o = pod.owner.as_ref()?;
            let w = workload_owner(o, cron.get(o.name.as_str()).copied())?;
            Some((pod.name.as_str(), w))
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
            *e = e.worst(*u);
        }
    }
    // Request и limit контейнера на под — наибольшие среди работающих подов workload'а.
    let mut amounts: HashMap<(WorkloadKind, String), BTreeMap<String, (Amounts, Amounts)>> =
        HashMap::new();
    for pod in pods.iter().filter(|p| is_running(Some(&p.phase))) {
        if let Some(w) = owner.get(pod.name.as_str()) {
            let m = amounts.entry(w.clone()).or_default();
            for c in pod.containers.iter().filter(|c| !c.init || c.sidecar) {
                let e = m.entry(c.name.clone()).or_default();
                e.0 = larger_amounts(e.0, c.amounts.requests());
                e.1 = larger_amounts(e.1, c.amounts.limits());
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
    use crate::health::snapshot::{ContainerAmounts, PodSnap, WorkloadSnap};
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

    fn workload(kind: WorkloadKind, name: &str, cron: Option<&str>) -> WorkloadSnap {
        WorkloadSnap {
            kind,
            name: name.into(),
            created: None,
            desired: Some(1),
            available: Some(1),
            progressing: None,
            available_cond: None,
            containers: vec![],
            cron: cron.map(String::from),
        }
    }

    fn snapshot(pods: Vec<PodSnap>, usage: &[(&str, Use)]) -> ClusterSnapshot {
        snapshot_of(
            pods,
            vec![workload(WorkloadKind::Deployment, "web", None)],
            usage,
        )
    }

    fn snapshot_of(
        pods: Vec<PodSnap>,
        workloads: Vec<WorkloadSnap>,
        usage: &[(&str, Use)],
    ) -> ClusterSnapshot {
        let mut p = with_pods("team-a", pods);
        p.workloads = Some(workloads);
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
                // Под, удалённый за сутки, — по форме имени: его максимум памяти выше, CPU ниже;
                // берётся худшее по каждому значению отдельно.
                ("web-5f6d7c8b9d-q2w4x", u(0.2, 980.0 * MI as f64, 0.1)),
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
        assert_eq!(f[1].params["containers"][0], "web 980Mi / 1Gi");
        assert_eq!(f[2].params["cpu"][0], "web 2 / p95 300m");
        assert_eq!(f[2].params["memory"][0], "web 4Gi / max 980Mi");
    }

    #[test]
    fn two_running_pods_take_the_larger_request_and_limit() {
        // Один под с большим request и малым limit памяти, другой — наоборот: как во вкладке
        // «Ресурсы», берётся наибольшее среди подов, в каком бы порядке они ни пришли.
        let big_request = web_pod(
            "web-7d9f8c6b5d-aaaaa",
            2000,
            Some(1000),
            4096 * MI,
            1024 * MI,
        );
        let big_limit = web_pod("web-7d9f8c6b5d-bbbbb", 500, Some(2000), 256 * MI, 2048 * MI);
        let usage = [("web-7d9f8c6b5d-aaaaa", u(0.3, 950.0 * MI as f64, 0.4))];
        for pods in [
            vec![big_request.clone(), big_limit.clone()],
            vec![big_limit, big_request],
        ] {
            let f = run(&snapshot(pods, &usage), Environment::Prod);
            let ids: Vec<_> = f.iter().map(|f| f.rule_id.as_str()).collect();
            // Limit памяти 2Gi — 950Mi до порога далеко; request 2 ядра и 4Gi — избыток.
            assert_eq!(ids, ["resources.cpuThrottled", "resources.overProvisioned"]);
            assert_eq!(f[1].params["cpu"][0], "web 2 / p95 300m");
            assert_eq!(f[1].params["memory"][0], "web 4Gi / max 950Mi");
        }
    }

    #[test]
    fn cronjob_pods_belong_to_the_cronjob_not_to_the_transient_job() {
        let run_of = |job: &str, pod: &str| {
            let mut p = web_pod(pod, 2000, Some(1000), 4096 * MI, 1024 * MI);
            p.owner = Some(crate::k8s::pods::OwnerRef {
                kind: "Job".into(),
                name: job.into(),
            });
            let s = snapshot_of(
                vec![p],
                vec![
                    workload(WorkloadKind::CronJob, "report", None),
                    workload(WorkloadKind::Job, job, Some("report")),
                ],
                &[(pod, u(0.3, 950.0 * MI as f64, 0.4))],
            );
            run(&s, Environment::Prod)
        };
        let first = run_of("report-29000", "report-29000-x2x4z");
        let keys: Vec<_> = first.iter().map(|f| f.key.as_str()).collect();
        assert_eq!(
            keys,
            [
                "resources.cpuThrottled:workload:team-a:CronJob/report",
                "resources.memoryNearLimit:workload:team-a:CronJob/report",
                "resources.overProvisioned:workload:team-a:CronJob/report",
            ]
        );
        // Следующий запуск — другой Job и другой под, а ключ тот же: замечание не пропадает
        // и не появляется заново, снятое остаётся снятым.
        let next = run_of("report-29001", "report-29001-q7w8e");
        let next_keys: Vec<_> = next.iter().map(|f| f.key.as_str()).collect();
        assert_eq!(next_keys, keys);
        // Job без CronJob остаётся собой.
        let mut p = web_pod("once-x2x4z", 2000, Some(1000), 4096 * MI, 1024 * MI);
        p.owner = Some(crate::k8s::pods::OwnerRef {
            kind: "Job".into(),
            name: "once".into(),
        });
        let s = snapshot_of(
            vec![p],
            vec![workload(WorkloadKind::Job, "once", None)],
            &[("once-x2x4z", u(0.3, 950.0 * MI as f64, 0.4))],
        );
        let f = run(&s, Environment::Prod);
        assert_eq!(f[0].key, "resources.cpuThrottled:workload:team-a:Job/once");
    }

    #[test]
    fn a_pod_counts_while_it_has_not_finished() {
        let usage = [("web-7d9f8c6b5d-x2x4z", u(0.3, 950.0 * MI as f64, 0.4))];
        let with_phase = |phase: &str| {
            let mut p = web_pod(
                "web-7d9f8c6b5d-x2x4z",
                2000,
                Some(1000),
                4096 * MI,
                1024 * MI,
            );
            p.phase = phase.into();
            run(&snapshot(vec![p], &usage), Environment::Prod).len()
        };
        // Как во вкладке «Ресурсы»: не работают только завершённые поды.
        for phase in ["Running", "Pending", "Unknown"] {
            assert_eq!(with_phase(phase), 3, "{phase}");
        }
        for phase in ["Succeeded", "Failed"] {
            assert_eq!(with_phase(phase), 0, "{phase}");
        }
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
