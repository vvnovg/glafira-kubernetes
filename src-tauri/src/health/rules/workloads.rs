//! Правила workloads и контейнеров (спец. §8.2): недоступность, застрявшая выкладка, одна
//! реплика, requests и limits, readiness-проба, тег `latest`.

use serde_json::json;

use super::pods::{by_owner, workload_kind};
use super::{Findings, GRACE_S, listed};
use crate::health::model::Target;
use crate::health::snapshot::{ClusterSnapshot, ContainerSnap, PodSnap, WorkloadSnap};
use crate::k8s::workloads::WorkloadKind;

/// Виды, которые держат реплики постоянно (у Job и CronJob — завершения).
const LONG_LIVED: [WorkloadKind; 4] = [
    WorkloadKind::Deployment,
    WorkloadKind::StatefulSet,
    WorkloadKind::DaemonSet,
    WorkloadKind::DeploymentConfig,
];

/// Виды, у которых одна реплика — риск (`workload.singleReplica`).
const REPLICATED: [WorkloadKind; 3] = [
    WorkloadKind::Deployment,
    WorkloadKind::StatefulSet,
    WorkloadKind::DeploymentConfig,
];

/// Образ без тега или с тегом `latest` и не по digest: `web`, `web:latest`, `host:5000/web`.
pub fn latest_tag(image: &str) -> bool {
    if image.contains('@') {
        return false;
    }
    let name = image.rsplit('/').next().unwrap_or(image);
    match name.rsplit_once(':') {
        Some((_, tag)) => tag == "latest",
        None => true,
    }
}

/// С какого момента workload не набирает реплик: самый ранний момент, с которого не готов ни
/// один из его неготовых подов (только `Running` и `Pending`: завершённые `Failed` и `Succeeded`,
/// например вытесненные дни назад, нехватки не начинают); без таких подов — условие `Available` =
/// `False`. Создание workload'а берётся, только если подов нет вовсе (`any_pods` — считая
/// удаляемые): при выкладке StatefulSet и DaemonSet (у них нет `Available`) один под удаляется, а
/// остальные готовы — начала нехватки ещё нет.
fn short_since(w: &WorkloadSnap, pods: &[&PodSnap], any_pods: bool) -> Option<i64> {
    let unready = pods
        .iter()
        .filter(|p| !p.ready && matches!(p.phase.as_str(), "Running" | "Pending"))
        .filter_map(|p| p.not_ready_since.or(p.created))
        .min();
    unready.or_else(|| {
        w.available_cond
            .as_ref()
            .filter(|c| c.is("False"))
            .and_then(|c| c.since)
            .or_else(|| w.created.filter(|_| !any_pods))
    })
}

/// Контейнеры, которые проверяются у работающих подов: основные и нативные sidecar'ы.
fn running_containers<'a>(pods: &[&'a PodSnap]) -> Vec<&'a ContainerSnap> {
    pods.iter()
        .filter(|p| p.phase == "Running")
        .flat_map(|p| p.containers.iter())
        .filter(|c| !c.init || c.sidecar)
        .collect()
}

fn names<'a>(cs: impl IntoIterator<Item = &'a ContainerSnap>) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for c in cs {
        if !out.contains(&c.name) {
            out.push(c.name.clone());
        }
    }
    out
}

pub fn check(s: &ClusterSnapshot, out: &mut Findings) {
    for project in &s.projects {
        let ns = project.namespace.as_str();
        let groups = project
            .pods
            .as_deref()
            .map(|p| by_owner(ns, p))
            .unwrap_or_default();
        for w in project.workloads.iter().flatten() {
            let target = || Target::workload(ns, w.kind, &w.name);
            let owned: Vec<&PodSnap> = groups.get(&target()).cloned().unwrap_or_default();
            // `by_owner` не отдаёт удаляемые поды, а «подов нет вовсе» с ними неверно: владельца
            // сопоставляем так же, как `by_owner`, но по всем подам проекта.
            let any_pods = project.pods.iter().flatten().any(|p| {
                p.owner
                    .as_ref()
                    .is_some_and(|o| workload_kind(&o.kind) == Some(w.kind) && o.name == w.name)
            });
            let (desired, available) = (w.desired.unwrap_or(0), w.available.unwrap_or(0));
            if LONG_LIVED.contains(&w.kind)
                && desired > 0
                && available < desired
                && let Some(since) = short_since(w, &owned, any_pods)
                && s.now - since >= GRACE_S
            {
                out.emit(
                    "workload.unavailable",
                    target(),
                    None,
                    json!({ "desired": desired, "available": available,
                        "minutes": (s.now - since) / 60 }),
                );
            }
            if let Some(p) = &w.progressing
                && p.is("False")
                && p.reason.as_deref() == Some("ProgressDeadlineExceeded")
            {
                out.emit(
                    "workload.rolloutStuck",
                    target(),
                    None,
                    json!({ "reason": p.reason, "message": p.message }),
                );
            }
            // Без прав на HPA одной реплике не поверить: HPA мог бы держать больше.
            if REPLICATED.contains(&w.kind)
                && w.desired == Some(1)
                && let Some(hpas) = &project.hpas
            {
                let hpa = hpas.iter().find(|h| {
                    workload_kind(&h.target_kind) == Some(w.kind) && h.target_name == w.name
                });
                match hpa {
                    Some(h) if h.min_replicas.unwrap_or(1) > 1 => {}
                    h => out.emit(
                        "workload.singleReplica",
                        target(),
                        None,
                        json!({ "hpa": h.map(|h| h.name.clone()) }),
                    ),
                }
            }
            if LONG_LIVED.contains(&w.kind) {
                let missing: Vec<String> = w
                    .containers
                    .iter()
                    .filter(|c| !c.readiness_probe)
                    .map(|c| c.name.clone())
                    .collect();
                if !missing.is_empty() {
                    out.emit(
                        "container.noReadinessProbe",
                        target(),
                        None,
                        json!({ "containers": listed(&missing) }),
                    );
                }
            }
        }
        for (target, pods) in &groups {
            let cs = running_containers(pods);
            let cpu = names(cs.iter().copied().filter(|c| !c.cpu_request));
            let memory = names(cs.iter().copied().filter(|c| !c.memory_request));
            if !cpu.is_empty() || !memory.is_empty() {
                out.emit(
                    "container.noRequests",
                    target.clone(),
                    None,
                    json!({ "cpu": listed(&cpu), "memory": listed(&memory) }),
                );
            }
            let no_limit = names(cs.iter().copied().filter(|c| !c.memory_limit));
            if !no_limit.is_empty() {
                out.emit(
                    "container.noMemoryLimit",
                    target.clone(),
                    None,
                    json!({ "containers": listed(&no_limit) }),
                );
            }
            let latest: Vec<&ContainerSnap> = pods
                .iter()
                .filter(|p| p.phase == "Running")
                .flat_map(|p| p.containers.iter())
                .filter(|c| latest_tag(&c.image))
                .collect();
            if !latest.is_empty() {
                let mut images: Vec<String> = Vec::new();
                for c in &latest {
                    if !images.contains(&c.image) {
                        images.push(c.image.clone());
                    }
                }
                out.emit(
                    "container.latestTag",
                    target.clone(),
                    None,
                    json!({ "containers": listed(&names(latest)), "images": listed(&images) }),
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::fixtures::NOW;
    use super::super::pods::fixtures::*;
    use super::*;
    use crate::health::model::{Finding, Severity};
    use crate::health::snapshot::*;
    use crate::platform::PlatformKind;
    use crate::storage::profile::Environment;

    fn run(projects: Vec<ProjectSnap>, env: Environment) -> Vec<Finding> {
        let mut s = ClusterSnapshot::empty(NOW, PlatformKind::Kubernetes);
        s.projects = projects;
        crate::health::rules::evaluate(&s, env)
    }

    fn ids(v: &[Finding]) -> Vec<String> {
        v.iter()
            .map(|f| format!("{} {}", f.rule_id, f.target.name.as_deref().unwrap_or("-")))
            .collect()
    }

    fn workload(kind: WorkloadKind, name: &str, desired: i32, available: i32) -> WorkloadSnap {
        WorkloadSnap {
            kind,
            name: name.into(),
            created: Some(NOW - 86_400),
            desired: Some(desired),
            available: Some(available),
            progressing: None,
            available_cond: None,
            containers: vec![TemplateContainer {
                name: "web".into(),
                readiness_probe: true,
            }],
        }
    }

    fn with(pods: Vec<PodSnap>, workloads: Vec<WorkloadSnap>) -> ProjectSnap {
        ProjectSnap {
            workloads: Some(workloads),
            hpas: Some(vec![]),
            ..project("team-a", pods)
        }
    }

    #[test]
    fn images_with_latest_or_no_tag() {
        for (image, latest) in [
            ("web", true),
            ("web:latest", true),
            ("registry.example.com:5000/team/web", true),
            ("registry.example.com:5000/team/web:1.4", false),
            ("web@sha256:abc", false),
            ("web:latest@sha256:abc", false),
            ("web:1", false),
        ] {
            assert_eq!(latest_tag(image), latest, "{image}");
        }
    }

    #[test]
    fn unavailable_for_five_minutes_by_the_oldest_unready_pod() {
        let unready = |name: &str, since: i64| {
            let mut p = pod(name, Some(("Deployment", "web")));
            p.ready = false;
            p.not_ready_since = Some(NOW - since);
            p
        };
        let mut no_pods = workload(WorkloadKind::StatefulSet, "db", 3, 0);
        no_pods.created = Some(NOW - 120);
        let mut quota = workload(WorkloadKind::Deployment, "api", 2, 0);
        quota.available_cond = Some(Cond {
            status: "False".into(),
            reason: Some("MinimumReplicasUnavailable".into()),
            message: None,
            since: Some(NOW - 900),
        });
        let v = run(
            vec![with(
                vec![
                    pod("web-1", Some(("Deployment", "web"))),
                    unready("web-2", GRACE_S),
                    unready("web-3", 10),
                ],
                vec![
                    workload(WorkloadKind::Deployment, "web", 3, 1),
                    no_pods,
                    quota,
                    workload(WorkloadKind::Job, "migrate", 1, 0),
                ],
            )],
            Environment::Prod,
        );
        assert_eq!(
            ids(&v),
            ["workload.unavailable api", "workload.unavailable web"]
        );
        assert_eq!(
            v[0].params["minutes"], 15,
            "подов нет: Available=False 15 минут назад"
        );
        assert_eq!(
            (
                v[1].params["desired"].as_i64(),
                v[1].params["available"].as_i64(),
                v[1].params["minutes"].as_i64()
            ),
            (Some(3), Some(1), Some(5))
        );
    }

    #[test]
    fn rolling_update_with_a_terminating_pod_is_not_unavailable() {
        let owned = |name: &str, kind: &str, owner: &str| pod(name, Some((kind, owner)));
        let mut leaving = owned("db-2", "StatefulSet", "db");
        leaving.ready = false;
        leaving.terminating = true;
        leaving.not_ready_since = Some(NOW - 600);
        let mut gone = owned("agent-1", "DaemonSet", "agent");
        gone.terminating = true;
        let v = run(
            vec![with(
                vec![
                    owned("db-0", "StatefulSet", "db"),
                    owned("db-1", "StatefulSet", "db"),
                    leaving,
                    gone,
                ],
                vec![
                    workload(WorkloadKind::StatefulSet, "db", 3, 2),
                    workload(WorkloadKind::DaemonSet, "agent", 2, 1),
                ],
            )],
            Environment::Prod,
        );
        assert!(
            ids(&v)
                .iter()
                .all(|i| !i.starts_with("workload.unavailable")),
            "поды есть, но начала нехватки нет — не считаем от создания: {:?}",
            ids(&v)
        );
    }

    #[test]
    fn finished_pods_do_not_start_the_shortage() {
        let unready = |name: &str, phase: &str, since: i64| {
            let mut p = pod(name, Some(("Deployment", "web")));
            p.ready = false;
            p.phase = phase.into();
            p.created = Some(NOW - since);
            p.not_ready_since = Some(NOW - since);
            p
        };
        let pods = |fresh: i64| {
            vec![
                pod("web-1", Some(("Deployment", "web"))),
                pod("web-2", Some(("Deployment", "web"))),
                pod("web-3", Some(("Deployment", "web"))),
                unready("web-4", "Running", fresh),
                unready("web-old", "Failed", 3 * 86_400),
                unready("web-done", "Succeeded", 2 * 86_400),
            ]
        };
        let web = || workload(WorkloadKind::Deployment, "web", 4, 3);
        let v = run(vec![with(pods(10), vec![web()])], Environment::Prod);
        assert!(ids(&v).is_empty(), "свежий неготовый под: {:?}", ids(&v));
        let v = run(vec![with(pods(400), vec![web()])], Environment::Prod);
        assert_eq!(ids(&v), ["workload.unavailable web"]);
        assert_eq!(
            v[0].params["minutes"], 6,
            "от неготового пода, а не от завершённого"
        );
    }

    #[test]
    fn workload_without_any_pods_counts_from_creation() {
        let mut old = workload(WorkloadKind::StatefulSet, "db", 3, 0);
        old.created = Some(NOW - 600);
        let mut young = workload(WorkloadKind::DaemonSet, "agent", 2, 0);
        young.created = Some(NOW - 120);
        let v = run(
            vec![with(
                vec![pod("other-1", Some(("Deployment", "other")))],
                vec![old, young],
            )],
            Environment::Prod,
        );
        assert_eq!(ids(&v), ["workload.unavailable db"]);
        assert_eq!(v[0].params["minutes"], 10);
    }

    #[test]
    fn stuck_rollout_and_single_replica_without_hpa() {
        let mut stuck = workload(WorkloadKind::Deployment, "web", 1, 1);
        stuck.progressing = Some(Cond {
            status: "False".into(),
            reason: Some("ProgressDeadlineExceeded".into()),
            message: Some("ReplicaSet \"web-7d9f\" has timed out progressing.".into()),
            since: None,
        });
        let mut project = with(
            vec![],
            vec![
                stuck,
                workload(WorkloadKind::StatefulSet, "db", 1, 1),
                workload(WorkloadKind::DaemonSet, "agent", 1, 1),
                workload(WorkloadKind::Deployment, "api", 1, 1),
            ],
        );
        project.hpas = Some(vec![
            HpaSnap {
                name: "db".into(),
                target_kind: "StatefulSet".into(),
                target_name: "db".into(),
                min_replicas: Some(1),
                max_replicas: 3,
                ..Default::default()
            },
            HpaSnap {
                name: "api".into(),
                target_kind: "Deployment".into(),
                target_name: "api".into(),
                min_replicas: Some(2),
                max_replicas: 5,
                ..Default::default()
            },
        ]);
        let v = run(vec![project.clone()], Environment::Prod);
        assert_eq!(
            ids(&v),
            [
                "workload.rolloutStuck web",
                "workload.singleReplica db",
                "workload.singleReplica web"
            ]
        );
        assert_eq!(v[1].params["hpa"], "db");
        assert!(v[2].params["hpa"].is_null());
        let stage = run(vec![project.clone()], Environment::Stage);
        assert_eq!(stage[1].severity, Severity::Info);
        assert_eq!(
            run(vec![project.clone()], Environment::Dev).len(),
            1,
            "— у прочих"
        );
        project.hpas = None;
        assert_eq!(
            run(vec![project], Environment::Prod).len(),
            1,
            "HPA не прочитаны — не судим"
        );
    }

    #[test]
    fn containers_without_requests_limits_probes_and_with_latest() {
        let mut web = pod("web-1", Some(("Deployment", "web")));
        web.containers[0].cpu_request = false;
        let mut proxy = container("proxy");
        proxy.memory_request = false;
        proxy.memory_limit = false;
        proxy.image = "envoy:latest".into();
        web.containers.push(proxy);
        let mut init = container("migrate");
        init.init = true;
        init.cpu_request = false;
        web.containers.push(init);
        let mut pending = pod("web-2", Some(("Deployment", "web")));
        pending.phase = "Pending".into();
        pending.created = Some(NOW - 60);
        pending.containers[0].memory_limit = false;
        let mut probe = workload(WorkloadKind::Deployment, "web", 2, 2);
        probe.containers.push(TemplateContainer {
            name: "proxy".into(),
            readiness_probe: false,
        });
        let mut job = workload(WorkloadKind::Job, "migrate", 1, 0);
        job.containers[0].readiness_probe = false;
        let v = run(
            vec![with(vec![web, pending], vec![probe, job])],
            Environment::Prod,
        );
        assert_eq!(
            ids(&v),
            [
                "container.noRequests web",
                "container.noMemoryLimit web",
                "container.noReadinessProbe web",
                "container.latestTag web"
            ]
        );
        assert_eq!(
            v[0].params["cpu"],
            json!(["web"]),
            "init-контейнер не считается"
        );
        assert_eq!(v[0].params["memory"], json!(["proxy"]));
        assert_eq!(
            v[1].params["containers"],
            json!(["proxy"]),
            "только работающие поды"
        );
        assert_eq!(v[2].params["containers"], json!(["proxy"]));
        assert_eq!(v[3].params["images"], json!(["envoy:latest"]));
        let dev = run(vec![with(vec![pod("x", None)], vec![])], Environment::Dev);
        assert!(dev.is_empty());
    }
}
