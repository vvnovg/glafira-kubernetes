//! Правила подов (спец. §8.2): цикл перезапусков, образ, ожидание размещения, OOMKilled, частые
//! рестарты. Находки по подам одного workload'а собираются в одну находку на workload («2 из 5
//! подов»); под без workload'а — объект своей находки.

use std::collections::BTreeMap;

use serde_json::json;

use super::{Findings, GRACE_S, listed};
use crate::health::model::{Target, TargetKind};
use crate::health::snapshot::{ClusterSnapshot, ContainerSnap, PodSnap};
use crate::k8s::workloads::WorkloadKind;

/// Причины ожидания контейнера, которые означают, что образ не скачать.
pub const IMAGE_PULL: &[&str] = &["ErrImagePull", "ImagePullBackOff", "InvalidImageName"];
/// OOMKilled считается, если случился за последние сутки.
pub const OOM_WINDOW_S: i64 = 24 * 60 * 60;
/// Частые рестарты: не меньше стольких, последний — не раньше чем час назад.
pub const FREQUENT_RESTARTS: i32 = 5;
pub const RECENT_RESTART_S: i64 = 60 * 60;

/// Вид workload'а по виду владельца пода; ReplicaSet без Deployment и подобные — `None`.
pub fn workload_kind(kind: &str) -> Option<WorkloadKind> {
    WorkloadKind::ALL
        .into_iter()
        .find(|k| format!("{k:?}") == kind)
}

/// Поды проекта по объекту находки: workload-владелец или сам под (без владельца-workload'а).
/// Удаляемые поды не проверяются.
pub fn by_owner<'a>(namespace: &str, pods: &'a [PodSnap]) -> BTreeMap<Target, Vec<&'a PodSnap>> {
    let mut out: BTreeMap<Target, Vec<&PodSnap>> = BTreeMap::new();
    for p in pods.iter().filter(|p| !p.terminating) {
        let target = match &p.owner {
            Some(o) => match workload_kind(&o.kind) {
                Some(k) => Target::workload(namespace, k, &o.name),
                None => Target::object(TargetKind::Pod, namespace, &p.name),
            },
            None => Target::object(TargetKind::Pod, namespace, &p.name),
        };
        out.entry(target).or_default().push(p);
    }
    out
}

fn image_pull(c: &ContainerSnap) -> bool {
    c.waiting_reason
        .as_deref()
        .is_some_and(|r| IMAGE_PULL.contains(&r))
}

fn crash_loop(p: &PodSnap) -> bool {
    p.containers.iter().any(|c| c.crash_loop)
}

/// Контейнер завершён по памяти (сейчас или в прошлый раз) за последние сутки.
fn oom_recently(c: &ContainerSnap, now: i64) -> bool {
    [&c.exit, &c.last_exit].into_iter().flatten().any(|e| {
        e.reason.as_deref() == Some("OOMKilled")
            && e.finished.is_none_or(|t| now - t <= OOM_WINDOW_S)
    })
}

/// Имена без повторов в порядке появления.
fn unique(names: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for n in names {
        if !out.contains(&n) {
            out.push(n);
        }
    }
    out
}

/// Общие параметры находки по подам: какие поды, сколько из скольких.
fn pod_params(affected: &[&PodSnap], total: usize) -> serde_json::Map<String, serde_json::Value> {
    let names: Vec<String> = affected.iter().map(|p| p.name.clone()).collect();
    let mut m = serde_json::Map::new();
    m.insert("pods".into(), json!(listed(&names)));
    m.insert("count".into(), json!(affected.len()));
    m.insert("total".into(), json!(total));
    m
}

pub fn check(s: &ClusterSnapshot, out: &mut Findings) {
    for project in &s.projects {
        let Some(pods) = &project.pods else {
            continue;
        };
        for (target, group) in by_owner(&project.namespace, pods) {
            let total = group.len();
            let emit = |out: &mut Findings,
                        rule: &'static str,
                        affected: &[&PodSnap],
                        extra: serde_json::Value| {
                if affected.is_empty() {
                    return;
                }
                let mut params = pod_params(affected, total);
                if let serde_json::Value::Object(m) = extra {
                    params.extend(m);
                }
                out.emit(
                    rule,
                    target.clone(),
                    None,
                    serde_json::Value::Object(params),
                );
            };

            let crashing: Vec<&PodSnap> = group.iter().copied().filter(|p| crash_loop(p)).collect();
            let looping: Vec<&ContainerSnap> = crashing
                .iter()
                .flat_map(|p| p.containers.iter().filter(|c| c.crash_loop))
                .collect();
            let first_exit = looping
                .iter()
                .find_map(|c| c.last_exit.as_ref().or(c.exit.as_ref()));
            emit(
                out,
                "pod.crashLoop",
                &crashing,
                json!({
                    "containers": listed(&unique(looping.iter().map(|c| c.name.clone()))),
                    "reason": first_exit.and_then(|e| e.reason.clone()),
                    "exitCode": first_exit.map(|e| e.exit_code),
                    "restarts": looping.iter().map(|c| c.restarts).max(),
                }),
            );

            let pulling: Vec<&PodSnap> = group
                .iter()
                .copied()
                .filter(|p| p.containers.iter().any(image_pull))
                .collect();
            let first = pulling
                .iter()
                .flat_map(|p| p.containers.iter())
                .find(|c| image_pull(c));
            emit(
                out,
                "pod.imagePull",
                &pulling,
                json!({
                    "image": first.map(|c| c.image.clone()),
                    "reason": first.and_then(|c| c.waiting_reason.clone()),
                    "message": first.and_then(|c| c.waiting_message.clone()),
                }),
            );

            // Под, который ждёт образ или перезапускается в цикле, — уже в своей находке.
            let pending: Vec<&PodSnap> = group
                .iter()
                .copied()
                .filter(|p| {
                    p.phase == "Pending"
                        && p.created.is_some_and(|c| s.now - c >= GRACE_S)
                        && !crash_loop(p)
                        && !p.containers.iter().any(image_pull)
                })
                .collect();
            let why = pending.iter().find_map(|p| match &p.scheduled {
                Some(c) if c.is("False") => Some((c.reason.clone(), c.message.clone())),
                _ => p
                    .containers
                    .iter()
                    .find_map(|c| c.waiting_reason.clone())
                    .map(|r| (Some(r), None)),
            });
            emit(
                out,
                "pod.pending",
                &pending,
                json!({
                    "minutes": pending.iter().filter_map(|p| p.created).map(|c| (s.now - c) / 60).max(),
                    "reason": why.as_ref().and_then(|w| w.0.clone()),
                    "message": why.as_ref().and_then(|w| w.1.clone()),
                }),
            );

            let oom: Vec<&PodSnap> = group
                .iter()
                .copied()
                .filter(|p| p.containers.iter().any(|c| oom_recently(c, s.now)))
                .collect();
            emit(
                out,
                "pod.oomKilled",
                &oom,
                json!({
                    "containers": listed(&unique(
                        oom.iter()
                            .flat_map(|p| p.containers.iter())
                            .filter(|c| oom_recently(c, s.now))
                            .map(|c| c.name.clone())
                    )),
                }),
            );

            let last_restart = |p: &PodSnap| {
                p.containers
                    .iter()
                    .filter_map(|c| c.last_exit.as_ref().and_then(|e| e.finished))
                    .max()
            };
            let restarts = |p: &PodSnap| p.containers.iter().map(|c| c.restarts).sum::<i32>();
            let frequent: Vec<&PodSnap> = group
                .iter()
                .copied()
                .filter(|p| {
                    !crash_loop(p)
                        && restarts(p) >= FREQUENT_RESTARTS
                        && last_restart(p).is_some_and(|t| s.now - t < RECENT_RESTART_S)
                })
                .collect();
            emit(
                out,
                "pod.restartsFrequent",
                &frequent,
                json!({
                    "restarts": frequent.iter().map(|p| restarts(p)).max(),
                    "lastRestartMinutes": frequent
                        .iter()
                        .filter_map(|p| last_restart(p))
                        .map(|t| (s.now - t) / 60)
                        .min(),
                }),
            );
        }
    }
}

#[cfg(test)]
pub(crate) mod fixtures {
    use super::super::fixtures::NOW;
    use crate::health::snapshot::*;
    use crate::k8s::pods::OwnerRef;

    pub fn container(name: &str) -> ContainerSnap {
        ContainerSnap {
            name: name.into(),
            image: "registry.example.com/web:1.4".into(),
            cpu_request: true,
            memory_request: true,
            memory_limit: true,
            ..Default::default()
        }
    }

    /// Работающий готовый под Deployment `owner` с одним контейнером `web`.
    pub fn pod(name: &str, owner: Option<(&str, &str)>) -> PodSnap {
        PodSnap {
            name: name.into(),
            phase: "Running".into(),
            created: Some(NOW - 3600),
            owner: owner.map(|(kind, name)| OwnerRef {
                kind: kind.into(),
                name: name.into(),
            }),
            node: Some("w1".into()),
            ready: true,
            containers: vec![container("web")],
            ..Default::default()
        }
    }

    pub fn project(ns: &str, pods: Vec<PodSnap>) -> ProjectSnap {
        ProjectSnap {
            namespace: ns.into(),
            pods: Some(pods),
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::fixtures::{ENVS, NOW};
    use super::fixtures::*;
    use super::*;
    use crate::health::model::{Finding, Severity};
    use crate::health::snapshot::*;
    use crate::platform::PlatformKind;
    use crate::storage::profile::Environment;

    const WEB: Option<(&str, &str)> = Some(("Deployment", "web"));

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

    fn crashing(name: &str, owner: Option<(&str, &str)>) -> PodSnap {
        let mut p = pod(name, owner);
        p.ready = false;
        p.containers[0].crash_loop = true;
        p.containers[0].restarts = 7;
        p.containers[0].last_exit = Some(Exit {
            reason: Some("Error".into()),
            exit_code: 1,
            finished: Some(NOW - 30),
        });
        p
    }

    #[test]
    fn pods_of_one_workload_make_one_finding() {
        let pods = vec![
            crashing("web-1", WEB),
            crashing("web-2", WEB),
            pod("web-3", WEB),
            crashing("solo", None),
            crashing("orphan-x", Some(("ReplicaSet", "orphan"))),
        ];
        let v = run(vec![project("team-a", pods)], Environment::Prod);
        assert_eq!(
            ids(&v),
            [
                "pod.crashLoop web",
                "pod.crashLoop orphan-x",
                "pod.crashLoop solo"
            ],
            "под ReplicaSet без Deployment и под без владельца — каждый сам по себе"
        );
        let f = &v[0];
        assert_eq!(f.target.workload_kind, Some(WorkloadKind::Deployment));
        assert_eq!(f.key, "pod.crashLoop:workload:team-a:Deployment/web");
        assert_eq!(
            (f.params["count"].as_u64(), f.params["total"].as_u64()),
            (Some(2), Some(3))
        );
        assert_eq!(f.params["pods"], json!(["web-1", "web-2"]));
        assert_eq!(f.params["containers"], json!(["web"]));
        assert_eq!(
            (
                f.params["reason"].as_str(),
                f.params["exitCode"].as_i64(),
                f.params["restarts"].as_i64()
            ),
            (Some("Error"), Some(1), Some(7))
        );
        assert_eq!(v[1].target.kind, TargetKind::Pod);
        for (env, sev) in ENVS.into_iter().zip([
            Severity::Critical,
            Severity::Warning,
            Severity::Warning,
            Severity::Warning,
            Severity::Warning,
        ]) {
            assert_eq!(
                run(vec![project("a", vec![crashing("x", None)])], env)[0].severity,
                sev
            );
        }
    }

    #[test]
    fn image_pull_and_pending_after_five_minutes() {
        let mut pull = pod("pull-1", Some(("Deployment", "pull")));
        pull.phase = "Pending".into();
        pull.containers[0].waiting_reason = Some("ImagePullBackOff".into());
        pull.containers[0].waiting_message = Some("Back-off pulling image".into());
        pull.containers[0].image = "registry.invalid/none:1".into();
        let mut pending = pod("pending-1", Some(("Deployment", "pending")));
        pending.phase = "Pending".into();
        pending.created = Some(NOW - GRACE_S);
        pending.scheduled = Some(Cond {
            status: "False".into(),
            reason: Some("Unschedulable".into()),
            message: Some("0/3 nodes are available".into()),
            since: None,
        });
        let mut young = pod("young-1", Some(("Deployment", "young")));
        young.phase = "Pending".into();
        young.created = Some(NOW - GRACE_S + 1);
        let mut creating = pod("db-0", Some(("StatefulSet", "db")));
        creating.phase = "Pending".into();
        creating.created = Some(NOW - 600);
        creating.containers[0].waiting_reason = Some("ContainerCreating".into());
        let v = run(
            vec![project("team-a", vec![pull, pending, young, creating])],
            Environment::Prod,
        );
        assert_eq!(
            ids(&v),
            [
                "pod.imagePull pull",
                "pod.pending db",
                "pod.pending pending"
            ]
        );
        assert_eq!(v[0].params["image"], "registry.invalid/none:1");
        assert_eq!(v[0].params["reason"], "ImagePullBackOff");
        assert_eq!(v[1].params["reason"], "ContainerCreating");
        assert_eq!(v[1].params["minutes"], 10);
        assert_eq!(v[2].params["reason"], "Unschedulable");
        assert_eq!(v[2].params["message"], "0/3 nodes are available");
        assert_eq!(v[2].params["minutes"], 5, "ровно 5 минут — уже находка");
    }

    #[test]
    fn oom_in_the_last_day_and_frequent_restarts_within_the_hour() {
        let oom = |name: &str, ago: i64| {
            let mut p = pod(name, Some(("StatefulSet", "db")));
            p.containers[0].restarts = 1;
            p.containers[0].last_exit = Some(Exit {
                reason: Some("OOMKilled".into()),
                exit_code: 137,
                finished: Some(NOW - ago),
            });
            p
        };
        let restarting = |name: &str, restarts: i32, ago: i64| {
            let mut p = pod(name, Some(("Deployment", "api")));
            p.containers[0].restarts = restarts;
            p.containers[0].last_exit = Some(Exit {
                reason: Some("Error".into()),
                exit_code: 2,
                finished: Some(NOW - ago),
            });
            p
        };
        let v = run(
            vec![project(
                "team-a",
                vec![
                    oom("db-0", OOM_WINDOW_S),
                    oom("db-1", OOM_WINDOW_S + 1),
                    restarting("api-1", 5, RECENT_RESTART_S - 1),
                    restarting("api-2", 9, RECENT_RESTART_S),
                    restarting("api-3", 4, 10),
                ],
            )],
            Environment::Prod,
        );
        assert_eq!(ids(&v), ["pod.oomKilled db", "pod.restartsFrequent api"]);
        assert_eq!(v[0].params["pods"], json!(["db-0"]), "старше суток — нет");
        assert_eq!(v[0].params["containers"], json!(["web"]));
        assert_eq!(v[1].params["pods"], json!(["api-1"]));
        assert_eq!(v[1].params["restarts"], 5);
        assert_eq!(v[1].params["lastRestartMinutes"], 59);
        assert_eq!(
            run(vec![project("a", vec![oom("db-0", 60)])], Environment::Dev)[0].severity,
            Severity::Info
        );
    }

    #[test]
    fn crash_loop_is_not_also_frequent_restarts_and_terminating_pods_are_skipped() {
        let mut gone = crashing("web-9", WEB);
        gone.terminating = true;
        let v = run(
            vec![project("team-a", vec![crashing("web-1", WEB), gone])],
            Environment::Prod,
        );
        assert_eq!(ids(&v), ["pod.crashLoop web"]);
        assert_eq!(v[0].params["total"], 1);
    }
}
