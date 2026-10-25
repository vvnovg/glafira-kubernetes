//! Поды проекта и workload'а (спец. §6.3, §6.5): статус, как его показывает `kubectl get pods`,
//! проблема (CrashLoopBackOff, ImagePullBackOff, OOMKilled, Pending, Failed), готовность,
//! рестарты, владелец; подробности — состояние каждого контейнера.

use std::collections::{BTreeMap, BTreeSet};

use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{ContainerState, ContainerStatus, Pod};
use kube::api::ListParams;
use serde::{Deserialize, Serialize};

use crate::error::AppResult;
use crate::k8s::containers::{ContainerSpecView, container_view};
use crate::k8s::read::{Listed, ReadClient, by_labels};
use crate::k8s::view::{ConditionView, conditions_of, created, name, seconds};
use crate::k8s::workloads::{WorkloadKind, pod_selector};

/// Причины ожидания контейнера, которые означают, что образ не скачать.
const IMAGE_PULL: &[&str] = &["ErrImagePull", "ImagePullBackOff", "InvalidImageName"];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum PodProblem {
    ImagePull,
    OomKilled,
    CrashLoop,
    Pending,
    Failed,
}

/// Владелец объекта: workload, если его можно узнать, иначе — прямой владелец.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OwnerRef {
    pub kind: String,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PodRow {
    pub name: String,
    pub phase: String,
    /// Статус, как в `kubectl get pods`: `Running`, `CrashLoopBackOff`, `Init:1/2`, `Completed`…
    pub status: String,
    pub problem: Option<PodProblem>,
    pub ready: i32,
    pub containers: i32,
    pub restarts: i32,
    /// Когда контейнер завершился в последний раз (перед рестартом).
    pub last_restart: Option<i64>,
    pub node: Option<String>,
    pub created: Option<i64>,
    pub qos: Option<String>,
    pub owner: Option<OwnerRef>,
}

fn statuses(p: &Pod, init: bool) -> &[ContainerStatus] {
    let s = p.status.as_ref();
    let list = if init {
        s.and_then(|s| s.init_container_statuses.as_deref())
    } else {
        s.and_then(|s| s.container_statuses.as_deref())
    };
    list.unwrap_or(&[])
}

fn exit_text(t: &k8s_openapi::api::core::v1::ContainerStateTerminated) -> String {
    match t.signal {
        Some(s) if s != 0 => format!("Signal:{s}"),
        _ => format!("ExitCode:{}", t.exit_code),
    }
}

/// Статус пода по правилам `kubectl get pods` (printPod).
pub fn pod_status(p: &Pod) -> String {
    let status = p.status.as_ref();
    let mut reason = status
        .and_then(|s| s.reason.clone())
        .filter(|r| !r.is_empty())
        .or_else(|| status.and_then(|s| s.phase.clone()))
        .unwrap_or_else(|| "Unknown".into());
    let init_total = p
        .spec
        .as_ref()
        .and_then(|s| s.init_containers.as_ref())
        .map_or(0, Vec::len);
    let mut initializing = false;
    for (i, c) in statuses(p, true).iter().enumerate() {
        let state = c.state.as_ref();
        if let Some(t) = state.and_then(|s| s.terminated.as_ref()) {
            if t.exit_code == 0 {
                continue;
            }
            reason = match t.reason.as_deref().filter(|r| !r.is_empty()) {
                Some(r) => format!("Init:{r}"),
                None => format!("Init:{}", exit_text(t)),
            };
        } else if let Some(r) = state
            .and_then(|s| s.waiting.as_ref())
            .and_then(|w| w.reason.as_deref())
            .filter(|r| !r.is_empty() && *r != "PodInitializing")
        {
            reason = format!("Init:{r}");
        } else {
            reason = format!("Init:{i}/{init_total}");
        }
        initializing = true;
        break;
    }
    if !initializing {
        let mut running = false;
        for c in statuses(p, false).iter().rev() {
            let state = c.state.as_ref();
            if let Some(r) = state
                .and_then(|s| s.waiting.as_ref())
                .and_then(|w| w.reason.clone())
                .filter(|r| !r.is_empty())
            {
                reason = r;
            } else if let Some(t) = state.and_then(|s| s.terminated.as_ref()) {
                reason = t
                    .reason
                    .clone()
                    .filter(|r| !r.is_empty())
                    .unwrap_or_else(|| exit_text(t));
            } else if c.ready && state.and_then(|s| s.running.as_ref()).is_some() {
                running = true;
            }
        }
        if reason == "Completed" && running {
            let ready = status
                .and_then(|s| s.conditions.as_ref())
                .is_some_and(|cs| cs.iter().any(|c| c.type_ == "Ready" && c.status == "True"));
            reason = if ready { "Running" } else { "NotReady" }.into();
        }
    }
    if p.metadata.deletion_timestamp.is_some() {
        reason = if status.and_then(|s| s.reason.as_deref()) == Some("NodeLost") {
            "Unknown".into()
        } else {
            "Terminating".into()
        };
    }
    reason
}

fn waiting_reason(c: &ContainerStatus) -> Option<&str> {
    c.state
        .as_ref()
        .and_then(|s| s.waiting.as_ref())
        .and_then(|w| w.reason.as_deref())
}

fn oom(state: Option<&ContainerState>) -> bool {
    state
        .and_then(|s| s.terminated.as_ref())
        .is_some_and(|t| t.reason.as_deref() == Some("OOMKilled"))
}

/// Проблема пода — то, что видно сейчас: образ не скачивается, контейнер завершён по памяти
/// (сейчас или в последний раз), перезапускается в цикле, под ждёт размещения или упал.
pub fn pod_problem(p: &Pod) -> Option<PodProblem> {
    let all: Vec<&ContainerStatus> = statuses(p, true).iter().chain(statuses(p, false)).collect();
    let phase = p.status.as_ref().and_then(|s| s.phase.as_deref());
    if all
        .iter()
        .any(|c| waiting_reason(c).is_some_and(|r| IMAGE_PULL.contains(&r)))
    {
        Some(PodProblem::ImagePull)
    } else if all
        .iter()
        .any(|c| oom(c.state.as_ref()) || oom(c.last_state.as_ref()))
    {
        Some(PodProblem::OomKilled)
    } else if all
        .iter()
        .any(|c| waiting_reason(c) == Some("CrashLoopBackOff"))
    {
        Some(PodProblem::CrashLoop)
    } else if phase == Some("Pending") {
        Some(PodProblem::Pending)
    } else if phase == Some("Failed") {
        Some(PodProblem::Failed)
    } else {
        None
    }
}

/// Владелец-workload: ReplicaSet с `pod-template-hash` — это Deployment, ReplicationController
/// с меткой DeploymentConfig — DeploymentConfig; иначе — прямой владелец-контроллер.
pub fn owner_of(p: &Pod) -> Option<OwnerRef> {
    let owner = p
        .metadata
        .owner_references
        .iter()
        .flatten()
        .find(|o| o.controller == Some(true))?;
    let labels = p.metadata.labels.clone().unwrap_or_default();
    let workload = |kind: &str, name: &str| {
        Some(OwnerRef {
            kind: kind.into(),
            name: name.into(),
        })
    };
    match owner.kind.as_str() {
        "ReplicaSet" => match labels.get("pod-template-hash") {
            Some(hash) if owner.name.ends_with(&format!("-{hash}")) => workload(
                "Deployment",
                &owner.name[..owner.name.len() - hash.len() - 1],
            ),
            _ => workload("ReplicaSet", &owner.name),
        },
        "ReplicationController" => match labels
            .get("openshift.io/deployment-config.name")
            .or_else(|| labels.get("deploymentconfig"))
        {
            Some(dc) => workload("DeploymentConfig", dc),
            None => workload("ReplicationController", &owner.name),
        },
        kind => workload(kind, &owner.name),
    }
}

pub fn pod_row(p: &Pod) -> PodRow {
    let status = p.status.as_ref();
    let main = statuses(p, false);
    let last_restart = main
        .iter()
        .filter_map(|c| {
            c.last_state
                .as_ref()
                .and_then(|s| s.terminated.as_ref())
                .and_then(|t| t.finished_at.as_ref())
                .map(seconds)
        })
        .max();
    PodRow {
        name: name(&p.metadata),
        phase: status
            .and_then(|s| s.phase.clone())
            .unwrap_or_else(|| "Unknown".into()),
        status: pod_status(p),
        problem: pod_problem(p),
        ready: main.iter().filter(|c| c.ready).count() as i32,
        containers: p.spec.as_ref().map_or(0, |s| s.containers.len() as i32),
        restarts: main.iter().map(|c| c.restart_count).sum(),
        last_restart,
        node: p.spec.as_ref().and_then(|s| s.node_name.clone()),
        created: created(&p.metadata),
        qos: status.and_then(|s| s.qos_class.clone()),
        owner: owner_of(p),
    }
}

/// Workload, поды которого нужны.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadRef {
    pub kind: WorkloadKind,
    pub name: String,
}

/// Поды проекта или workload'а: по селектору workload'а, а у CronJob — поды его Job.
pub async fn list_pods(
    r: &ReadClient,
    namespace: &str,
    workload: Option<&WorkloadRef>,
) -> AppResult<Listed<PodRow>> {
    let ns = Some(namespace);
    match workload {
        None => Ok(r
            .list::<Pod>(ns, &ListParams::default())
            .await?
            .map(|p| pod_row(&p))),
        Some(w) if w.kind == WorkloadKind::CronJob => {
            let jobs: BTreeSet<String> = r
                .list::<Job>(ns, &ListParams::default())
                .await?
                .items
                .into_iter()
                .filter(|j| {
                    j.metadata
                        .owner_references
                        .iter()
                        .flatten()
                        .any(|o| o.kind == "CronJob" && o.name == w.name)
                })
                .filter_map(|j| j.metadata.name)
                .collect();
            let pods = r.list::<Pod>(ns, &ListParams::default()).await?;
            let rows: Vec<PodRow> = pods
                .items
                .iter()
                .map(pod_row)
                .filter(|p| {
                    p.owner
                        .as_ref()
                        .is_some_and(|o| o.kind == "Job" && jobs.contains(&o.name))
                })
                .collect();
            Ok(Listed {
                items: rows,
                truncated: pods.truncated,
            })
        }
        Some(w) => {
            let selector = pod_selector(r, namespace, w.kind, &w.name).await?;
            Ok(r.list::<Pod>(ns, &by_labels(selector.as_deref()))
                .await?
                .map(|p| pod_row(&p)))
        }
    }
}

/// Состояние контейнера: работает, ждёт или завершён — с причиной, кодом выхода и временем.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StateView {
    pub state: ContainerPhase,
    pub reason: Option<String>,
    pub message: Option<String>,
    pub exit_code: Option<i32>,
    pub started: Option<i64>,
    pub finished: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ContainerPhase {
    Running,
    Waiting,
    Terminated,
}

fn state_view(s: &ContainerState) -> Option<StateView> {
    if let Some(r) = &s.running {
        Some(StateView {
            state: ContainerPhase::Running,
            reason: None,
            message: None,
            exit_code: None,
            started: r.started_at.as_ref().map(seconds),
            finished: None,
        })
    } else if let Some(w) = &s.waiting {
        Some(StateView {
            state: ContainerPhase::Waiting,
            reason: w.reason.clone(),
            message: w.message.clone(),
            exit_code: None,
            started: None,
            finished: None,
        })
    } else {
        s.terminated.as_ref().map(|t| StateView {
            state: ContainerPhase::Terminated,
            reason: t.reason.clone(),
            message: t.message.clone(),
            exit_code: Some(t.exit_code),
            started: t.started_at.as_ref().map(seconds),
            finished: t.finished_at.as_ref().map(seconds),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerStatusView {
    #[serde(flatten)]
    pub spec: ContainerSpecView,
    /// Образ с digest, который запущен на самом деле.
    pub image_id: Option<String>,
    pub state: Option<StateView>,
    pub last_state: Option<StateView>,
    pub ready: bool,
    pub restarts: i32,
}

/// Том пода: имя, вид (`configMap`, `secret`, `persistentVolumeClaim`, `emptyDir`…) и источник.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeView {
    pub name: String,
    pub kind: String,
    pub source: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PodDetails {
    pub pod: PodRow,
    pub conditions: Vec<ConditionView>,
    pub pod_ip: Option<String>,
    pub host_ip: Option<String>,
    pub service_account: Option<String>,
    pub started: Option<i64>,
    pub containers: Vec<ContainerStatusView>,
    pub labels: BTreeMap<String, String>,
    pub annotations: BTreeMap<String, String>,
    pub volumes: Vec<VolumeView>,
}

/// Вид тома и его источник — первое заданное поле `volumes[]` кроме имени.
fn volume_view(v: &serde_json::Value) -> Option<VolumeView> {
    let obj = v.as_object()?;
    let name = obj.get("name")?.as_str()?.to_string();
    let (kind, src) = obj.iter().find(|(k, _)| *k != "name")?;
    let source = ["claimName", "name", "secretName", "path", "medium"]
        .iter()
        .find_map(|k| src[*k].as_str().filter(|s| !s.is_empty()).map(String::from));
    Some(VolumeView {
        name,
        kind: kind.clone(),
        source,
    })
}

pub fn pod_details(p: &Pod) -> PodDetails {
    let spec = p.spec.as_ref();
    let status = p.status.as_ref();
    let by_name = |init: bool| -> BTreeMap<String, ContainerStatus> {
        statuses(p, init)
            .iter()
            .map(|c| (c.name.clone(), c.clone()))
            .collect()
    };
    let (init_status, main_status) = (by_name(true), by_name(false));
    let view = |c: &k8s_openapi::api::core::v1::Container, init: bool| {
        let st = if init {
            init_status.get(&c.name)
        } else {
            main_status.get(&c.name)
        };
        ContainerStatusView {
            spec: container_view(c, init),
            image_id: st.map(|s| s.image_id.clone()).filter(|i| !i.is_empty()),
            state: st.and_then(|s| s.state.as_ref()).and_then(state_view),
            last_state: st.and_then(|s| s.last_state.as_ref()).and_then(state_view),
            ready: st.is_some_and(|s| s.ready),
            restarts: st.map_or(0, |s| s.restart_count),
        }
    };
    let containers = spec
        .map(|s| {
            s.init_containers
                .iter()
                .flatten()
                .map(|c| view(c, true))
                .chain(s.containers.iter().map(|c| view(c, false)))
                .collect()
        })
        .unwrap_or_default();
    let mut annotations = p.metadata.annotations.clone().unwrap_or_default();
    annotations.remove("kubectl.kubernetes.io/last-applied-configuration");
    PodDetails {
        pod: pod_row(p),
        conditions: conditions_of(&status.and_then(|s| s.conditions.clone())),
        pod_ip: status.and_then(|s| s.pod_ip.clone()),
        host_ip: status.and_then(|s| s.host_ip.clone()),
        service_account: spec.and_then(|s| s.service_account_name.clone()),
        started: status.and_then(|s| s.start_time.as_ref()).map(seconds),
        containers,
        labels: p.metadata.labels.clone().unwrap_or_default(),
        annotations,
        volumes: spec
            .and_then(|s| s.volumes.as_ref())
            .map(|vs| {
                vs.iter()
                    .filter_map(|v| serde_json::to_value(v).ok())
                    .filter_map(|v| volume_view(&v))
                    .collect()
            })
            .unwrap_or_default(),
    }
}

pub async fn get_pod(r: &ReadClient, namespace: &str, pod: &str) -> AppResult<PodDetails> {
    Ok(pod_details(&r.get::<Pod>(namespace, pod).await?))
}

#[cfg(test)]
pub(crate) mod fixtures {
    /// Под с одним контейнером в состоянии `state` (JSON `ContainerState`).
    pub fn pod(name: &str, phase: &str, state: serde_json::Value) -> serde_json::Value {
        serde_json::json!({"apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": name, "namespace": "team-a", "creationTimestamp": "2026-10-25T10:00:00Z",
                "labels": {"app": "web", "pod-template-hash": "7d9f"},
                "ownerReferences": [{"apiVersion": "apps/v1", "kind": "ReplicaSet", "name": "web-7d9f",
                    "uid": "rs", "controller": true}]},
            "spec": {"nodeName": "w1", "containers": [{"name": "web", "image": "web:1"}]},
            "status": {"phase": phase, "qosClass": "Burstable",
                "containerStatuses": [{"name": "web", "image": "web:1", "imageID": "web@sha256:1",
                    "ready": phase == "Running" && state.get("running").is_some(),
                    "restartCount": 0, "state": state}]}})
    }

    pub fn running(name: &str) -> serde_json::Value {
        pod(
            name,
            "Running",
            serde_json::json!({"running": {"startedAt": "2026-10-25T10:00:00Z"}}),
        )
    }

    pub fn crash_looping(name: &str) -> serde_json::Value {
        let mut p = pod(
            name,
            "Running",
            serde_json::json!({"waiting": {"reason": "CrashLoopBackOff",
            "message": "back-off 5m0s restarting failed container"}}),
        );
        p["status"]["containerStatuses"][0]["restartCount"] = 7.into();
        p["status"]["containerStatuses"][0]["lastState"] = serde_json::json!({"terminated": {
            "exitCode": 1, "reason": "Error", "finishedAt": "2026-10-25T10:05:00Z"}});
        p
    }
}

#[cfg(test)]
mod tests {
    use super::fixtures::*;
    use super::*;
    use crate::k8s::testing::mock_client;
    use std::time::Duration;

    fn from(v: serde_json::Value) -> Pod {
        serde_json::from_value(v).unwrap()
    }

    #[test]
    fn status_and_problem_like_kubectl() {
        let cases = [
            (running("a"), "Running", None),
            (
                crash_looping("b"),
                "CrashLoopBackOff",
                Some(PodProblem::CrashLoop),
            ),
            (
                pod(
                    "c",
                    "Pending",
                    serde_json::json!({"waiting": {"reason": "ImagePullBackOff"}}),
                ),
                "ImagePullBackOff",
                Some(PodProblem::ImagePull),
            ),
            (
                pod(
                    "d",
                    "Running",
                    serde_json::json!({"terminated": {"exitCode": 137, "reason": "OOMKilled"}}),
                ),
                "OOMKilled",
                Some(PodProblem::OomKilled),
            ),
            (
                pod(
                    "e",
                    "Succeeded",
                    serde_json::json!({"terminated": {"exitCode": 0, "reason": "Completed"}}),
                ),
                "Completed",
                None,
            ),
            (
                pod(
                    "f",
                    "Failed",
                    serde_json::json!({"terminated": {"exitCode": 2}}),
                ),
                "ExitCode:2",
                Some(PodProblem::Failed),
            ),
        ];
        for (p, status, problem) in cases {
            let p = from(p);
            assert_eq!(pod_status(&p), status, "{:?}", p.metadata.name);
            assert_eq!(pod_problem(&p), problem, "{:?}", p.metadata.name);
        }
        let mut pending = from(pod("g", "Pending", serde_json::json!({})));
        pending.status.as_mut().unwrap().container_statuses = None;
        assert_eq!(pod_status(&pending), "Pending");
        assert_eq!(pod_problem(&pending), Some(PodProblem::Pending));
        let mut deleting = from(running("h"));
        deleting.metadata.deletion_timestamp =
            Some(k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(
                "2026-10-25T10:00:00Z".parse().unwrap(),
            ));
        assert_eq!(pod_status(&deleting), "Terminating");
    }

    #[test]
    fn oom_in_the_last_state_is_a_problem_even_when_running_again() {
        let mut p = running("a");
        p["status"]["containerStatuses"][0]["lastState"] =
            serde_json::json!({"terminated": {"exitCode": 137, "reason": "OOMKilled"}});
        assert_eq!(pod_problem(&from(p)), Some(PodProblem::OomKilled));
    }

    #[test]
    fn init_containers_like_kubectl() {
        let mut p = running("a");
        p["spec"]["initContainers"] = serde_json::json!([{"name": "i1"}, {"name": "i2"}]);
        p["status"]["phase"] = "Pending".into();
        p["status"]["initContainerStatuses"] = serde_json::json!([
            {"name": "i1", "image": "x", "imageID": "", "ready": true, "restartCount": 0,
                "state": {"terminated": {"exitCode": 0}}},
            {"name": "i2", "image": "x", "imageID": "", "ready": false, "restartCount": 0,
                "state": {"running": {}}}
        ]);
        assert_eq!(pod_status(&from(p.clone())), "Init:1/2");
        p["status"]["initContainerStatuses"][1]["state"] =
            serde_json::json!({"waiting": {"reason": "CrashLoopBackOff"}});
        let pod = from(p);
        assert_eq!(pod_status(&pod), "Init:CrashLoopBackOff");
        assert_eq!(pod_problem(&pod), Some(PodProblem::CrashLoop));
    }

    #[test]
    fn owners_are_workloads_when_known() {
        assert_eq!(
            owner_of(&from(running("web-7d9f-x"))),
            Some(OwnerRef {
                kind: "Deployment".into(),
                name: "web".into()
            })
        );
        let mut dc = running("legacy-7-abc");
        dc["metadata"]["labels"] =
            serde_json::json!({"openshift.io/deployment-config.name": "legacy"});
        dc["metadata"]["ownerReferences"][0]["kind"] = "ReplicationController".into();
        dc["metadata"]["ownerReferences"][0]["name"] = "legacy-7".into();
        assert_eq!(owner_of(&from(dc)).unwrap().kind, "DeploymentConfig");
        let mut ss = running("db-0");
        ss["metadata"]["ownerReferences"][0]["kind"] = "StatefulSet".into();
        ss["metadata"]["ownerReferences"][0]["name"] = "db".into();
        assert_eq!(owner_of(&from(ss)).unwrap().name, "db");
        let mut bare = running("debug");
        bare["metadata"]["ownerReferences"] = serde_json::json!([]);
        assert_eq!(owner_of(&from(bare)), None);
    }

    #[test]
    fn row_counts_and_details() {
        let row = pod_row(&from(crash_looping("web-7d9f-x")));
        assert_eq!((row.ready, row.containers, row.restarts), (0, 1, 7));
        assert_eq!(row.last_restart, Some(1_792_922_700));
        assert_eq!(row.node.as_deref(), Some("w1"));
        assert_eq!(row.qos.as_deref(), Some("Burstable"));
        let mut p = crash_looping("web-7d9f-x");
        p["spec"]["volumes"] = serde_json::json!([
            {"name": "data", "persistentVolumeClaim": {"claimName": "web-data"}},
            {"name": "cfg", "configMap": {"name": "web-config"}},
            {"name": "tmp", "emptyDir": {}}
        ]);
        p["spec"]["containers"][0]["env"] =
            serde_json::json!([{"name": "TOKEN", "value": "t0ps3cret"}]);
        let d = pod_details(&from(p));
        let c = &d.containers[0];
        assert_eq!(c.image_id.as_deref(), Some("web@sha256:1"));
        assert_eq!(
            c.state.as_ref().unwrap().reason.as_deref(),
            Some("CrashLoopBackOff")
        );
        let last = c.last_state.as_ref().unwrap();
        assert_eq!(
            (last.state, last.exit_code),
            (ContainerPhase::Terminated, Some(1))
        );
        assert_eq!(
            d.volumes,
            [
                VolumeView {
                    name: "data".into(),
                    kind: "persistentVolumeClaim".into(),
                    source: Some("web-data".into())
                },
                VolumeView {
                    name: "cfg".into(),
                    kind: "configMap".into(),
                    source: Some("web-config".into())
                },
                VolumeView {
                    name: "tmp".into(),
                    kind: "emptyDir".into(),
                    source: None
                },
            ]
        );
        let json = serde_json::to_string(&d).unwrap();
        assert!(
            json.contains("\"imageId\""),
            "поля контейнера на одном уровне: {json}"
        );
        assert!(!json.contains("t0ps3cret"), "значения env не уходят");
    }

    fn list(items: Vec<serde_json::Value>) -> serde_json::Value {
        serde_json::json!({"apiVersion": "v1", "kind": "List", "metadata": {}, "items": items})
    }

    #[tokio::test]
    async fn pods_of_a_workload_by_its_selector_and_of_a_cron_job_by_its_jobs() {
        let (client, server) = mock_client(|s| match s.path() {
            "/apis/apps/v1/namespaces/team-a/deployments/web" => (
                200,
                crate::k8s::workloads::fixtures::deployment("web", 1, 1),
            ),
            "/apis/batch/v1/namespaces/team-a/jobs" => (
                200,
                list(vec![
                    serde_json::json!({"metadata": {"name": "report-1",
                        "ownerReferences": [{"apiVersion": "batch/v1", "kind": "CronJob", "name": "report",
                            "uid": "c", "controller": true}]}}),
                    serde_json::json!({"metadata": {"name": "manual"}}),
                ]),
            ),
            _ => {
                let job_pod = |name: &str, job: &str| {
                    let mut p = running(name);
                    p["metadata"]["ownerReferences"] = serde_json::json!([{"apiVersion": "batch/v1",
                        "kind": "Job", "name": job, "uid": "j", "controller": true}]);
                    p
                };
                (
                    200,
                    list(vec![
                        running("web-7d9f-a"),
                        job_pod("report-1-x", "report-1"),
                        job_pod("manual-y", "manual"),
                    ]),
                )
            }
        });
        let r = ReadClient::new(client, Duration::from_secs(5));
        let web = WorkloadRef {
            kind: WorkloadKind::Deployment,
            name: "web".into(),
        };
        list_pods(&r, "team-a", Some(&web)).await.unwrap();
        let cron = WorkloadRef {
            kind: WorkloadKind::CronJob,
            name: "report".into(),
        };
        let pods = list_pods(&r, "team-a", Some(&cron)).await.unwrap();
        let names: Vec<_> = pods.items.iter().map(|p| p.name.as_str()).collect();
        assert_eq!(names, ["report-1-x"]);
        assert_eq!(list_pods(&r, "team-a", None).await.unwrap().items.len(), 3);
        drop(r);
        let seen = server.await.unwrap();
        let pods_of_web = seen
            .iter()
            .find(|s| s.path() == "/api/v1/namespaces/team-a/pods")
            .unwrap();
        assert!(
            pods_of_web.uri.contains("labelSelector=app%3Dweb"),
            "{}",
            pods_of_web.uri
        );
    }
}
