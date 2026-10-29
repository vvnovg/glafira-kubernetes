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
use crate::k8s::yaml::redact_annotations;

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

/// Имена нативных sidecar-контейнеров пода: init-контейнеры с `restartPolicy: Always`
/// (Kubernetes 1.29+, OpenShift 4.16+). Статус init-контейнера сопоставляется со спецификацией
/// по имени: `started` бывает и у обычного init-контейнера, пока он работает, но проходным
/// он считается только у sidecar'а.
fn sidecar_names(p: &Pod) -> BTreeSet<&str> {
    p.spec
        .iter()
        .flat_map(|s| s.init_containers.iter().flatten())
        .filter(|c| c.restart_policy.as_deref() == Some("Always"))
        .map(|c| c.name.as_str())
        .collect()
}

/// Статусы нативных sidecar-контейнеров пода.
fn sidecar_statuses(p: &Pod) -> impl Iterator<Item = &ContainerStatus> {
    let names = sidecar_names(p);
    statuses(p, true)
        .iter()
        .filter(move |c| names.contains(c.name.as_str()))
}

/// Условие `Initialized` выполнено: kubectl тогда разбирает и основные контейнеры, даже если
/// sidecar сейчас перезапускается.
fn initialized(p: &Pod) -> bool {
    p.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .and_then(|cs| cs.iter().find(|c| c.type_ == "Initialized"))
        .is_some_and(|c| c.status == "True")
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
    let sidecars = sidecar_names(p);
    let mut initializing = false;
    for (i, c) in statuses(p, true).iter().enumerate() {
        let state = c.state.as_ref();
        let terminated = state.and_then(|s| s.terminated.as_ref());
        if terminated.is_some_and(|t| t.exit_code == 0) {
            continue;
        }
        // Запущенный sidecar не держит инициализацию: он работает, пока работает под.
        if sidecars.contains(c.name.as_str()) && c.started == Some(true) {
            continue;
        }
        if let Some(t) = terminated {
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
    if !initializing || initialized(p) {
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

/// Контейнер перезапускается в цикле: ждёт с причиной `CrashLoopBackOff` или — между
/// перезапусками, пока kubelet не написал эту причину (Kubernetes 1.37 держит контейнер так
/// минутами), — завершён с ошибкой (код ≠ 0 или сигнал) и уже перезапускался, а под не завершён,
/// не удаляется и перезапускает этот контейнер (`restartPolicy` пода не `Never`; нативный
/// sidecar перезапускается всегда). Решение пользователя на этапе O2.
pub fn in_crash_loop(p: &Pod, c: &ContainerStatus) -> bool {
    if waiting_reason(c) == Some("CrashLoopBackOff") {
        return true;
    }
    let failed = c
        .state
        .as_ref()
        .and_then(|s| s.terminated.as_ref())
        .is_some_and(|t| t.exit_code != 0 || t.signal.is_some_and(|s| s != 0));
    let phase = p.status.as_ref().and_then(|s| s.phase.as_deref());
    let restarts = p.spec.as_ref().and_then(|s| s.restart_policy.as_deref()) != Some("Never")
        || sidecar_names(p).contains(c.name.as_str());
    failed
        && c.restart_count >= 1
        && restarts
        && !matches!(phase, Some("Succeeded" | "Failed"))
        && p.metadata.deletion_timestamp.is_none()
}

/// Проблема пода — то, что видно сейчас: образ не скачивается, контейнер завершён по памяти
/// (сейчас или в последний раз), перезапускается в цикле (`in_crash_loop`), под ждёт
/// размещения или упал.
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
    } else if all.iter().any(|c| in_crash_loop(p, c)) {
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
    // Как в kubectl: нативные sidecar'ы считаются и в READY, и в общем числе контейнеров,
    // а их рестарты — в RESTARTS.
    let sidecars: Vec<&ContainerStatus> = sidecar_statuses(p).collect();
    let counted: Vec<&ContainerStatus> = main.iter().chain(sidecars.iter().copied()).collect();
    let last_restart = counted
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
        ready: (main.iter().filter(|c| c.ready).count()
            + sidecars
                .iter()
                .filter(|c| c.started == Some(true) && c.ready)
                .count()) as i32,
        containers: (p.spec.as_ref().map_or(0, |s| s.containers.len()) + sidecar_names(p).len())
            as i32,
        restarts: counted.iter().map(|c| c.restart_count).sum(),
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
    match workload {
        None => Ok(r
            .list::<Pod>(Some(namespace), &ListParams::default())
            .await?
            .map(|p| pod_row(&p))),
        Some(w) => Ok(workload_pods(r, namespace, w).await?.map(|p| pod_row(&p))),
    }
}

/// Поды workload'а целиком: по селектору workload'а, а у CronJob — поды его Job.
pub async fn workload_pods(
    r: &ReadClient,
    namespace: &str,
    w: &WorkloadRef,
) -> AppResult<Listed<Pod>> {
    let ns = Some(namespace);
    if w.kind == WorkloadKind::CronJob {
        let all_jobs = r.list::<Job>(ns, &ListParams::default()).await?;
        let jobs: BTreeSet<String> = all_jobs
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
        return Ok(Listed {
            truncated: all_jobs.truncated || pods.truncated,
            items: pods
                .items
                .into_iter()
                .filter(|p| owner_of(p).is_some_and(|o| o.kind == "Job" && jobs.contains(&o.name)))
                .collect(),
        });
    }
    let selector = pod_selector(r, namespace, w.kind, &w.name).await?;
    // Без селектора поды workload'а определить нечем; пустой селектор подошёл бы всем
    // подам проекта, а это чужие поды.
    let Some(selector) = selector.filter(|s| !s.is_empty()) else {
        return Ok(Listed {
            items: Vec::new(),
            truncated: false,
        });
    };
    r.list::<Pod>(ns, &by_labels(Some(&selector))).await
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
    /// Как в YAML (`redact_annotations`): без копий объекта, `change-cause` с секретом — `***`.
    pub annotations: BTreeMap<String, String>,
    pub volumes: Vec<VolumeView>,
}

/// Тома спецификации пода (или шаблона подов workload'а).
pub fn volumes_of(spec: &k8s_openapi::api::core::v1::PodSpec) -> Vec<VolumeView> {
    spec.volumes
        .as_ref()
        .map(|vs| {
            vs.iter()
                .filter_map(|v| serde_json::to_value(v).ok())
                .filter_map(|v| volume_view(&v))
                .collect()
        })
        .unwrap_or_default()
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
    redact_annotations(&mut annotations);
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
        volumes: spec.map(volumes_of).unwrap_or_default(),
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

    /// Между перезапусками kubelet 1.37 показывает упавший контейнер завершённым (`Error`), а не
    /// `CrashLoopBackOff`: такой под — тоже в цикле перезапусков, пока он вообще перезапускается.
    #[test]
    fn a_failed_container_between_restarts_is_a_crash_loop() {
        let between = || {
            let mut p = pod(
                "a",
                "Running",
                serde_json::json!({"terminated": {"exitCode": 1, "reason": "Error"}}),
            );
            p["status"]["containerStatuses"][0]["restartCount"] = 3.into();
            p
        };
        let p = from(between());
        assert_eq!(pod_status(&p), "Error", "статус — как у kubectl");
        assert_eq!(pod_problem(&p), Some(PodProblem::CrashLoop));
        let mut killed = between();
        killed["status"]["containerStatuses"][0]["state"] =
            serde_json::json!({"terminated": {"exitCode": 0, "signal": 9}});
        assert_eq!(pod_problem(&from(killed)), Some(PodProblem::CrashLoop));

        let mut first = between();
        first["status"]["containerStatuses"][0]["restartCount"] = 0.into();
        assert_eq!(pod_problem(&from(first)), None, "ещё не перезапускался");
        let mut clean = between();
        clean["status"]["containerStatuses"][0]["state"] =
            serde_json::json!({"terminated": {"exitCode": 0, "reason": "Completed"}});
        assert_eq!(pod_problem(&from(clean)), None, "завершился без ошибки");
        let mut never = between();
        never["spec"]["restartPolicy"] = "Never".into();
        assert_eq!(pod_problem(&from(never)), None, "не перезапускается");
        let mut failed = between();
        failed["status"]["phase"] = "Failed".into();
        assert_eq!(pod_problem(&from(failed)), Some(PodProblem::Failed));
        let mut deleting = from(between());
        deleting.metadata.deletion_timestamp =
            Some(k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(
                "2026-10-25T10:00:00Z".parse().unwrap(),
            ));
        assert_eq!(
            pod_problem(&deleting),
            None,
            "удаляемый под гасит контейнеры"
        );
    }

    #[test]
    fn a_sidecar_restarts_even_in_a_pod_that_never_restarts() {
        let mut p = running("a");
        p["spec"]["restartPolicy"] = "Never".into();
        p["spec"]["initContainers"] =
            serde_json::json!([{"name": "proxy", "image": "proxy:1", "restartPolicy": "Always"}]);
        p["status"]["initContainerStatuses"] = serde_json::json!([{"name": "proxy",
            "image": "proxy:1", "imageID": "", "ready": false, "restartCount": 2,
            "state": {"terminated": {"exitCode": 2, "reason": "Error"}}}]);
        assert_eq!(pod_problem(&from(p)), Some(PodProblem::CrashLoop));
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

    /// Под `base` с нативным sidecar'ом `proxy` (init-контейнер с `restartPolicy: Always`):
    /// `started` — поле статуса (`None` — не заполнено), `state` — JSON `ContainerState`.
    fn with_sidecar(
        mut base: serde_json::Value,
        started: Option<bool>,
        state: serde_json::Value,
    ) -> serde_json::Value {
        base["spec"]["initContainers"] =
            serde_json::json!([{"name": "proxy", "restartPolicy": "Always"}]);
        let mut status = serde_json::json!({"name": "proxy", "image": "x", "imageID": "",
            "ready": started == Some(true), "restartCount": 2, "state": state,
            "lastState": {"terminated": {"exitCode": 1, "finishedAt": "2026-10-25T10:07:00Z"}}});
        if let Some(s) = started {
            status["started"] = s.into();
        }
        base["status"]["initContainerStatuses"] = serde_json::json!([status]);
        base
    }

    #[test]
    fn a_started_sidecar_does_not_hold_the_pod_in_init() {
        let p = from(with_sidecar(
            running("a"),
            Some(true),
            serde_json::json!({"running": {}}),
        ));
        assert_eq!(pod_status(&p), "Running");
        let row = pod_row(&p);
        assert_eq!(
            (row.ready, row.containers, row.restarts),
            (2, 2, 2),
            "sidecar считается и в готовых, и в общем числе, и в рестартах"
        );
        assert_eq!(row.last_restart, Some(1_792_922_820));
        assert_eq!(row.problem, None);
        // Запущен, но проба ещё не прошла: в общем числе есть, готовым не считается.
        let mut not_ready =
            with_sidecar(running("a"), Some(true), serde_json::json!({"running": {}}));
        not_ready["status"]["initContainerStatuses"][0]["ready"] = false.into();
        let row = pod_row(&from(not_ready));
        assert_eq!((row.ready, row.containers), (1, 2));
        // Запущенный sidecar проходной, но следующий за ним обычный init-контейнер ещё работает:
        // номер в `Init:N/M` — позиция среди всех init-контейнеров.
        let mut mixed = with_sidecar(running("a"), Some(true), serde_json::json!({"running": {}}));
        mixed["spec"]["initContainers"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!({"name": "migrate"}));
        mixed["status"]["initContainerStatuses"]
            .as_array_mut()
            .unwrap()
            .push(
                serde_json::json!({"name": "migrate", "image": "x", "imageID": "", "ready": false,
                "restartCount": 0, "started": true, "state": {"running": {}}}),
            );
        assert_eq!(pod_status(&from(mixed)), "Init:1/2");
    }

    #[test]
    fn a_sidecar_that_has_not_started_holds_the_pod_in_init() {
        for started in [Some(false), None] {
            let mut v = with_sidecar(running("a"), started, serde_json::json!({"running": {}}));
            v["status"]["phase"] = "Pending".into();
            v["status"]["containerStatuses"] = serde_json::json!([{"name": "web", "image": "web:1",
                "imageID": "", "ready": false, "restartCount": 0,
                "state": {"waiting": {"reason": "PodInitializing"}}}]);
            let p = from(v);
            assert_eq!(pod_status(&p), "Init:0/1", "started: {started:?}");
            let row = pod_row(&p);
            assert_eq!((row.ready, row.containers), (0, 2));
        }
    }

    #[test]
    fn started_is_not_enough_for_a_regular_init_container() {
        // `started` бывает и у обычного init-контейнера, пока он работает: по статусу без
        // спецификации его не отличить от sidecar'а.
        let mut p = running("a");
        p["spec"]["initContainers"] = serde_json::json!([{"name": "migrate"}]);
        p["status"]["initContainerStatuses"] = serde_json::json!([{"name": "migrate",
            "image": "x", "imageID": "", "ready": false, "restartCount": 0, "started": true,
            "state": {"running": {}}}]);
        let p = from(p);
        assert_eq!(pod_status(&p), "Init:0/1");
        assert_eq!(pod_row(&p).containers, 1);
    }

    #[test]
    fn main_containers_are_read_once_the_pod_is_initialized() {
        // Sidecar перезапускается (`started` сброшен), а под уже инициализирован: kubectl
        // разбирает и основные контейнеры.
        let mut v = with_sidecar(
            crash_looping("a"),
            Some(false),
            serde_json::json!({"running": {}}),
        );
        assert_eq!(pod_status(&from(v.clone())), "Init:0/1");
        v["status"]["conditions"] = serde_json::json!([{"type": "Initialized", "status": "True"}]);
        assert_eq!(pod_status(&from(v)), "CrashLoopBackOff");
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

    /// «Обзор» пода скрывает в аннотациях то же, что YAML.
    #[test]
    fn pod_annotations_without_copies_and_secret_change_cause() {
        let mut p = running("web-7d9f-x");
        p["metadata"]["annotations"] = serde_json::json!({
            "kapp.k14s.io/original": "{\"kind\":\"Pod\",\"spec\":{\"containers\":[{\"env\":[{\"name\":\"DB_PASSWORD\",\"value\":\"s3cr3t\"}]}]}}",
            "kubectl.kubernetes.io/last-applied-configuration": "{\"DB_PASSWORD\":\"s3cr3t\"}",
            "kubernetes.io/change-cause": "kubectl run web --env=API_TOKEN=x",
            "prometheus.io/scrape": "true"});
        let d = pod_details(&from(p));
        let json = serde_json::to_string(&d).unwrap();
        assert!(
            !json.contains("s3cr3t") && !json.contains("API_TOKEN=x"),
            "{json}"
        );
        assert_eq!(
            d.annotations.keys().collect::<Vec<_>>(),
            ["kubernetes.io/change-cause", "prometheus.io/scrape"]
        );
        assert_eq!(d.annotations["kubernetes.io/change-cause"], "***");
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

    #[tokio::test]
    async fn a_workload_without_a_selector_has_no_pods_instead_of_all_of_them() {
        let (client, server) = mock_client(|s| match s.path() {
            // Пустой селектор: `matchLabels` нет.
            "/apis/apps/v1/namespaces/team-a/deployments/web" => {
                let mut d = crate::k8s::workloads::fixtures::deployment("web", 1, 1);
                d["spec"]["selector"] = serde_json::json!({});
                (200, d)
            }
            // Селектора нет вовсе.
            "/apis/batch/v1/namespaces/team-a/jobs/migrate" => (
                200,
                serde_json::json!({"apiVersion": "batch/v1", "kind": "Job",
                    "metadata": {"name": "migrate"}, "spec": {"template": {}}}),
            ),
            _ => (200, list(vec![running("web-7d9f-a"), running("other")])),
        });
        let r = ReadClient::new(client, Duration::from_secs(5));
        for (kind, name) in [
            (WorkloadKind::Deployment, "web"),
            (WorkloadKind::Job, "migrate"),
        ] {
            let w = WorkloadRef {
                kind,
                name: name.into(),
            };
            let l = list_pods(&r, "team-a", Some(&w)).await.unwrap();
            assert!(l.items.is_empty() && !l.truncated, "{name}: {:?}", l.items);
        }
        drop(r);
        let uris: Vec<_> = server.await.unwrap().into_iter().map(|s| s.uri).collect();
        assert!(
            uris.iter().all(|u| !u.contains("/pods")),
            "список подов не запрашивался: {uris:?}"
        );
    }

    #[tokio::test]
    async fn pods_of_a_cron_job_are_truncated_when_either_list_is() {
        let job_pod = |name: &str| {
            let mut p = running(name);
            p["metadata"]["ownerReferences"] = serde_json::json!([{"apiVersion": "batch/v1",
                "kind": "Job", "name": "report-1", "uid": "j", "controller": true}]);
            p
        };
        let cron = WorkloadRef {
            kind: WorkloadKind::CronJob,
            name: "report".into(),
        };
        // (Jobs не прочитаны до конца, Pod'ы не прочитаны до конца) → ожидаемые поды.
        for (more_jobs, more_pods, expected) in
            [(true, false, 1), (false, true, 0), (false, false, 1)]
        {
            let (client, server) = mock_client(move |s| {
                let more =
                    |on: bool| serde_json::json!({"continue": if on { "again" } else { "" }});
                if s.path() == "/apis/batch/v1/namespaces/team-a/jobs" {
                    (
                        200,
                        serde_json::json!({"apiVersion": "batch/v1", "kind": "JobList",
                            "metadata": more(more_jobs), "items": [{"metadata": {"name": "report-1",
                                "ownerReferences": [{"apiVersion": "batch/v1", "kind": "CronJob",
                                    "name": "report", "uid": "c", "controller": true}]}}]}),
                    )
                } else {
                    let items = if more_pods {
                        vec![]
                    } else {
                        vec![job_pod("report-1-x"), running("web-7d9f-a")]
                    };
                    (
                        200,
                        serde_json::json!({"apiVersion": "v1", "kind": "PodList",
                            "metadata": more(more_pods), "items": items}),
                    )
                }
            });
            let r = ReadClient::new(client, Duration::from_secs(5));
            let l = list_pods(&r, "team-a", Some(&cron)).await.unwrap();
            assert_eq!(l.truncated, more_jobs || more_pods);
            assert_eq!(l.items.len(), expected, "{more_jobs} {more_pods}");
            drop(r);
            server.await.unwrap();
        }
    }
}
