//! Логи подов для MCP (спец. §10.4, §10.7; уровень `logs`): чтение у API-сервера без follow,
//! строки — с временем и не длиннее `READ_LINE_BYTES` (остаток длинной строки пропускается).
//! Поиск по workload'у читает не больше 20 подов, 50 МБ (`limitBytes` запроса — остаток бюджета)
//! и 40 с; на каком пределе остановился — `stopped`. Маскирует и фильтрует крейт шлюза; строки
//! не хранятся дольше вызова.

use std::time::Duration;

use futures::StreamExt;
use glafira_mcp_core::BackendError;
use glafira_mcp_kubernetes::logs::{
    ContainerLog, LogError, READ_LINE_BYTES, RawLine, SEARCH_BYTES, SEARCH_PODS, SEARCH_SECONDS,
    Since, Stop, WorkloadLogs,
};
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::jiff::Timestamp;
use tokio::time::Instant;

use super::map::error_text;
use crate::error::AppResult;
use crate::k8s::pods::{WorkloadRef, workload_pods};
use crate::k8s::read::ReadClient;
use crate::logs::{lossy_lines, split_line};

/// Аннотация контейнера по умолчанию (как у `kubectl logs`).
const DEFAULT_CONTAINER: &str = "kubectl.kubernetes.io/default-container";
/// Запас на время в начале строки (`2026-10-30T10:00:00.123456789Z `).
const TS_ROOM: usize = 64;

pub fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(SEARCH_SECONDS)
}

/// Пределы просмотра логов workload'а (спец. §10.4); в тестах — маленькие.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub pods: usize,
    pub bytes: u64,
    pub deadline: Instant,
}

impl Limits {
    /// 20 подов, 50 МБ, 40 с с начала вызова.
    pub fn search() -> Self {
        Self {
            pods: SEARCH_PODS,
            bytes: SEARCH_BYTES,
            deadline: deadline(),
        }
    }
}

/// `sinceTime` модели — время RFC 3339; иначе ошибка аргумента до запроса к кластеру.
pub fn since_time(since: &Option<Since>) -> Result<Option<Timestamp>, BackendError> {
    match since {
        Some(Since::Time(t)) => t.parse::<Timestamp>().map(Some).map_err(|_| {
            BackendError::invalid_argument(format!(
                "sinceTime must be an RFC 3339 time, e.g. 2026-10-30T10:00:00Z, got {t}"
            ))
        }),
        _ => Ok(None),
    }
}

/// Контейнер для лога: заданный или контейнер по умолчанию пода (аннотация, иначе первый).
pub fn default_container(pod: &Pod) -> Option<String> {
    let names: Vec<&str> = pod
        .spec
        .iter()
        .flat_map(|s| s.containers.iter().map(|c| c.name.as_str()))
        .collect();
    pod.metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(DEFAULT_CONTAINER))
        .filter(|a| names.contains(&a.as_str()))
        .map(String::from)
        .or_else(|| names.first().map(|n| n.to_string()))
}

/// Прочитанные строки и причина остановки, если поток не дочитан до конца.
struct Read {
    lines: Vec<RawLine>,
    stopped: Option<Stop>,
}

/// Строки лога до конца потока, `deadline` или исчерпания `budget` байт (`budget` уменьшается
/// на прочитанное). Ошибка открытия потока — ошибка; обрыв посреди — тоже.
async fn read_log(
    r: &ReadClient,
    namespace: &str,
    pod: &str,
    mut lp: kube::api::LogParams,
    deadline: Instant,
    budget: &mut u64,
) -> AppResult<Read> {
    lp.timestamps = true;
    lp.follow = false;
    lp.limit_bytes = Some((*budget).max(1) as i64);
    let cap = READ_LINE_BYTES + TS_ROOM;
    // Срок вышел — не открывать поток: готовый сразу ответ `timeout_at` отдал бы.
    let late = || Read {
        lines: Vec::new(),
        stopped: Some(Stop::Time),
    };
    if Instant::now() >= deadline {
        return Ok(late());
    }
    let open = tokio::time::timeout_at(deadline, r.log_stream(namespace, pod, &lp)).await;
    let Ok(stream) = open else {
        return Ok(late());
    };
    let mut lines = lossy_lines(stream?, cap);
    let mut out = Vec::new();
    loop {
        if *budget == 0 {
            return Ok(Read {
                lines: out,
                stopped: Some(Stop::Bytes),
            });
        }
        let next = match tokio::time::timeout_at(deadline, lines.next()).await {
            Err(_) => {
                return Ok(Read {
                    lines: out,
                    stopped: Some(Stop::Time),
                });
            }
            Ok(None) => {
                return Ok(Read {
                    lines: out,
                    stopped: None,
                });
            }
            Ok(Some(line)) => {
                line.map_err(|e| crate::k8s::errors::from_transport_text(&e.to_string()))?
            }
        };
        *budget = budget.saturating_sub(next.len() as u64 + 1);
        let at_cap = next.len() >= cap;
        let (ts, text) = split_line(&next);
        let (text, cut) = glafira_mcp_kubernetes::logs::cut_to(text, READ_LINE_BYTES);
        out.push(RawLine {
            ts: ts.map(|t| t.to_string()),
            text,
            cut: cut || at_cap,
        });
    }
}

/// Лог одного контейнера пода.
#[allow(clippy::too_many_arguments)]
pub async fn pod_log(
    r: &ReadClient,
    namespace: &str,
    pod: &str,
    container: Option<String>,
    previous: bool,
    since: Option<Since>,
    since_time: Option<Timestamp>,
    tail_lines: Option<u32>,
) -> AppResult<ContainerLog> {
    let container = match container {
        Some(c) => c,
        None => {
            let p: Pod = r.get(namespace, pod).await?;
            default_container(&p).unwrap_or_default()
        }
    };
    let lp = kube::api::LogParams {
        container: (!container.is_empty()).then(|| container.clone()),
        previous,
        since_seconds: match since {
            Some(Since::Seconds(s)) => Some(s as i64),
            _ => None,
        },
        since_time,
        tail_lines: tail_lines.map(i64::from),
        ..Default::default()
    };
    let mut budget = SEARCH_BYTES;
    let read = read_log(r, namespace, pod, lp, deadline(), &mut budget).await?;
    if read.stopped == Some(Stop::Time) {
        return Err(crate::k8s::errors::timeout());
    }
    Ok(ContainerLog {
        pod: pod.to_string(),
        container,
        lines: read.lines,
    })
}

/// Логи контейнеров подов workload'а за `since_seconds`: поды — по имени, не больше
/// `limits.pods`; общий бюджет байт и времени. Контейнер, лог которого не прочитался, — в
/// `errors`, просмотр продолжается.
pub async fn workload_logs(
    r: &ReadClient,
    namespace: &str,
    workload: &WorkloadRef,
    since_seconds: u32,
    limits: Limits,
) -> AppResult<WorkloadLogs> {
    let deadline = limits.deadline;
    let mut pods = workload_pods(r, namespace, workload).await?.items;
    pods.sort_by(|a, b| a.metadata.name.cmp(&b.metadata.name));
    let mut out = WorkloadLogs {
        pods_total: pods.len(),
        ..WorkloadLogs::default()
    };
    let mut budget = limits.bytes;
    'pods: for p in pods.iter().take(limits.pods) {
        let name = p.metadata.name.clone().unwrap_or_default();
        let containers: Vec<String> = p
            .spec
            .iter()
            .flat_map(|s| s.containers.iter().map(|c| c.name.clone()))
            .collect();
        for container in containers {
            let lp = kube::api::LogParams {
                container: Some(container.clone()),
                since_seconds: Some(since_seconds as i64),
                ..Default::default()
            };
            match read_log(r, namespace, &name, lp, deadline, &mut budget).await {
                Ok(read) => {
                    out.logs.push(ContainerLog {
                        pod: name.clone(),
                        container,
                        lines: read.lines,
                    });
                    if read.stopped.is_some() {
                        out.stopped = read.stopped;
                        break 'pods;
                    }
                }
                Err(e) => out.errors.push(LogError {
                    pod: name.clone(),
                    container,
                    error: error_text(&e),
                }),
            }
        }
    }
    if out.stopped.is_none() && pods.len() > limits.pods {
        out.stopped = Some(Stop::Pods);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::k8s::pods::fixtures::running;
    use crate::k8s::testing::{Seen, mock_raw};
    use crate::k8s::workloads::WorkloadKind;

    fn reader(handler: impl Fn(&Seen) -> (u16, Vec<u8>) + Send + 'static) -> ReadClient {
        ReadClient::new(mock_raw(handler).0, Duration::from_secs(5))
    }

    fn json(v: serde_json::Value) -> (u16, Vec<u8>) {
        (200, serde_json::to_vec(&v).unwrap())
    }

    fn log(lines: &[&str]) -> (u16, Vec<u8>) {
        let body: String = lines
            .iter()
            .map(|l| format!("2026-10-30T10:00:00.000000001Z {l}\n"))
            .collect();
        (200, body.into_bytes())
    }

    /// Deployment web с двумя подами (web-b, web-a) и логами; у web-b контейнер ещё ждёт.
    fn cluster(s: &Seen) -> (u16, Vec<u8>) {
        match s.path() {
            "/apis/apps/v1/namespaces/team-a/deployments/web" => json(serde_json::json!({
                "apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": "web"},
                "spec": {"selector": {"matchLabels": {"app": "web"}},
                    "template": {"spec": {"containers": [{"name": "web", "image": "web:1"}]}}}})),
            "/api/v1/namespaces/team-a/pods" => json(serde_json::json!({
                "apiVersion": "v1", "kind": "List", "metadata": {},
                "items": [running("web-b"), running("web-a")]})),
            "/api/v1/namespaces/team-a/pods/web-a/log" => {
                log(&["GET /health 200", "ERROR connection refused"])
            }
            "/api/v1/namespaces/team-a/pods/web-b/log" => (
                400,
                serde_json::to_vec(&serde_json::json!({"kind": "Status", "apiVersion": "v1",
                    "status": "Failure", "reason": "BadRequest", "code": 400,
                    "message": "container \"web\" in pod \"web-b\" is waiting to start"}))
                .unwrap(),
            ),
            _ => (404, b"{}".to_vec()),
        }
    }

    fn web() -> WorkloadRef {
        WorkloadRef {
            kind: WorkloadKind::Deployment,
            name: "web".into(),
        }
    }

    #[tokio::test]
    async fn pod_log_reads_the_default_container_with_times() {
        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let s2 = seen.clone();
        let r = reader(move |s| {
            s2.lock().unwrap().push(s.uri.clone());
            match s.path() {
                "/api/v1/namespaces/team-a/pods/web-1" => json(running("web-1")),
                "/api/v1/namespaces/team-a/pods/web-1/log" => log(&["one", "two"]),
                _ => (404, b"{}".to_vec()),
            }
        });
        let l = pod_log(&r, "team-a", "web-1", None, true, None, None, Some(50))
            .await
            .unwrap();
        assert_eq!(l.container, "web");
        assert_eq!(
            l.lines,
            vec![
                RawLine {
                    ts: Some("2026-10-30T10:00:00.000000001Z".into()),
                    text: "one".into(),
                    cut: false
                },
                RawLine {
                    ts: Some("2026-10-30T10:00:00.000000001Z".into()),
                    text: "two".into(),
                    cut: false
                },
            ]
        );
        let uri = seen.lock().unwrap()[1].clone();
        for q in [
            "container=web",
            "previous=true",
            "tailLines=50",
            "timestamps=true",
            "limitBytes=52428800",
        ] {
            assert!(uri.contains(q), "{q}: {uri}");
        }
        assert!(!uri.contains("follow=true"), "{uri}");
    }

    #[tokio::test]
    async fn long_lines_are_kept_up_to_the_read_cap() {
        let long = "x".repeat(READ_LINE_BYTES * 2);
        let r = reader(move |_| log(&[&long, "short"]));
        let l = pod_log(
            &r,
            "team-a",
            "web-1",
            Some("web".into()),
            false,
            None,
            None,
            None,
        )
        .await
        .unwrap();
        assert!(l.lines[0].cut && l.lines[0].text.len() == READ_LINE_BYTES);
        assert_eq!((l.lines[1].text.as_str(), l.lines[1].cut), ("short", false));
    }

    #[tokio::test]
    async fn workload_pods_are_read_by_name_and_errors_are_kept() {
        let r = reader(cluster);
        let w = workload_logs(&r, "team-a", &web(), 600, Limits::search())
            .await
            .unwrap();
        assert_eq!((w.pods_total, w.stopped), (2, None));
        assert_eq!(w.logs.len(), 1);
        assert_eq!(w.logs[0].pod, "web-a");
        assert_eq!(w.logs[0].lines[1].text, "ERROR connection refused");
        assert_eq!(w.errors.len(), 1);
        assert_eq!(
            (w.errors[0].pod.as_str(), w.errors[0].container.as_str()),
            ("web-b", "web")
        );
        assert!(
            w.errors[0].error.contains("waiting to start"),
            "{:?}",
            w.errors
        );
    }

    #[tokio::test]
    async fn workload_scan_stops_at_its_limits() {
        let r = reader(cluster);
        let pods = Limits {
            pods: 1,
            ..Limits::search()
        };
        let w = workload_logs(&r, "team-a", &web(), 600, pods)
            .await
            .unwrap();
        assert_eq!((w.logs.len(), w.stopped), (1, Some(Stop::Pods)));

        let r = reader(cluster);
        let bytes = Limits {
            bytes: 10,
            ..Limits::search()
        };
        let w = workload_logs(&r, "team-a", &web(), 600, bytes)
            .await
            .unwrap();
        assert_eq!(w.stopped, Some(Stop::Bytes));
        assert_eq!(w.logs[0].lines.len(), 1, "после бюджета строки не читаются");

        let r = reader(cluster);
        let time = Limits {
            deadline: Instant::now(),
            ..Limits::search()
        };
        let w = workload_logs(&r, "team-a", &web(), 600, time)
            .await
            .unwrap();
        assert_eq!(w.stopped, Some(Stop::Time));
    }

    #[test]
    fn default_container_follows_the_annotation_then_the_first() {
        let pod = |annotation: Option<&str>| -> Pod {
            let mut v = serde_json::json!({"metadata": {"name": "p"},
                "spec": {"containers": [{"name": "app"}, {"name": "sidecar"}]}});
            if let Some(a) = annotation {
                v["metadata"]["annotations"] = serde_json::json!({DEFAULT_CONTAINER: a});
            }
            serde_json::from_value(v).unwrap()
        };
        assert_eq!(default_container(&pod(None)).as_deref(), Some("app"));
        assert_eq!(
            default_container(&pod(Some("sidecar"))).as_deref(),
            Some("sidecar")
        );
        assert_eq!(
            default_container(&pod(Some("ghost"))).as_deref(),
            Some("app")
        );
    }

    #[test]
    fn since_time_must_be_rfc_3339() {
        assert!(since_time(&None).unwrap().is_none());
        assert!(since_time(&Some(Since::Seconds(5))).unwrap().is_none());
        assert!(
            since_time(&Some(Since::Time("2026-10-30T10:00:00Z".into())))
                .unwrap()
                .is_some()
        );
        let e = since_time(&Some(Since::Time("yesterday".into()))).unwrap_err();
        assert_eq!(e.code, "invalid_argument");
    }
}
