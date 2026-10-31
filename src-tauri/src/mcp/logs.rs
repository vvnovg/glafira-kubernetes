//! Логи подов для MCP (спец. §10.4, §10.7; уровень `logs`): чтение у API-сервера без follow,
//! строки — с временем и не длиннее `READ_LINE_BYTES` (остаток длинной строки пропускается).
//! Поиск по workload'у читает не больше 20 подов, 50 МБ (`limitBytes` запроса — остаток бюджета)
//! и 40 с; на каком пределе остановился — `stopped`. Маскирует и фильтрует крейт шлюза; строки
//! не хранятся дольше вызова. Недоступный лог (контейнер не запускался, прошлого запуска нет,
//! узел удалил лог) — не ошибка запроса, а причина для модели (`log_unavailable`).

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use futures::StreamExt;
use futures::io::{AsyncBufRead, AsyncRead};
use glafira_mcp_core::BackendError;
use glafira_mcp_kubernetes::logs::{
    ContainerLog, LogError, READ_LINE_BYTES, RawLine, SEARCH_BYTES, SEARCH_PODS, SEARCH_SECONDS,
    Since, Stop, WorkloadLogs,
};
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::jiff::Timestamp;
use tokio::time::Instant;

use super::map::error_text;
use crate::error::{AppError, AppResult};
use crate::k8s::pods::{WorkloadRef, workload_pods};
use crate::k8s::read::ReadClient;
use crate::logs::{lossy_lines, split_line};

/// Аннотация контейнера по умолчанию (как у `kubectl logs`).
const DEFAULT_CONTAINER: &str = "kubectl.kubernetes.io/default-container";
/// Запас на время в начале строки (`2026-10-30T10:00:00.123456789Z `).
const TS_ROOM: usize = 64;
/// Ответы kubelet'а с кодом 200 вместо лога, который среда выполнения уже удалила (часто у
/// `previous` в CrashLoopBackOff): одна строка без времени, хотя оно запрошено. Вторая — когда
/// файла лога нет, а контейнер среда выполнения ещё помнит.
const KUBELET_LOG_GONE: [&str; 2] = [
    "unable to retrieve container logs for ",
    "failed to try resolving symlinks in path ",
];
/// Ответы 400 API-сервера на лог контейнера, который не запускался, не имел прошлого запуска
/// или завершился и узлом уже забыт.
const WAITING_TO_START: &str = " is waiting to start";
const NO_PREVIOUS: &str = "previous terminated container ";
const TERMINATED: &str = " is terminated";
/// Длина причины ожидания (из ответа API-сервера) в тексте для модели.
const MAX_REASON_CHARS: usize = 200;

/// Почему лог контейнера недоступен, хотя запрос верен.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Unavailable {
    /// Контейнер ещё не запускался; причина — из ответа API-сервера (`ContainerCreating`,
    /// `trying and failing to pull image`), может быть пустой.
    Waiting(String),
    /// Прошлого запуска нет (`previous` у контейнера без перезапусков).
    NoPrevious,
    /// Контейнер завершился, и узел его больше не знает (`container "x" in pod "y" is terminated`).
    Terminated,
    /// Среда выполнения узла уже удалила лог.
    Gone,
}

impl Unavailable {
    /// Ошибка открытия лога, которая значит «лога нет», а не «запрос неверен»: ответ 400
    /// API-сервера с узнаваемым текстом.
    fn of(e: &AppError) -> Option<Self> {
        if e.key != "errors.k8s.invalid" {
            return None;
        }
        let msg = e.params.get("message")?.trim();
        if let Some(i) = msg.find(WAITING_TO_START) {
            let reason = msg[i + WAITING_TO_START.len()..]
                .trim_start_matches(':')
                .trim();
            return Some(Self::Waiting(
                reason.chars().take(MAX_REASON_CHARS).collect(),
            ));
        }
        if msg.starts_with(NO_PREVIOUS) && msg.ends_with(" not found") {
            return Some(Self::NoPrevious);
        }
        (msg.starts_with("container ") && msg.ends_with(TERMINATED)).then_some(Self::Terminated)
    }

    /// Текст для модели: причина и что делать дальше.
    pub fn text(&self, container: &str, previous: bool) -> String {
        match self {
            Self::Waiting(r) if r.is_empty() => format!(
                "The log of container \"{container}\" is unavailable: the container is waiting to start. Check the pod state with describe_pod"
            ),
            Self::Waiting(r) => format!(
                "The log of container \"{container}\" is unavailable: the container is waiting to start ({r}). Check the pod state with describe_pod"
            ),
            Self::NoPrevious => format!(
                "Container \"{container}\" has no previous terminated instance, so there is no previous log. Read the current log without previous, or check the pod state with describe_pod"
            ),
            Self::Terminated => format!(
                "The log of container \"{container}\" is unavailable: the container has terminated and the node no longer keeps it. Check the pod state with describe_pod"
            ),
            Self::Gone if previous => format!(
                "The previous log of container \"{container}\" is no longer available on the node: the container runtime has removed it. Read the current log without previous, or check the pod state with describe_pod"
            ),
            Self::Gone => format!(
                "The log of container \"{container}\" is no longer available on the node: the container runtime has removed it. Check the pod state with describe_pod"
            ),
        }
    }
}

/// Лог контейнера пода или причина, по которой его нет.
#[derive(Debug)]
pub enum PodLog {
    Log(ContainerLog),
    Unavailable(String),
}

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

/// Читатель, считающий байты, снятые с потока: `lossy_lines` пропускает хвосты длинных строк,
/// и по длине отданных строк расход бюджета занижался бы.
struct Counting<R> {
    inner: R,
    read: Arc<AtomicU64>,
}

impl<R: AsyncBufRead + Unpin> AsyncRead for Counting<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        let res = Pin::new(&mut self.inner).poll_read(cx, buf);
        if let Poll::Ready(Ok(n)) = &res {
            self.read.fetch_add(*n as u64, Ordering::Relaxed);
        }
        res
    }
}

impl<R: AsyncBufRead + Unpin> AsyncBufRead for Counting<R> {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<&[u8]>> {
        Pin::new(&mut self.get_mut().inner).poll_fill_buf(cx)
    }

    fn consume(mut self: Pin<&mut Self>, amt: usize) {
        self.read.fetch_add(amt as u64, Ordering::Relaxed);
        Pin::new(&mut self.inner).consume(amt);
    }
}

/// Прочитанные строки и причина остановки, если поток не дочитан до конца; или причина, по
/// которой лога нет.
#[derive(Default)]
struct Read {
    lines: Vec<RawLine>,
    stopped: Option<Stop>,
    unavailable: Option<Unavailable>,
}

impl Read {
    fn unavailable(u: Unavailable) -> Self {
        Self {
            unavailable: Some(u),
            ..Self::default()
        }
    }
}

/// Весь ответ — одна строка kubelet'а без времени об удалённом логе. Строка без времени в
/// другом месте лога — строка приложения, её не трогаем.
fn kubelet_log_gone(lines: &[RawLine]) -> bool {
    matches!(lines, [only] if only.ts.is_none()
        && KUBELET_LOG_GONE.iter().any(|p| only.text.starts_with(p)))
}

/// Строки лога до конца потока, `deadline` или исчерпания `budget` байт (`budget` уменьшается
/// на прочитанное). Ошибка открытия потока — ошибка, кроме «контейнер не запускался», «нет
/// прошлого запуска» и «завершился и забыт узлом» (`unavailable`); обрыв посреди — тоже
/// ошибка. Ответ из одной строки kubelet'а об удалённом логе (или его файле) — тоже
/// `unavailable`, а не строка лога.
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
        stopped: Some(Stop::Time),
        ..Read::default()
    };
    if Instant::now() >= deadline {
        return Ok(late());
    }
    let open = tokio::time::timeout_at(deadline, r.log_stream(namespace, pod, &lp)).await;
    let stream = match open {
        Err(_) => return Ok(late()),
        Ok(Err(e)) => return Unavailable::of(&e).map(Read::unavailable).ok_or(e),
        Ok(Ok(stream)) => stream,
    };
    // Бюджет списывается по байтам, снятым с потока, а не по длине отданных строк.
    let start = *budget;
    let consumed = Arc::new(AtomicU64::new(0));
    let counting = Counting {
        inner: stream,
        read: consumed.clone(),
    };
    let mut lines = lossy_lines(counting, cap);
    let mut out = Vec::new();
    loop {
        if *budget == 0 {
            return Ok(Read {
                lines: out,
                stopped: Some(Stop::Bytes),
                unavailable: None,
            });
        }
        let next = match tokio::time::timeout_at(deadline, lines.next()).await {
            Err(_) => {
                return Ok(Read {
                    lines: out,
                    stopped: Some(Stop::Time),
                    unavailable: None,
                });
            }
            Ok(None) if kubelet_log_gone(&out) => {
                return Ok(Read::unavailable(Unavailable::Gone));
            }
            Ok(None) => {
                return Ok(Read {
                    lines: out,
                    ..Read::default()
                });
            }
            Ok(Some(line)) => {
                line.map_err(|e| crate::k8s::errors::from_transport_text(&e.to_string()))?
            }
        };
        *budget = start.saturating_sub(consumed.load(Ordering::Relaxed));
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

/// Лог одного контейнера пода или причина, по которой его нет.
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
) -> AppResult<PodLog> {
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
    if let Some(u) = read.unavailable {
        return Ok(PodLog::Unavailable(u.text(&container, previous)));
    }
    Ok(PodLog::Log(ContainerLog {
        pod: pod.to_string(),
        container,
        lines: read.lines,
    }))
}

/// Логи контейнеров подов workload'а за `since_seconds`: поды — по имени, не больше
/// `limits.pods`; общий бюджет байт и времени. Контейнер, лог которого не прочитался или
/// недоступен (с причиной), — в `errors`, просмотр продолжается.
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
                Ok(Read {
                    unavailable: Some(u),
                    ..
                }) => out.errors.push(LogError {
                    pod: name.clone(),
                    error: u.text(&container, false),
                    container,
                }),
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

    fn log_of(l: PodLog) -> ContainerLog {
        match l {
            PodLog::Log(l) => l,
            PodLog::Unavailable(why) => panic!("лог недоступен: {why}"),
        }
    }

    fn bad_request(message: &str) -> (u16, Vec<u8>) {
        (
            400,
            serde_json::to_vec(&serde_json::json!({"kind": "Status", "apiVersion": "v1",
                "status": "Failure", "reason": "BadRequest", "code": 400, "message": message}))
            .unwrap(),
        )
    }

    /// Лог контейнера `container` пода web-1 (`previous`), когда API-сервер отвечает `answer`.
    async fn pod_log_answered(
        container: &str,
        previous: bool,
        answer: impl Fn() -> (u16, Vec<u8>) + Send + 'static,
    ) -> AppResult<PodLog> {
        let r = reader(move |_| answer());
        pod_log(
            &r,
            "team-a",
            "web-1",
            Some(container.into()),
            previous,
            None,
            None,
            None,
        )
        .await
    }

    fn unavailable(l: AppResult<PodLog>) -> String {
        match l.unwrap() {
            PodLog::Unavailable(why) => why,
            PodLog::Log(l) => panic!("лог вместо причины: {:?}", l.lines),
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
        let l = log_of(
            pod_log(&r, "team-a", "web-1", None, true, None, None, Some(50))
                .await
                .unwrap(),
        );
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
        let l = log_of(
            pod_log(
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
            .unwrap(),
        );
        assert!(l.lines[0].cut && l.lines[0].text.len() == READ_LINE_BYTES);
        assert_eq!((l.lines[1].text.as_str(), l.lines[1].cut), ("short", false));
    }

    #[tokio::test]
    async fn skipped_tails_of_long_lines_are_charged_to_the_byte_budget() {
        let long = "x".repeat(READ_LINE_BYTES * 4);
        let r = reader(move |_| log(&[&long, "short"]));
        let mut budget = (READ_LINE_BYTES + TS_ROOM + 100) as u64;
        let read = read_log(
            &r,
            "team-a",
            "web-1",
            kube::api::LogParams::default(),
            deadline(),
            &mut budget,
        )
        .await
        .unwrap();
        assert_eq!(budget, 0);
        assert_eq!(read.stopped, Some(Stop::Bytes));
        assert_eq!(read.lines.len(), 1, "после бюджета строки не читаются");
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
        assert_eq!(
            w.errors[0].error,
            "The log of container \"web\" is unavailable: the container is waiting to start. Check the pod state with describe_pod"
        );
    }

    #[tokio::test]
    async fn kubelet_answer_about_a_removed_log_is_not_a_log_line() {
        let gone = || {
            (
                200,
                b"unable to retrieve container logs for containerd://0dc3c255abcdef".to_vec(),
            )
        };
        assert_eq!(
            unavailable(pod_log_answered("crash", true, gone).await),
            "The previous log of container \"crash\" is no longer available on the node: the container runtime has removed it. Read the current log without previous, or check the pod state with describe_pod"
        );
        // Та же строка не единственная — строка приложения, её не трогаем.
        let mixed = || {
            (
                200,
                b"unable to retrieve container logs for x\n2026-10-30T10:00:00Z next\n".to_vec(),
            )
        };
        let l = log_of(pod_log_answered("crash", true, mixed).await.unwrap());
        assert_eq!(l.lines.len(), 2);
        assert_eq!(
            (l.lines[0].ts.as_deref(), l.lines[0].text.as_str()),
            (None, "unable to retrieve container logs for x")
        );
        // Файла лога нет, контейнер среда выполнения ещё помнит — тоже весь ответ.
        let symlink = || {
            (
                200,
                b"failed to try resolving symlinks in path \"/var/log/pods/team-a_web-1/crash/1.log\": lstat /var/log/pods/team-a_web-1/crash/1.log: no such file or directory".to_vec(),
            )
        };
        assert!(
            unavailable(pod_log_answered("crash", true, symlink).await).starts_with(
                "The previous log of container \"crash\" is no longer available on the node"
            )
        );
        let after = || {
            (
                200,
                b"2026-10-30T10:00:00Z first\nunable to retrieve container logs for x\n".to_vec(),
            )
        };
        let l = log_of(pod_log_answered("crash", false, after).await.unwrap());
        assert_eq!(l.lines.len(), 2);
    }

    #[tokio::test]
    async fn waiting_or_never_restarted_container_has_no_log_with_a_reason() {
        let pull = || {
            bad_request(
                "container \"pull\" in pod \"web-1\" is waiting to start: trying and failing to pull image",
            )
        };
        assert_eq!(
            unavailable(pod_log_answered("pull", false, pull).await),
            "The log of container \"pull\" is unavailable: the container is waiting to start (trying and failing to pull image). Check the pod state with describe_pod"
        );
        let init = || {
            bad_request("container \"web\" in pod \"web-1\" is waiting to start: PodInitializing")
        };
        assert!(
            unavailable(pod_log_answered("web", false, init).await)
                .contains("waiting to start (PodInitializing)")
        );
        // Перевод строки в конце ответа не мешает распознаванию.
        let none =
            || bad_request("previous terminated container \"web\" in pod \"web-1\" not found\n");
        assert_eq!(
            unavailable(pod_log_answered("web", true, none).await),
            "Container \"web\" has no previous terminated instance, so there is no previous log. Read the current log without previous, or check the pod state with describe_pod"
        );
        let terminated = || bad_request("container \"web\" in pod \"web-1\" is terminated");
        assert_eq!(
            unavailable(pod_log_answered("web", false, terminated).await),
            "The log of container \"web\" is unavailable: the container has terminated and the node no longer keeps it. Check the pod state with describe_pod"
        );
        // Другой отказ 400 — по-прежнему ошибка запроса.
        let other = || bad_request("a container name must be specified for pod web-1");
        let e = pod_log_answered("web", false, other).await.unwrap_err();
        assert_eq!(e.key, "errors.k8s.invalid");
    }

    #[tokio::test]
    async fn unavailable_logs_of_a_workload_are_errors_with_the_reason() {
        let r = reader(|s| match s.path() {
            "/api/v1/namespaces/team-a/pods/web-a/log" => (
                200,
                b"unable to retrieve container logs for containerd://0dc3".to_vec(),
            ),
            "/api/v1/namespaces/team-a/pods/web-b/log" => bad_request(
                "container \"web\" in pod \"web-b\" is waiting to start: ContainerCreating",
            ),
            _ => cluster(s),
        });
        let w = workload_logs(&r, "team-a", &web(), 600, Limits::search())
            .await
            .unwrap();
        assert!(w.logs.is_empty(), "{:?}", w.logs);
        let errors: Vec<(&str, &str)> = w
            .errors
            .iter()
            .map(|e| (e.pod.as_str(), e.error.as_str()))
            .collect();
        assert_eq!(
            errors,
            vec![
                (
                    "web-a",
                    "The log of container \"web\" is no longer available on the node: the container runtime has removed it. Check the pod state with describe_pod"
                ),
                (
                    "web-b",
                    "The log of container \"web\" is unavailable: the container is waiting to start (ContainerCreating). Check the pod state with describe_pod"
                ),
            ]
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
