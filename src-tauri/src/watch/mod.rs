//! Сессии watch открытых списков проекта (спец. §6.7): поды, workloads и события через
//! `kube::runtime::watcher`; если прав на watch нет — опрос раз в 10 с. Строки — те же, что
//! у команд списков. Изменения уходят во фронтенд пакетами в порядке, в котором пришли.

use std::collections::BTreeMap;
use std::time::Duration;

use futures::future::BoxFuture;
use futures::stream::{BoxStream, StreamExt};
use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, StatefulSet};
use k8s_openapi::api::batch::v1::{CronJob, Job};
use k8s_openapi::api::core::v1::{Event, Pod};
use kube::runtime::{WatchStreamExt, watcher};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::error::{AppError, AppResult, ErrorCode};
use crate::k8s::errors::{from_kube, from_status};
use crate::k8s::events::{EventRow, event_row, list_events};
use crate::k8s::pods::{PodRow, WorkloadRef, list_pods, pod_row};
use crate::k8s::read::ReadClient;
use crate::k8s::workloads::{
    KindError, WorkloadKind, WorkloadRow, cron_job_row, daemon_set_row, deployment_config_row,
    deployment_configs, deployment_row, job_row, list_workloads, stateful_set_row,
};
use crate::platform::ApiGroups;
use crate::sessions::{BATCH_WINDOW, Batch, Sink};

/// Как часто перечитывается список, если прав на watch нет.
pub const POLL_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum WatchKind {
    Pods,
    Workloads,
    Events,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WatchParams {
    pub connection_id: String,
    pub namespace: String,
    pub kind: WatchKind,
    /// Только поды этого workload'а (по его селектору); CronJob не поддерживается.
    #[serde(default)]
    pub workload: Option<WorkloadRef>,
}

/// Строка списка: под, workload или событие.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum Row {
    Pod(PodRow),
    Workload(WorkloadRow),
    Event(EventRow),
}

impl Row {
    pub fn key(&self) -> String {
        match self {
            Row::Pod(p) => p.name.clone(),
            Row::Workload(w) => w.key(),
            Row::Event(e) => e.key.clone(),
        }
    }
}

/// Изменение списка; применяются по порядку.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "op", rename_all = "camelCase")]
pub enum RowChange {
    Upsert { row: Row },
    Delete { key: String },
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum WatchEvent {
    /// Весь список: после первого чтения, после каждого перечитывания и при опросе. `errors` —
    /// виды workloads, которые не читаются.
    Reset {
        items: Vec<Row>,
        errors: Vec<KindError>,
    },
    Changes {
        changes: Vec<RowChange>,
    },
    /// Прав на watch нет: список перечитывается раз в 10 с.
    Polling,
    /// Ошибка чтения; `fatal` — сессия завершена (например, истекла сессия входа).
    Error {
        error: AppError,
        fatal: bool,
    },
}

/// Поток изменений одного вида объектов, уже в строках.
pub struct Source {
    /// Вид workload'а (для ошибки вида); у подов и событий — `None`.
    pub kind: Option<WorkloadKind>,
    pub stream: BoxStream<'static, watcher::Result<watcher::Event<Row>>>,
}

/// Полный список для режима опроса.
pub type Poll = Box<dyn Fn() -> BoxFuture<'static, AppResult<(Vec<Row>, Vec<KindError>)>> + Send>;

/// Чем кончилась ошибка watcher'а.
#[derive(Debug)]
enum Failure {
    /// Список не читается (403): вид без прав.
    ListForbidden(AppError),
    /// Список читается, а watch — нет: перейти на опрос.
    WatchForbidden,
    /// Дальше читать нельзя (401): сессия завершается.
    Fatal(AppError),
    /// Сеть, сервер: watcher повторит сам.
    Transient(AppError),
}

fn classify(e: &watcher::Error) -> Failure {
    let (error, listing) = match e {
        watcher::Error::InitialListFailed(k) => (from_kube(k), true),
        watcher::Error::WatchStartFailed(k) | watcher::Error::WatchFailed(k) => {
            (from_kube(k), false)
        }
        watcher::Error::WatchError(s) => (from_status(s), false),
        watcher::Error::NoResourceVersion => (
            AppError::internal_with("no resourceVersion in the list"),
            true,
        ),
    };
    match (error.code, listing) {
        (ErrorCode::SessionExpired, _) => Failure::Fatal(error),
        (ErrorCode::Forbidden, true) => Failure::ListForbidden(error),
        (ErrorCode::Forbidden, false) => Failure::WatchForbidden,
        _ => Failure::Transient(error),
    }
}

/// Что сделать с изменением потока.
#[derive(Debug)]
enum Step {
    Nothing,
    Reset(Vec<Row>, Vec<KindError>),
    Change(RowChange),
    Poll,
    Fatal(AppError),
    Transient(AppError),
}

/// Строки всех потоков: список сначала собирается (`Init` … `InitDone`) и уходит целиком, когда
/// готовы все потоки; дальше — изменения.
struct State {
    kinds: Vec<Option<WorkloadKind>>,
    stores: Vec<BTreeMap<String, Row>>,
    pending: Vec<Option<BTreeMap<String, Row>>>,
    ready: Vec<bool>,
    errors: Vec<Option<KindError>>,
    live: bool,
}

impl State {
    fn new(kinds: Vec<Option<WorkloadKind>>) -> Self {
        let n = kinds.len();
        Self {
            kinds,
            stores: vec![BTreeMap::new(); n],
            pending: vec![None; n],
            ready: vec![false; n],
            errors: vec![None; n],
            live: false,
        }
    }

    fn reset(&mut self) -> Step {
        if !self.ready.iter().all(|r| *r) {
            return Step::Nothing;
        }
        self.live = true;
        Step::Reset(
            self.stores
                .iter()
                .flat_map(|s| s.values().cloned())
                .collect(),
            self.errors.iter().flatten().cloned().collect(),
        )
    }

    fn apply(&mut self, i: usize, event: watcher::Result<watcher::Event<Row>>) -> Step {
        use watcher::Event;
        match event {
            Ok(Event::Init) => {
                self.pending[i] = Some(BTreeMap::new());
                Step::Nothing
            }
            Ok(Event::InitApply(row)) => {
                self.pending[i]
                    .get_or_insert_with(BTreeMap::new)
                    .insert(row.key(), row);
                Step::Nothing
            }
            Ok(Event::InitDone) => {
                self.stores[i] = self.pending[i].take().unwrap_or_default();
                self.ready[i] = true;
                self.errors[i] = None;
                self.reset()
            }
            Ok(Event::Apply(row)) => {
                self.stores[i].insert(row.key(), row.clone());
                match self.live {
                    true => Step::Change(RowChange::Upsert { row }),
                    false => Step::Nothing,
                }
            }
            Ok(Event::Delete(row)) => {
                let key = row.key();
                self.stores[i].remove(&key);
                match self.live {
                    true => Step::Change(RowChange::Delete { key }),
                    false => Step::Nothing,
                }
            }
            Err(e) => match classify(&e) {
                // Вид workload'а без прав не скрывает остальные; повторная ошибка — та же.
                Failure::ListForbidden(error) if self.kinds.len() > 1 => {
                    if self.errors[i].is_some() {
                        return Step::Nothing;
                    }
                    self.stores[i].clear();
                    self.ready[i] = true;
                    self.errors[i] = self.kinds[i].map(|kind| KindError { kind, error });
                    self.reset()
                }
                Failure::ListForbidden(error) | Failure::Fatal(error) => Step::Fatal(error),
                Failure::WatchForbidden => Step::Poll,
                Failure::Transient(error) => Step::Transient(error),
            },
        }
    }
}

/// Сессия: изменения потоков пакетами, а без прав на watch — опрос. Завершается по отмене, при
/// неустранимой ошибке и когда получателя больше нет.
pub async fn run(
    sources: Vec<Source>,
    poll: Poll,
    sink: impl Sink<WatchEvent>,
    cancel: CancellationToken,
) {
    let mut state = State::new(sources.iter().map(|s| s.kind).collect());
    let mut streams = futures::stream::select_all(
        sources
            .into_iter()
            .enumerate()
            .map(|(i, s)| s.stream.map(move |e| (i, e)).boxed()),
    );
    let mut batch: Batch<RowChange> = Batch::default();
    let mut tick = tokio::time::interval(BATCH_WINDOW);
    // Ошибка уже показана: повторы той же ошибки (watcher пробует снова) не шлём.
    let mut failing = false;
    let polling = loop {
        let step = tokio::select! {
            _ = cancel.cancelled() => return,
            next = streams.next() => match next {
                Some((i, event)) => state.apply(i, event),
                None => return,
            },
            _ = tick.tick() => {
                if batch.is_due() && !sink.send(WatchEvent::Changes { changes: batch.take() }) {
                    return;
                }
                continue;
            }
        };
        let sent = match step {
            Step::Nothing => true,
            Step::Reset(items, errors) => {
                failing = false;
                batch.take();
                sink.send(WatchEvent::Reset { items, errors })
            }
            Step::Change(change) => {
                failing = false;
                match batch.push(change) {
                    Some(full) => sink.send(WatchEvent::Changes { changes: full }),
                    None => true,
                }
            }
            Step::Poll => break true,
            Step::Fatal(error) => {
                sink.send(WatchEvent::Error { error, fatal: true });
                return;
            }
            Step::Transient(error) if !failing => {
                failing = true;
                sink.send(WatchEvent::Error {
                    error,
                    fatal: false,
                })
            }
            Step::Transient(_) => true,
        };
        if !sent {
            return;
        }
    };
    if polling && sink.send(WatchEvent::Polling) {
        drop(streams);
        poll_loop(poll, sink, cancel).await;
    }
}

async fn poll_loop(poll: Poll, sink: impl Sink<WatchEvent>, cancel: CancellationToken) {
    loop {
        let sent = tokio::select! {
            _ = cancel.cancelled() => return,
            r = poll() => match r {
                Ok((items, errors)) => sink.send(WatchEvent::Reset { items, errors }),
                Err(error) if error.code == ErrorCode::SessionExpired => {
                    sink.send(WatchEvent::Error { error, fatal: true });
                    return;
                }
                Err(error) => sink.send(WatchEvent::Error { error, fatal: false }),
            },
        };
        if !sent {
            return;
        }
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(POLL_INTERVAL) => {}
        }
    }
}

/// Событие watcher'а со строкой вместо объекта.
fn to_rows<K>(
    e: watcher::Result<watcher::Event<K>>,
    f: impl Fn(&K) -> Row,
) -> watcher::Result<watcher::Event<Row>> {
    use watcher::Event;
    e.map(|e| match e {
        Event::Apply(k) => Event::Apply(f(&k)),
        Event::Delete(k) => Event::Delete(f(&k)),
        Event::Init => Event::Init,
        Event::InitApply(k) => Event::InitApply(f(&k)),
        Event::InitDone => Event::InitDone,
    })
}

/// Потоки вида: поды (по селектору workload'а), workloads всех видов или события.
pub fn sources(
    r: &ReadClient,
    namespace: &str,
    kind: WatchKind,
    selector: Option<&str>,
    groups: &ApiGroups,
) -> Vec<Source> {
    let config = || match selector.filter(|s| !s.is_empty()) {
        Some(s) => watcher::Config::default().labels(s),
        None => watcher::Config::default(),
    };
    macro_rules! typed {
        ($kind:expr, $t:ty, $f:expr) => {
            Source {
                kind: $kind,
                stream: r
                    .watch::<$t>(namespace, config())
                    .default_backoff()
                    .map(|e| to_rows(e, $f))
                    .boxed(),
            }
        };
    }
    match kind {
        WatchKind::Pods => vec![typed!(None, Pod, |p| Row::Pod(pod_row(p)))],
        WatchKind::Events => vec![typed!(None, Event, |e| Row::Event(event_row(e)))],
        WatchKind::Workloads => {
            let mut out = vec![
                typed!(Some(WorkloadKind::Deployment), Deployment, |o| {
                    Row::Workload(deployment_row(o))
                }),
                typed!(Some(WorkloadKind::StatefulSet), StatefulSet, |o| {
                    Row::Workload(stateful_set_row(o))
                }),
                typed!(Some(WorkloadKind::DaemonSet), DaemonSet, |o| {
                    Row::Workload(daemon_set_row(o))
                }),
                typed!(Some(WorkloadKind::Job), Job, |o| Row::Workload(job_row(o))),
                typed!(Some(WorkloadKind::CronJob), CronJob, |o| {
                    Row::Workload(cron_job_row(o))
                }),
            ];
            if WorkloadKind::DeploymentConfig.available(groups) {
                out.push(Source {
                    kind: Some(WorkloadKind::DeploymentConfig),
                    stream: r
                        .watch_dynamic(&deployment_configs(), namespace, config())
                        .default_backoff()
                        .map(|e| to_rows(e, |o| Row::Workload(deployment_config_row(o))))
                        .boxed(),
                });
            }
            out
        }
    }
}

/// Опрос вида: те же чтения, что у команд списков.
pub fn poll(
    r: ReadClient,
    namespace: String,
    kind: WatchKind,
    workload: Option<WorkloadRef>,
    groups: ApiGroups,
) -> Poll {
    Box::new(move || {
        let (r, ns, workload) = (r.clone(), namespace.clone(), workload.clone());
        Box::pin(async move {
            Ok(match kind {
                WatchKind::Pods => (
                    list_pods(&r, &ns, workload.as_ref())
                        .await?
                        .items
                        .into_iter()
                        .map(Row::Pod)
                        .collect(),
                    vec![],
                ),
                WatchKind::Events => (
                    list_events(&r, &ns, None)
                        .await?
                        .items
                        .into_iter()
                        .map(Row::Event)
                        .collect(),
                    vec![],
                ),
                WatchKind::Workloads => {
                    let l = list_workloads(&r, &ns, &groups).await;
                    (l.items.into_iter().map(Row::Workload).collect(), l.errors)
                }
            })
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sessions::Collect;
    use watcher::Event as E;

    fn pod(name: &str, status: &str) -> Row {
        let mut p: Pod = serde_json::from_value(crate::k8s::pods::fixtures::running(name)).unwrap();
        p.status.as_mut().unwrap().phase = Some(status.into());
        Row::Pod(pod_row(&p))
    }

    fn api_error(code: u16) -> kube::Error {
        kube::Error::Api(
            kube::core::Status::failure(
                "denied",
                if code == 403 {
                    "Forbidden"
                } else {
                    "Unauthorized"
                },
            )
            .with_code(code)
            .boxed(),
        )
    }

    /// Поток, который отдаёт события и дальше молчит, как живой watch.
    fn source(
        kind: Option<WorkloadKind>,
        events: Vec<watcher::Result<watcher::Event<Row>>>,
    ) -> Source {
        Source {
            kind,
            stream: futures::stream::iter(events)
                .chain(futures::stream::pending())
                .boxed(),
        }
    }

    fn no_poll() -> Poll {
        Box::new(|| Box::pin(async { panic!("опроса быть не должно") }))
    }

    /// Запускает сессию на `wait` (время на паузе) и возвращает всё, что она отправила.
    async fn session(sources: Vec<Source>, poll: Poll, wait: Duration) -> Vec<WatchEvent> {
        let sink = Collect::default();
        let cancel = CancellationToken::new();
        let task = tokio::spawn(run(sources, poll, sink.clone(), cancel.clone()));
        tokio::time::sleep(wait).await;
        cancel.cancel();
        task.await.unwrap();
        sink.items()
    }

    fn summary(events: &[WatchEvent]) -> Vec<String> {
        events
            .iter()
            .map(|e| match e {
                WatchEvent::Reset { items, errors } => format!(
                    "reset {} {}",
                    items.iter().map(Row::key).collect::<Vec<_>>().join(","),
                    errors
                        .iter()
                        .map(|e| format!("{:?}", e.kind))
                        .collect::<Vec<_>>()
                        .join(",")
                ),
                WatchEvent::Changes { changes } => format!(
                    "changes {}",
                    changes
                        .iter()
                        .map(|c| match c {
                            RowChange::Upsert { row } => format!("+{}", row.key()),
                            RowChange::Delete { key } => format!("-{key}"),
                        })
                        .collect::<Vec<_>>()
                        .join(",")
                ),
                WatchEvent::Polling => "polling".into(),
                WatchEvent::Error { error, fatal } => format!("error {} {fatal}", error.key),
            })
            .collect()
    }

    #[tokio::test(start_paused = true)]
    async fn list_then_changes_in_order() {
        let s = source(
            None,
            vec![
                Ok(E::Init),
                Ok(E::InitApply(pod("a", "Running"))),
                Ok(E::InitApply(pod("b", "Running"))),
                Ok(E::InitDone),
                Ok(E::Apply(pod("c", "Pending"))),
                Ok(E::Delete(pod("a", "Running"))),
                Ok(E::Apply(pod("a", "Running"))),
            ],
        );
        let events = session(vec![s], no_poll(), Duration::from_secs(1)).await;
        assert_eq!(summary(&events), ["reset a,b ", "changes +c,-a,+a"]);
    }

    #[tokio::test(start_paused = true)]
    async fn workloads_wait_for_every_kind_and_report_forbidden_ones() {
        let row = |name: &str| {
            Row::Workload(deployment_row(
                &serde_json::from_value(crate::k8s::workloads::fixtures::deployment(name, 1, 1))
                    .unwrap(),
            ))
        };
        let deployments = source(
            Some(WorkloadKind::Deployment),
            vec![Ok(E::Init), Ok(E::InitApply(row("web"))), Ok(E::InitDone)],
        );
        let forbidden = || Err(watcher::Error::InitialListFailed(api_error(403)));
        let cron = source(Some(WorkloadKind::CronJob), vec![forbidden(), forbidden()]);
        let events = session(vec![deployments, cron], no_poll(), Duration::from_secs(1)).await;
        assert_eq!(summary(&events), ["reset Deployment/web CronJob"]);
    }

    #[tokio::test(start_paused = true)]
    async fn without_watch_rights_the_list_is_polled() {
        let s = source(
            None,
            vec![
                Ok(E::Init),
                Ok(E::InitDone),
                Err(watcher::Error::WatchStartFailed(api_error(403))),
            ],
        );
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let poll: Poll = {
            let calls = calls.clone();
            Box::new(move || {
                let n = calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Box::pin(async move { Ok((vec![pod(&format!("p{n}"), "Running")], vec![])) })
            })
        };
        let events = session(vec![s], poll, Duration::from_secs(25)).await;
        assert_eq!(
            summary(&events),
            ["reset  ", "polling", "reset p0 ", "reset p1 ", "reset p2 "]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn expired_session_ends_and_transient_errors_are_reported_once() {
        let s = source(
            None,
            vec![
                Err(watcher::Error::InitialListFailed(api_error(500))),
                Err(watcher::Error::InitialListFailed(api_error(500))),
                Ok(E::Init),
                Ok(E::InitDone),
                Err(watcher::Error::WatchFailed(api_error(401))),
                Ok(E::Apply(pod("late", "Running"))),
            ],
        );
        let events = session(vec![s], no_poll(), Duration::from_secs(1)).await;
        assert_eq!(
            summary(&events),
            [
                "error errors.k8s.serverError false",
                "reset  ",
                "error errors.k8s.unauthorized true"
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_single_kind_without_list_rights_is_fatal() {
        let s = source(
            None,
            vec![Err(watcher::Error::InitialListFailed(api_error(403)))],
        );
        let events = session(vec![s], no_poll(), Duration::from_secs(1)).await;
        assert_eq!(summary(&events), ["error errors.k8s.forbidden true"]);
    }

    /// Настоящий watcher против мока: список читается, watch запрещён — опрос.
    #[tokio::test]
    async fn real_watcher_falls_back_to_polling() {
        let (client, _server) = crate::k8s::testing::mock_client(|s| {
            if s.uri.contains("watch=true") {
                crate::k8s::testing::status(
                    403,
                    "Forbidden",
                    r#"pods is forbidden: User "u" cannot watch resource "pods" in API group "" in the namespace "team-a""#,
                )
            } else {
                (
                    200,
                    serde_json::json!({"apiVersion": "v1", "kind": "PodList",
                        "metadata": {"resourceVersion": "7"},
                        "items": [crate::k8s::pods::fixtures::running("web-1")]}),
                )
            }
        });
        let r = ReadClient::new(client, Duration::from_secs(5));
        let groups = ApiGroups::default();
        let sink = Collect::default();
        let cancel = CancellationToken::new();
        let task = tokio::spawn(run(
            sources(&r, "team-a", WatchKind::Pods, None, &groups),
            poll(r.clone(), "team-a".into(), WatchKind::Pods, None, groups),
            sink.clone(),
            cancel.clone(),
        ));
        let polled = async {
            while summary(&sink.items()).len() < 3 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        };
        tokio::time::timeout(Duration::from_secs(10), polled)
            .await
            .expect("перешли на опрос");
        cancel.cancel();
        task.await.unwrap();
        assert_eq!(
            summary(&sink.items())[..3],
            ["reset web-1 ", "polling", "reset web-1 "]
        );
    }
}
