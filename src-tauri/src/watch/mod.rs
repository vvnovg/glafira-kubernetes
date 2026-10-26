//! Сессии watch открытых списков проекта (спец. §6.7): поды, workloads и события через
//! `kube::runtime::watcher`; если прав на watch нет — опрос раз в 10 с. Строки — те же, что
//! у команд списков, и их не больше `MAX_ITEMS` на сессию (`truncated`). Изменения уходят во
//! фронтенд пакетами в порядке, в котором пришли.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use futures::stream::{BoxStream, StreamExt};
use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, StatefulSet};
use k8s_openapi::api::batch::v1::{CronJob, Job};
use k8s_openapi::api::core::v1::{Event, Pod};
use kube::runtime::{WatchStreamExt, watcher};
use serde::{Deserialize, Serialize};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::error::{AppError, AppResult, ErrorCode};
use crate::k8s::errors::{breaks_client, from_kube, from_status, timeout};
use crate::k8s::events::{EventRow, event_row, list_events};
use crate::k8s::pods::{PodRow, WorkloadRef, list_pods, pod_row};
use crate::k8s::read::{MAX_ITEMS, ReadClient};
use crate::k8s::workloads::{
    KindError, WorkloadKind, WorkloadRow, cron_job_row, daemon_set_row, deployment_config_row,
    deployment_configs, deployment_row, job_row, list_workloads, stateful_set_row,
};
use crate::platform::ApiGroups;
use crate::sessions::{BATCH_WINDOW, Batch, Sink, StreamClient};

/// Как часто перечитывается список, если прав на watch нет.
pub const POLL_INTERVAL: Duration = Duration::from_secs(10);

/// Через столько секунд сервер сам заканчивает каждый запрос watch (`timeoutSeconds`), и watcher
/// начинает новый: живой watch не молчит дольше таймаута чтения клиента
/// (`k8s::config::READ_TIMEOUT`), а watch на мёртвом соединении тот обрывает (спец. §4.1).
pub const WATCH_TIMEOUT: u32 = 290;

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
    /// виды workloads, которые не читаются; `truncated` — объектов больше `MAX_ITEMS`, в списке
    /// не все.
    Reset {
        items: Vec<Row>,
        errors: Vec<KindError>,
        truncated: bool,
    },
    /// Изменения по порядку. Пакет без изменений тоже бывает: сервер снова отвечает, и ошибка
    /// без `fatal` больше не показывается.
    Changes { changes: Vec<RowChange> },
    /// Прав на watch нет: список перечитывается раз в 10 с.
    Polling,
    /// Ошибка чтения; `fatal` — сессия завершена (например, истекла сессия входа).
    Error { error: AppError, fatal: bool },
}

/// Весь список для `Reset`.
#[derive(Debug, Default)]
pub struct Snapshot {
    pub items: Vec<Row>,
    pub errors: Vec<KindError>,
    pub truncated: bool,
}

impl From<Snapshot> for WatchEvent {
    fn from(s: Snapshot) -> Self {
        WatchEvent::Reset {
            items: s.items,
            errors: s.errors,
            truncated: s.truncated,
        }
    }
}

/// События watcher'а одного вида, уже в строках.
pub type Events = BoxStream<'static, watcher::Result<watcher::Event<Row>>>;

/// Поток изменений одного вида объектов.
pub struct Source {
    /// Вид workload'а (для ошибки вида); у подов и событий — `None`.
    pub kind: Option<WorkloadKind>,
    pub stream: Events,
    /// Как начать поток заново, если страница списка не пришла вовремя; `None` — ждать сколько
    /// угодно.
    pub restart: Option<Restart>,
}

/// Таймаут чтения списка. У watcher'а его нет, а таймаут чтения клиента (`READ_TIMEOUT`, почти
/// 5 минут) — только для мёртвого соединения. Поэтому, пока вид читает список (`Init` …
/// `InitDone`), каждая страница ждётся не дольше `after`; не пришла — поток начинается заново
/// (`open`), а сессия сообщает о таймауте.
pub struct Restart {
    /// `ReadClient::timeout`.
    pub after: Duration,
    /// Поток тем клиентом, который у вида сейчас (`client`).
    pub open: Box<dyn Fn() -> Events + Send>,
    /// Клиент потока. После ошибки, с которой клиент дальше не работает (истекли учётные данные
    /// exec-плагина), поток начинается заново новым клиентом (`StreamClient::renew`), а не кончает
    /// сессию. `None` — такая ошибка кончает сессию.
    pub client: Option<Arc<StreamClient>>,
}

/// Полный список для режима опроса.
pub type Poll = Box<dyn Fn() -> BoxFuture<'static, AppResult<Snapshot>> + Send>;

/// Получает ошибки запросов сессии (кроме 410 в потоке watch); что с ними делать (сбросить
/// клиент подключения после `breaks_client`, забыть права проекта после 403), решает тот, кто
/// начал сессию.
pub type Report = Box<dyn Fn(&AppError) + Send + Sync>;

/// Чем кончилась ошибка watcher'а.
#[derive(Debug)]
enum Failure {
    /// Список вида не прочитан: нет прав, сервер, сеть.
    List(AppError),
    /// Список читается, а watch — нет (403): перейти на опрос.
    WatchForbidden(AppError),
    /// Дальше читать нельзя: истекла сессия входа, учётные данные или TLS (`breaks_client`).
    /// Сессия завершается.
    Fatal(AppError),
    /// Watch прервался (сеть, сервер): watcher повторит сам.
    Transient(AppError),
    /// 410 Gone в потоке watch: версия списка устарела, watcher сам перечитает список.
    Resync,
}

impl Failure {
    /// Ошибка чтения, о которой узнаёт `Report`; у 410 её нет.
    fn error(&self) -> Option<&AppError> {
        match self {
            Failure::List(e)
            | Failure::WatchForbidden(e)
            | Failure::Fatal(e)
            | Failure::Transient(e) => Some(e),
            Failure::Resync => None,
        }
    }
}

fn classify(e: &watcher::Error) -> Failure {
    let (error, listing) = match e {
        // Ответ 410 на сам запрос watch watcher так не обрабатывает: это ошибка.
        watcher::Error::WatchError(s) if s.code == 410 => return Failure::Resync,
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
        (code, _) if breaks_client(code) => Failure::Fatal(error),
        (_, true) => Failure::List(error),
        (ErrorCode::Forbidden, false) => Failure::WatchForbidden(error),
        _ => Failure::Transient(error),
    }
}

/// Что пришло из потока вида: событие watcher'а или `None` — страница списка не пришла за
/// `Restart::after`, и поток начат заново.
type Next = Option<watcher::Result<watcher::Event<Row>>>;

/// Ответил сервер: строка или конец списка, изменение. `Init` watcher отдаёт сам, до запроса.
fn is_answer(next: &Next) -> bool {
    use watcher::Event as E;
    matches!(
        next,
        Some(Ok(E::InitApply(_)
            | E::InitDone
            | E::Apply(_)
            | E::Delete(_)))
    )
}

/// Поток вида с таймаутом чтения списка и новым клиентом после ошибки, с которой прежний
/// дальше не работает (`Restart`).
fn guarded(source: Source) -> BoxStream<'static, Next> {
    use watcher::Event as E;
    let Some(restart) = source.restart else {
        return source.stream.map(Some).boxed();
    };
    let deadline = Some(Instant::now() + restart.after);
    futures::stream::unfold(
        (source.stream, restart, deadline),
        |(mut stream, restart, mut deadline)| async move {
            loop {
                let next = match deadline {
                    Some(at) => match tokio::time::timeout_at(at, stream.next()).await {
                        Ok(next) => next,
                        Err(_) => {
                            // Зависший запрос обрывается вместе с прежним потоком.
                            let stream = (restart.open)();
                            let deadline = Some(Instant::now() + restart.after);
                            return Some((None, (stream, restart, deadline)));
                        }
                    },
                    None => stream.next().await,
                };
                let event = next?;
                // Кластер не принял клиент: после каждого запроса watch, который сервер закончил
                // (`WATCH_TIMEOUT`), watcher начинает новый тем же клиентом, а учётные данные
                // exec-плагина уже истекли. Поток начинается заново новым клиентом; не вышло —
                // ошибка идёт дальше и кончает сессию.
                let renew = renewal(&restart, &event);
                if let Some((client, error)) = renew
                    && client.renew(&error).await.unwrap_or(false)
                {
                    stream = (restart.open)();
                    deadline = Some(Instant::now() + restart.after);
                    continue;
                }
                let deadline = match &event {
                    // Список начат или пришла его строка: ждём следующую страницу.
                    Ok(E::Init | E::InitApply(_)) => Some(Instant::now() + restart.after),
                    // Список прочитан: простой watch watcher отслеживает сам. После ошибки он
                    // ждёт перед повтором (до 30 с) и начинает список снова с `Init`.
                    Ok(E::InitDone) | Err(_) => None,
                    Ok(E::Apply(_) | E::Delete(_)) => deadline,
                };
                return Some((Some(event), (stream, restart, deadline)));
            }
        },
    )
    .boxed()
}

/// Клиент потока и ошибка события, если с ней клиент дальше не работает (`Failure::Fatal`).
fn renewal(
    restart: &Restart,
    event: &watcher::Result<watcher::Event<Row>>,
) -> Option<(Arc<StreamClient>, AppError)> {
    let client = restart.client.clone()?;
    match classify(event.as_ref().err()?) {
        Failure::Fatal(error) => Some((client, error)),
        _ => None,
    }
}

/// Что сделать с изменением потока.
#[derive(Debug)]
enum Step {
    Nothing,
    Reset(Snapshot),
    Change(RowChange),
    Poll,
    Fatal(AppError),
    Transient(AppError),
}

/// Строки всех потоков: список сначала собирается (`Init` … `InitDone`) и уходит целиком, когда
/// готовы все потоки; дальше — изменения. Строк в сессии не больше `MAX_ITEMS` на все виды:
/// лишние строки списка пропускаются (watcher дочитывает список до `InitDone`), и список —
/// `truncated`. Изменение строки, которой в сессии нет, берётся, только пока строк меньше
/// `MAX_ITEMS`: это может быть объект, который в список не попал. В первый раз, когда из-за
/// этого список становится `truncated`, уходит `Reset`. Список снова полный, когда вид
/// перечитает его целиком.
struct State {
    kinds: Vec<Option<WorkloadKind>>,
    stores: Vec<BTreeMap<String, Row>>,
    pending: Vec<Option<BTreeMap<String, Row>>>,
    /// В список вида попали не все объекты: в показанный (`truncated`) и в тот, что читается.
    truncated: Vec<bool>,
    pending_truncated: Vec<bool>,
    ready: Vec<bool>,
    errors: Vec<Option<KindError>>,
    live: bool,
    report: Report,
}

impl State {
    fn new(kinds: Vec<Option<WorkloadKind>>, report: Report) -> Self {
        let n = kinds.len();
        Self {
            kinds,
            stores: vec![BTreeMap::new(); n],
            pending: vec![None; n],
            truncated: vec![false; n],
            pending_truncated: vec![false; n],
            ready: vec![false; n],
            errors: vec![None; n],
            live: false,
            report,
        }
    }

    /// Строк в сессии, кроме вида `except`. У вида, который перечитывает список, — больше из
    /// прежнего и нового: так строк не станет больше `MAX_ITEMS` и когда список дочитается.
    fn held(&self, except: Option<usize>) -> usize {
        (0..self.kinds.len())
            .filter(|j| Some(*j) != except)
            .map(|j| {
                let pending = self.pending[j].as_ref().map_or(0, BTreeMap::len);
                self.stores[j].len().max(pending)
            })
            .sum()
    }

    fn is_truncated(&self) -> bool {
        self.truncated.iter().any(|t| *t)
    }

    fn reset(&mut self) -> Step {
        if !self.ready.iter().all(|r| *r) {
            return Step::Nothing;
        }
        self.live = true;
        Step::Reset(Snapshot {
            items: self
                .stores
                .iter()
                .flat_map(|s| s.values().cloned())
                .collect(),
            errors: self.errors.iter().flatten().cloned().collect(),
            truncated: self.is_truncated(),
        })
    }

    /// Список вида не прочитан. Вид workload'а не скрывает остальные: он готов — без строк, с
    /// ошибкой вида (`errors`), и уходит `Reset`; повторная ошибка — та же, пока список не
    /// прочитается (`InitDone`). Единственный вид без прав на список завершает сессию, а сеть
    /// и сервер watcher повторит сам.
    fn list_failed(&mut self, i: usize, error: AppError) -> Step {
        self.pending[i] = None;
        if self.kinds.len() > 1 {
            if self.errors[i].is_some() {
                return Step::Nothing;
            }
            self.stores[i].clear();
            self.truncated[i] = false;
            self.ready[i] = true;
            self.errors[i] = self.kinds[i].map(|kind| KindError { kind, error });
            return self.reset();
        }
        match error.code {
            ErrorCode::Forbidden => Step::Fatal(error),
            _ => Step::Transient(error),
        }
    }

    fn apply(&mut self, i: usize, next: Next) -> Step {
        use watcher::Event;
        let event = match next {
            Some(event) => event,
            None => return self.list_failed(i, timeout()),
        };
        match event {
            Ok(Event::Init) => {
                self.pending[i] = Some(BTreeMap::new());
                self.pending_truncated[i] = false;
                Step::Nothing
            }
            Ok(Event::InitApply(row)) => {
                let held = self.held(Some(i)) + self.pending[i].as_ref().map_or(0, BTreeMap::len);
                let pending = self.pending[i].get_or_insert_with(BTreeMap::new);
                let key = row.key();
                if held < MAX_ITEMS || pending.contains_key(&key) {
                    pending.insert(key, row);
                } else {
                    self.pending_truncated[i] = true;
                }
                Step::Nothing
            }
            Ok(Event::InitDone) => {
                self.stores[i] = self.pending[i].take().unwrap_or_default();
                self.truncated[i] = std::mem::take(&mut self.pending_truncated[i]);
                self.ready[i] = true;
                self.errors[i] = None;
                self.reset()
            }
            Ok(Event::Apply(row)) => {
                let key = row.key();
                if !self.stores[i].contains_key(&key) && self.held(None) >= MAX_ITEMS {
                    let was = self.is_truncated();
                    self.truncated[i] = true;
                    return match self.live && !was {
                        true => self.reset(),
                        false => Step::Nothing,
                    };
                }
                self.stores[i].insert(key, row.clone());
                match self.live {
                    true => Step::Change(RowChange::Upsert { row }),
                    false => Step::Nothing,
                }
            }
            Ok(Event::Delete(row)) => {
                let key = row.key();
                match self.stores[i].remove(&key).is_some() && self.live {
                    true => Step::Change(RowChange::Delete { key }),
                    false => Step::Nothing,
                }
            }
            Err(e) => {
                let failure = classify(&e);
                if let Some(error) = failure.error() {
                    (self.report)(error);
                }
                match failure {
                    Failure::Resync => Step::Nothing,
                    Failure::List(error) => self.list_failed(i, error),
                    Failure::WatchForbidden(_) => Step::Poll,
                    Failure::Fatal(error) => Step::Fatal(error),
                    Failure::Transient(error) => Step::Transient(error),
                }
            }
        }
    }
}

/// Отправляет накопленные изменения, если они есть. `false` — получателя больше нет.
fn flush(batch: &mut Batch<RowChange>, sink: &impl Sink<WatchEvent>) -> bool {
    batch.is_empty()
        || sink.send(WatchEvent::Changes {
            changes: batch.take(),
        })
}

/// Сессия: изменения потоков пакетами, а без прав на watch — опрос. Завершается по отмене, при
/// неустранимой ошибке и когда получателя больше нет.
pub async fn run(
    sources: Vec<Source>,
    poll: Poll,
    sink: impl Sink<WatchEvent>,
    cancel: CancellationToken,
) {
    run_reporting(sources, poll, sink, cancel, Box::new(|_| {})).await;
}

/// `run`, которая отдаёт ошибки чтения в `report`.
pub async fn run_reporting(
    sources: Vec<Source>,
    poll: Poll,
    sink: impl Sink<WatchEvent>,
    cancel: CancellationToken,
    report: Report,
) {
    if sources.is_empty() {
        // Смотреть нечего (у workload'а нет селектора подов): список пуст, как у `list_pods`.
        sink.send(Snapshot::default().into());
        return;
    }
    let mut state = State::new(sources.iter().map(|s| s.kind).collect(), report);
    let mut streams = futures::stream::select_all(
        sources
            .into_iter()
            .enumerate()
            .map(|(i, s)| guarded(s).map(move |n| (i, n)).boxed()),
    );
    let mut batch: Batch<RowChange> = Batch::default();
    let mut tick = tokio::time::interval(BATCH_WINDOW);
    // Интерфейс показывает ошибку без `fatal`, пока не придёт `Reset` или `Changes`: повторы
    // (watcher пробует снова) до тех пор не шлём.
    let mut shown = false;
    let polling = loop {
        let (step, answered) = tokio::select! {
            _ = cancel.cancelled() => return,
            next = streams.next() => match next {
                Some((i, next)) => {
                    let answered = is_answer(&next);
                    (state.apply(i, next), answered)
                }
                None => return,
            },
            _ = tick.tick() => {
                if batch.is_due() {
                    if !sink.send(WatchEvent::Changes { changes: batch.take() }) {
                        return;
                    }
                    shown = false;
                }
                continue;
            }
        };
        let sent = match step {
            // Сервер снова отвечает, а `Reset` и `Changes` ещё не идут (список перечитывается):
            // пакет без изменений убирает ошибку из интерфейса.
            Step::Nothing if shown && answered && state.live => {
                shown = false;
                sink.send(WatchEvent::Changes {
                    changes: batch.take(),
                })
            }
            Step::Nothing => true,
            Step::Reset(snapshot) => {
                shown = false;
                batch.take();
                sink.send(snapshot.into())
            }
            Step::Change(change) => match batch.push(change) {
                Some(full) => {
                    shown = false;
                    sink.send(WatchEvent::Changes { changes: full })
                }
                None => true,
            },
            Step::Poll => break flush(&mut batch, &sink),
            Step::Fatal(error) => {
                if flush(&mut batch, &sink) {
                    sink.send(WatchEvent::Error { error, fatal: true });
                }
                return;
            }
            // Ошибка уходит после изменений, пришедших раньше: пакет `Changes` убрал бы её.
            Step::Transient(error) if !shown || !batch.is_empty() => {
                shown = true;
                flush(&mut batch, &sink)
                    && sink.send(WatchEvent::Error {
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
        poll_loop(poll, sink, cancel, state.report).await;
    }
}

async fn poll_loop(
    poll: Poll,
    sink: impl Sink<WatchEvent>,
    cancel: CancellationToken,
    report: Report,
) {
    loop {
        let sent = tokio::select! {
            _ = cancel.cancelled() => return,
            r = poll() => match r {
                Ok(snapshot) => {
                    snapshot.errors.iter().for_each(|e| report(&e.error));
                    // Ошибка вида, с которой клиент дальше не работает, — ошибка всей сессии.
                    if let Some(e) = snapshot.errors.iter().find(|e| breaks_client(e.error.code)) {
                        sink.send(WatchEvent::Error { error: e.error.clone(), fatal: true });
                        return;
                    }
                    sink.send(snapshot.into())
                }
                Err(error) => {
                    report(&error);
                    if breaks_client(error.code) {
                        sink.send(WatchEvent::Error { error, fatal: true });
                        return;
                    }
                    sink.send(WatchEvent::Error { error, fatal: false })
                }
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

/// Источник со своим клиентом (`StreamClient::fork`) и таймаутом чтения списка — таймаутом
/// запроса клиента: `open` даёт поток текущим клиентом и при запуске, и заново.
fn restarting(
    kind: Option<WorkloadKind>,
    client: &StreamClient,
    open: impl Fn(&ReadClient) -> Events + Send + 'static,
) -> Source {
    let client = Arc::new(client.fork());
    let after = client.reader().timeout();
    let open = {
        let client = client.clone();
        move || open(&client.reader())
    };
    Source {
        kind,
        stream: open(),
        restart: Some(Restart {
            after,
            open: Box::new(open),
            client: Some(client),
        }),
    }
}

/// Настройки watcher'а: селектор меток и `WATCH_TIMEOUT` для каждого запроса.
fn watch_config(selector: Option<&str>) -> watcher::Config {
    let config = watcher::Config::default().timeout(WATCH_TIMEOUT);
    match selector {
        Some(s) => config.labels(s),
        None => config,
    }
}

/// Потоки вида: поды (по селектору workload'а), workloads всех видов или события. Пустой
/// селектор подошёл бы всем объектам проекта, а не подам workload'а: потоков нет, как и подов
/// у `list_pods`. У каждого потока свой клиент от `client` (`StreamClient::fork`).
pub fn sources(
    client: &StreamClient,
    namespace: &str,
    kind: WatchKind,
    selector: Option<&str>,
    groups: &ApiGroups,
) -> Vec<Source> {
    if selector.is_some_and(str::is_empty) {
        return Vec::new();
    }
    let config = || watch_config(selector);
    macro_rules! typed {
        ($kind:expr, $t:ty, $f:expr) => {{
            let (ns, config) = (namespace.to_string(), config());
            restarting($kind, client, move |r| {
                r.watch::<$t>(&ns, config.clone())
                    .default_backoff()
                    .map(|e| to_rows(e, $f))
                    .boxed()
            })
        }};
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
                let (ns, config, ar) = (namespace.to_string(), config(), deployment_configs());
                out.push(restarting(
                    Some(WorkloadKind::DeploymentConfig),
                    client,
                    move |r| {
                        r.watch_dynamic(&ar, &ns, config.clone())
                            .default_backoff()
                            .map(|e| to_rows(e, |o| Row::Workload(deployment_config_row(o))))
                            .boxed()
                    },
                ));
            }
            out
        }
    }
}

/// Опрос вида: те же чтения, что у команд списков. Опрос идёт дольше учётных данных exec-плагина,
/// поэтому перед каждым клиент берётся заново (`StreamClient::refresh`), а если кластер его не
/// принял (учётные данные истекли, а срока плагин не сообщил), список читается ещё раз новым
/// клиентом (`StreamClient::renew`).
pub fn poll(
    client: StreamClient,
    namespace: String,
    kind: WatchKind,
    workload: Option<WorkloadRef>,
    groups: ApiGroups,
) -> Poll {
    let client = Arc::new(client);
    Box::new(move || {
        let (client, ns, workload) = (client.clone(), namespace.clone(), workload.clone());
        Box::pin(async move {
            client.refresh().await?;
            let first = snapshot(&client.reader(), &ns, kind, workload.as_ref(), &groups).await;
            match broken(&first) {
                Some(e) if client.renew(&e).await.unwrap_or(false) => {
                    snapshot(&client.reader(), &ns, kind, workload.as_ref(), &groups).await
                }
                _ => first,
            }
        })
    })
}

/// Полный список вида.
async fn snapshot(
    r: &ReadClient,
    ns: &str,
    kind: WatchKind,
    workload: Option<&WorkloadRef>,
    groups: &ApiGroups,
) -> AppResult<Snapshot> {
    Ok(match kind {
        WatchKind::Pods => {
            let l = list_pods(r, ns, workload).await?;
            Snapshot {
                items: l.items.into_iter().map(Row::Pod).collect(),
                errors: vec![],
                truncated: l.truncated,
            }
        }
        WatchKind::Events => {
            let l = list_events(r, ns, None).await?;
            Snapshot {
                items: l.items.into_iter().map(Row::Event).collect(),
                errors: vec![],
                truncated: l.truncated,
            }
        }
        WatchKind::Workloads => {
            let l = list_workloads(r, ns, groups).await;
            Snapshot {
                items: l.items.into_iter().map(Row::Workload).collect(),
                errors: l.errors,
                truncated: l.truncated,
            }
        }
    })
}

/// Ошибка опроса, с которой клиент дальше не работает: самого чтения или вида workloads.
fn broken(r: &AppResult<Snapshot>) -> Option<AppError> {
    match r {
        Err(e) => breaks_client(e.code).then(|| e.clone()),
        Ok(s) => s
            .errors
            .iter()
            .find(|e| breaks_client(e.error.code))
            .map(|e| e.error.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sessions::Collect;
    use crate::sync::MutexExt;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use watcher::Event as E;

    fn pod(name: &str, status: &str) -> Row {
        let mut p: Pod = serde_json::from_value(crate::k8s::pods::fixtures::running(name)).unwrap();
        p.status.as_mut().unwrap().phase = Some(status.into());
        Row::Pod(pod_row(&p))
    }

    fn workload(kind: WorkloadKind, name: &str) -> Row {
        let mut w = deployment_row(
            &serde_json::from_value(crate::k8s::workloads::fixtures::deployment(name, 1, 1))
                .unwrap(),
        );
        w.kind = kind;
        Row::Workload(w)
    }

    /// Строки списка `n` объектов: `prefix00000`, `prefix00001`…
    fn many(base: Row, prefix: &str, n: usize) -> Vec<Row> {
        (0..n)
            .map(|k| {
                let mut row = base.clone();
                let name = format!("{prefix}{k:05}");
                match &mut row {
                    Row::Pod(p) => p.name = name,
                    Row::Workload(w) => w.name = name,
                    Row::Event(e) => e.key = name,
                }
                row
            })
            .collect()
    }

    /// События полного списка `rows`.
    fn listed(rows: Vec<Row>) -> Vec<watcher::Result<watcher::Event<Row>>> {
        std::iter::once(Ok(E::Init))
            .chain(rows.into_iter().map(|r| Ok(E::InitApply(r))))
            .chain(std::iter::once(Ok(E::InitDone)))
            .collect()
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

    /// Ошибка клиента, когда сертификат сервера не прошёл проверку.
    fn tls_error() -> kube::Error {
        kube::Error::Service(Box::new(std::io::Error::other(
            "invalid peer certificate: UnknownIssuer",
        )))
    }

    /// Поток, который отдаёт события и дальше молчит, как живой watch.
    fn stream(events: Vec<watcher::Result<watcher::Event<Row>>>) -> Events {
        futures::stream::iter(events)
            .chain(futures::stream::pending())
            .boxed()
    }

    fn source(
        kind: Option<WorkloadKind>,
        events: Vec<watcher::Result<watcher::Event<Row>>>,
    ) -> Source {
        Source {
            kind,
            stream: stream(events),
            restart: None,
        }
    }

    /// Поток, который отдаёт `first`, через `pause` — `then` и дальше молчит.
    fn later(
        first: Vec<watcher::Result<watcher::Event<Row>>>,
        pause: Duration,
        then: Vec<watcher::Result<watcher::Event<Row>>>,
    ) -> Events {
        futures::stream::iter(first)
            .chain(futures::stream::once(tokio::time::sleep(pause)).filter_map(|_| async { None }))
            .chain(stream(then))
            .boxed()
    }

    /// Таймаут чтения списка 30 с; `open` считает, сколько раз поток начинали заново.
    fn restart(
        opens: &Arc<AtomicUsize>,
        open: impl Fn(usize) -> Events + Send + 'static,
    ) -> Option<Restart> {
        let opens = opens.clone();
        Some(Restart {
            after: Duration::from_secs(30),
            open: Box::new(move || open(opens.fetch_add(1, Ordering::SeqCst))),
            client: None,
        })
    }

    fn no_poll() -> Poll {
        Box::new(|| Box::pin(async { panic!("опроса быть не должно") }))
    }

    /// Ключи ошибок, которые сессия отдала в `Report`.
    #[derive(Clone, Default)]
    struct Reported(Arc<Mutex<Vec<String>>>);

    impl Reported {
        fn report(&self) -> Report {
            let seen = self.0.clone();
            Box::new(move |e| seen.lock_safe().push(e.key.clone()))
        }

        fn keys(&self) -> Vec<String> {
            self.0.lock_safe().clone()
        }
    }

    /// Запускает сессию на `wait` (время на паузе) и возвращает всё, что она отправила.
    async fn session(sources: Vec<Source>, poll: Poll, wait: Duration) -> Vec<WatchEvent> {
        session_reporting(sources, poll, wait, Box::new(|_| {})).await
    }

    async fn session_reporting(
        sources: Vec<Source>,
        poll: Poll,
        wait: Duration,
        report: Report,
    ) -> Vec<WatchEvent> {
        let sink = Collect::default();
        let cancel = CancellationToken::new();
        let task = tokio::spawn(run_reporting(
            sources,
            poll,
            sink.clone(),
            cancel.clone(),
            report,
        ));
        tokio::time::sleep(wait).await;
        cancel.cancel();
        task.await.unwrap();
        sink.items()
    }

    fn summary(events: &[WatchEvent]) -> Vec<String> {
        events
            .iter()
            .map(|e| match e {
                WatchEvent::Reset {
                    items,
                    errors,
                    truncated,
                } => format!(
                    "reset {} {}{}",
                    items.iter().map(Row::key).collect::<Vec<_>>().join(","),
                    errors
                        .iter()
                        .map(|e| format!("{:?}", e.kind))
                        .collect::<Vec<_>>()
                        .join(","),
                    if *truncated { " truncated" } else { "" }
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

    #[test]
    fn events_in_json() {
        let reset = WatchEvent::Reset {
            items: vec![],
            errors: vec![],
            truncated: true,
        };
        assert_eq!(
            serde_json::to_value(reset).unwrap(),
            serde_json::json!({"type": "reset", "items": [], "errors": [], "truncated": true})
        );
        let changes = WatchEvent::Changes { changes: vec![] };
        assert_eq!(
            serde_json::to_value(changes).unwrap(),
            serde_json::json!({"type": "changes", "changes": []})
        );
    }

    #[tokio::test(start_paused = true)]
    async fn workloads_wait_for_every_kind_and_report_forbidden_ones() {
        let deployments = source(
            Some(WorkloadKind::Deployment),
            vec![
                Ok(E::Init),
                Ok(E::InitApply(workload(WorkloadKind::Deployment, "web"))),
                Ok(E::InitDone),
            ],
        );
        let forbidden = || Err(watcher::Error::InitialListFailed(api_error(403)));
        let cron = source(Some(WorkloadKind::CronJob), vec![forbidden(), forbidden()]);
        let reported = Reported::default();
        let events = session_reporting(
            vec![deployments, cron],
            no_poll(),
            Duration::from_secs(1),
            reported.report(),
        )
        .await;
        assert_eq!(summary(&events), ["reset Deployment/web CronJob"]);
        assert_eq!(
            reported.keys(),
            ["errors.k8s.forbidden", "errors.k8s.forbidden"],
            "права проекта перечитаются"
        );
    }

    /// Вид workload'а, который сервер не отдаёт (503 агрегированного API у DeploymentConfig),
    /// не задерживает остальные; прочитался — ошибка вида уходит.
    #[tokio::test(start_paused = true)]
    async fn a_kind_with_a_server_error_does_not_hold_back_the_others() {
        let deployments = source(
            Some(WorkloadKind::Deployment),
            vec![
                Ok(E::Init),
                Ok(E::InitApply(workload(WorkloadKind::Deployment, "web"))),
                Ok(E::InitDone),
            ],
        );
        let unavailable = || Err(watcher::Error::InitialListFailed(api_error(503)));
        let configs = Source {
            kind: Some(WorkloadKind::DeploymentConfig),
            stream: later(
                vec![unavailable(), unavailable()],
                Duration::from_secs(5),
                listed(vec![workload(WorkloadKind::DeploymentConfig, "app")]),
            ),
            restart: None,
        };
        let events = session(
            vec![deployments, configs],
            no_poll(),
            Duration::from_secs(10),
        )
        .await;
        assert_eq!(
            summary(&events),
            [
                "reset Deployment/web DeploymentConfig",
                "reset Deployment/web,DeploymentConfig/app "
            ]
        );
        let WatchEvent::Reset { errors, .. } = &events[0] else {
            panic!("{events:?}");
        };
        assert_eq!(errors[0].error.key, "errors.k8s.serverError");
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
        let calls = Arc::new(AtomicUsize::new(0));
        let poll: Poll = {
            let calls = calls.clone();
            Box::new(move || {
                let n = calls.fetch_add(1, Ordering::SeqCst);
                Box::pin(async move {
                    Ok(Snapshot {
                        items: vec![pod(&format!("p{n}"), "Running")],
                        errors: vec![],
                        truncated: n == 1,
                    })
                })
            })
        };
        let events = session(vec![s], poll, Duration::from_secs(25)).await;
        assert_eq!(
            summary(&events),
            [
                "reset  ",
                "polling",
                "reset p0 ",
                "reset p1  truncated",
                "reset p2 "
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn polling_ends_on_an_error_that_breaks_the_client() {
        let no_watch = || {
            source(
                None,
                vec![
                    Ok(E::Init),
                    Ok(E::InitDone),
                    Err(watcher::Error::WatchStartFailed(api_error(403))),
                ],
            )
        };
        // Вид workload'а, у которого истекла сессия входа: list_workloads отдаёт его ошибкой вида.
        let expired: Poll = Box::new(|| {
            Box::pin(async {
                Ok(Snapshot {
                    errors: vec![KindError {
                        kind: WorkloadKind::Job,
                        error: from_kube(&api_error(401)),
                    }],
                    ..Snapshot::default()
                })
            })
        });
        let reported = Reported::default();
        let events = session_reporting(
            vec![no_watch()],
            expired,
            Duration::from_secs(25),
            reported.report(),
        )
        .await;
        assert_eq!(
            summary(&events),
            ["reset  ", "polling", "error errors.k8s.unauthorized true"]
        );
        assert_eq!(
            reported.keys(),
            ["errors.k8s.forbidden", "errors.k8s.unauthorized"]
        );
        let tls: Poll = Box::new(|| Box::pin(async { Err(from_kube(&tls_error())) }));
        let events = session(vec![no_watch()], tls, Duration::from_secs(25)).await;
        assert_eq!(
            summary(&events),
            ["reset  ", "polling", "error errors.k8s.caUntrusted true"]
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

    /// Учётные данные и TLS не починятся повтором: сессия завершается, клиент сбрасывается
    /// (`Report`). Ошибка вида workload'а тут — ошибка всей сессии.
    #[tokio::test(start_paused = true)]
    async fn credential_and_tls_errors_end_the_session() {
        let s = source(
            None,
            vec![
                Ok(E::Init),
                Ok(E::InitDone),
                Err(watcher::Error::WatchFailed(tls_error())),
            ],
        );
        let reported = Reported::default();
        let events = session_reporting(
            vec![s],
            no_poll(),
            Duration::from_secs(1),
            reported.report(),
        )
        .await;
        assert_eq!(
            summary(&events),
            ["reset  ", "error errors.k8s.caUntrusted true"]
        );
        assert_eq!(reported.keys(), ["errors.k8s.caUntrusted"]);
        let deployments = source(
            Some(WorkloadKind::Deployment),
            vec![Err(watcher::Error::InitialListFailed(tls_error()))],
        );
        let jobs = Source {
            kind: Some(WorkloadKind::Job),
            stream: later(vec![], Duration::from_secs(1), listed(vec![])),
            restart: None,
        };
        let events = session(vec![deployments, jobs], no_poll(), Duration::from_secs(5)).await;
        assert_eq!(summary(&events), ["error errors.k8s.caUntrusted true"]);
    }

    #[tokio::test(start_paused = true)]
    async fn changes_go_before_the_error_that_follows_them() {
        for (code, expected) in [
            (401, "error errors.k8s.unauthorized true"),
            (500, "error errors.k8s.serverError false"),
        ] {
            let s = source(
                None,
                vec![
                    Ok(E::Init),
                    Ok(E::InitDone),
                    Ok(E::Apply(pod("c", "Pending"))),
                    Err(watcher::Error::WatchFailed(api_error(code))),
                ],
            );
            let events = session(vec![s], no_poll(), Duration::from_secs(1)).await;
            assert_eq!(summary(&events), ["reset  ", "changes +c", expected]);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_resync_after_410_is_not_an_error() {
        let gone =
            kube::core::Status::failure("too old resource version", "Expired").with_code(410);
        let s = source(
            None,
            vec![
                Ok(E::Init),
                Ok(E::InitDone),
                Err(watcher::Error::WatchError(Box::new(gone))),
                Ok(E::Init),
                Ok(E::InitApply(pod("a", "Running"))),
                Ok(E::InitDone),
            ],
        );
        let reported = Reported::default();
        let events = session_reporting(
            vec![s],
            no_poll(),
            Duration::from_secs(1),
            reported.report(),
        )
        .await;
        assert_eq!(summary(&events), ["reset  ", "reset a "]);
        assert!(reported.keys().is_empty());
    }

    /// Список перечитывается после ошибки: первая же его строка убирает ошибку из интерфейса
    /// пакетом без изменений, не дожидаясь конца списка.
    #[tokio::test(start_paused = true)]
    async fn an_answer_after_an_error_clears_it() {
        let gone =
            kube::core::Status::failure("too old resource version", "Expired").with_code(410);
        let s = source(
            None,
            vec![
                Ok(E::Init),
                Ok(E::InitDone),
                Err(watcher::Error::WatchError(Box::new(gone))),
                Ok(E::Init),
                Err(watcher::Error::InitialListFailed(api_error(503))),
                Ok(E::Init),
                Ok(E::InitApply(pod("a", "Running"))),
                Ok(E::InitApply(pod("b", "Running"))),
                Ok(E::InitDone),
            ],
        );
        let events = session(vec![s], no_poll(), Duration::from_secs(1)).await;
        assert_eq!(
            summary(&events),
            [
                "reset  ",
                "error errors.k8s.serverError false",
                "changes ",
                "reset a,b "
            ]
        );
    }

    /// Сервер не отвечает на чтение списка: через `ReadClient::timeout` — ошибка (один раз) и
    /// поток с начала, пока список не прочитается.
    #[tokio::test(start_paused = true)]
    async fn a_list_without_an_answer_times_out_and_starts_again() {
        let opens = Arc::new(AtomicUsize::new(0));
        let s = Source {
            kind: None,
            stream: stream(vec![Ok(E::Init)]),
            restart: restart(&opens, |n| match n {
                0 => stream(vec![Ok(E::Init)]),
                _ => stream(listed(vec![pod("a", "Running")])),
            }),
        };
        let events = session(vec![s], no_poll(), Duration::from_secs(95)).await;
        assert_eq!(
            summary(&events),
            ["error errors.k8s.timeout false", "reset a "]
        );
        assert_eq!(opens.load(Ordering::SeqCst), 2);
    }

    /// Клиент потока, который подключается заново через `connect` (тот считает вызовы, а
    /// сброшенные клиенты кладёт в `broken`).
    fn renewing(
        connects: &Arc<AtomicUsize>,
        broken: &Arc<Mutex<Vec<String>>>,
    ) -> Arc<StreamClient> {
        let (connects, broken) = (connects.clone(), broken.clone());
        let connect: crate::sessions::Connect = Arc::new(move |b| {
            connects.fetch_add(1, Ordering::SeqCst);
            if let Some((_, e)) = b {
                broken.lock_safe().push(e.key);
            }
            Box::pin(async {
                Ok(crate::k8s::pool::test_connected(|_| {
                    (200, serde_json::json!({}))
                }))
            })
        });
        let first = crate::k8s::pool::test_connected(|_| (200, serde_json::json!({})));
        Arc::new(StreamClient::new(first, connect))
    }

    /// Кластер не принял клиент (истекли учётные данные exec-плагина): поток начинается заново
    /// новым клиентом, и сессия идёт дальше без ошибки. Тот же отказ меньше чем через минуту —
    /// ошибка сессии; позже — снова новый клиент.
    #[tokio::test(start_paused = true)]
    async fn a_rejected_client_restarts_the_stream_with_a_new_one() {
        let expired = || Err(watcher::Error::WatchStartFailed(api_error(401)));
        let (opens, connects) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
        let broken = Arc::new(Mutex::new(Vec::new()));
        let client = renewing(&connects, &broken);
        let mut restart = restart(&opens, move |n| match n {
            0 => later(
                listed(vec![pod("b", "Running")]),
                Duration::from_secs(61),
                vec![expired()],
            ),
            _ => later(
                listed(vec![pod("c", "Running")]),
                Duration::from_secs(5),
                vec![expired()],
            ),
        });
        restart.as_mut().unwrap().client = Some(client);
        let s = Source {
            kind: None,
            stream: stream(
                listed(vec![pod("a", "Running")])
                    .into_iter()
                    .chain([expired()])
                    .collect(),
            ),
            restart,
        };
        let reported = Reported::default();
        let events = session_reporting(
            vec![s],
            no_poll(),
            Duration::from_secs(120),
            reported.report(),
        )
        .await;
        assert_eq!(
            summary(&events),
            [
                "reset a ",
                "reset b ",
                "reset c ",
                "error errors.k8s.unauthorized true"
            ]
        );
        assert_eq!(opens.load(Ordering::SeqCst), 2, "поток начат заново дважды");
        assert_eq!(connects.load(Ordering::SeqCst), 2);
        assert_eq!(
            *broken.lock_safe(),
            ["errors.k8s.unauthorized", "errors.k8s.unauthorized"],
            "прежний клиент сброшен"
        );
        assert_eq!(
            reported.keys(),
            ["errors.k8s.unauthorized"],
            "в `Report` — только отказ, который кончил сессию"
        );
    }

    /// Настройки watcher'а: сервер сам заканчивает каждый watch раньше, чем таймаут чтения
    /// клиента оборвал бы живое соединение.
    #[test]
    fn every_watch_request_has_a_server_timeout() {
        for selector in [None, Some("app=web")] {
            let c = watch_config(selector);
            assert_eq!(c.timeout, Some(WATCH_TIMEOUT));
            assert_eq!(c.label_selector.as_deref(), selector);
        }
        assert!(
            Duration::from_secs(WATCH_TIMEOUT.into()) < crate::k8s::config::READ_TIMEOUT,
            "живой watch не молчит до таймаута чтения"
        );
    }

    /// После ошибки watcher ждёт перед повтором (до 30 с): это не зависший список.
    #[tokio::test(start_paused = true)]
    async fn the_timeout_does_not_cut_the_wait_before_a_retry() {
        let opens = Arc::new(AtomicUsize::new(0));
        let s = Source {
            kind: None,
            stream: stream(vec![Err(watcher::Error::InitialListFailed(api_error(500)))]),
            restart: restart(&opens, |_| stream(vec![])),
        };
        let events = session(vec![s], no_poll(), Duration::from_secs(95)).await;
        assert_eq!(summary(&events), ["error errors.k8s.serverError false"]);
        assert_eq!(opens.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_kind_without_an_answer_does_not_hold_back_the_others() {
        let opens = Arc::new(AtomicUsize::new(0));
        let deployments = Source {
            kind: Some(WorkloadKind::Deployment),
            stream: stream(vec![Ok(E::Init)]),
            restart: restart(&opens, |_| stream(vec![Ok(E::Init)])),
        };
        let sets = source(
            Some(WorkloadKind::StatefulSet),
            listed(vec![workload(WorkloadKind::StatefulSet, "db")]),
        );
        let events = session(vec![deployments, sets], no_poll(), Duration::from_secs(65)).await;
        assert_eq!(summary(&events), ["reset StatefulSet/db Deployment"]);
        let WatchEvent::Reset { errors, .. } = &events[0] else {
            panic!("{events:?}");
        };
        assert_eq!(errors[0].error.key, "errors.k8s.timeout");
        assert_eq!(opens.load(Ordering::SeqCst), 2);
    }

    /// Строк не больше `MAX_ITEMS` на все виды; watcher дочитывает список, дальше — изменения.
    /// Новая строка берётся, только пока есть место.
    #[tokio::test(start_paused = true)]
    async fn the_list_is_capped_across_kinds() {
        let d = |name: &str| workload(WorkloadKind::Deployment, name);
        let each = MAX_ITEMS / 2 + 1_000;
        let deployments = Source {
            kind: Some(WorkloadKind::Deployment),
            stream: later(
                listed(many(d("w"), "w", each)),
                Duration::from_secs(1),
                vec![
                    Ok(E::Apply(d("new-1"))),
                    Ok(E::Apply(d("w00000"))),
                    Ok(E::Delete(d("w00001"))),
                    Ok(E::Apply(d("new-2"))),
                    Ok(E::Apply(d("new-3"))),
                    Ok(E::Delete(d("new-3"))),
                ],
            ),
            restart: None,
        };
        let sets = source(
            Some(WorkloadKind::StatefulSet),
            listed(many(workload(WorkloadKind::StatefulSet, "s"), "s", each)),
        );
        let events = session(vec![deployments, sets], no_poll(), Duration::from_secs(5)).await;
        assert_eq!(events.len(), 2, "{:?}", summary(&events[1..]));
        let WatchEvent::Reset {
            items,
            errors,
            truncated,
        } = &events[0]
        else {
            panic!("{:?}", summary(&events));
        };
        assert!(*truncated && errors.is_empty());
        assert_eq!(items.len(), MAX_ITEMS);
        let deployments = items
            .iter()
            .filter(|r| r.key().starts_with("Deployment/"))
            .count();
        assert!(deployments > 0 && deployments < MAX_ITEMS);
        assert_eq!(
            summary(&events[1..]),
            ["changes +Deployment/w00000,-Deployment/w00001,+Deployment/new-2"]
        );
    }

    /// Полный список, а объектов стало больше `MAX_ITEMS`: `Reset` с `truncated` один раз.
    #[tokio::test(start_paused = true)]
    async fn a_full_list_becomes_truncated_once() {
        let s = Source {
            kind: None,
            stream: later(
                listed(many(pod("p", "Running"), "p", MAX_ITEMS)),
                Duration::from_secs(1),
                vec![
                    Ok(E::Apply(pod("extra-1", "Pending"))),
                    Ok(E::Apply(pod("extra-2", "Pending"))),
                    Ok(E::Delete(pod("p00000", "Running"))),
                    Ok(E::Apply(pod("extra-3", "Pending"))),
                ],
            ),
            restart: None,
        };
        let events = session(vec![s], no_poll(), Duration::from_secs(5)).await;
        let resets: Vec<(usize, bool)> = events
            .iter()
            .filter_map(|e| match e {
                WatchEvent::Reset {
                    items, truncated, ..
                } => Some((items.len(), *truncated)),
                _ => None,
            })
            .collect();
        assert_eq!(resets, [(MAX_ITEMS, false), (MAX_ITEMS, true)]);
        assert_eq!(summary(&events[2..]), ["changes -p00000,+extra-3"]);
    }

    #[tokio::test(start_paused = true)]
    async fn nothing_to_watch_is_an_empty_list() {
        let events = session(vec![], no_poll(), Duration::from_secs(1)).await;
        assert_eq!(summary(&events), ["reset  "]);
        let (client, _server) =
            crate::k8s::testing::mock_client(|_| panic!("запросов быть не должно"));
        let r = ReadClient::new(client, Duration::from_secs(5));
        let groups = ApiGroups::default();
        assert!(
            sources(
                &StreamClient::fixed(r),
                "team-a",
                WatchKind::Pods,
                Some(""),
                &groups
            )
            .is_empty()
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

    /// Опрос — те же чтения, что у команд списков, и с тем же `truncated`.
    #[tokio::test]
    async fn polling_keeps_truncated() {
        let (client, _server) = crate::k8s::testing::mock_client(|_| {
            (
                200,
                serde_json::json!({"apiVersion": "v1", "kind": "PodList",
                    "metadata": {"resourceVersion": "7", "continue": "more"},
                    "items": [crate::k8s::pods::fixtures::running("web-1")]}),
            )
        });
        let r = ReadClient::new(client, Duration::from_secs(5));
        let p = poll(
            StreamClient::fixed(r),
            "team-a".into(),
            WatchKind::Pods,
            None,
            ApiGroups::default(),
        );
        let s = p().await.unwrap();
        assert!(s.truncated && !s.items.is_empty());
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
            sources(
                &StreamClient::fixed(r.clone()),
                "team-a",
                WatchKind::Pods,
                None,
                &groups,
            ),
            poll(
                StreamClient::fixed(r),
                "team-a".into(),
                WatchKind::Pods,
                None,
                groups,
            ),
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
