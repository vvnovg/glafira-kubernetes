//! Логи контейнера (спец. §6.6): хвост (по умолчанию 500 строк) или с момента, предыдущий
//! контейнер, follow. Фильтр (подстрока или regex) применяется здесь, до отправки в интерфейс;
//! строка длиннее 64 КБ обрезается, байты не в UTF-8 заменяются (U+FFFD). В follow при потере
//! соединения — переподключение с `sinceTime` последней строки (повторы отбрасываются), через
//! 60 с без восстановления — ошибка; контейнер завершился или пода нет — конец лога. Без follow
//! строка ждётся не дольше таймаута запроса, а обрыв — ошибка.

use std::path::Path;
use std::time::Duration;

use futures::future::BoxFuture;
use futures::stream::{BoxStream, StreamExt};
use futures::{AsyncBufRead, AsyncBufReadExt};
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::jiff::Timestamp;
use serde::{Deserialize, Serialize};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::error::{AppError, AppResult, ErrorCode};
use crate::k8s::errors::{breaks_client, chain_text, from_transport_text, timeout};
use crate::sessions::{BATCH_WINDOW, Batch, Sink, StreamClient};
use crate::storage::fs::AtomicFile;

/// Сколько строк с конца читается, если не задано ни начало, ни число строк.
pub const DEFAULT_TAIL: i64 = 500;
/// Строка длиннее обрезается.
pub const MAX_LINE: usize = 64 * 1024;
/// Сколько follow ждёт восстановления соединения.
pub const RECONNECT_FOR: Duration = Duration::from_secs(60);
/// Сколько байт строки читается для просмотра: `MAX_LINE` и запас на время в начале строки,
/// чтобы `truncate` увидел, что строка длиннее. Остаток длинной строки пропускается.
const READ_LINE: usize = MAX_LINE + 1024;
/// Строка длиннее при сохранении в файл обрезается: память на одну строку ограничена.
pub const SAVE_LINE: usize = 16 * 1024 * 1024;
/// Сколько байт копится перед записью в файл при сохранении.
const SAVE_CHUNK: usize = 1024 * 1024;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogFilter {
    pub text: String,
    #[serde(default)]
    pub regex: bool,
    #[serde(default)]
    pub case_sensitive: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogParams {
    pub connection_id: String,
    pub namespace: String,
    pub pod: String,
    #[serde(default)]
    pub container: Option<String>,
    #[serde(default)]
    pub previous: bool,
    #[serde(default)]
    pub tail_lines: Option<i64>,
    #[serde(default)]
    pub since_seconds: Option<i64>,
    /// RFC 3339.
    #[serde(default)]
    pub since_time: Option<String>,
    #[serde(default)]
    pub follow: bool,
    #[serde(default)]
    pub filter: Option<LogFilter>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LogLine {
    /// Время строки (RFC 3339 с долями секунды), если API его отдал.
    pub ts: Option<String>,
    pub text: String,
    /// Строка длиннее 64 КБ показана не целиком.
    pub truncated: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum LogEvent {
    /// Строки, подошедшие под фильтр; `scanned` и `shown` — сколько всего просмотрено и показано.
    /// Без строк — только новые счётчики (идут строки, не подходящие под фильтр) или сигнал, что
    /// поток снова открылся после `Reconnecting`.
    Lines {
        lines: Vec<LogLine>,
        scanned: u64,
        shown: u64,
    },
    /// Соединение потеряно, идёт переподключение.
    Reconnecting {
        since: Option<String>,
    },
    /// Лог прочитан до конца (без follow или контейнер завершился).
    End {
        scanned: u64,
        shown: u64,
    },
    Error {
        error: AppError,
    },
}

/// Фильтр строк.
pub enum Matcher {
    All,
    Text {
        needle: String,
        case_sensitive: bool,
    },
    Regex(regex::Regex),
}

impl Matcher {
    pub fn new(f: Option<&LogFilter>) -> AppResult<Self> {
        let Some(f) = f.filter(|f| !f.text.is_empty()) else {
            return Ok(Matcher::All);
        };
        if f.regex {
            regex::RegexBuilder::new(&f.text)
                .case_insensitive(!f.case_sensitive)
                .size_limit(1 << 20)
                .build()
                .map(Matcher::Regex)
                .map_err(|e| {
                    app_err!(
                        ErrorCode::InvalidInput,
                        "errors.logs.badRegex",
                        "Invalid regular expression: {{error}}",
                        error = e
                    )
                })
        } else {
            Ok(Matcher::Text {
                needle: if f.case_sensitive {
                    f.text.clone()
                } else {
                    f.text.to_lowercase()
                },
                case_sensitive: f.case_sensitive,
            })
        }
    }

    pub fn matches(&self, line: &str) -> bool {
        match self {
            Matcher::All => true,
            Matcher::Text {
                needle,
                case_sensitive: true,
            } => line.contains(needle.as_str()),
            Matcher::Text { needle, .. } => line.to_lowercase().contains(needle.as_str()),
            Matcher::Regex(r) => r.is_match(line),
        }
    }
}

/// Время и текст строки лога `2026-10-25T10:00:00.123456789Z текст` (API с `timestamps=true`).
pub fn split_line(raw: &str) -> (Option<Timestamp>, &str) {
    match raw.split_once(' ') {
        Some((ts, text)) => match ts.parse::<Timestamp>() {
            Ok(t) => (Some(t), text),
            Err(_) => (None, raw),
        },
        None => match raw.parse::<Timestamp>() {
            Ok(t) => (Some(t), ""),
            Err(_) => (None, raw),
        },
    }
}

/// Строка не длиннее `MAX_LINE` байт (по границе символа) и признак обрезки.
pub fn truncate(text: &str) -> (String, bool) {
    if text.len() <= MAX_LINE {
        return (text.to_string(), false);
    }
    let mut end = MAX_LINE;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    (text[..end].to_string(), true)
}

/// Строки потока байтов. Не UTF-8 — с заменой (U+FFFD), а не ошибкой; от строки длиннее `cap`
/// байт остаётся начало, остальное до конца строки пропускается: память на строку ограничена.
pub fn lossy_lines(
    reader: impl AsyncBufRead + Send + 'static,
    cap: usize,
) -> BoxStream<'static, std::io::Result<String>> {
    futures::stream::unfold(Some(Box::pin(reader)), move |reader| async move {
        let mut reader = reader?;
        let mut line = Vec::new();
        loop {
            let (used, done) = match reader.fill_buf().await {
                Err(e) => return Some((Err(e), None)),
                Ok([]) => {
                    return (!line.is_empty()).then(|| (Ok(decode(line)), None));
                }
                Ok(buf) => {
                    let (used, done) = match buf.iter().position(|b| *b == b'\n') {
                        Some(i) => (i + 1, true),
                        None => (buf.len(), false),
                    };
                    let room = cap.saturating_sub(line.len());
                    line.extend_from_slice(&buf[..used.min(room)]);
                    (used, done)
                }
            };
            reader.consume_unpin(used);
            if done {
                return Some((Ok(decode(line)), Some(reader)));
            }
        }
    })
    .boxed()
}

/// Текст строки без `\n` (и `\r` перед ним).
fn decode(mut line: Vec<u8>) -> String {
    if line.last() == Some(&b'\n') {
        line.pop();
        if line.last() == Some(&b'\r') {
            line.pop();
        }
    }
    String::from_utf8(line).unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned())
}

/// Откуда читается лог: в приложении — API-сервер, в тестах — заготовленные строки.
pub trait LogSource: Send + Sync {
    /// Поток строк (с временем в начале) для просмотра: по параметрам сессии или, при
    /// переподключении, с `since`. От строки длиннее `MAX_LINE` может прийти только начало.
    fn open(
        &self,
        since: Option<Timestamp>,
    ) -> BoxFuture<'_, AppResult<BoxStream<'static, std::io::Result<String>>>>;

    /// Работает ли ещё контейнер: если нет, конец потока — конец лога, а не обрыв.
    fn running(&self) -> BoxFuture<'_, AppResult<bool>>;

    /// Поток строк для сохранения в файл: строки целиком (до `SAVE_LINE`).
    fn open_whole(&self) -> BoxFuture<'_, AppResult<BoxStream<'static, std::io::Result<String>>>> {
        self.open(None)
    }

    /// Сколько ждать следующей строки лога без follow; `None` — сколько угодно.
    fn read_timeout(&self) -> Option<Duration> {
        None
    }

    /// Клиент заново перед тем, как открыть поток снова (`StreamClient::refresh`): follow живёт
    /// дольше учётных данных exec-плагина. По умолчанию клиент один на всю сессию.
    fn refresh(&self) -> BoxFuture<'_, AppResult<()>> {
        Box::pin(async { Ok(()) })
    }

    /// Новый клиент после ошибки `error`, с которой этот дальше не работает
    /// (`StreamClient::renew`). `false` — заменить нечем.
    fn renew<'a>(&'a self, _error: &'a AppError) -> BoxFuture<'a, AppResult<bool>> {
        Box::pin(async { Ok(false) })
    }
}

/// Лог контейнера через API-сервер.
pub struct KubeLogs {
    pub client: StreamClient,
    pub params: LogParams,
}

impl KubeLogs {
    fn kube_params(&self, since: Option<Timestamp>) -> kube::api::LogParams {
        let p = &self.params;
        let mut lp = kube::api::LogParams {
            container: p.container.clone(),
            // У прежнего экземпляра контейнера лог уже не растёт.
            follow: p.follow && !p.previous,
            previous: p.previous,
            timestamps: true,
            ..Default::default()
        };
        if let Some(t) = since {
            lp.since_time = Some(t);
        } else {
            lp.since_seconds = p.since_seconds;
            lp.since_time = p.since_time.as_deref().and_then(|t| t.parse().ok());
            lp.tail_lines = p.tail_lines.or_else(|| {
                (p.since_seconds.is_none() && p.since_time.is_none()).then_some(DEFAULT_TAIL)
            });
        }
        lp
    }

    /// Строки лога: от каждой — не больше `cap` байт.
    fn lines(
        &self,
        since: Option<Timestamp>,
        cap: usize,
    ) -> BoxFuture<'_, AppResult<BoxStream<'static, std::io::Result<String>>>> {
        Box::pin(async move {
            let lp = self.kube_params(since);
            let r = self
                .client
                .reader()
                .log_stream(&self.params.namespace, &self.params.pod, &lp)
                .await?;
            Ok(lossy_lines(r, cap))
        })
    }
}

impl LogSource for KubeLogs {
    fn open(
        &self,
        since: Option<Timestamp>,
    ) -> BoxFuture<'_, AppResult<BoxStream<'static, std::io::Result<String>>>> {
        self.lines(since, READ_LINE)
    }

    fn open_whole(&self) -> BoxFuture<'_, AppResult<BoxStream<'static, std::io::Result<String>>>> {
        self.lines(None, SAVE_LINE)
    }

    fn read_timeout(&self) -> Option<Duration> {
        Some(self.client.reader().timeout())
    }

    fn refresh(&self) -> BoxFuture<'_, AppResult<()>> {
        Box::pin(self.client.refresh())
    }

    fn renew<'a>(&'a self, error: &'a AppError) -> BoxFuture<'a, AppResult<bool>> {
        Box::pin(self.client.renew(error))
    }

    /// Прежний экземпляр контейнера (`previous`) уже не работает: его лог кончается вместе с
    /// потоком.
    fn running(&self) -> BoxFuture<'_, AppResult<bool>> {
        Box::pin(async move {
            if self.params.previous {
                return Ok(false);
            }
            let pod: Pod = self
                .client
                .reader()
                .get(&self.params.namespace, &self.params.pod)
                .await?;
            let statuses = pod
                .status
                .and_then(|s| s.container_statuses)
                .unwrap_or_default();
            let status = match &self.params.container {
                Some(c) => statuses.into_iter().find(|s| &s.name == c),
                None => statuses.into_iter().next(),
            };
            Ok(status
                .and_then(|s| s.state)
                .is_some_and(|s| s.running.is_some()))
        })
    }
}

/// Источник, об ошибках открытия лога и проверки контейнера которого узнаёт `report`: что с
/// ними делать (сбросить клиент подключения после `breaks_client`, забыть права проекта после
/// 403), решает тот, кто начал сессию, — как у watch (`watch::Report`).
pub struct Reported<S, F> {
    pub source: S,
    pub report: F,
}

impl<S, F: Fn(&AppError)> Reported<S, F> {
    fn noted<T>(&self, r: AppResult<T>) -> AppResult<T> {
        if let Err(e) = &r {
            (self.report)(e);
        }
        r
    }
}

impl<S: LogSource, F: Fn(&AppError) + Send + Sync> LogSource for Reported<S, F> {
    fn open(
        &self,
        since: Option<Timestamp>,
    ) -> BoxFuture<'_, AppResult<BoxStream<'static, std::io::Result<String>>>> {
        Box::pin(async move { self.noted(self.source.open(since).await) })
    }

    fn running(&self) -> BoxFuture<'_, AppResult<bool>> {
        Box::pin(async move { self.noted(self.source.running().await) })
    }

    fn open_whole(&self) -> BoxFuture<'_, AppResult<BoxStream<'static, std::io::Result<String>>>> {
        Box::pin(async move { self.noted(self.source.open_whole().await) })
    }

    fn read_timeout(&self) -> Option<Duration> {
        self.source.read_timeout()
    }

    fn refresh(&self) -> BoxFuture<'_, AppResult<()>> {
        self.source.refresh()
    }

    fn renew<'a>(&'a self, error: &'a AppError) -> BoxFuture<'a, AppResult<bool>> {
        self.source.renew(error)
    }
}

/// `since_time` — время RFC 3339. Неверное время иначе молча читало бы весь лог.
pub fn check_since(p: &LogParams) -> AppResult<()> {
    match p.since_time.as_deref() {
        Some(t) if t.parse::<Timestamp>().is_err() => Err(app_err!(
            ErrorCode::InvalidInput,
            "errors.logs.badSinceTime",
            "Invalid start time “{{value}}”: use RFC 3339, for example 2026-10-25T10:00:00Z",
            value = t
        )),
        _ => Ok(()),
    }
}

/// Сохранение лога остановлено: подключение отключили или удалили.
pub fn save_stopped() -> AppError {
    app_err!(
        ErrorCode::ConnectionFailed,
        "errors.logs.saveStopped",
        "Saving the log was stopped: the connection was closed"
    )
}

fn lost() -> AppError {
    app_err!(
        ErrorCode::ConnectionFailed,
        "errors.logs.lost",
        "The log stream was lost and did not recover in 60 s"
    )
}

/// Ошибка чтения уже открытого потока: таймаут строки или сеть.
fn read_error(e: &std::io::Error) -> AppError {
    match e.kind() {
        std::io::ErrorKind::TimedOut => timeout(),
        _ => from_transport_text(&chain_text(e)),
    }
}

/// Поток оборвал таймаут чтения клиента: `TimedOut` где-то в цепочке ошибки (hyper-timeout
/// кладёт его под ошибку hyper, а kube — под `io::Error::other`).
fn idle_timed_out(e: &std::io::Error) -> bool {
    let mut cur: Option<&(dyn std::error::Error + 'static)> = Some(e);
    while let Some(err) = cur {
        if err
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io| io.kind() == std::io::ErrorKind::TimedOut)
        {
            return true;
        }
        // `io::Error::source` пропускает обёрнутую ошибку и сразу отдаёт её источник.
        cur = match err
            .downcast_ref::<std::io::Error>()
            .and_then(|io| io.get_ref())
        {
            Some(inner) => Some(inner as &(dyn std::error::Error + 'static)),
            None => err.source(),
        };
    }
    false
}

/// Следующая строка потока; не пришла к `deadline` — ошибка `TimedOut`.
async fn next_before(
    stream: &mut BoxStream<'static, std::io::Result<String>>,
    deadline: Option<Instant>,
) -> Option<std::io::Result<String>> {
    match deadline {
        Some(at) => tokio::time::timeout_at(at, stream.next())
            .await
            .unwrap_or_else(|_| Some(Err(std::io::ErrorKind::TimedOut.into()))),
        None => stream.next().await,
    }
}

/// Пауза перед попыткой переподключения: 1, 2, 4, 8, затем 10 с.
fn backoff(attempt: u32) -> Duration {
    Duration::from_secs((1u64 << attempt.min(4)).min(10))
}

/// Сессия логов. Завершается по отмене, в конце лога, при ошибке и когда получателя нет.
pub async fn run(
    source: impl LogSource,
    follow: bool,
    matcher: Matcher,
    sink: impl Sink<LogEvent>,
    cancel: CancellationToken,
) {
    let (mut scanned, mut shown) = (0u64, 0u64);
    // Счётчики последнего отправленного `Lines`: изменились — они уходят и без новых строк.
    let mut sent = (0u64, 0u64);
    let mut last: Option<Timestamp> = None;
    let mut since: Option<Timestamp> = None;
    let mut lost_at: Option<Instant> = None;
    let mut attempt = 0;
    // Поток открывается снова молча (`Reconnecting` не ушёл): прежний простоял без данных до
    // таймаута чтения клиента.
    let mut quiet = false;
    // Без follow строка ждётся не дольше таймаута запроса.
    let idle = if follow { None } else { source.read_timeout() };
    loop {
        let reopening = lost_at.is_some();
        // Поток открывается снова: follow мог прожить дольше учётных данных exec-плагина, и клиент
        // берётся заново. Не вышло — как ошибка открытия, но клиент не меняется.
        let refreshed = match reopening {
            true => tokio::select! {
                _ = cancel.cancelled() => return,
                r = source.refresh() => r,
            },
            false => Ok(()),
        };
        let opened = match &refreshed {
            Ok(()) => tokio::select! {
                _ = cancel.cancelled() => return,
                s = source.open(since) => s,
            },
            Err(e) => Err(e.clone()),
        };
        let mut stream = match opened {
            Ok(s) => s,
            Err(error) => {
                // Кластер не принял клиент, которым открывали (учётные данные истекли, а срока
                // exec-плагин не сообщил): поток открывается снова новым клиентом.
                if reopening && refreshed.is_ok() && breaks_client(error.code) {
                    let renewed = tokio::select! {
                        _ = cancel.cancelled() => return,
                        r = source.renew(&error) => r,
                    };
                    if matches!(renewed, Ok(true)) {
                        continue;
                    }
                }
                // Пода больше нет (удалили, пока шло переподключение): лог кончился.
                if reopening && error.code == ErrorCode::NotFound {
                    sink.send(LogEvent::End { scanned, shown });
                    return;
                }
                // Первое открытие не повторяется. Учётные данные, TLS и права повтор не
                // починит: их ошибка сразу, а не `errors.logs.lost` через минуту.
                if !reopening || breaks_client(error.code) || error.code == ErrorCode::Forbidden {
                    sink.send(LogEvent::Error { error });
                    return;
                }
                if lost_at.is_some_and(|t| t.elapsed() >= RECONNECT_FOR) {
                    sink.send(LogEvent::Error { error: lost() });
                    return;
                }
                // Молча открыть поток снова не вышло: теперь это обрыв, и интерфейс о нём знает.
                if std::mem::take(&mut quiet)
                    && !sink.send(LogEvent::Reconnecting {
                        since: last.map(|t| t.to_string()),
                    })
                {
                    return;
                }
                tokio::select! {
                    _ = cancel.cancelled() => return,
                    _ = tokio::time::sleep(backoff(attempt)) => {}
                }
                attempt += 1;
                continue;
            }
        };
        // Поток открылся после `Reconnecting` — даже если он молчит, интерфейс не должен
        // оставаться в «переподключении» до первой строки. После молчаливого открытия
        // сообщать нечего.
        if reopening && !std::mem::take(&mut quiet) {
            sent = (scanned, shown);
            if !sink.send(LogEvent::Lines {
                lines: Vec::new(),
                scanned,
                shown,
            }) {
                return;
            }
        }
        let opened_at = Instant::now();
        let mut batch: Batch<LogLine> = Batch::default();
        // Первый тик — через окно, а не сразу: пока ничего не прочитано, отправлять нечего.
        let mut tick = tokio::time::interval_at(opened_at + BATCH_WINDOW, BATCH_WINDOW);
        // После переподключения строки до `last` включительно уже показаны.
        let resume_after = since.and(last);
        let mut deadline = idle.map(|d| Instant::now() + d);
        let failure = loop {
            let line = tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tick.tick() => {
                    let lines = if batch.is_due() {
                        batch.take()
                    } else if batch.is_empty() && (scanned, shown) != sent {
                        // Строки идут, но не подходят под фильтр (или пакет уже ушёл): счётчики
                        // доходят и без новых строк. Неполный пакет ждёт своего срока, чтобы
                        // счётчики не обгоняли строки.
                        Vec::new()
                    } else {
                        continue;
                    };
                    sent = (scanned, shown);
                    if !sink.send(LogEvent::Lines { lines, scanned, shown }) {
                        return;
                    }
                    continue;
                }
                line = next_before(&mut stream, deadline) => line,
            };
            let raw = match line {
                Some(Ok(raw)) => raw,
                Some(Err(e)) => break Some(e),
                None => break None,
            };
            deadline = idle.map(|d| Instant::now() + d);
            let (ts, text) = split_line(&raw);
            if resume_after.is_some_and(|after| ts.is_some_and(|t| t <= after)) {
                continue;
            }
            // Строки приходят — соединение восстановлено.
            lost_at = None;
            attempt = 0;
            // Время строк не идёт назад: с него начнётся переподключение.
            if let Some(t) = ts {
                last = Some(last.map_or(t, |l| l.max(t)));
            }
            scanned += 1;
            if matcher.matches(text) {
                shown += 1;
                let (text, truncated) = truncate(text);
                let line = LogLine {
                    ts: ts.map(|t| t.to_string()),
                    text,
                    truncated,
                };
                if let Some(full) = batch.push(line) {
                    sent = (scanned, shown);
                    if !sink.send(LogEvent::Lines {
                        lines: full,
                        scanned,
                        shown,
                    }) {
                        return;
                    }
                }
            }
        };
        if !batch.is_empty() {
            sent = (scanned, shown);
            if !sink.send(LogEvent::Lines {
                lines: batch.take(),
                scanned,
                shown,
            }) {
                return;
            }
        }
        // Без follow обрыв и таймаут — ошибка, а не конец лога.
        if !follow {
            sink.send(match failure {
                Some(e) => LogEvent::Error {
                    error: read_error(&e),
                },
                None => LogEvent::End { scanned, shown },
            });
            return;
        }
        let running = tokio::select! {
            _ = cancel.cancelled() => return,
            r = source.running() => r,
        };
        // Контейнер завершился или пода больше нет: лог кончился.
        if matches!(&running, Ok(false)) || running.is_err_and(|e| e.code == ErrorCode::NotFound) {
            sink.send(LogEvent::End { scanned, shown });
            return;
        }
        // Поток продержался дольше `RECONNECT_FOR` — соединение восстанавливалось, даже если
        // строк не было: контейнер молчит, а прокси рвёт соединения без данных.
        let lived = opened_at.elapsed() >= RECONNECT_FOR;
        if lived {
            lost_at = None;
            attempt = 0;
        }
        let started = *lost_at.get_or_insert_with(Instant::now);
        if started.elapsed() >= RECONNECT_FOR {
            sink.send(LogEvent::Error { error: lost() });
            return;
        }
        // С начала секунды последней строки: API округляет `sinceTime` до секунд.
        since = Some(last.map_or_else(Timestamp::now, |t| {
            Timestamp::from_second(t.as_second()).unwrap_or(t)
        }));
        // Долгий поток кончился таймаутом чтения клиента (`READ_TIMEOUT`): контейнер молчал
        // почти 5 минут или соединение умерло (сон, смена сети). Поток открывается снова сразу и
        // без `Reconnecting`; если открыть не выйдет, `Reconnecting` уйдёт тогда.
        if lived && failure.as_ref().is_some_and(idle_timed_out) {
            quiet = true;
            continue;
        }
        if !sink.send(LogEvent::Reconnecting {
            since: last.map(|t| t.to_string()),
        }) {
            return;
        }
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(backoff(attempt)) => {}
        }
        attempt += 1;
    }
}

/// Сохраняет лог (без follow) в файл: подошедшие под фильтр строки целиком (до `SAVE_LINE`), с
/// временем, если `timestamps`. Строки пишутся частями во временный файл рядом (0600), и файл
/// появляется по пути, только когда лог прочитан весь: при ошибке, таймауте строки и отмене
/// (future брошен) его нет. Возвращает число записанных строк.
pub async fn save(
    source: &impl LogSource,
    matcher: &Matcher,
    path: &Path,
    timestamps: bool,
) -> AppResult<u64> {
    let mut stream = source.open_whole().await?;
    let idle = source.read_timeout();
    let target = path.to_path_buf();
    let mut file = blocking(move || AtomicFile::create(&target)).await?;
    let mut out = Vec::new();
    let mut n = 0;
    loop {
        let next = match idle {
            Some(d) => tokio::time::timeout(d, stream.next())
                .await
                .map_err(|_| timeout())?,
            None => stream.next().await,
        };
        let Some(raw) = next else { break };
        let raw = raw.map_err(|e| read_error(&e))?;
        let (ts, text) = split_line(&raw);
        if !matcher.matches(text) {
            continue;
        }
        if timestamps && let Some(t) = ts {
            out.extend_from_slice(t.to_string().as_bytes());
            out.push(b' ');
        }
        out.extend_from_slice(text.as_bytes());
        out.push(b'\n');
        n += 1;
        if out.len() >= SAVE_CHUNK {
            file = write_part(file, std::mem::take(&mut out)).await?;
        }
    }
    let file = write_part(file, out).await?;
    blocking(move || file.commit()).await?;
    Ok(n)
}

/// Часть файла пишется в фоновом потоке: запись на диск не занимает поток tokio.
async fn write_part(mut file: AtomicFile, bytes: Vec<u8>) -> AppResult<AtomicFile> {
    blocking(move || {
        file.write_all(&bytes)?;
        Ok(file)
    })
    .await
}

async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> AppResult<T> + Send + 'static,
) -> AppResult<T> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| AppError::internal_with(e.to_string()))?
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::k8s::read::ReadClient;
    use crate::sessions::Collect;
    use std::sync::Mutex;

    /// Заготовленные потоки: каждый `open` берёт следующий; `None` — ошибка открытия.
    struct Scripted {
        streams: Mutex<Vec<Option<Vec<std::io::Result<String>>>>>,
        opened: Mutex<Vec<Option<Timestamp>>>,
        running: bool,
        /// Последний поток не кончается (живой follow).
        hold_last: bool,
    }

    impl Scripted {
        fn new(streams: Vec<Option<Vec<&str>>>, running: bool) -> Self {
            Self {
                streams: Mutex::new(
                    streams
                        .into_iter()
                        .rev()
                        .map(|s| s.map(|l| l.into_iter().map(|x| Ok(x.to_string())).collect()))
                        .collect(),
                ),
                opened: Mutex::new(vec![]),
                running,
                hold_last: false,
            }
        }
    }

    impl LogSource for Scripted {
        fn open(
            &self,
            since: Option<Timestamp>,
        ) -> BoxFuture<'_, AppResult<BoxStream<'static, std::io::Result<String>>>> {
            self.opened.lock().unwrap().push(since);
            let next = self.streams.lock().unwrap().pop();
            let last = self.streams.lock().unwrap().is_empty();
            let hold = self.hold_last && last;
            Box::pin(async move {
                match next {
                    Some(Some(lines)) if hold => Ok(futures::stream::iter(lines)
                        .chain(futures::stream::pending())
                        .boxed()),
                    Some(Some(lines)) => Ok(futures::stream::iter(lines).boxed()),
                    _ => Err(crate::k8s::errors::from_transport_text(
                        "connection refused",
                    )),
                }
            })
        }

        fn running(&self) -> BoxFuture<'_, AppResult<bool>> {
            let running = self.running;
            Box::pin(async move { Ok(running) })
        }
    }

    async fn session(
        source: Scripted,
        follow: bool,
        matcher: Matcher,
        wait: Duration,
    ) -> Vec<LogEvent> {
        let sink = Collect::default();
        let cancel = CancellationToken::new();
        let task = tokio::spawn(run(source, follow, matcher, sink.clone(), cancel.clone()));
        tokio::time::sleep(wait).await;
        cancel.cancel();
        task.await.unwrap();
        sink.items()
    }

    fn texts(events: &[LogEvent]) -> Vec<String> {
        events
            .iter()
            .flat_map(|e| match e {
                LogEvent::Lines { lines, .. } => lines.iter().map(|l| l.text.clone()).collect(),
                LogEvent::Reconnecting { .. } => vec!["<reconnecting>".into()],
                LogEvent::End { scanned, shown } => vec![format!("<end {shown}/{scanned}>")],
                LogEvent::Error { error } => vec![format!("<error {}>", error.key)],
            })
            .collect()
    }

    // Без нулей в конце долей секунды: так время печатает и Rust.
    const T1: &str = "2026-10-25T10:00:00.123Z";
    const T2: &str = "2026-10-25T10:00:00.987Z";
    const T3: &str = "2026-10-25T10:00:01.5Z";

    #[test]
    fn filters_lines_and_truncation() {
        let f = |text: &str, regex, case_sensitive| {
            Matcher::new(Some(&LogFilter {
                text: text.into(),
                regex,
                case_sensitive,
            }))
            .unwrap()
        };
        assert!(f("error", false, false).matches("ERROR: boom"));
        assert!(!f("error", false, true).matches("ERROR: boom"));
        assert!(f(r"code=\d{3}", true, false).matches("status code=503"));
        assert!(Matcher::new(None).unwrap().matches("x"));
        let bad = Matcher::new(Some(&LogFilter {
            text: "(".into(),
            regex: true,
            case_sensitive: false,
        }));
        assert_eq!(bad.err().unwrap().key, "errors.logs.badRegex");
        let raw = format!("{T1} hello world");
        let (ts, text) = split_line(&raw);
        assert_eq!(
            (ts.map(|t| t.to_string()), text),
            (Some(T1.into()), "hello world")
        );
        assert_eq!(split_line("no time here").0, None);
        let long = "я".repeat(MAX_LINE);
        let (cut, truncated) = truncate(&long);
        assert!(truncated && cut.len() <= MAX_LINE && cut.len() % 2 == 0);
        assert_eq!(truncate("short"), ("short".into(), false));
    }

    #[tokio::test(start_paused = true)]
    async fn without_follow_the_log_ends_with_counters() {
        let source = Scripted::new(
            vec![Some(vec![
                &format!("{T1} GET /health 200"),
                &format!("{T2} GET /api 500 error"),
                &format!("{T3} GET /api 200"),
            ])],
            true,
        );
        let matcher = Matcher::new(Some(&LogFilter {
            text: "/api".into(),
            regex: false,
            case_sensitive: false,
        }))
        .unwrap();
        let events = session(source, false, matcher, Duration::from_secs(1)).await;
        assert_eq!(
            texts(&events),
            ["GET /api 500 error", "GET /api 200", "<end 2/3>"]
        );
        let LogEvent::Lines {
            lines,
            scanned,
            shown,
        } = &events[0]
        else {
            panic!("строки");
        };
        assert_eq!((*scanned, *shown), (3, 2));
        assert_eq!(lines[0].ts.as_deref(), Some(T2));
    }

    #[tokio::test(start_paused = true)]
    async fn follow_reconnects_from_the_last_line_without_repeats() {
        let mut source = Scripted::new(
            vec![
                Some(vec![&format!("{T1} one"), &format!("{T2} two")]),
                None,
                Some(vec![
                    &format!("{T1} one"),
                    &format!("{T2} two"),
                    &format!("{T3} three"),
                ]),
            ],
            true,
        );
        source.hold_last = true;
        let opened = std::sync::Arc::new(source);
        struct Shared(std::sync::Arc<Scripted>);
        impl LogSource for Shared {
            fn open(
                &self,
                since: Option<Timestamp>,
            ) -> BoxFuture<'_, AppResult<BoxStream<'static, std::io::Result<String>>>> {
                self.0.open(since)
            }
            fn running(&self) -> BoxFuture<'_, AppResult<bool>> {
                self.0.running()
            }
        }
        let sink = Collect::default();
        let cancel = CancellationToken::new();
        let task = tokio::spawn(run(
            Shared(opened.clone()),
            true,
            Matcher::All,
            sink.clone(),
            cancel.clone(),
        ));
        tokio::time::sleep(Duration::from_secs(10)).await;
        cancel.cancel();
        task.await.unwrap();
        assert_eq!(
            texts(&sink.items()),
            ["one", "two", "<reconnecting>", "three"]
        );
        let since = opened.opened.lock().unwrap().clone();
        assert_eq!(since.len(), 3);
        assert_eq!(since[0], None);
        assert_eq!(
            since[1].map(|t| t.to_string()).as_deref(),
            Some("2026-10-25T10:00:00Z"),
            "с начала секунды последней строки"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn follow_gives_up_after_a_minute_and_ends_when_the_container_stops() {
        let source = Scripted::new(
            [Some(vec![&format!("{T1} one") as &str])]
                .into_iter()
                .chain(std::iter::repeat_n(None, 20))
                .collect(),
            true,
        );
        let events = session(source, true, Matcher::All, Duration::from_secs(120)).await;
        assert_eq!(
            texts(&events),
            ["one", "<reconnecting>", "<error errors.logs.lost>"]
        );

        let stopped = Scripted::new(vec![Some(vec![&format!("{T1} bye") as &str])], false);
        let events = session(stopped, true, Matcher::All, Duration::from_secs(1)).await;
        assert_eq!(texts(&events), ["bye", "<end 1/1>"]);

        let refused = Scripted::new(vec![None], true);
        let events = session(refused, true, Matcher::All, Duration::from_secs(1)).await;
        assert_eq!(
            texts(&events),
            ["<error errors.k8s.connect>"],
            "первое открытие не повторяется"
        );
    }

    /// Первый поток кончается, а при переподключении кластер отвечает 401.
    struct Expired {
        opens: std::sync::atomic::AtomicUsize,
    }

    impl LogSource for Expired {
        fn open(
            &self,
            _since: Option<Timestamp>,
        ) -> BoxFuture<'_, AppResult<BoxStream<'static, std::io::Result<String>>>> {
            let n = self.opens.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Box::pin(async move {
                match n {
                    0 => Ok(futures::stream::iter(vec![Ok(format!("{T1} one"))]).boxed()),
                    _ => Err(crate::k8s::errors::from_status(
                        &kube::core::Status::failure("expired", "Unauthorized").with_code(401),
                    )),
                }
            })
        }

        fn running(&self) -> BoxFuture<'_, AppResult<bool>> {
            Box::pin(async { Ok(true) })
        }
    }

    /// Учётные данные при переподключении не повторяются минуту: ошибка сразу, и о ней узнаёт
    /// `report` (сброс клиента).
    #[tokio::test(start_paused = true)]
    async fn a_credential_error_while_reconnecting_ends_the_session() {
        let reported = std::sync::Arc::new(Mutex::new(Vec::<String>::new()));
        let source = Reported {
            source: Expired {
                opens: Default::default(),
            },
            report: {
                let reported = reported.clone();
                move |e: &AppError| reported.lock().unwrap().push(e.key.clone())
            },
        };
        let sink = Collect::default();
        let cancel = CancellationToken::new();
        let task = tokio::spawn(run(
            source,
            true,
            Matcher::All,
            sink.clone(),
            cancel.clone(),
        ));
        tokio::time::sleep(Duration::from_secs(120)).await;
        cancel.cancel();
        task.await.unwrap();
        assert_eq!(
            texts(&sink.items()),
            ["one", "<reconnecting>", "<error errors.k8s.unauthorized>"]
        );
        assert_eq!(*reported.lock().unwrap(), ["errors.k8s.unauthorized"]);
    }

    type Lines = BoxStream<'static, std::io::Result<String>>;

    /// Источник по шагам: каждый `open` берёт следующий поток или ошибку (дальше — поток без
    /// строк, который не кончается); `running` — заданный ответ, а `None` — ответа нет.
    struct Steps {
        opens: Mutex<std::collections::VecDeque<AppResult<Lines>>>,
        since: std::sync::Arc<Mutex<Vec<Option<Timestamp>>>>,
        running: Option<AppResult<bool>>,
        timeout: Option<Duration>,
        /// Клиент можно заменить (`renew`).
        renewable: bool,
        /// Сколько раз клиент брали заново (`refresh`) и меняли (`renew`).
        refreshes: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        renews: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    impl Steps {
        fn new(opens: Vec<AppResult<Lines>>) -> Self {
            Self {
                opens: Mutex::new(opens.into()),
                since: Default::default(),
                running: Some(Ok(true)),
                timeout: None,
                renewable: false,
                refreshes: Default::default(),
                renews: Default::default(),
            }
        }
    }

    impl LogSource for Steps {
        fn open(
            &self,
            since: Option<Timestamp>,
        ) -> BoxFuture<'_, AppResult<BoxStream<'static, std::io::Result<String>>>> {
            self.since.lock().unwrap().push(since);
            let next = self
                .opens
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| Ok(futures::stream::pending().boxed()));
            Box::pin(async move { next })
        }

        fn running(&self) -> BoxFuture<'_, AppResult<bool>> {
            let running = self.running.clone();
            Box::pin(async move {
                match running {
                    Some(r) => r,
                    None => futures::future::pending().await,
                }
            })
        }

        fn read_timeout(&self) -> Option<Duration> {
            self.timeout
        }

        fn refresh(&self) -> BoxFuture<'_, AppResult<()>> {
            self.refreshes
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Box::pin(async { Ok(()) })
        }

        fn renew<'a>(&'a self, _error: &'a AppError) -> BoxFuture<'a, AppResult<bool>> {
            if self.renewable {
                self.renews
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            let renewed = self.renewable;
            Box::pin(async move { Ok(renewed) })
        }
    }

    fn lines(xs: &[&str]) -> Lines {
        let xs: Vec<_> = xs.iter().map(|x| Ok(x.to_string())).collect();
        futures::stream::iter(xs).boxed()
    }

    /// Строки, и дальше поток молчит.
    fn held(xs: &[&str]) -> Lines {
        lines(xs).chain(futures::stream::pending()).boxed()
    }

    /// Поток без строк, который кончается через `after`.
    fn quiet_for(after: Duration) -> Lines {
        futures::stream::once(tokio::time::sleep(after))
            .filter_map(|_| async { None })
            .boxed()
    }

    fn status(code: u16) -> AppError {
        crate::k8s::errors::from_status(&kube::core::Status::failure("x", "Reason").with_code(code))
    }

    /// Запускает сессию на `wait` (время на паузе); после отмены она кончается сразу.
    async fn run_for(
        source: impl LogSource + 'static,
        follow: bool,
        wait: Duration,
    ) -> Vec<LogEvent> {
        run_with(source, follow, Matcher::All, wait).await
    }

    async fn run_with(
        source: impl LogSource + 'static,
        follow: bool,
        matcher: Matcher,
        wait: Duration,
    ) -> Vec<LogEvent> {
        let sink = Collect::default();
        let cancel = CancellationToken::new();
        let task = tokio::spawn(run(source, follow, matcher, sink.clone(), cancel.clone()));
        tokio::time::sleep(wait).await;
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("сессия кончилась по отмене")
            .unwrap();
        sink.items()
    }

    /// События целиком, и пустые `Lines` тоже: `L<строк>:<показано>/<просмотрено>`.
    fn trace(events: &[LogEvent]) -> Vec<String> {
        events
            .iter()
            .map(|e| match e {
                LogEvent::Lines {
                    lines,
                    scanned,
                    shown,
                } => format!("L{}:{shown}/{scanned}", lines.len()),
                LogEvent::Reconnecting { .. } => "R".into(),
                LogEvent::End { scanned, shown } => format!("E:{shown}/{scanned}"),
                LogEvent::Error { error } => format!("X:{}", error.key),
            })
            .collect()
    }

    /// Строка, которая приходит позже остальных.
    fn late(after: Duration, line: String) -> Lines {
        futures::stream::once(async move {
            tokio::time::sleep(after).await;
            Ok(line)
        })
        .boxed()
    }

    /// Молчащий контейнер и фильтр, под который ничего не подходит: счётчики доходят до
    /// интерфейса без строк, один раз на изменение, а при первом открытии пустого события нет.
    #[tokio::test(start_paused = true)]
    async fn a_quiet_stream_still_reports_counters() {
        let none = || {
            Matcher::new(Some(&LogFilter {
                text: "zzz".into(),
                regex: false,
                case_sensitive: false,
            }))
            .unwrap()
        };
        let source = Steps::new(vec![Ok(held(&[&format!("{T1} a"), &format!("{T2} b")]))]);
        let events = run_with(source, true, none(), Duration::from_secs(5)).await;
        assert_eq!(trace(&events), ["L0:0/2"]);

        // Пакет со строкой уходит вместе со счётчиками; позже просмотренные строки — пустым событием.
        let mixed: Lines = lines(&[&format!("{T1} err one")])
            .chain(late(Duration::from_secs(1), format!("{T2} info")))
            .chain(futures::stream::pending())
            .boxed();
        let errors = Matcher::new(Some(&LogFilter {
            text: "err".into(),
            regex: false,
            case_sensitive: false,
        }))
        .unwrap();
        let events = run_with(
            Steps::new(vec![Ok(mixed)]),
            true,
            errors,
            Duration::from_secs(5),
        )
        .await;
        assert_eq!(trace(&events), ["L1:1/1", "L0:1/2"]);
    }

    /// После `Reconnecting` и удачного открытия уходит пустое `Lines` — раньше первой строки и
    /// даже если строк так и нет; неудачные попытки открытия его не дают.
    #[tokio::test(start_paused = true)]
    async fn a_successful_reopen_after_reconnecting_is_announced_before_any_line() {
        let one = format!("{T1} one");
        let refused = || crate::k8s::errors::from_transport_text("connection refused");
        let source = Steps::new(vec![
            Ok(lines(&[&one])),
            Err(refused()),
            Ok(lines(&[&one])
                .chain(late(Duration::from_secs(3), format!("{T3} three")))
                .chain(futures::stream::pending())
                .boxed()),
        ]);
        let events = run_for(source, true, Duration::from_secs(20)).await;
        assert_eq!(trace(&events), ["L1:1/1", "R", "L0:1/1", "L1:2/2"]);

        let silent = Steps::new(vec![Ok(lines(&[&one])), Ok(held(&[]))]);
        let events = run_for(silent, true, Duration::from_secs(20)).await;
        assert_eq!(trace(&events), ["L1:1/1", "R", "L0:1/1"]);
    }

    /// Конец лога — `End`, а не обрыв: под удалили (404 при переподключении или при проверке
    /// контейнера). 403 при переподключении повтор не починит — ошибка сразу.
    #[tokio::test(start_paused = true)]
    async fn the_end_of_a_log_is_not_a_lost_connection() {
        let one = format!("{T1} one");
        let wait = Duration::from_secs(120);
        let deleted = Steps::new(vec![Ok(lines(&[&one])), Err(status(404))]);
        assert_eq!(
            texts(&run_for(deleted, true, wait).await),
            ["one", "<reconnecting>", "<end 1/1>"]
        );
        let mut gone = Steps::new(vec![Ok(lines(&[&one]))]);
        gone.running = Some(Err(status(404)));
        assert_eq!(
            texts(&run_for(gone, true, wait).await),
            ["one", "<end 1/1>"]
        );
        let forbidden = Steps::new(vec![Ok(lines(&[&one])), Err(status(403))]);
        assert_eq!(
            texts(&run_for(forbidden, true, wait).await),
            ["one", "<reconnecting>", "<error errors.k8s.forbidden>"]
        );
    }

    /// Переподключение удалось, контейнер молчит дольше минуты, и поток снова рвётся: это новый
    /// обрыв, а не тот же, что минуту назад.
    #[tokio::test(start_paused = true)]
    async fn a_reconnect_that_stays_open_is_a_recovery() {
        let source = Steps::new(vec![
            Ok(lines(&[&format!("{T1} one")])),
            Ok(quiet_for(Duration::from_secs(120))),
        ]);
        let events = run_for(source, true, Duration::from_secs(300)).await;
        assert_eq!(texts(&events), ["one", "<reconnecting>", "<reconnecting>"]);
    }

    /// Ошибка таймаута чтения клиента, как её отдаёт kube: `io::Error::other` над ошибкой hyper,
    /// источник которой — `TimedOut` от hyper-timeout.
    fn idle() -> std::io::Error {
        #[derive(Debug)]
        struct Hyper(std::io::Error);
        impl std::fmt::Display for Hyper {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("error reading a body from connection")
            }
        }
        impl std::error::Error for Hyper {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                Some(&self.0)
            }
        }
        std::io::Error::other(Hyper(std::io::ErrorKind::TimedOut.into()))
    }

    /// Строка, а через `after` — таймаут чтения клиента.
    fn idle_after(after: Duration, line: &str) -> Lines {
        lines(&[line])
            .chain(futures::stream::once(async move {
                tokio::time::sleep(after).await;
                Err(idle())
            }))
            .boxed()
    }

    /// Follow молчащего контейнера кончается таймаутом чтения клиента раз в ~5 минут: поток,
    /// проживший дольше `RECONNECT_FOR`, открывается снова молча — без `Reconnecting` и пустого
    /// `Lines`. Не открылся — `Reconnecting` уходит тогда. Короткий поток с той же ошибкой — обрыв.
    #[tokio::test(start_paused = true)]
    async fn an_idle_follow_stream_reopens_silently() {
        let (one, two) = (format!("{T1} one"), format!("{T3} two"));
        let long = Duration::from_secs(300);
        let quiet = Steps::new(vec![Ok(idle_after(long, &one)), Ok(held(&[&two]))]);
        let wait = Duration::from_secs(320);
        assert_eq!(
            trace(&run_for(quiet, true, wait).await),
            ["L1:1/1", "L1:2/2"]
        );

        let refused = || crate::k8s::errors::from_transport_text("connection refused");
        let failing = Steps::new(vec![
            Ok(idle_after(long, &one)),
            Err(refused()),
            Ok(held(&[&two])),
        ]);
        assert_eq!(
            trace(&run_for(failing, true, wait).await),
            ["L1:1/1", "R", "L0:1/1", "L1:2/2"]
        );

        let short = Steps::new(vec![
            Ok(idle_after(Duration::from_secs(10), &one)),
            Ok(held(&[&two])),
        ]);
        assert_eq!(
            trace(&run_for(short, true, wait).await),
            ["L1:1/1", "R", "L0:1/1", "L1:2/2"]
        );
    }

    /// Каждое повторное открытие берёт клиент заново (`refresh`: follow живёт дольше учётных
    /// данных exec-плагина), а если кластер не принял и его — новый клиент (`renew`), и лог идёт
    /// дальше.
    #[tokio::test(start_paused = true)]
    async fn a_reconnect_takes_a_fresh_client_and_a_new_one_after_a_rejection() {
        let mut source = Steps::new(vec![
            Ok(lines(&[&format!("{T1} one")])),
            Err(status(401)),
            Ok(held(&[&format!("{T3} two")])),
        ]);
        source.renewable = true;
        let (refreshes, renews) = (source.refreshes.clone(), source.renews.clone());
        assert_eq!(
            texts(&run_for(source, true, Duration::from_secs(20)).await),
            ["one", "<reconnecting>", "two"]
        );
        let count =
            |n: &std::sync::atomic::AtomicUsize| n.load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!(
            (count(&refreshes), count(&renews)),
            (2, 1),
            "первое открытие клиент не берёт заново"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn reconnect_starts_from_the_latest_line() {
        let source = Steps::new(vec![Ok(lines(&[
            &format!("{T3} three"),
            &format!("{T1} one"),
        ]))]);
        let since = source.since.clone();
        run_for(source, true, Duration::from_secs(5)).await;
        let since = since.lock().unwrap().clone();
        assert_eq!(
            since[1].map(|t| t.to_string()).as_deref(),
            Some("2026-10-25T10:00:01Z")
        );
    }

    #[tokio::test(start_paused = true)]
    async fn cancel_does_not_wait_for_the_container_check() {
        let mut source = Steps::new(vec![Ok(lines(&[&format!("{T1} one")]))]);
        source.running = None;
        assert_eq!(
            texts(&run_for(source, true, Duration::from_secs(1)).await),
            ["one"]
        );
    }

    /// Без follow обрыв потока и строка, которой нет дольше таймаута запроса, — ошибка, а не
    /// конец лога; у сохранения — тоже, и файла нет.
    #[tokio::test(start_paused = true)]
    async fn without_follow_a_broken_or_stalled_stream_is_an_error() {
        let one = format!("{T1} one");
        let broken = futures::stream::iter(vec![
            Ok(one.clone()),
            Err(std::io::Error::other("connection reset by peer")),
        ])
        .boxed();
        assert_eq!(
            texts(&run_for(Steps::new(vec![Ok(broken)]), false, Duration::from_secs(1)).await),
            ["one", "<error errors.k8s.connect>"]
        );
        let stalled = || {
            let mut s = Steps::new(vec![Ok(held(&[&one]))]);
            s.timeout = Some(Duration::from_secs(5));
            s
        };
        assert_eq!(
            texts(&run_for(stalled(), false, Duration::from_secs(30)).await),
            ["one", "<error errors.k8s.timeout>"]
        );
        let dir = tempfile::tempdir().unwrap();
        let e = save(
            &stalled(),
            &Matcher::All,
            &dir.path().join("web-1.log"),
            false,
        )
        .await
        .unwrap_err();
        assert_eq!(e.key, "errors.k8s.timeout");
        assert_eq!(
            std::fs::read_dir(dir.path()).unwrap().count(),
            0,
            "ни файла, ни временного файла"
        );
    }

    #[tokio::test]
    async fn lines_are_cut_at_the_cap_and_decoded_lossily() {
        let data = b"short\r\n0123456789abcdef\n\xffok\nlast".to_vec();
        // Маленький буфер: строки приходят частями.
        let reader = futures::io::BufReader::with_capacity(4, futures::io::Cursor::new(data));
        let got: Vec<String> = lossy_lines(reader, 10).map(|l| l.unwrap()).collect().await;
        assert_eq!(got, ["short", "0123456789", "\u{FFFD}ok", "last"]);
    }

    /// Ответ API: `body` на запрос лога, под — на остальные (их число — в `others`).
    fn kube_logs(
        body: Vec<u8>,
        previous: bool,
        follow: bool,
    ) -> (
        KubeLogs,
        std::sync::Arc<std::sync::atomic::AtomicUsize>,
        tokio::task::JoinHandle<Vec<crate::k8s::testing::Seen>>,
    ) {
        let others = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = others.clone();
        let (client, server) = crate::k8s::testing::mock_raw(move |s| {
            if s.path().ends_with("/log") {
                (200, body.clone())
            } else {
                counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let pod = crate::k8s::pods::fixtures::running("web-1");
                (200, serde_json::to_vec(&pod).unwrap())
            }
        });
        let logs = KubeLogs {
            client: StreamClient::fixed(ReadClient::new(client, Duration::from_secs(5))),
            params: LogParams {
                connection_id: "a".into(),
                namespace: "team-a".into(),
                pod: "web-1".into(),
                container: None,
                previous,
                tail_lines: None,
                since_seconds: None,
                since_time: None,
                follow,
                filter: None,
            },
        };
        (logs, others, server)
    }

    /// Сессия без отмены: ждёт, пока она кончится сама.
    async fn run_to_end(source: impl LogSource + 'static, follow: bool) -> Vec<LogEvent> {
        let sink = Collect::default();
        let task = tokio::spawn(run(
            source,
            follow,
            Matcher::All,
            sink.clone(),
            CancellationToken::new(),
        ));
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("лог кончился")
            .unwrap();
        sink.items()
    }

    #[tokio::test]
    async fn lines_that_are_not_utf8_are_shown_and_saved_with_replacement() {
        let body = [
            format!("{T1} one\n").into_bytes(),
            format!("{T2} bad ").into_bytes(),
            b"\xff\xfe end\n".to_vec(),
            format!("{T3} three\n").into_bytes(),
        ]
        .concat();
        let (logs, _, _server) = kube_logs(body.clone(), false, false);
        assert_eq!(
            texts(&run_to_end(logs, false).await),
            ["one", "bad \u{FFFD}\u{FFFD} end", "three", "<end 3/3>"]
        );
        let (logs, _, _server) = kube_logs(body, false, false);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("web-1.log");
        assert_eq!(save(&logs, &Matcher::All, &path, false).await.unwrap(), 3);
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "one\nbad \u{FFFD}\u{FFFD} end\nthree\n"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600, "в логе бывают секреты");
        }
    }

    /// Лог прежнего экземпляра контейнера с follow: конец потока — конец лога, без
    /// переподключения и без проверки контейнера.
    #[tokio::test]
    async fn previous_with_follow_ends_without_reconnecting() {
        let body = format!("{T1} first\n{T2} second\n").into_bytes();
        let (logs, others, server) = kube_logs(body, true, true);
        assert_eq!(
            texts(&run_to_end(logs, true).await),
            ["first", "second", "<end 2/2>"]
        );
        assert_eq!(others.load(std::sync::atomic::Ordering::SeqCst), 0);
        let seen = server.await.unwrap();
        let uris: Vec<&str> = seen.iter().map(|s| s.uri.as_str()).collect();
        assert!(uris.iter().all(|u| !u.contains("follow")), "{uris:?}");
    }

    #[test]
    fn a_start_time_must_be_rfc3339() {
        let bad = LogParams {
            connection_id: "a".into(),
            namespace: "team-a".into(),
            pod: "web-1".into(),
            container: None,
            previous: false,
            tail_lines: None,
            since_seconds: None,
            since_time: Some("yesterday".into()),
            follow: false,
            filter: None,
        };
        assert_eq!(
            check_since(&bad).unwrap_err().key,
            "errors.logs.badSinceTime"
        );
        let ok = LogParams {
            since_time: Some(T1.into()),
            ..bad
        };
        assert!(check_since(&ok).is_ok());
    }

    #[tokio::test]
    async fn kube_logs_ask_for_a_tail_with_timestamps_and_save_writes_a_file() {
        let (client, server) = crate::k8s::testing::mock_raw(|s| {
            if s.path().ends_with("/log") {
                (200, format!("{T1} first\n{T2} second error\n").into_bytes())
            } else {
                let pod = crate::k8s::pods::fixtures::running("web-1");
                (200, serde_json::to_vec(&pod).unwrap())
            }
        });
        let reader = ReadClient::new(client, Duration::from_secs(5));
        let logs = KubeLogs {
            client: StreamClient::fixed(reader.clone()),
            params: LogParams {
                connection_id: "a".into(),
                namespace: "team-a".into(),
                pod: "web-1".into(),
                container: Some("web".into()),
                previous: true,
                tail_lines: None,
                since_seconds: None,
                since_time: None,
                follow: false,
                filter: None,
            },
        };
        assert!(
            !logs.running().await.unwrap(),
            "прежний экземпляр контейнера не работает"
        );
        let current = KubeLogs {
            client: StreamClient::fixed(reader),
            params: LogParams {
                previous: false,
                ..logs.params.clone()
            },
        };
        assert!(current.running().await.unwrap());
        drop(current);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("web-1.log");
        let matcher = Matcher::new(Some(&LogFilter {
            text: "error".into(),
            regex: false,
            case_sensitive: false,
        }))
        .unwrap();
        assert_eq!(save(&logs, &matcher, &path, true).await.unwrap(), 1);
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            format!("{T2} second error\n")
        );
        let since = Timestamp::from_second(1_792_922_400).unwrap();
        let lp = logs.kube_params(Some(since));
        assert_eq!((lp.tail_lines, lp.since_time), (None, Some(since)));
        drop(logs);
        let seen = server.await.unwrap();
        let log = seen.iter().find(|s| s.path().ends_with("/log")).unwrap();
        assert_eq!(log.path(), "/api/v1/namespaces/team-a/pods/web-1/log");
        for q in [
            "container=web",
            "previous=true",
            "tailLines=500",
            "timestamps=true",
        ] {
            assert!(log.uri.contains(q), "{q}: {}", log.uri);
        }
        assert!(!log.uri.contains("follow"));
    }
}
