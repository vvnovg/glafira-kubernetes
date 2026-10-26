//! Логи контейнера (спец. §6.6): хвост (по умолчанию 500 строк) или с момента, предыдущий
//! контейнер, follow. Фильтр (подстрока или regex) применяется здесь, до отправки в интерфейс;
//! строка длиннее 64 КБ обрезается. В follow при потере соединения — переподключение с
//! `sinceTime` последней строки (повторы отбрасываются), через 60 с без восстановления — ошибка.

use std::path::Path;
use std::time::Duration;

use futures::AsyncBufReadExt;
use futures::future::BoxFuture;
use futures::stream::{BoxStream, StreamExt};
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::jiff::Timestamp;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::error::{AppError, AppResult, ErrorCode};
use crate::k8s::read::ReadClient;
use crate::sessions::{BATCH_WINDOW, Batch, Sink};

/// Сколько строк с конца читается, если не задано ни начало, ни число строк.
pub const DEFAULT_TAIL: i64 = 500;
/// Строка длиннее обрезается.
pub const MAX_LINE: usize = 64 * 1024;
/// Сколько follow ждёт восстановления соединения.
pub const RECONNECT_FOR: Duration = Duration::from_secs(60);

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

/// Откуда читается лог: в приложении — API-сервер, в тестах — заготовленные строки.
pub trait LogSource: Send + Sync {
    /// Поток строк (с временем в начале): по параметрам сессии или, при переподключении, с `since`.
    fn open(
        &self,
        since: Option<Timestamp>,
    ) -> BoxFuture<'_, AppResult<BoxStream<'static, std::io::Result<String>>>>;

    /// Работает ли ещё контейнер: если нет, конец потока — конец лога, а не обрыв.
    fn running(&self) -> BoxFuture<'_, AppResult<bool>>;
}

/// Лог контейнера через API-сервер.
pub struct KubeLogs {
    pub reader: ReadClient,
    pub params: LogParams,
}

impl KubeLogs {
    fn kube_params(&self, since: Option<Timestamp>) -> kube::api::LogParams {
        let p = &self.params;
        let mut lp = kube::api::LogParams {
            container: p.container.clone(),
            follow: p.follow,
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
}

impl LogSource for KubeLogs {
    fn open(
        &self,
        since: Option<Timestamp>,
    ) -> BoxFuture<'_, AppResult<BoxStream<'static, std::io::Result<String>>>> {
        Box::pin(async move {
            let lp = self.kube_params(since);
            let r = self
                .reader
                .log_stream(&self.params.namespace, &self.params.pod, &lp)
                .await?;
            Ok(r.lines().boxed())
        })
    }

    fn running(&self) -> BoxFuture<'_, AppResult<bool>> {
        Box::pin(async move {
            let pod: Pod = self
                .reader
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

fn lost() -> AppError {
    app_err!(
        ErrorCode::ConnectionFailed,
        "errors.logs.lost",
        "The log stream was lost and did not recover in 60 s"
    )
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
    let mut last: Option<Timestamp> = None;
    let mut since: Option<Timestamp> = None;
    let mut lost_at: Option<tokio::time::Instant> = None;
    let mut attempt = 0;
    loop {
        let opened = tokio::select! {
            _ = cancel.cancelled() => return,
            s = source.open(since) => s,
        };
        let mut stream = match opened {
            Ok(s) => s,
            Err(error) if lost_at.is_none() => {
                sink.send(LogEvent::Error { error });
                return;
            }
            Err(_) => {
                if lost_at.is_some_and(|t| t.elapsed() >= RECONNECT_FOR) {
                    sink.send(LogEvent::Error { error: lost() });
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
        let mut batch: Batch<LogLine> = Batch::default();
        let mut tick = tokio::time::interval(BATCH_WINDOW);
        // После переподключения строки до `last` включительно уже показаны.
        let resume_after = since.and(last);
        loop {
            let line = tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tick.tick() => {
                    if batch.is_due()
                        && !sink.send(LogEvent::Lines { lines: batch.take(), scanned, shown })
                    {
                        return;
                    }
                    continue;
                }
                line = stream.next() => line,
            };
            let Some(Ok(raw)) = line else { break };
            let (ts, text) = split_line(&raw);
            if resume_after.is_some_and(|after| ts.is_some_and(|t| t <= after)) {
                continue;
            }
            // Строки приходят — соединение восстановлено.
            lost_at = None;
            attempt = 0;
            if ts.is_some() {
                last = ts;
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
                if let Some(full) = batch.push(line)
                    && !sink.send(LogEvent::Lines {
                        lines: full,
                        scanned,
                        shown,
                    })
                {
                    return;
                }
            }
        }
        if !batch.is_empty()
            && !sink.send(LogEvent::Lines {
                lines: batch.take(),
                scanned,
                shown,
            })
        {
            return;
        }
        if !follow || matches!(source.running().await, Ok(false)) {
            sink.send(LogEvent::End { scanned, shown });
            return;
        }
        let started = *lost_at.get_or_insert_with(tokio::time::Instant::now);
        if started.elapsed() >= RECONNECT_FOR {
            sink.send(LogEvent::Error { error: lost() });
            return;
        }
        // С начала секунды последней строки: API округляет `sinceTime` до секунд.
        since = Some(last.map_or_else(Timestamp::now, |t| {
            Timestamp::from_second(t.as_second()).unwrap_or(t)
        }));
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

/// Сохраняет лог (без follow) в файл: подошедшие под фильтр строки целиком, с временем, если
/// `timestamps`. Возвращает число записанных строк.
pub async fn save(
    source: &impl LogSource,
    matcher: &Matcher,
    path: &Path,
    timestamps: bool,
) -> AppResult<u64> {
    let mut stream = source.open(None).await?;
    let mut out = String::new();
    let mut n = 0;
    while let Some(raw) = stream.next().await {
        let raw = raw?;
        let (ts, text) = split_line(&raw);
        if !matcher.matches(text) {
            continue;
        }
        if timestamps && let Some(t) = ts {
            out.push_str(&t.to_string());
            out.push(' ');
        }
        out.push_str(text);
        out.push('\n');
        n += 1;
    }
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || std::fs::write(path, out))
        .await
        .map_err(|e| AppError::internal_with(e.to_string()))??;
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;
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
        let logs = KubeLogs {
            reader: ReadClient::new(client, Duration::from_secs(5)),
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
        assert!(logs.running().await.unwrap());
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
