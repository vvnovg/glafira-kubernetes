//! MCP-сервер шлюза (спец. §10.1, §10.7): приём соединений шима, рукопожатие, сессии, право на
//! вызовы, лимиты и журнал. Инструменты задаёт крейт продукта: его обработчик (`ServerHandler`)
//! создаётся на каждое соединение функцией `make` и вызывает инструменты через [`Session::call`].

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rmcp::model::{CallToolResult, ContentBlock, Implementation};
use rmcp::service::RequestContext;
use rmcp::{RoleServer, ServerHandler, ServiceExt};
use serde::Serialize;
use serde_json::Value;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::audit::{
    AuditConnection, AuditEvent, AuditLog, Outcome, now_ms, read_counts, truncate_args,
};
use crate::endpoint::{BoxStream, Endpoint, Listener};
use crate::fit::HARD_LIMIT_BYTES;
use crate::limits::{CALLS_PER_MINUTE, GLOBAL_CONCURRENCY, RateLimiter, SESSION_CONCURRENCY};
use crate::session::{BackendError, ClientIdent, Hooks, SessionAccess, SessionView};

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionSnapshot {
    pub id: String,
    pub client_name: Option<String>,
    pub client_version: Option<String>,
    pub started_ms: u64,
    pub calls: u64,
    /// Идёт вызов (для подсветки индикатора).
    pub active: bool,
}

struct SessionEntry {
    client: Option<ClientIdent>,
    started_ms: u64,
    calls: u64,
    in_flight: u32,
    limiter: RateLimiter,
    cancel: CancellationToken,
}

struct Shared {
    hooks: Arc<dyn Hooks>,
    audit: Arc<AuditLog>,
    global: Arc<Semaphore>,
    sessions: Mutex<HashMap<String, SessionEntry>>,
}

impl Shared {
    fn sessions(&self) -> std::sync::MutexGuard<'_, HashMap<String, SessionEntry>> {
        self.sessions.lock().unwrap_or_else(|e| e.into_inner())
    }
}

static NEXT_SESSION: AtomicU64 = AtomicU64::new(1);

pub struct McpServerHandle {
    shared: Arc<Shared>,
    stop: CancellationToken,
    task: JoinHandle<()>,
}

impl McpServerHandle {
    pub fn sessions(&self) -> Vec<SessionSnapshot> {
        let mut v: Vec<SessionSnapshot> = self
            .shared
            .sessions()
            .iter()
            .map(|(id, e)| SessionSnapshot {
                id: id.clone(),
                client_name: e.client.as_ref().map(|c| c.name.clone()),
                client_version: e.client.as_ref().map(|c| c.version.clone()),
                started_ms: e.started_ms,
                calls: e.calls,
                active: e.in_flight > 0,
            })
            .collect();
        v.sort_by_key(|s| s.started_ms);
        v
    }

    pub fn disconnect(&self, id: &str) -> bool {
        match self.shared.sessions().get(id) {
            Some(e) => {
                e.cancel.cancel();
                true
            }
            None => false,
        }
    }

    pub fn disconnect_client(&self, name: &str) -> usize {
        let sessions = self.shared.sessions();
        let mut n = 0;
        for e in sessions.values() {
            if e.client.as_ref().is_some_and(|c| c.name == name) {
                e.cancel.cancel();
                n += 1;
            }
        }
        n
    }

    pub fn disconnect_all(&self) {
        for e in self.shared.sessions().values() {
            e.cancel.cancel();
        }
    }

    /// Закрывает все сессии и перестаёт слушать; файл сокета удаляется.
    pub async fn shutdown(self) {
        self.stop.cancel();
        let _ = self.task.await;
    }
}

/// Начинает слушать `endpoint`; на каждое соединение с верным токеном `make` создаёт обработчик
/// MCP. Вызывать внутри runtime tokio.
pub async fn start<H, F>(
    endpoint: &Endpoint,
    token: String,
    hooks: Arc<dyn Hooks>,
    audit: Arc<AuditLog>,
    make: F,
) -> std::io::Result<McpServerHandle>
where
    H: ServerHandler,
    F: Fn(Session) -> H + Send + Sync + 'static,
{
    let mut listener = Listener::bind(endpoint).await?;
    audit.record(AuditEvent::ServerStarted {
        endpoint: endpoint.clone(),
    });
    let shared = Arc::new(Shared {
        hooks,
        audit,
        global: Arc::new(Semaphore::new(GLOBAL_CONCURRENCY)),
        sessions: Mutex::new(HashMap::new()),
    });
    let make = Arc::new(make);
    let stop = CancellationToken::new();
    let task = {
        let (shared, stop) = (shared.clone(), stop.clone());
        tokio::spawn(async move {
            let mut conns = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    _ = stop.cancelled() => break,
                    r = listener.accept() => match r {
                        Ok(stream) => {
                            conns.spawn(handle_conn(
                                shared.clone(),
                                stream,
                                token.clone(),
                                stop.child_token(),
                                make.clone(),
                            ));
                        }
                        Err(e) => {
                            tracing::warn!("mcp accept failed: {e}");
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                    },
                }
            }
            drop(listener);
            while conns.join_next().await.is_some() {}
            shared.audit.record(AuditEvent::ServerStopped);
        })
    };
    Ok(McpServerHandle { shared, stop, task })
}

async fn handle_conn<H, F>(
    shared: Arc<Shared>,
    mut stream: BoxStream,
    token: String,
    ct: CancellationToken,
    make: Arc<F>,
) where
    H: ServerHandler,
    F: Fn(Session) -> H + Send + Sync + 'static,
{
    // Остановка сервера не ждёт молчащее рукопожатие (до 5 с).
    let handshake = tokio::select! {
        _ = ct.cancelled() => return,
        r = crate::handshake::server(&mut stream, &token) => r,
    };
    if let Err(e) = handshake {
        tracing::debug!("mcp handshake failed: {e}");
        return;
    }
    let id = format!("s{}", NEXT_SESSION.fetch_add(1, Ordering::Relaxed));
    shared.sessions().insert(
        id.clone(),
        SessionEntry {
            client: None,
            started_ms: now_ms(),
            calls: 0,
            in_flight: 0,
            limiter: RateLimiter::new(CALLS_PER_MINUTE),
            cancel: ct.clone(),
        },
    );
    shared.audit.record(AuditEvent::SessionStarted {
        session: id.clone(),
    });
    shared.hooks.sessions_changed();
    let handler = make(Session {
        shared: shared.clone(),
        id: id.clone(),
        local: Arc::new(Semaphore::new(SESSION_CONCURRENCY)),
        long: Arc::new(Semaphore::new(1)),
    });
    match handler.serve_with_ct(stream, ct).await {
        Ok(running) => {
            let _ = running.waiting().await;
        }
        Err(e) => tracing::debug!("mcp session {id} failed to start: {e}"),
    }
    let entry = shared.sessions().remove(&id);
    // Ждущие слота длительной операции уходят вместе с сессией (токены запросов — его дети).
    if let Some(e) = &entry {
        e.cancel.cancel();
    }
    shared.audit.record(AuditEvent::SessionEnded {
        session: id.clone(),
        client: entry
            .as_ref()
            .and_then(|e| e.client.as_ref())
            .map(|c| c.name.clone()),
        calls: entry.map_or(0, |e| e.calls),
    });
    shared.hooks.session_closed(&id);
    shared.hooks.sessions_changed();
}

/// Имя и версия клиента приходят от него самого: длина ограничена (диалог, `settings.json`).
const MAX_CLIENT_FIELD_CHARS: usize = 128;

fn capped(s: &str) -> String {
    s.trim().chars().take(MAX_CLIENT_FIELD_CHARS).collect()
}

fn normalize_client(c: Implementation) -> Option<ClientIdent> {
    let name = capped(&c.name);
    (!name.is_empty()).then(|| ClientIdent {
        name,
        version: capped(&c.version),
    })
}

fn tool_error(e: &BackendError) -> CallToolResult {
    CallToolResult::error(vec![ContentBlock::text(format!(
        "{}: {}",
        e.code, e.message
    ))])
}

/// Значение ответа инструмента; ошибка сериализации — `internal`.
pub fn to_value<T: Serialize>(v: &T) -> Result<Value, BackendError> {
    serde_json::to_value(v).map_err(|e| BackendError::new("internal", e.to_string()))
}

/// Сессия одного соединения: через неё обработчик продукта узнаёт клиента и вызывает
/// инструменты с проверкой права, лимитами и журналом.
#[derive(Clone)]
pub struct Session {
    shared: Arc<Shared>,
    id: String,
    local: Arc<Semaphore>,
    /// Слот длительной операции (наблюдение, логи): один на сессию; ждущие — по очереди (FIFO).
    long: Arc<Semaphore>,
}

/// Слоты параллельности операции с кластером: сессии и всех сессий.
pub struct Slots {
    _local: OwnedSemaphorePermit,
    _global: OwnedSemaphorePermit,
}

/// Снимает отметку «идёт вызов» при любом выходе.
pub struct Flag {
    session: Session,
}

impl Drop for Flag {
    fn drop(&mut self) {
        if let Some(e) = self.session.shared.sessions().get_mut(&self.session.id) {
            e.in_flight = e.in_flight.saturating_sub(1);
        }
        self.session.shared.hooks.sessions_changed();
    }
}

/// Слот длительной операции сессии; освобождается при любом выходе.
pub struct Exclusive {
    _permit: OwnedSemaphorePermit,
}

/// Срок ожидания для текста ошибки: секунды, короче секунды (в тестах) — миллисекунды.
fn waited(d: Duration) -> String {
    if d >= Duration::from_secs(1) {
        format!("{} s", d.as_secs())
    } else {
        format!("{} ms", d.as_millis())
    }
}

impl Session {
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Клиент запоминается при первом запросе, где он известен, и дальше не меняется.
    /// Имя без пробелов по краям; пустое — как отсутствующее.
    pub fn identify(&self, client: Option<Implementation>) -> SessionView {
        let client = client.and_then(normalize_client);
        let mut sessions = self.shared.sessions();
        let entry = sessions.get_mut(&self.id);
        let mut changed = false;
        let known = match entry {
            Some(e) => {
                if e.client.is_none()
                    && let Some(c) = client
                {
                    e.client = Some(c);
                    changed = true;
                }
                e.client.clone()
            }
            None => None,
        };
        drop(sessions);
        if changed {
            self.shared.hooks.sessions_changed();
        }
        SessionView {
            id: self.id.clone(),
            client: known,
        }
    }

    /// `initialize` или запрос с клиентом в `_meta` (2026-07-28): клиент известен — подтверждение
    /// запрашивается сразу, а не при первом вызове.
    pub fn initialized(&self, client: Implementation) {
        let view = self.identify(Some(client));
        let _ = self.shared.hooks.access(&view);
    }

    fn cancel_session(&self) {
        if let Some(e) = self.shared.sessions().get(&self.id) {
            e.cancel.cancel();
        }
    }

    /// Право, лимит частоты и отметка «идёт вызов».
    fn admit(&self, view: &SessionView) -> Result<Flag, BackendError> {
        match self.shared.hooks.access(view) {
            SessionAccess::Approved => {}
            SessionAccess::Pending => {
                return Err(BackendError::new(
                    "client_not_approved",
                    "The user has not approved this MCP client in Glafira yet. Ask them to approve it in the Glafira window, then retry.",
                ));
            }
            SessionAccess::Rejected => {
                self.cancel_session();
                return Err(BackendError::new(
                    "client_rejected",
                    "The user rejected this MCP client in Glafira",
                ));
            }
            SessionAccess::Disabled => {
                return Err(BackendError::new(
                    "mcp_disabled",
                    "MCP is disabled in Glafira",
                ));
            }
        }
        let mut sessions = self.shared.sessions();
        let Some(e) = sessions.get_mut(&self.id) else {
            return Err(BackendError::new(
                "mcp_disabled",
                "The MCP session is closed",
            ));
        };
        if !e.limiter.try_take() {
            return Err(BackendError::new(
                "rate_limited",
                format!("Too many calls: at most {CALLS_PER_MINUTE} per minute; retry later"),
            ));
        }
        e.calls += 1;
        e.in_flight += 1;
        drop(sessions);
        self.shared.hooks.sessions_changed();
        Ok(Flag {
            session: self.clone(),
        })
    }

    /// Слоты параллельности для операции с кластером: сначала сессии, затем всех сессий.
    pub async fn slots(&self) -> Result<Slots, BackendError> {
        let local = self
            .local
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| BackendError::unexpected())?;
        let global = self
            .shared
            .global
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| BackendError::unexpected())?;
        Ok(Slots {
            _local: local,
            _global: global,
        })
    }

    /// Длительная операция (наблюдение, логи) — одна на сессию. Клиенты шлют вызовы пачкой,
    /// поэтому следующий не отказывает сразу, а ждёт в очереди (FIFO) до `wait`; не дождался —
    /// `busy`. Отмена запроса клиентом или конец сессии (токен запроса — потомок токена сессии)
    /// снимает ожидание; слот занимают и освобождают только дождавшиеся.
    pub async fn exclusive(
        &self,
        ctx: &RequestContext<RoleServer>,
        wait: Duration,
    ) -> Result<Exclusive, BackendError> {
        let acquire = tokio::time::timeout(wait, self.long.clone().acquire_owned());
        tokio::select! {
            biased;
            _ = ctx.ct.cancelled() => Err(BackendError::new(
                "cancelled",
                "The request was cancelled while waiting for another long operation",
            )),
            r = acquire => match r {
                Ok(Ok(permit)) => Ok(Exclusive { _permit: permit }),
                Ok(Err(_)) => Err(BackendError::unexpected()),
                Err(_) => Err(BackendError::new(
                    "busy",
                    format!(
                        "Another long operation is still running in this session after waiting {}; try again later",
                        waited(wait)
                    ),
                )),
            },
        }
    }

    /// Общая обвязка инструмента: право, лимит, предел размера ответа, журнал, ответ или ошибка
    /// для LLM. `args` — аргументы для журнала (без текстов, которые писать нельзя).
    pub async fn call<F>(
        &self,
        ctx: &RequestContext<RoleServer>,
        tool: &str,
        args: Value,
        connection_id: Option<&str>,
        body: F,
    ) -> CallToolResult
    where
        F: Future<Output = Result<Value, BackendError>>,
    {
        let started = tokio::time::Instant::now();
        let view = self.identify(ctx.client_info());
        // Отметка «идёт вызов» снимается после записи в журнал: интерфейс перечитывает журнал по
        // событию статуса и должен застать в нём этот вызов.
        let (flag, result) = match self.admit(&view) {
            Ok(flag) => (Some(flag), body.await),
            Err(e) => (None, Err(e)),
        };
        let result = result.and_then(|v| {
            if serde_json::to_vec(&v).map_or(0, |b| b.len()) > HARD_LIMIT_BYTES {
                Err(BackendError::new(
                    "response_too_large",
                    "The response is larger than 100 KB even after truncation; narrow the request: a filter, fewer items or a smaller page",
                ))
            } else {
                Ok(v)
            }
        });
        let counts = result.as_ref().ok().and_then(read_counts);
        let (out, outcome, code, bytes) = match result {
            Ok(v) => {
                let r = CallToolResult::structured(v);
                let bytes = r
                    .content
                    .iter()
                    .map(|c| c.as_text().map_or(0, |t| t.text.len()))
                    .sum();
                (r, Outcome::Ok, None, bytes)
            }
            Err(e) => (tool_error(&e), Outcome::Error, Some(e.code), 0),
        };
        self.shared.audit.record(AuditEvent::Call {
            session: self.id.clone(),
            client: view.client.map(|c| format!("{} {}", c.name, c.version)),
            tool: tool.to_string(),
            connection: connection_id.map(|id| AuditConnection {
                id: id.to_string(),
                name: self.shared.hooks.connection_name(id).unwrap_or_default(),
            }),
            args: truncate_args(args),
            outcome,
            code,
            bytes,
            ms: started.elapsed().as_millis() as u64,
            scanned: counts.map(|c| c.0),
            returned: counts.map(|c| c.1),
            masked: counts.map(|c| c.2),
        });
        drop(flag);
        out
    }
}
