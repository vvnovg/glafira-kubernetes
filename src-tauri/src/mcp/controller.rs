//! Жизненный цикл MCP-сервера (спец. §10.1): запуск и остановка по настройке и политике,
//! `mcp.json`, статус для интерфейса, ответы на подтверждение, отзыв клиентов.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use glafira_mcp_core::audit::now_ms;
use glafira_mcp_core::{AuditEvent, AuditLog, McpServerHandle, SessionSnapshot};
use glafira_mcp_kubernetes::GatewayOptions;
use serde::Serialize;

use super::approvals::{ApprovalRequest, Approvals};
use super::backend::{AppBackend, OnChange};
use super::files::{self, McpFile};
use crate::error::{AppResult, ErrorCode};
use crate::i18n::LocalizedText;
use crate::settings::ApprovedClient;
use crate::state::AppState;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ServerState {
    Off,
    Running,
    /// MCP обслуживает другой запущенный экземпляр Glafira.
    OtherInstance,
    Error,
    DisabledByPolicy,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct McpSessionInfo {
    #[serde(flatten)]
    pub session: SessionSnapshot,
    pub approved: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct McpStatus {
    pub state: ServerState,
    pub enabled: bool,
    pub policy_allows: bool,
    pub error: Option<LocalizedText>,
    pub sessions: Vec<McpSessionInfo>,
    pub pending: Vec<ApprovalRequest>,
    pub approved_clients: Vec<ApprovedClient>,
    /// Подключений с доступом для LLM (для текста диалога подтверждения).
    pub allowed_connections: usize,
}

/// Связь с окном: событие статуса и привлечение внимания.
pub trait McpUi: Send + Sync {
    fn status_changed(&self, status: &McpStatus);
    fn request_attention(&self);
}

struct Inner {
    handle: Option<McpServerHandle>,
    /// Записанный этим сервером `mcp.json`: удаляется только он.
    file: Option<McpFile>,
    state: ServerState,
    error: Option<LocalizedText>,
}

pub struct McpController {
    state: AppState,
    data_dir: PathBuf,
    audit: Arc<AuditLog>,
    approvals: Arc<Approvals>,
    ui: OnceLock<Box<dyn McpUi>>,
    inner: Mutex<Inner>,
    /// Запуск и остановка не пересекаются.
    ops: tokio::sync::Mutex<()>,
}

impl McpController {
    pub fn new(state: AppState, data_dir: &Path) -> Arc<Self> {
        Arc::new(Self {
            audit: Arc::new(AuditLog::new(data_dir)),
            approvals: Arc::new(Approvals::new(state.settings.clone())),
            state,
            data_dir: data_dir.to_path_buf(),
            ui: OnceLock::new(),
            inner: Mutex::new(Inner {
                handle: None,
                file: None,
                state: ServerState::Off,
                error: None,
            }),
            ops: tokio::sync::Mutex::new(()),
        })
    }

    pub fn set_ui(&self, ui: Box<dyn McpUi>) {
        let _ = self.ui.set(ui);
    }

    fn inner(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn policy_allows(&self) -> bool {
        self.state.policy.status.mcp_enabled
    }

    pub fn status(&self) -> McpStatus {
        let inner = self.inner();
        let sessions = inner
            .handle
            .as_ref()
            .map(|h| h.sessions())
            .unwrap_or_default()
            .into_iter()
            .map(|s| McpSessionInfo {
                approved: self.approvals.is_approved(&s.id),
                session: s,
            })
            .collect();
        McpStatus {
            state: inner.state,
            enabled: self.state.settings.mcp().enabled,
            policy_allows: self.policy_allows(),
            error: inner.error.clone(),
            sessions,
            pending: self.approvals.pending(),
            approved_clients: self.state.settings.mcp().approved_clients,
            allowed_connections: self
                .state
                .connections
                .list()
                .iter()
                .filter(|p| !p.llm_access.is_off())
                .count(),
        }
    }

    fn emit(&self) {
        if let Some(ui) = self.ui.get() {
            ui.status_changed(&self.status());
        }
    }

    fn attention(&self) {
        if let Some(ui) = self.ui.get() {
            ui.request_attention();
        }
    }

    /// Приводит сервер в соответствие с настройкой и политикой. Вызывать в runtime tokio.
    pub async fn apply(self: &Arc<Self>) {
        let _op = self.ops.lock().await;
        let wanted = self.state.settings.mcp().enabled && self.policy_allows();
        let running = self.inner().handle.is_some();
        if wanted && !running {
            self.start().await;
        } else if !wanted && running {
            self.stop().await;
        } else if !wanted {
            let mut inner = self.inner();
            inner.state = ServerState::Off;
            inner.error = None;
        }
        if !self.policy_allows() {
            self.inner().state = ServerState::DisabledByPolicy;
        }
        self.emit();
    }

    async fn start(self: &Arc<Self>) {
        if let Some(f) = files::read(&self.data_dir) {
            if glafira_mcp_core::endpoint::connect(&f.endpoint)
                .await
                .is_ok()
            {
                let mut inner = self.inner();
                inner.state = ServerState::OtherInstance;
                inner.error = None;
                return;
            }
            // Сервер из файла не отвечает (крах прежнего запуска): файл удаляется до bind, чтобы
            // шим не ходил по мёртвому адресу, если новый сервер не поднимется.
            files::remove_if(&self.data_dir, &f);
        }
        let endpoint = glafira_mcp_core::endpoint::choose(
            &self.data_dir,
            crate::product::MCP_SERVER_NAME,
            &files::new_suffix(),
        );
        let token = files::new_token();
        let weak = Arc::downgrade(self);
        let on_change: OnChange = Arc::new(move |attention| {
            if let Some(c) = weak.upgrade() {
                if attention {
                    c.attention();
                }
                c.emit();
            }
        });
        let backend = Arc::new(AppBackend::new(
            self.state.clone(),
            self.approvals.clone(),
            on_change,
        ));
        let options =
            GatewayOptions::new(crate::product::MCP_SERVER_NAME, env!("CARGO_PKG_VERSION"));
        let started = glafira_mcp_kubernetes::start(
            &endpoint,
            token.clone(),
            backend,
            self.audit.clone(),
            options,
        )
        .await;
        let handle = match started {
            Ok(h) => h,
            Err(e) => {
                tracing::warn!("mcp server not started: {e}");
                let mut inner = self.inner();
                inner.state = ServerState::Error;
                inner.error = Some(ltext!(
                    "errors.mcp.startFailed",
                    "The MCP server did not start: {{error}}",
                    error = e
                ));
                return;
            }
        };
        let file = McpFile {
            version: files::VERSION,
            endpoint,
            token,
            pid: std::process::id(),
        };
        if let Err(e) = files::write(&self.data_dir, &file) {
            handle.shutdown().await;
            let mut inner = self.inner();
            inner.state = ServerState::Error;
            inner.error = Some(e.text());
            return;
        }
        tracing::info!("mcp server started");
        let mut inner = self.inner();
        inner.handle = Some(handle);
        inner.file = Some(file);
        inner.state = ServerState::Running;
        inner.error = None;
    }

    async fn stop(&self) {
        let (handle, file) = {
            let mut inner = self.inner();
            (inner.handle.take(), inner.file.take())
        };
        if let Some(f) = &file {
            files::remove_if(&self.data_dir, f);
        }
        if let Some(h) = handle {
            h.shutdown().await;
        }
        let mut inner = self.inner();
        inner.state = ServerState::Off;
        inner.error = None;
    }

    /// При выходе из приложения: сервер останавливается, `mcp.json` удаляется.
    pub async fn shutdown(&self) {
        let _op = self.ops.lock().await;
        if self.inner().handle.is_some() {
            self.stop().await;
        }
    }

    /// Выход мимо `RunEvent::Exit` (установщик обновления на Windows вызывает `process::exit` из
    /// синхронного хука, runtime не дождаться): свой `mcp.json` удаляется сразу, сессии
    /// закрываются; слушатель уходит вместе с процессом.
    pub fn exit_now(&self) {
        let inner = self.inner();
        if let Some(f) = &inner.file {
            files::remove_if(&self.data_dir, f);
        }
        if let Some(h) = &inner.handle {
            h.disconnect_all();
        }
    }

    pub async fn set_enabled(self: &Arc<Self>, enabled: bool) -> AppResult<McpStatus> {
        if enabled && !self.policy_allows() {
            return Err(app_err!(
                ErrorCode::InvalidInput,
                "errors.mcp.disabledByPolicy",
                "MCP is disabled by your organization"
            ));
        }
        let st = self.state.clone();
        crate::commands::blocking(move || st.settings.set_mcp_enabled(enabled)).await?;
        self.apply().await;
        Ok(self.status())
    }

    /// Ответ на подтверждение. Блокирующий вызов (запись настроек).
    pub fn answer(&self, session_id: &str, approve: bool) -> AppResult<McpStatus> {
        if let Some(answered) = self.approvals.answer(session_id, approve, now_ms())? {
            let client = answered.client.map(|c| c.name);
            let session = session_id.to_string();
            if approve {
                for session in std::iter::once(session).chain(answered.also_approved) {
                    self.audit.record(AuditEvent::Approved {
                        session,
                        client: client.clone(),
                    });
                }
            } else {
                self.audit.record(AuditEvent::Rejected { session, client });
            }
            if !approve && let Some(h) = &self.inner().handle {
                h.disconnect(session_id);
            }
        }
        self.emit();
        Ok(self.status())
    }

    /// Отзыв запомненного клиента: его сессии закрываются. Блокирующий вызов.
    pub fn revoke(&self, name: &str) -> AppResult<McpStatus> {
        if self.approvals.revoke(name)? {
            self.audit.record(AuditEvent::Revoked {
                client: name.to_string(),
            });
        }
        if let Some(h) = &self.inner().handle {
            h.disconnect_client(name);
        }
        self.emit();
        Ok(self.status())
    }

    /// Закрывает одну сессию или все (`None`).
    pub fn disconnect(&self, session_id: Option<&str>) -> McpStatus {
        if let Some(h) = &self.inner().handle {
            match session_id {
                Some(id) => {
                    h.disconnect(id);
                }
                None => h.disconnect_all(),
            }
        }
        self.status()
    }

    pub fn audit_recent(&self, limit: usize) -> Vec<serde_json::Value> {
        self.audit.recent(limit)
    }

    pub fn audit_path(&self) -> PathBuf {
        self.audit.path()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::secrets::MemoryStore;
    use crate::storage::profile::{LlmAccess, sample_profile};

    struct Recorder(Mutex<Vec<McpStatus>>, Mutex<u32>);

    impl McpUi for Arc<Recorder> {
        fn status_changed(&self, status: &McpStatus) {
            self.0.lock().unwrap().push(status.clone());
        }
        fn request_attention(&self) {
            *self.1.lock().unwrap() += 1;
        }
    }

    fn controller(policy_allows: bool) -> (tempfile::TempDir, Arc<McpController>, Arc<Recorder>) {
        let dir = tempfile::tempdir().unwrap();
        let mut st = AppState::new(dir.path(), Arc::new(MemoryStore::default())).unwrap();
        let mut policy = (*st.policy).clone();
        policy.status.mcp_enabled = policy_allows;
        st.policy = Arc::new(policy);
        let mut p = sample_profile("c1");
        p.llm_access = LlmAccess::Metadata;
        st.connections.upsert(p).unwrap();
        let c = McpController::new(st, dir.path());
        let rec = Arc::new(Recorder(Mutex::new(Vec::new()), Mutex::new(0)));
        c.set_ui(Box::new(rec.clone()));
        (dir, c, rec)
    }

    #[tokio::test]
    async fn enabling_starts_server_and_writes_file_disabling_cleans_up() {
        let (dir, c, rec) = controller(true);
        c.apply().await;
        assert_eq!(c.status().state, ServerState::Off);
        let s = c.set_enabled(true).await.unwrap();
        assert_eq!(
            (s.state, s.enabled, s.allowed_connections),
            (ServerState::Running, true, 1)
        );
        let f = files::read(dir.path()).unwrap();
        assert!(
            glafira_mcp_core::endpoint::connect(&f.endpoint)
                .await
                .is_ok()
        );
        let s = c.set_enabled(false).await.unwrap();
        assert_eq!(s.state, ServerState::Off);
        assert_eq!(files::read(dir.path()), None);
        assert!(
            rec.0.lock().unwrap().len() >= 2,
            "статус уходит в интерфейс"
        );
    }

    #[tokio::test]
    async fn second_instance_does_not_take_over() {
        let (dir, first, _) = controller(true);
        first.set_enabled(true).await.unwrap();
        let st2 = {
            let mut st = AppState::new(dir.path(), Arc::new(MemoryStore::default())).unwrap();
            st.policy = first.state.policy.clone();
            st
        };
        let second = McpController::new(st2, dir.path());
        second.apply().await;
        assert_eq!(second.status().state, ServerState::OtherInstance);
        first.shutdown().await;
        assert_eq!(files::read(dir.path()), None);
    }

    #[tokio::test]
    async fn stale_file_of_a_crashed_instance_is_removed_before_bind() {
        let (dir, c, _) = controller(true);
        files::write(
            dir.path(),
            &McpFile {
                version: files::VERSION,
                endpoint: dir.path().join("dead.sock").to_string_lossy().into_owned(),
                token: files::new_token(),
                pid: 1,
            },
        )
        .unwrap();
        // Сервер не поднимется (на месте сокета — каталог), но старый файл уже не нужен.
        #[cfg(unix)]
        std::fs::create_dir(dir.path().join(glafira_mcp_core::endpoint::SOCKET_NAME)).unwrap();
        let s = c.set_enabled(true).await.unwrap();
        #[cfg(unix)]
        assert_eq!(s.state, ServerState::Error);
        #[cfg(not(unix))]
        let _ = s;
        let left = files::read(dir.path());
        assert!(
            left.as_ref().is_none_or(|f| f.pid == std::process::id()),
            "{left:?}"
        );
        c.shutdown().await;
    }

    #[tokio::test]
    async fn only_own_file_is_removed() {
        let (dir, c, _) = controller(true);
        c.set_enabled(true).await.unwrap();
        let own = files::read(dir.path()).unwrap();
        let foreign = McpFile {
            token: files::new_token(),
            pid: own.pid + 1,
            ..own
        };
        files::write(dir.path(), &foreign).unwrap();
        c.set_enabled(false).await.unwrap();
        assert_eq!(
            files::read(dir.path()),
            Some(foreign),
            "файл другого экземпляра остаётся"
        );
    }

    #[tokio::test]
    async fn exit_without_runtime_removes_the_file() {
        let (dir, c, _) = controller(true);
        c.set_enabled(true).await.unwrap();
        assert!(files::read(dir.path()).is_some());
        c.exit_now();
        assert_eq!(files::read(dir.path()), None);
        c.shutdown().await;
    }

    #[tokio::test]
    async fn policy_forbids_enabling() {
        let (_dir, c, _) = controller(false);
        let e = c.set_enabled(true).await.unwrap_err();
        assert_eq!(e.key, "errors.mcp.disabledByPolicy");
        c.apply().await;
        let s = c.status();
        assert_eq!(
            (s.state, s.policy_allows),
            (ServerState::DisabledByPolicy, false)
        );
    }

    #[tokio::test]
    async fn new_client_waits_for_the_user_who_approves_it() {
        let (dir, c, rec) = controller(true);
        c.set_enabled(true).await.unwrap();
        let f = files::read(dir.path()).unwrap();
        let mut s = glafira_mcp_core::endpoint::connect(&f.endpoint)
            .await
            .unwrap();
        glafira_mcp_core::handshake::client(&mut s, &f.token)
            .await
            .unwrap();
        let client = rmcp::ServiceExt::serve((), s).await.unwrap();
        let pending = c.status().pending;
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].client_name.as_deref(), Some("rmcp"));
        assert_eq!(*rec.1.lock().unwrap(), 1, "окно привлекает внимание");
        let s = tokio::task::spawn_blocking({
            let c = c.clone();
            let id = pending[0].session_id.clone();
            move || c.answer(&id, true)
        })
        .await
        .unwrap()
        .unwrap();
        assert!(s.pending.is_empty());
        assert!(s.sessions[0].approved);
        assert_eq!(s.approved_clients[0].name, "rmcp");
        let r = client
            .call_tool(rmcp::model::CallToolRequestParams::new("list_connections"))
            .await
            .unwrap();
        assert_eq!(r.is_error, Some(false));
        let audit = c.audit_recent(10);
        assert!(audit.iter().any(|a| a["event"] == "approved"));
        c.shutdown().await;
    }
}
