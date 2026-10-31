//! Сервер целиком: настоящий сокет (pipe на Windows), рукопожатие, MCP-клиент rmcp и игрушечный
//! обработчик с инструментами, которые проверяют обвязку вызова.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use glafira_mcp_core::{
    AuditLog, BackendError, Hooks, McpServerHandle, Session, SessionAccess, SessionView, endpoint,
    handshake, to_value,
};
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{
    CallToolRequestParams, CallToolResult, ClientCapabilities, ClientConfig, Implementation,
    InitializeRequestParams, InitializeResult, ServerCapabilities,
};
use rmcp::service::{RequestContext, RunningService};
use rmcp::{
    ErrorData, RoleClient, RoleServer, ServerHandler, ServiceExt, tool, tool_handler, tool_router,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const TOKEN: &str = "test-token";
const PRODUCT: &str = "glafira-kubernetes";

struct Fake {
    access: Mutex<SessionAccess>,
    seen: Mutex<Vec<SessionView>>,
    closed: Mutex<Vec<String>>,
    names: BTreeMap<&'static str, &'static str>,
    /// Операций с кластером сейчас и наибольшее число одновременных.
    active: AtomicUsize,
    peak: AtomicUsize,
}

impl Fake {
    fn new(access: SessionAccess) -> Arc<Self> {
        Arc::new(Self {
            access: Mutex::new(access),
            seen: Mutex::new(Vec::new()),
            closed: Mutex::new(Vec::new()),
            names: BTreeMap::from([("c1", "dev")]),
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        })
    }
}

impl Hooks for Fake {
    fn access(&self, session: &SessionView) -> SessionAccess {
        self.seen.lock().unwrap().push(session.clone());
        *self.access.lock().unwrap()
    }

    fn connection_name(&self, id: &str) -> Option<String> {
        self.names.get(id).map(|n| n.to_string())
    }

    fn session_closed(&self, session_id: &str) {
        self.closed.lock().unwrap().push(session_id.to_string());
    }

    fn sessions_changed(&self) {}
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct EchoParams {
    /// Connection id.
    connection_id: String,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct WaitParams {
    /// Pause in milliseconds.
    ms: u64,
}

/// Игрушечный обработчик: каждый инструмент проверяет одну сторону обвязки.
#[derive(Clone)]
struct Toy {
    session: Session,
    fake: Arc<Fake>,
    #[allow(dead_code)] // поле читает макрос tool_handler
    tool_router: ToolRouter<Self>,
}

#[tool_router]
impl Toy {
    #[tool(description = "Echo the connection id back")]
    async fn echo(
        &self,
        Parameters(p): Parameters<EchoParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let args = serde_json::to_value(&p).unwrap();
        Ok(self
            .session
            .call(&ctx, "echo", args, Some(&p.connection_id), async {
                to_value(&json!({ "connectionId": p.connection_id }))
            })
            .await)
    }

    #[tool(description = "Hold a cluster slot for a while")]
    async fn slow(
        &self,
        Parameters(p): Parameters<WaitParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        Ok(self
            .session
            .call(&ctx, "slow", Value::Null, None, async {
                let _slots = self.session.slots().await?;
                let now = self.fake.active.fetch_add(1, Ordering::SeqCst) + 1;
                self.fake.peak.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(p.ms)).await;
                self.fake.active.fetch_sub(1, Ordering::SeqCst);
                to_value(&json!({}))
            })
            .await)
    }

    #[tool(description = "Run a long exclusive operation")]
    async fn long(
        &self,
        Parameters(p): Parameters<WaitParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        Ok(self
            .session
            .call(&ctx, "long", Value::Null, None, async {
                let _busy = self.session.exclusive()?;
                tokio::time::sleep(Duration::from_millis(p.ms)).await;
                to_value(&json!({}))
            })
            .await)
    }

    #[tool(description = "Read some lines")]
    async fn scan(&self, ctx: RequestContext<RoleServer>) -> Result<CallToolResult, ErrorData> {
        Ok(self
            .session
            .call(&ctx, "scan", Value::Null, None, async {
                to_value(
                    &json!({ "lines": ["secret line"], "scanned": 7, "returned": 1, "masked": 2 }),
                )
            })
            .await)
    }

    #[tool(description = "Return a response that is too large")]
    async fn big(&self, ctx: RequestContext<RoleServer>) -> Result<CallToolResult, ErrorData> {
        Ok(self
            .session
            .call(&ctx, "big", Value::Null, None, async {
                to_value(&json!({ "blob": "x".repeat(200 * 1024) }))
            })
            .await)
    }

    #[tool(description = "Fail with a backend error")]
    async fn fail(&self, ctx: RequestContext<RoleServer>) -> Result<CallToolResult, ErrorData> {
        Ok(self
            .session
            .call(&ctx, "fail", Value::Null, None, async {
                Err(BackendError::not_found("Nothing here"))
            })
            .await)
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for Toy {
    fn get_info(&self) -> rmcp::model::ServerConfig {
        rmcp::model::ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
    }

    async fn initialize(
        &self,
        request: InitializeRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<InitializeResult, ErrorData> {
        context.peer.set_peer_info(request.clone());
        self.session.initialized(request.client_info.clone());
        self.negotiate_initialize(&request)
    }
}

struct Env {
    _dir: tempfile::TempDir,
    ep: String,
    handle: McpServerHandle,
    audit: Arc<AuditLog>,
}

async fn env(fake: Arc<Fake>) -> Env {
    let dir = tempfile::tempdir().unwrap();
    let ep = endpoint::choose(
        dir.path(),
        PRODUCT,
        &dir.path().file_name().unwrap().to_string_lossy(),
    );
    let audit = Arc::new(AuditLog::new(dir.path()));
    let hooks: Arc<dyn Hooks> = fake.clone();
    let handle = glafira_mcp_core::start(&ep, TOKEN.into(), hooks, audit.clone(), move |s| Toy {
        session: s,
        fake: fake.clone(),
        tool_router: Toy::tool_router(),
    })
    .await
    .unwrap();
    Env {
        _dir: dir,
        ep,
        handle,
        audit,
    }
}

async fn client(ep: &str) -> RunningService<RoleClient, ()> {
    let mut s = endpoint::connect(&ep.to_string()).await.unwrap();
    handshake::client(&mut s, TOKEN).await.unwrap();
    ().serve(s).await.unwrap()
}

/// Клиент с заданными именем и версией в `initialize`.
async fn named_client(
    ep: &str,
    name: &str,
    version: &str,
) -> RunningService<RoleClient, ClientConfig> {
    let mut s = endpoint::connect(&ep.to_string()).await.unwrap();
    handshake::client(&mut s, TOKEN).await.unwrap();
    ClientConfig::new(
        ClientCapabilities::default(),
        Implementation::new(name, version),
    )
    .serve(s)
    .await
    .unwrap()
}

async fn call(c: &RunningService<RoleClient, ()>, tool: &str, args: Value) -> CallToolResult {
    let mut p = CallToolRequestParams::new(tool.to_string());
    if let Value::Object(m) = args {
        p = p.with_arguments(m);
    }
    c.call_tool(p).await.unwrap()
}

fn text(r: &CallToolResult) -> String {
    r.content
        .iter()
        .filter_map(|c| c.as_text().map(|t| t.text.clone()))
        .collect()
}

fn ok(r: CallToolResult) -> Value {
    assert_eq!(r.is_error, Some(false), "{}", text(&r));
    r.structured_content.unwrap()
}

fn err(r: &CallToolResult) -> String {
    assert_eq!(r.is_error, Some(true));
    text(r)
}

#[tokio::test]
async fn approved_call_returns_structured_content_and_its_text() {
    let e = env(Fake::new(SessionAccess::Approved)).await;
    let c = client(&e.ep).await;
    let r = call(&c, "echo", json!({"connectionId": "c1"})).await;
    let copy: Value = serde_json::from_str(&text(&r)).unwrap();
    assert_eq!(ok(r), copy, "текст — копия structuredContent");
    let msg = err(&call(&c, "fail", json!({})).await);
    assert_eq!(msg, "not_found: Nothing here");
}

#[tokio::test]
async fn pending_client_gets_a_hint_then_works_after_approval() {
    let fake = Fake::new(SessionAccess::Pending);
    let e = env(fake.clone()).await;
    let c = client(&e.ep).await;
    let first = fake.seen.lock().unwrap()[0].clone();
    assert_eq!(
        first.client.as_ref().map(|c| c.name.as_str()),
        Some("rmcp"),
        "клиент из initialize, подтверждение запрошено сразу"
    );
    let msg = err(&call(&c, "echo", json!({"connectionId": "c1"})).await);
    assert!(msg.starts_with("client_not_approved:"), "{msg}");
    *fake.access.lock().unwrap() = SessionAccess::Approved;
    ok(call(&c, "echo", json!({"connectionId": "c1"})).await);
    let sessions = e.handle.sessions();
    assert_eq!(
        (sessions.len(), sessions[0].calls),
        (1, 1),
        "отклонённый вызов не считается"
    );
    assert_eq!(sessions[0].client_name.as_deref(), Some("rmcp"));
}

#[tokio::test]
async fn disabled_gateway_answers_mcp_disabled() {
    let e = env(Fake::new(SessionAccess::Disabled)).await;
    let c = client(&e.ep).await;
    let msg = err(&call(&c, "echo", json!({"connectionId": "c1"})).await);
    assert!(msg.starts_with("mcp_disabled:"), "{msg}");
}

#[tokio::test]
async fn client_name_is_trimmed_and_capped() {
    let fake = Fake::new(SessionAccess::Approved);
    let e = env(fake.clone()).await;
    let _blank = named_client(&e.ep, " \t ", "1.0").await;
    let _long = named_client(&e.ep, &format!("  {}", "ж".repeat(300)), &"v".repeat(300)).await;
    let seen = fake.seen.lock().unwrap().clone();
    assert_eq!(seen[0].client, None, "пустое имя — как отсутствующее");
    let c = seen.last().unwrap().client.clone().unwrap();
    assert_eq!(c.name, "ж".repeat(128));
    assert_eq!(c.version, "v".repeat(128));
    let mut names: Vec<Option<String>> = e
        .handle
        .sessions()
        .into_iter()
        .map(|s| s.client_name)
        .collect();
    names.sort();
    assert_eq!(names, [None, Some("ж".repeat(128))]);
}

#[tokio::test]
async fn rejected_client_is_disconnected() {
    let fake = Fake::new(SessionAccess::Rejected);
    let e = env(fake.clone()).await;
    let c = client(&e.ep).await;
    let msg = err(&call(&c, "echo", json!({"connectionId": "c1"})).await);
    assert!(msg.starts_with("client_rejected:"), "{msg}");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(e.handle.sessions().is_empty());
    assert_eq!(fake.closed.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn calls_are_rate_limited_and_audited() {
    let e = env(Fake::new(SessionAccess::Approved)).await;
    let c = client(&e.ep).await;
    for _ in 0..60 {
        ok(call(&c, "echo", json!({"connectionId": "c1"})).await);
    }
    let msg = err(&call(&c, "echo", json!({"connectionId": "c1"})).await);
    assert!(msg.starts_with("rate_limited:"), "{msg}");
    assert!(
        msg.contains(&format!(
            "at most {} per minute",
            glafira_mcp_core::limits::CALLS_PER_MINUTE
        )),
        "{msg}"
    );
    let other = client(&e.ep).await;
    ok(call(&other, "echo", json!({"connectionId": "c1"})).await);
    let recent = e.audit.recent(3);
    assert_eq!(recent[0]["event"], "call");
    assert_eq!(recent[0]["tool"], "echo");
    assert_eq!(recent[0]["connection"]["name"], "dev");
    assert_eq!(recent[0]["args"]["connectionId"], "c1");
    assert!(recent[0]["bytes"].as_u64().unwrap() > 0);
    assert!(
        recent.iter().any(|r| r["code"] == "rate_limited"),
        "{recent:?}"
    );
}

#[tokio::test]
async fn read_counts_are_audited_without_the_content() {
    let e = env(Fake::new(SessionAccess::Approved)).await;
    let c = client(&e.ep).await;
    ok(call(&c, "scan", json!({})).await);
    ok(call(&c, "echo", json!({"connectionId": "c1"})).await);
    let recent = e.audit.recent(2);
    assert_eq!(recent[0]["tool"], "echo");
    assert!(recent[0].get("scanned").is_none(), "{recent:?}");
    assert_eq!(
        (
            &recent[1]["scanned"],
            &recent[1]["returned"],
            &recent[1]["masked"]
        ),
        (&json!(7), &json!(1), &json!(2))
    );
    let line = serde_json::to_string(&recent[1]).unwrap();
    assert!(!line.contains("secret line"), "{line}");
}

#[tokio::test]
async fn cluster_operations_are_limited_per_session_and_globally() {
    let fake = Fake::new(SessionAccess::Approved);
    let e = env(fake.clone()).await;
    let slow = |c: Arc<RunningService<RoleClient, ()>>| {
        tokio::spawn(async move { ok(call(&c, "slow", json!({"ms": 150})).await) })
    };
    let one = Arc::new(client(&e.ep).await);
    let calls: Vec<_> = (0..5).map(|_| slow(one.clone())).collect();
    for h in calls {
        h.await.unwrap();
    }
    assert_eq!(fake.peak.load(Ordering::SeqCst), 2, "не больше 2 на сессию");

    fake.peak.store(0, Ordering::SeqCst);
    let mut clients = vec![one];
    for _ in 0..2 {
        clients.push(Arc::new(client(&e.ep).await));
    }
    let calls: Vec<_> = clients
        .iter()
        .flat_map(|c| (0..3).map(|_| slow(c.clone())).collect::<Vec<_>>())
        .collect();
    for h in calls {
        h.await.unwrap();
    }
    assert_eq!(
        fake.peak.load(Ordering::SeqCst),
        4,
        "не больше 4 на все сессии"
    );
}

#[tokio::test]
async fn response_that_cannot_be_cut_is_refused() {
    let e = env(Fake::new(SessionAccess::Approved)).await;
    let c = client(&e.ep).await;
    let msg = err(&call(&c, "big", json!({})).await);
    assert!(msg.starts_with("response_too_large:"), "{msg}");
    assert_eq!(e.audit.recent(1)[0]["code"], "response_too_large");
}

#[tokio::test]
async fn one_long_operation_per_session() {
    let e = env(Fake::new(SessionAccess::Approved)).await;
    let c = Arc::new(client(&e.ep).await);
    let long = {
        let c = c.clone();
        tokio::spawn(async move { call(&c, "long", json!({"ms": 500})).await })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    let msg = err(&call(&c, "long", json!({"ms": 10})).await);
    assert!(msg.starts_with("busy:"), "{msg}");
    let other = client(&e.ep).await;
    ok(call(&other, "long", json!({"ms": 10})).await);
    ok(long.await.unwrap());
    ok(call(&c, "long", json!({"ms": 10})).await);
}

#[tokio::test]
async fn disconnect_and_shutdown_close_sessions() {
    let fake = Fake::new(SessionAccess::Approved);
    let e = env(fake.clone()).await;
    let c = client(&e.ep).await;
    ok(call(&c, "echo", json!({"connectionId": "c1"})).await);
    let id = e.handle.sessions()[0].id.clone();
    assert!(e.handle.disconnect(&id));
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(e.handle.sessions().is_empty());
    assert_eq!(fake.closed.lock().unwrap().as_slice(), [id]);
    let c2 = client(&e.ep).await;
    e.handle.shutdown().await;
    assert!(
        c2.call_tool(CallToolRequestParams::new("echo"))
            .await
            .is_err()
    );
    assert!(
        endpoint::connect(&e.ep).await.is_err(),
        "сервер больше не слушает"
    );
}

#[tokio::test]
async fn shutdown_does_not_wait_for_a_silent_handshake() {
    let e = env(Fake::new(SessionAccess::Approved)).await;
    // Соединение без строки рукопожатия: сервер ждал бы её до 5 с.
    let _silent = endpoint::connect(&e.ep).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let started = std::time::Instant::now();
    e.handle.shutdown().await;
    let elapsed = started.elapsed();
    assert!(elapsed < Duration::from_secs(1), "{elapsed:?}");
}

#[tokio::test]
async fn wrong_token_is_rejected_before_mcp() {
    let e = env(Fake::new(SessionAccess::Approved)).await;
    let mut s = endpoint::connect(&e.ep).await.unwrap();
    let r = handshake::client(&mut s, "wrong").await;
    assert!(
        matches!(r, Err(handshake::HandshakeError::Rejected { ref code, .. }) if code == "bad_token")
    );
    assert!(e.handle.sessions().is_empty());
}
