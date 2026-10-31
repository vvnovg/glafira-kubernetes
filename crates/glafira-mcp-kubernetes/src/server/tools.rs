//! Инструменты чтения (спец. §10.4): параметры (их описания уходят в JSON Schema для LLM), тела
//! ответов и сами инструменты. Все — только чтение.

use glafira_mcp_core::audit::now_ms;
use glafira_mcp_core::fit::{self, Page, TRUNCATED_HINT};
use glafira_mcp_core::{BackendError, to_value};
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, ProgressNotificationParam};
use rmcp::service::RequestContext;
use rmcp::{ErrorData, RoleServer, tool, tool_router};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{KubeServer, mask_text, text_masker};
use crate::mask::Masker;
use crate::model::*;
use crate::observe::{self, Observation};

/// Наибольший `sinceSeconds` у `list_events`: сутки.
pub const MAX_EVENTS_SINCE_S: u32 = 86_400;
pub const DEFAULT_EVENTS_SINCE_S: u32 = 3600;
/// Начальное и конечное состояние `observe_workload` не обрезаются: если одно больше этого,
/// два не поместятся в 100 КБ вместе с хронологией — отказ сразу после первого образца.
pub const MAX_STATE_BYTES: usize = 45 * 1024;

/// Ответ со списком, обрезанным под предел размера.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Fitted<T: Serialize> {
    #[serde(flatten)]
    body: T,
    truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    hint: Option<&'static str>,
}

fn wrap<T: Serialize>(body: T, truncated: bool) -> Result<Value, BackendError> {
    to_value(&Fitted {
        body,
        truncated,
        hint: truncated.then_some(TRUNCATED_HINT),
    })
}

/// Страница после обрезки под предел размера: следующая начинается сразу за последним
/// показанным элементом. `next_offset`, посчитанный для запрошенной страницы, пропустил бы
/// вырезанные.
fn resume_after_cut<T>(page: &mut Page<T>, offset: usize) {
    let end = offset.saturating_add(page.items.len());
    page.next_offset = (end < page.total).then_some(end);
}

macro_rules! expect {
    ($out:expr, $variant:ident) => {
        match $out {
            ReadOutput::$variant(v) => Ok(v),
            _ => Err(BackendError::unexpected()),
        }
    };
}

macro_rules! args {
    ($p:expr) => {
        serde_json::to_value(&$p).unwrap_or(Value::Null)
    };
}

fn in_range(
    v: Option<u32>,
    default: u32,
    min: u32,
    max: u32,
    name: &str,
) -> Result<u32, BackendError> {
    match v {
        None => Ok(default),
        Some(x) if (min..=max).contains(&x) => Ok(x),
        Some(x) => Err(BackendError::invalid_argument(format!(
            "{name} must be between {min} and {max}, got {x}"
        ))),
    }
}

fn limit_of(limit: Option<u32>) -> Result<usize, BackendError> {
    fit::checked_limit(limit).map_err(BackendError::invalid_argument)
}

fn items_of<T>(p: &mut Page<T>) -> &mut Vec<T> {
    &mut p.items
}

// ---- Параметры инструментов (описания полей уходят в JSON Schema для LLM) ----

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionParams {
    /// Connection id from list_connections.
    pub connection_id: String,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ListProjectsParams {
    /// Connection id from list_connections.
    pub connection_id: String,
    /// Case-insensitive substring of the project name.
    #[serde(default)]
    pub filter: Option<String>,
    /// Skip this many projects (sorted by name). Default: 0.
    #[serde(default)]
    pub offset: Option<u32>,
    /// Page size, 1..=500. Default: 200.
    #[serde(default)]
    pub limit: Option<u32>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ListNodesParams {
    /// Connection id from list_connections.
    pub connection_id: String,
    /// Case-insensitive substring of the node name.
    #[serde(default)]
    pub filter: Option<String>,
    /// Only nodes with this role, e.g. worker, master, control-plane, infra.
    #[serde(default)]
    pub role: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NodeParams {
    /// Connection id from list_connections.
    pub connection_id: String,
    /// Node name from list_nodes.
    pub name: String,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ListWorkloadsParams {
    /// Connection id from list_connections.
    pub connection_id: String,
    /// Project (namespace) from list_projects.
    pub namespace: String,
    /// Only this kind: Deployment, StatefulSet, DaemonSet, DeploymentConfig, Job or CronJob.
    #[serde(default)]
    pub kind: Option<WorkloadKind>,
    /// Case-insensitive substring of the workload name.
    #[serde(default)]
    pub filter: Option<String>,
    /// Skip this many workloads (sorted by name). Default: 0.
    #[serde(default)]
    pub offset: Option<u32>,
    /// Page size, 1..=500. Default: 200.
    #[serde(default)]
    pub limit: Option<u32>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadParams {
    /// Connection id from list_connections.
    pub connection_id: String,
    /// Project (namespace) from list_projects.
    pub namespace: String,
    /// Deployment, StatefulSet, DaemonSet, DeploymentConfig, Job or CronJob.
    pub kind: WorkloadKind,
    pub name: String,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ListPodsParams {
    /// Connection id from list_connections.
    pub connection_id: String,
    /// Project (namespace) from list_projects.
    pub namespace: String,
    /// Only the pods of this workload.
    #[serde(default)]
    pub workload: Option<WorkloadRef>,
    /// Only pods with a problem: image pull, OOMKilled, crash loop, pending or failed. Default: false.
    #[serde(default)]
    pub problems_only: Option<bool>,
    /// Skip this many pods (sorted by name). Default: 0.
    #[serde(default)]
    pub offset: Option<u32>,
    /// Page size, 1..=500. Default: 200.
    #[serde(default)]
    pub limit: Option<u32>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PodParams {
    /// Connection id from list_connections.
    pub connection_id: String,
    /// Project (namespace) from list_projects.
    pub namespace: String,
    /// Pod name from list_pods.
    pub name: String,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ListEventsParams {
    /// Connection id from list_connections.
    pub connection_id: String,
    /// Project (namespace) from list_projects.
    pub namespace: String,
    /// Only the events of this object (for a workload — of the objects it creates too).
    #[serde(default)]
    pub object: Option<ObjectRef>,
    /// Only events of this type: Warning or Normal.
    #[serde(default, rename = "type")]
    pub kind: Option<String>,
    /// Only events seen in the last this many seconds, 1..=86400. Default: 3600.
    #[serde(default)]
    pub since_seconds: Option<u32>,
    /// At most this many events, newest first, 1..=500. Default: 200.
    #[serde(default)]
    pub limit: Option<u32>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NamespaceParams {
    /// Connection id from list_connections.
    pub connection_id: String,
    /// Project (namespace) from list_projects.
    pub namespace: String,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NetworkParams {
    /// Connection id from list_connections.
    pub connection_id: String,
    /// Project (namespace) from list_projects.
    pub namespace: String,
    /// Case-insensitive substring of the object name.
    #[serde(default)]
    pub filter: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ObserveParams {
    /// Connection id from list_connections.
    pub connection_id: String,
    /// Project (namespace) from list_projects.
    pub namespace: String,
    pub workload: WorkloadRef,
    /// Observation window, 10..=50 seconds. Default: 30.
    #[serde(default)]
    pub duration_seconds: Option<u32>,
}

// ---- Тела ответов ----

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ProjectsPage {
    #[serde(flatten)]
    page: Page<Project>,
    /// Список проектов кластера не читается: показаны проекты из профиля подключения.
    #[serde(skip_serializing_if = "Option::is_none")]
    list_error: Option<String>,
}

fn project_items(p: &mut ProjectsPage) -> &mut Vec<Project> {
    &mut p.page.items
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct NodesBody {
    items: Vec<NodeItem>,
    #[serde(skip_serializing_if = "Option::is_none")]
    requests_error: Option<String>,
}

fn node_items(b: &mut NodesBody) -> &mut Vec<NodeItem> {
    &mut b.items
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WorkloadsPage {
    #[serde(flatten)]
    page: Page<WorkloadItem>,
    /// Виды, которые не прочитались.
    errors: Vec<KindError>,
}

fn workload_items(p: &mut WorkloadsPage) -> &mut Vec<WorkloadItem> {
    &mut p.page.items
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Masked<T: Serialize> {
    #[serde(flatten)]
    body: T,
    /// Сколько значений скрыто маскировкой.
    masked: usize,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EventsBody {
    items: Vec<Event>,
}

fn event_items(b: &mut EventsBody) -> &mut Vec<Event> {
    &mut b.items
}

fn events_of(o: &mut Observation) -> &mut Vec<observe::Event> {
    &mut o.events
}

fn mask_opt(m: &Masker, s: &mut Option<String>) -> usize {
    s.as_mut().map_or(0, |s| mask_text(m, s))
}

fn mask_all(m: &Masker, items: &mut [String]) -> usize {
    items.iter_mut().map(|s| mask_text(m, s)).sum()
}

fn mask_conditions(m: &Masker, conditions: &mut [Condition]) -> usize {
    conditions
        .iter_mut()
        .map(|c| mask_opt(m, &mut c.message))
        .sum()
}

fn mask_events(m: &Masker, events: &mut [Event]) -> usize {
    events
        .iter_mut()
        .map(|e| mask_text(m, &mut e.message))
        .sum()
}

fn mask_state(m: &Masker, state: &mut Option<ContainerState>) -> usize {
    state.as_mut().map_or(0, |s| mask_opt(m, &mut s.message))
}

/// `command` и `args` контейнера — имена полей и пары, образ и пробы — встроенные шаблоны;
/// возвращает число замен.
fn mask_spec(m: &Masker, c: &mut ContainerSpec) -> usize {
    let (command, a) = m.args(&c.command);
    let (args, b) = m.args(&c.args);
    c.command = command;
    c.args = args;
    a + b
        + mask_text(m, &mut c.image)
        + mask_opt(m, &mut c.liveness)
        + mask_opt(m, &mut c.readiness)
        + mask_opt(m, &mut c.startup)
}

fn mask_annotations(m: &Masker, a: &mut std::collections::BTreeMap<String, String>) -> usize {
    let (masked, n) = m.annotations(a);
    *a = masked;
    n
}

/// Весь текст из кластера в `describe_workload`; возвращает число замен.
fn mask_workload(m: &Masker, w: &mut WorkloadDetails) -> usize {
    let mut n = mask_all(m, &mut w.workload.images)
        + mask_conditions(m, &mut w.conditions)
        + mask_annotations(m, &mut w.annotations);
    n += w
        .containers
        .iter_mut()
        .map(|c| mask_spec(m, c))
        .sum::<usize>();
    if let Part::Data(Some(hpa)) = &mut w.hpa {
        n += mask_conditions(m, &mut hpa.conditions);
    }
    if let Some(Part::Data(history)) = &mut w.history {
        n += history
            .iter_mut()
            .map(|r| mask_all(m, &mut r.images))
            .sum::<usize>();
    }
    n
}

/// Весь текст из кластера в `describe_pod`; возвращает число замен.
fn mask_pod(m: &Masker, p: &mut PodDetails) -> usize {
    let mut n = mask_conditions(m, &mut p.conditions) + mask_annotations(m, &mut p.annotations);
    for c in &mut p.containers {
        n += mask_spec(m, &mut c.spec)
            + mask_opt(m, &mut c.image_id)
            + mask_state(m, &mut c.state)
            + mask_state(m, &mut c.last_state);
    }
    if let Part::Data(events) = &mut p.events {
        n += mask_events(m, events);
    }
    n
}

/// Сообщения ClusterOperator'ов и условий ClusterVersion.
fn mask_overview(m: &Masker, o: &mut ClusterOverview) {
    let Some(openshift) = &mut o.openshift else {
        return;
    };
    if let Part::Data(version) = &mut openshift.cluster_version {
        for c in [&mut version.failing, &mut version.progressing]
            .into_iter()
            .flatten()
        {
            mask_opt(m, &mut c.message);
        }
    }
    if let Part::Data(operators) = &mut openshift.operators {
        for op in &mut operators.not_ok {
            mask_opt(m, &mut op.message);
        }
    }
}

/// Сообщения Route о приёме и условия родителей HTTPRoute и GRPCRoute.
fn mask_network(m: &Masker, n: &mut Network) {
    if let Some(Part::Data(routes)) = &mut n.routes {
        for r in routes {
            mask_opt(m, &mut r.admitted_message);
        }
    }
    for routes in [&mut n.http_routes, &mut n.grpc_routes] {
        if let Some(Part::Data(routes)) = routes {
            for parent in routes.iter_mut().flat_map(|r| r.parents.iter_mut()) {
                for c in [&mut parent.accepted, &mut parent.resolved_refs]
                    .into_iter()
                    .flatten()
                {
                    mask_opt(m, &mut c.message);
                }
            }
        }
    }
}

/// Сообщения событий `Warning` в хронологии наблюдения.
fn mask_observation(m: &Masker, o: &mut Observation) {
    for e in &mut o.events {
        if let observe::EventKind::WarningEvent { message, .. } = &mut e.kind {
            mask_text(m, message);
        }
    }
}

fn filtered<T>(items: Option<Part<Vec<T>>>, keep: impl Fn(&T) -> bool) -> Option<Part<Vec<T>>> {
    items.map(|p| match p {
        Part::Data(v) => Part::Data(v.into_iter().filter(|x| keep(x)).collect()),
        e => e,
    })
}

impl KubeServer {
    async fn observe(
        &self,
        p: &ObserveParams,
        ctx: &RequestContext<RoleServer>,
    ) -> Result<Value, BackendError> {
        use observe::*;
        let duration = in_range(
            p.duration_seconds,
            DEFAULT_DURATION_S,
            MIN_DURATION_S,
            MAX_DURATION_S,
            "durationSeconds",
        )?;
        self.scope(&p.connection_id, Some(&p.namespace))?;
        let _busy = self.session.exclusive()?;
        let state = || ReadOp::WorkloadState {
            connection_id: p.connection_id.clone(),
            namespace: p.namespace.clone(),
            workload: p.workload.clone(),
        };
        // Расписание — от начала наблюдения: время чтений не растягивает окно.
        let (started_at_ms, started) = (now_ms(), tokio::time::Instant::now());
        let unit = self.options.observe_unit;
        let reads_until = started + unit * (duration + READ_GRACE_S);
        let late = || {
            BackendError::new(
                "TIMEOUT",
                "The workload state did not arrive before the end of the observation",
            )
        };
        let first = tokio::time::timeout_at(reads_until, self.read(state()))
            .await
            .map_err(|_| late())??;
        let first = expect!(first, WorkloadState)?;
        if serde_json::to_vec(&Snapshot::from(&first)).map_or(0, |b| b.len()) > MAX_STATE_BYTES {
            return Err(BackendError::new(
                "response_too_large",
                "The workload has too many pods to observe: its state alone does not fit into 100 KB; use list_pods and list_events instead",
            ));
        }
        let total = (duration / INTERVAL_S) as usize + 1;
        let progress = ctx.meta.get_progress_token();
        let mut rec = Recorder::default();
        rec.sample(started.elapsed().as_millis() as u64, &first);
        for i in 1..total {
            let tick = started + unit * (INTERVAL_S * i as u32);
            tokio::select! {
                _ = ctx.ct.cancelled() => break,
                _ = tokio::time::sleep_until(tick) => {}
            }
            let at = started.elapsed().as_millis() as u64;
            match tokio::time::timeout_at(reads_until, self.read(state())).await {
                Err(_) => {
                    rec.failed(at, late());
                    break;
                }
                Ok(r) => match r.and_then(|o| expect!(o, WorkloadState)) {
                    Ok(s) => rec.sample(at, &s),
                    Err(e) => rec.failed(at, e),
                },
            }
            if let Some(t) = &progress {
                let note = ProgressNotificationParam::new(t.clone(), (i + 1) as f64)
                    .with_total(total as f64);
                let _ = ctx.peer.notify_progress(note).await;
            }
        }
        let mut observation = rec.finish(started_at_ms, started.elapsed().as_millis() as u64);
        mask_observation(&text_masker(), &mut observation);
        // Предел 100 КБ: лишние события отбрасываются, счётчики сводки остаются полными.
        if fit::fit(&mut observation, events_of) {
            observation.truncated = true;
        }
        to_value(&observation)
    }
}

#[tool_router(vis = "pub(crate)")]
impl KubeServer {
    #[tool(
        description = "List Kubernetes connections that the user allowed for LLM access: id, name, environment (local/dev/test/stage/prod), folder path, read-only flag, LLM access level and, while the connection is open in Glafira, its platform (openshift or kubernetes), version and distribution. Use the id in every other tool.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            open_world_hint = false
        )
    )]
    async fn list_connections(
        &self,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        Ok(self
            .session
            .call(&ctx, "list_connections", Value::Null, None, async {
                let list = expect!(self.read(ReadOp::ListConnections).await?, Connections)?;
                to_value(&serde_json::json!({ "connections": list }))
            })
            .await)
    }

    #[tool(
        description = "Cluster overview: platform, version and distribution, API server readiness (/readyz with failed checks), nodes ready/total by role, ingress and gateway classes, what the user may read at cluster level; on OpenShift also the cluster version, channel, available updates, infrastructure, ClusterOperators and MachineConfigPools that are not healthy.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            open_world_hint = false
        )
    )]
    async fn get_cluster_overview(
        &self,
        Parameters(p): Parameters<ConnectionParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        Ok(self
            .session
            .call(
                &ctx,
                "get_cluster_overview",
                args!(p),
                Some(&p.connection_id),
                async {
                    let mut o = expect!(
                        self.read(ReadOp::ClusterOverview {
                            connection_id: p.connection_id.clone()
                        })
                        .await?,
                        Cluster
                    )?;
                    mask_overview(&text_masker(), &mut o);
                    to_value(&o)
                },
            )
            .await)
    }

    #[tool(
        description = "List projects (namespaces) of a connection that the user allowed for LLM access, sorted by name: name, display name, phase. Supports a name filter and paging (offset, limit up to 500).",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            open_world_hint = false
        )
    )]
    async fn list_projects(
        &self,
        Parameters(p): Parameters<ListProjectsParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        Ok(self
            .session
            .call(
                &ctx,
                "list_projects",
                args!(p),
                Some(&p.connection_id),
                async {
                    let limit = limit_of(p.limit)?;
                    let scope = self.scope(&p.connection_id, None)?;
                    let list = expect!(
                        self.read(ReadOp::ListProjects {
                            connection_id: p.connection_id.clone()
                        })
                        .await?,
                        Projects
                    )?;
                    let mut items: Vec<Project> = list
                        .items
                        .into_iter()
                        .filter(|x| scope.allows(&x.name))
                        .collect();
                    // Список кластера не читается: проекты, открытые для LLM, видны и так —
                    // доступ к ним проверит API-сервер.
                    if list.error.is_some()
                        && let Some(allowed) = &scope.namespaces
                    {
                        for ns in allowed {
                            if !items.iter().any(|x| &x.name == ns) {
                                items.push(Project {
                                    name: ns.clone(),
                                    display_name: None,
                                    phase: None,
                                });
                            }
                        }
                    }
                    items.retain(|x| fit::matches_filter(&x.name, p.filter.as_deref()));
                    items.sort_by(|a, b| a.name.cmp(&b.name));
                    let offset = p.offset.unwrap_or(0) as usize;
                    let mut body = ProjectsPage {
                        page: fit::page(items, offset, limit),
                        list_error: list.error,
                    };
                    let cut = fit::fit(&mut body, project_items);
                    if cut {
                        resume_after_cut(&mut body.page, offset);
                    }
                    wrap(body, cut | list.truncated)
                },
            )
            .await)
    }

    #[tool(
        description = "List nodes sorted by name: roles, readiness, conditions that are not healthy (memory, disk or PID pressure), unschedulable flag, allocatable CPU and memory, the sum of CPU and memory requests of the pods on the node (when pods of all projects can be read), taints count, kubelet version. Needs cluster-level read access.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            open_world_hint = false
        )
    )]
    async fn list_nodes(
        &self,
        Parameters(p): Parameters<ListNodesParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        Ok(self
            .session
            .call(
                &ctx,
                "list_nodes",
                args!(p),
                Some(&p.connection_id),
                async {
                    let list = expect!(
                        self.read(ReadOp::ListNodes {
                            connection_id: p.connection_id.clone()
                        })
                        .await?,
                        Nodes
                    )?;
                    let mut items: Vec<NodeItem> = list
                        .items
                        .into_iter()
                        .filter(|n| fit::matches_filter(&n.name, p.filter.as_deref()))
                        .filter(|n| {
                            p.role
                                .as_deref()
                                .is_none_or(|r| n.roles.iter().any(|x| x.eq_ignore_ascii_case(r)))
                        })
                        .collect();
                    items.sort_by(|a, b| a.name.cmp(&b.name));
                    let mut body = NodesBody {
                        items,
                        requests_error: list.requests_error,
                    };
                    let truncated = fit::fit(&mut body, node_items) | list.truncated;
                    wrap(body, truncated)
                },
            )
            .await)
    }

    #[tool(
        description = "Node details: conditions, taints, capacity and allocatable, the number of pods on the node with the sum of their requests and limits, kubelet version, OS image, container runtime.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            open_world_hint = false
        )
    )]
    async fn describe_node(
        &self,
        Parameters(p): Parameters<NodeParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        Ok(self
            .session
            .call(
                &ctx,
                "describe_node",
                args!(p),
                Some(&p.connection_id),
                async {
                    let mut n = expect!(
                        self.read(ReadOp::DescribeNode {
                            connection_id: p.connection_id.clone(),
                            name: p.name.clone()
                        })
                        .await?,
                        Node
                    )?;
                    mask_conditions(&text_masker(), &mut n.conditions);
                    to_value(&n)
                },
            )
            .await)
    }

    #[tool(
        description = "List workloads of a project sorted by name: Deployment, StatefulSet, DaemonSet, DeploymentConfig, Job and CronJob with desired/ready/available/updated replicas, images, age and problems (unavailable, rolloutStuck, failed). Kinds that cannot be read are listed in errors. Supports a kind and a name filter and paging (offset, limit up to 500).",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            open_world_hint = false
        )
    )]
    async fn list_workloads(
        &self,
        Parameters(p): Parameters<ListWorkloadsParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        Ok(self
            .session
            .call(
                &ctx,
                "list_workloads",
                args!(p),
                Some(&p.connection_id),
                async {
                    let limit = limit_of(p.limit)?;
                    let WorkloadList {
                        mut items,
                        errors,
                        truncated,
                    } = expect!(
                        self.read(ReadOp::ListWorkloads {
                            connection_id: p.connection_id.clone(),
                            namespace: p.namespace.clone()
                        })
                        .await?,
                        Workloads
                    )?;
                    items.retain(|w| {
                        p.kind.is_none_or(|k| w.kind == k)
                            && fit::matches_filter(&w.name, p.filter.as_deref())
                    });
                    items.sort_by(|a, b| a.name.cmp(&b.name).then(a.kind.cmp(&b.kind)));
                    let m = text_masker();
                    for w in &mut items {
                        mask_all(&m, &mut w.images);
                    }
                    let offset = p.offset.unwrap_or(0) as usize;
                    let mut body = WorkloadsPage {
                        page: fit::page(items, offset, limit),
                        errors,
                    };
                    let cut = fit::fit(&mut body, workload_items);
                    if cut {
                        resume_after_cut(&mut body.page, offset);
                    }
                    wrap(body, cut | truncated)
                },
            )
            .await)
    }

    #[tool(
        description = "Workload details: replicas, conditions, selector, strategy, containers (image, command and args with secrets masked, requests and limits, probes, environment variable names and sources without values), volume types, labels and annotations (secrets masked), HPA, PDB, rollout history (ReplicaSets, ReplicationControllers or ControllerRevisions) and active jobs of a CronJob.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            open_world_hint = false
        )
    )]
    async fn describe_workload(
        &self,
        Parameters(p): Parameters<WorkloadParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        Ok(self
            .session
            .call(
                &ctx,
                "describe_workload",
                args!(p),
                Some(&p.connection_id),
                async {
                    let scope = self.scope(&p.connection_id, Some(&p.namespace))?;
                    let mut w = expect!(
                        self.read(ReadOp::DescribeWorkload {
                            connection_id: p.connection_id.clone(),
                            namespace: p.namespace.clone(),
                            workload: WorkloadRef {
                                kind: p.kind,
                                name: p.name.clone()
                            }
                        })
                        .await?,
                        Workload
                    )?;
                    let masked = mask_workload(&scope.masker(), &mut w);
                    to_value(&Masked { body: w, masked })
                },
            )
            .await)
    }

    #[tool(
        description = "List pods of a project or of one workload, sorted by name: phase, status as kubectl shows it, problem (imagePull, oomKilled, crashLoop, pending, failed), ready containers, restarts and when the last one was, node, age and owner. problemsOnly leaves only pods with a problem. Supports paging (offset, limit up to 500).",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            open_world_hint = false
        )
    )]
    async fn list_pods(
        &self,
        Parameters(p): Parameters<ListPodsParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        Ok(self
            .session
            .call(&ctx, "list_pods", args!(p), Some(&p.connection_id), async {
                let limit = limit_of(p.limit)?;
                let PodList {
                    mut items,
                    truncated,
                } = expect!(
                    self.read(ReadOp::ListPods {
                        connection_id: p.connection_id.clone(),
                        namespace: p.namespace.clone(),
                        workload: p.workload.clone()
                    })
                    .await?,
                    Pods
                )?;
                if p.problems_only.unwrap_or(false) {
                    items.retain(|x| x.problem.is_some());
                }
                items.sort_by(|a, b| a.name.cmp(&b.name));
                let offset = p.offset.unwrap_or(0) as usize;
                let mut page = fit::page(items, offset, limit);
                let cut = fit::fit(&mut page, items_of);
                if cut {
                    resume_after_cut(&mut page, offset);
                }
                wrap(page, cut | truncated)
            })
            .await)
    }

    #[tool(
        description = "Pod details: phase, conditions, QoS, node, owner, every container (init ones too) with its image and digest, state and last state with reason and exit code, restarts, requests and limits, probes, command and args with secrets masked and environment variable names and sources without values; volumes, labels, annotations (secrets masked) and the latest events of the pod.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            open_world_hint = false
        )
    )]
    async fn describe_pod(
        &self,
        Parameters(p): Parameters<PodParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        Ok(self
            .session
            .call(
                &ctx,
                "describe_pod",
                args!(p),
                Some(&p.connection_id),
                async {
                    let scope = self.scope(&p.connection_id, Some(&p.namespace))?;
                    let mut pod = expect!(
                        self.read(ReadOp::DescribePod {
                            connection_id: p.connection_id.clone(),
                            namespace: p.namespace.clone(),
                            name: p.name.clone()
                        })
                        .await?,
                        Pod
                    )?;
                    let masked = mask_pod(&scope.masker(), &mut pod);
                    to_value(&Masked { body: pod, masked })
                },
            )
            .await)
    }

    #[tool(
        description = "Events of a project or of one object, newest first: type, reason, object, message, count and when last seen. For a workload, events of the objects it creates (ReplicaSets, Jobs, pods) are included. Filter by type (Warning or Normal) and age (sinceSeconds up to 86400, default 3600).",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            open_world_hint = false
        )
    )]
    async fn list_events(
        &self,
        Parameters(p): Parameters<ListEventsParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        Ok(self
            .session
            .call(
                &ctx,
                "list_events",
                args!(p),
                Some(&p.connection_id),
                async {
                    let limit = limit_of(p.limit)?;
                    let since = in_range(
                        p.since_seconds,
                        DEFAULT_EVENTS_SINCE_S,
                        1,
                        MAX_EVENTS_SINCE_S,
                        "sinceSeconds",
                    )?;
                    let EventList { items, truncated } = expect!(
                        self.read(ReadOp::ListEvents {
                            connection_id: p.connection_id.clone(),
                            namespace: p.namespace.clone(),
                            object: p.object.clone()
                        })
                        .await?,
                        Events
                    )?;
                    let mut items: Vec<Event> = items
                        .into_iter()
                        .filter(|e| {
                            p.kind
                                .as_deref()
                                .is_none_or(|k| e.kind.eq_ignore_ascii_case(k))
                        })
                        .filter(|e| {
                            e.last_seen_seconds_ago
                                .is_none_or(|s| s <= i64::from(since))
                        })
                        .collect();
                    let more = items.len() > limit;
                    items.truncate(limit);
                    mask_events(&text_masker(), &mut items);
                    let mut body = EventsBody { items };
                    let cut = fit::fit(&mut body, event_items);
                    wrap(body, cut | more | truncated)
                },
            )
            .await)
    }

    #[tool(
        description = "ResourceQuotas of a project (used and hard per resource with the used ratio) and LimitRanges (min, max, default, defaultRequest, maxLimitRequestRatio per resource).",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            open_world_hint = false
        )
    )]
    async fn get_quotas(
        &self,
        Parameters(p): Parameters<NamespaceParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        Ok(self
            .session
            .call(
                &ctx,
                "get_quotas",
                args!(p),
                Some(&p.connection_id),
                async {
                    let q = expect!(
                        self.read(ReadOp::Quotas {
                            connection_id: p.connection_id.clone(),
                            namespace: p.namespace.clone()
                        })
                        .await?,
                        Quotas
                    )?;
                    to_value(&q)
                },
            )
            .await)
    }

    #[tool(
        description = "Network objects of a project: Services (type, ports, ready endpoints), Routes on OpenShift (host, path, service, TLS termination, admitted, embedded certificate expiry), Ingresses (class, hosts and paths to services, TLS hosts and Secret names, addresses), Gateway API HTTPRoutes and GRPCRoutes (hostnames, parents with Accepted and ResolvedRefs, backends) and Gateways (class, listeners, programmed). Kinds without their API group in the cluster are null.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            open_world_hint = false
        )
    )]
    async fn get_network(
        &self,
        Parameters(p): Parameters<NetworkParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        Ok(self
            .session
            .call(&ctx, "get_network", args!(p), Some(&p.connection_id), async {
                let n = expect!(
                    self.read(ReadOp::Network {
                        connection_id: p.connection_id.clone(),
                        namespace: p.namespace.clone()
                    })
                    .await?,
                    Network
                )?;
                let f = p.filter.as_deref();
                let keep = |name: &str| fit::matches_filter(name, f);
                let mut n = Network {
                    services: filtered(Some(n.services), |s: &Service| keep(&s.name))
                        .unwrap_or(Part::Data(vec![])),
                    endpoints_error: n.endpoints_error,
                    routes: filtered(n.routes, |r: &Route| keep(&r.name)),
                    ingresses: filtered(Some(n.ingresses), |i: &Ingress| keep(&i.name))
                        .unwrap_or(Part::Data(vec![])),
                    gateways: filtered(n.gateways, |g: &Gateway| keep(&g.name)),
                    http_routes: filtered(n.http_routes, |r: &GatewayRoute| keep(&r.name)),
                    grpc_routes: filtered(n.grpc_routes, |r: &GatewayRoute| keep(&r.name)),
                };
                mask_network(&text_masker(), &mut n);
                let v = to_value(&n)?;
                if serde_json::to_vec(&v).map_or(0, |b| b.len()) > fit::MAX_RESPONSE_BYTES {
                    return Err(BackendError::new(
                        "response_too_large",
                        "The network objects of this project do not fit into 100 KB; narrow them with filter",
                    ));
                }
                Ok(v)
            })
            .await)
    }

    #[tool(
        description = "Watch a workload for 10..=50 seconds (default 30), sampling it every 2 seconds. Returns a timeline: replica changes, pods added and removed, pods becoming ready or not ready, container restarts with reason and exit code, rollout progress (updated replicas) and new Warning events, plus a summary with the time spent below the desired replicas. Use it during a rollout or when pods keep restarting.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            open_world_hint = false
        )
    )]
    async fn observe_workload(
        &self,
        Parameters(p): Parameters<ObserveParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        Ok(self
            .session
            .call(
                &ctx,
                "observe_workload",
                args!(p),
                Some(&p.connection_id),
                self.observe(&p, &ctx),
            )
            .await)
    }
}
