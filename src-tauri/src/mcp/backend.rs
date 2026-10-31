//! Реализация `glafira_mcp_kubernetes::Backend` (спец. §10): чтения O1b через `ReadClient`
//! (`AppState::read` и `read_in`: клиент из общего пула или новое подключение), проверка кластера
//! и замечания LLM. Функций, меняющих кластер, здесь нет и быть не должно (сторожок в
//! `mcp/mod.rs`); замечания пишутся только в файл Glafira.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use glafira_mcp_core::{BackendError, BoxFuture, Hooks, SessionAccess, SessionView};
use glafira_mcp_kubernetes::health as mh;
use glafira_mcp_kubernetes::logs::{LogsOp, LogsOutput};
use glafira_mcp_kubernetes::model::{self as m, LogsAccess, Scope, not_allowed};
use glafira_mcp_kubernetes::{Backend, ReadOp, ReadOutput};
use k8s_openapi::api::autoscaling::v2::HorizontalPodAutoscaler;
use k8s_openapi::api::core::v1::{Node, PersistentVolumeClaim, Pod, ResourceQuota, Service};
use k8s_openapi::api::networking::v1::Ingress;
use kube::api::ListParams;

use super::approvals::Approvals;
use super::map::{self, code, error_text, reshape};
use crate::commands::blocking;
use crate::error::{AppError, AppResult, ErrorCode};
use crate::health::collect::requests_by_node;
use crate::health::model::{Finding, Target, TargetKind};
use crate::health::rules;
use crate::health::service::now_ms;
use crate::health::store::{
    Annotated, AnnotationStatus, Author, CheckRun, MAX_TITLE_CHARS, NewAnnotation, Removed,
    Trigger, sanitize,
};
use crate::health::texts;
use crate::k8s::configs::{ConfigValue, config_map_values};
use crate::k8s::events::{EventObject, list_events};
use crate::k8s::network::{gateways, grpc_routes, http_routes, routes};
use crate::k8s::pods::{WorkloadRef, get_pod, list_pods};
use crate::k8s::read::{ReadClient, by_labels, check_field_value};
use crate::k8s::workloads::{WorkloadKind, get_row, get_workload, list_workloads};
use crate::state::AppState;
use crate::storage::profile::{ConnectionProfile, LlmAccess, is_valid_namespace};

/// Что-то изменилось для интерфейса; `true` — нужен диалог подтверждения (привлечь внимание).
pub type OnChange = Arc<dyn Fn(bool) + Send + Sync>;

/// Сколько последних событий пода отдаёт `describe_pod`.
const POD_EVENTS: usize = 20;

pub struct AppBackend {
    pub(super) state: AppState,
    approvals: Arc<Approvals>,
    on_change: OnChange,
}

/// Код ошибки — как в приложении (`FORBIDDEN`, `NOT_FOUND`, `SESSION_EXPIRED`…), текст —
/// английский. Подробности (`details`) LLM не получает, а тексты с данными профиля — адресом,
/// командой плагина, именем пользователя — заменены фиксированной фразой (`map::llm_message`).
pub(super) fn to_backend(e: AppError) -> BackendError {
    BackendError::new(code(&e), map::llm_message(&e))
}

/// Текст для LLM при сбое файла `health/<id>.json`: путь к файлу не раскрывается.
const HEALTH_FILE_FAILED: &str = "Glafira could not read or save its data for this connection";
/// Текст для LLM, когда фоновая задача Glafira не завершилась (паника blocking-потока).
const BACKGROUND_FAILED: &str = "A background task in Glafira failed; retry the call";

/// Ошибки проверки и замечаний: сбой файла Glafira и паника фоновой задачи — `internal` с
/// общим текстом; остальные (кластер, подключение) — как у чтений.
pub(super) fn health_to_backend(e: AppError) -> BackendError {
    if ["errors.io", "errors.json"].contains(&e.key.as_str())
        || e.key.starts_with("errors.storage.")
    {
        BackendError::new("internal", HEALTH_FILE_FAILED)
    } else if e.key == "errors.app.taskPanicked" {
        BackendError::new("internal", BACKGROUND_FAILED)
    } else {
        to_backend(e)
    }
}

fn now_s() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as i64)
}

/// Значения env контейнеров: без уровня logs — никаких; на нём литералы остаются, а ключи
/// ConfigMap (`configMapKeyRef`) читаются — каждый ConfigMap один раз. Не прочиталось — причина
/// в `valueError`. Маскирует крейт шлюза.
async fn env_values<'a>(
    r: &ReadClient,
    namespace: &str,
    containers: impl Iterator<Item = &'a mut m::ContainerSpec>,
    values: bool,
) {
    let mut config_maps: std::collections::HashMap<
        String,
        Result<std::collections::BTreeMap<String, ConfigValue>, String>,
    > = Default::default();
    for c in containers {
        for e in &mut c.env {
            if !values {
                e.value = None;
                continue;
            }
            if e.source != "configMapKeyRef" {
                continue;
            }
            let Some((name, key)) = e.reference.as_deref().and_then(|r| r.split_once('/')) else {
                continue;
            };
            if !config_maps.contains_key(name) {
                let read = config_map_values(r, namespace, name)
                    .await
                    .map_err(|e| error_text(&e));
                config_maps.insert(name.to_string(), read);
            }
            match &config_maps[name] {
                Err(error) => e.value_error = Some(error.clone()),
                Ok(map) => match map.get(key) {
                    Some(ConfigValue {
                        text: Some(text), ..
                    }) => e.value = Some(text.clone()),
                    Some(ConfigValue {
                        binary_size: Some(n),
                        ..
                    }) => e.value_error = Some(format!("binary data, {n} bytes")),
                    _ => e.value_error = Some(format!("no key {key} in ConfigMap {name}")),
                },
            }
        }
    }
}

fn workload_kind(k: m::WorkloadKind) -> WorkloadKind {
    reshape(&k).unwrap_or(WorkloadKind::Deployment)
}

impl AppBackend {
    pub fn new(state: AppState, approvals: Arc<Approvals>, on_change: OnChange) -> Self {
        Self {
            state,
            approvals,
            on_change,
        }
    }

    /// Подключения, которые видит LLM; платформа — у тех, что сейчас в пуле.
    pub fn connections(&self) -> Vec<m::Connection> {
        let store = &self.state.connections;
        store
            .list()
            .into_iter()
            .filter(|p| !p.llm_access.is_off())
            .map(|p| m::Connection {
                folder_path: p
                    .folder_id
                    .as_deref()
                    .map(|f| store.folder_path(f))
                    .unwrap_or_default(),
                environment: serde_json::to_value(p.environment)
                    .ok()
                    .and_then(|v| v.as_str().map(String::from))
                    .unwrap_or_default(),
                llm_access: p
                    .llm_access
                    .effective(self.state.policy.status.mcp_logs_enabled)
                    .as_str()
                    .to_string(),
                platform: self
                    .state
                    .pool
                    .get(&p.id)
                    .map(|c| map::platform(&c.info.platform)),
                id: p.id,
                name: p.name,
                read_only: p.read_only,
            })
            .collect()
    }

    /// Профиль подключения, открытого для LLM; нет подключения и доступ выключен — одна ошибка.
    pub(super) fn allowed(&self, id: &str) -> Result<ConnectionProfile, BackendError> {
        match self.state.connections.get(id) {
            Ok(p) if !p.llm_access.is_off() => Ok(p),
            _ => Err(not_allowed()),
        }
    }

    /// Проект в области LLM подключения (крейт проверяет это до вызова; здесь — ещё раз).
    pub(super) fn in_scope(p: &ConnectionProfile, namespace: &str) -> Result<(), BackendError> {
        match &p.llm_namespaces {
            Some(list) if !list.iter().any(|n| n == namespace) => Err(not_allowed()),
            _ => Ok(()),
        }
    }

    async fn read_impl(&self, op: ReadOp) -> Result<ReadOutput, BackendError> {
        if let Some(id) = op.connection_id() {
            let p = self.allowed(id)?;
            if let Some(ns) = op.namespace() {
                Self::in_scope(&p, ns)?;
            }
        }
        // Значения env и ConfigMap — только на уровне logs (спец. §10.2).
        let values = op
            .connection_id()
            .and_then(|id| self.scope(id))
            .is_some_and(|s| s.logs == LogsAccess::Allowed);
        let st = &self.state;
        let now = now_s();
        let out = match op {
            ReadOp::ListConnections => ReadOutput::Connections(self.connections()),
            ReadOp::ClusterOverview { connection_id } => ReadOutput::Cluster(
                Box::pin(st.read(&connection_id, |c| async move {
                    let o = crate::k8s::overview::cluster_overview(&c.reader(), &c.info).await;
                    Ok(map::cluster(o, &c.info.capabilities, now))
                }))
                .await
                .map_err(to_backend)?,
            ),
            ReadOp::ListProjects { connection_id } => {
                let profile = self.allowed(&connection_id)?;
                let l = Box::pin(st.read(&connection_id, |c| async move {
                    crate::k8s::projects::list_projects(
                        &c.reader(),
                        c.info.api_groups.projects,
                        &profile,
                    )
                    .await
                }))
                .await
                .map_err(to_backend)?;
                ReadOutput::Projects(m::ProjectList {
                    items: l.items.into_iter().map(map::project).collect(),
                    truncated: l.truncated,
                    error: l.error.as_ref().map(error_text),
                })
            }
            ReadOp::ListNodes { connection_id } => ReadOutput::Nodes(
                Box::pin(st.read(&connection_id, |c| async move {
                    let r = c.reader();
                    let nodes = crate::k8s::nodes::list_nodes(&r).await?;
                    let (requests, requests_error) = match c.info.capabilities.list_all_pods {
                        true => match r.list::<Pod>(None, &ListParams::default()).await {
                            Ok(l) => (Some(requests_by_node(&l.items)), None),
                            Err(e) => (None, Some(error_text(&e))),
                        },
                        false => (
                            None,
                            Some("FORBIDDEN: pods of all projects cannot be listed".to_string()),
                        ),
                    };
                    Ok(m::NodeList {
                        items: nodes
                            .items
                            .into_iter()
                            .map(|n| {
                                let req = requests
                                    .as_ref()
                                    .map(|m| m.get(&n.name).copied().unwrap_or_default());
                                map::node_item(n, req, now)
                            })
                            .collect(),
                        truncated: nodes.truncated,
                        requests_error,
                    })
                }))
                .await
                .map_err(to_backend)?,
            ),
            ReadOp::DescribeNode {
                connection_id,
                name,
            } => ReadOutput::Node(
                Box::pin(st.read(&connection_id, |c| async move {
                    let r = c.reader();
                    let d = crate::k8s::nodes::get_node(&r, &name).await?;
                    let usage = match c.info.capabilities.list_all_pods {
                        true => {
                            check_field_value(&name)?;
                            let lp = ListParams::default().fields(&format!("spec.nodeName={name}"));
                            match r.list::<Pod>(None, &lp).await {
                                Ok(l) => m::Part::Data(map::node_usage(&l.items)),
                                Err(e) => m::Part::error(error_text(&e)),
                            }
                        }
                        false => m::Part::error("FORBIDDEN: pods of all projects cannot be listed"),
                    };
                    Ok(map::node_details(d, usage, now))
                }))
                .await
                .map_err(to_backend)?,
            ),
            ReadOp::ListWorkloads {
                connection_id,
                namespace,
            } => {
                let ns = namespace.clone();
                let l = Box::pin(st.read_in(&connection_id, &namespace, |c| async move {
                    Ok(list_workloads(&c.reader(), &ns, &c.info.api_groups).await)
                }))
                .await
                .map_err(to_backend)?;
                ReadOutput::Workloads(m::WorkloadList {
                    items: l
                        .items
                        .into_iter()
                        .map(|w| map::workload_item(w, now))
                        .collect(),
                    errors: l
                        .errors
                        .into_iter()
                        .map(|e| m::KindError {
                            kind: reshape(&e.kind).unwrap_or(m::WorkloadKind::Deployment),
                            error: error_text(&e.error),
                        })
                        .collect(),
                    truncated: l.truncated,
                })
            }
            ReadOp::DescribeWorkload {
                connection_id,
                namespace,
                workload,
            } => {
                let ns = namespace.clone();
                let d = Box::pin(st.read_in(&connection_id, &namespace, |c| async move {
                    let r = c.reader();
                    let d =
                        get_workload(&r, &ns, workload_kind(workload.kind), &workload.name).await?;
                    let mut d = map::workload_details(d, now);
                    env_values(&r, &ns, d.containers.iter_mut(), values).await;
                    Ok(d)
                }))
                .await
                .map_err(to_backend)?;
                ReadOutput::Workload(d)
            }
            ReadOp::ListPods {
                connection_id,
                namespace,
                workload,
            } => {
                let ns = namespace.clone();
                let w = workload.map(|w| WorkloadRef {
                    kind: workload_kind(w.kind),
                    name: w.name,
                });
                let l = Box::pin(st.read_in(&connection_id, &namespace, |c| async move {
                    list_pods(&c.reader(), &ns, w.as_ref()).await
                }))
                .await
                .map_err(to_backend)?;
                ReadOutput::Pods(m::PodList {
                    items: l.items.into_iter().map(|p| map::pod_item(p, now)).collect(),
                    truncated: l.truncated,
                })
            }
            ReadOp::DescribePod {
                connection_id,
                namespace,
                name,
            } => {
                let ns = namespace.clone();
                let d = Box::pin(st.read_in(&connection_id, &namespace, |c| async move {
                    let r = c.reader();
                    let object = EventObject {
                        kind: "Pod".into(),
                        name: name.clone(),
                    };
                    let (pod, events) =
                        tokio::join!(get_pod(&r, &ns, &name), list_events(&r, &ns, Some(&object)));
                    let events = match events {
                        Ok(l) => m::Part::Data(
                            l.items
                                .into_iter()
                                .take(POD_EVENTS)
                                .map(|e| map::event(e, now))
                                .collect(),
                        ),
                        Err(e) => m::Part::error(error_text(&e)),
                    };
                    let mut d = map::pod_details(pod?, events, now);
                    env_values(
                        &r,
                        &ns,
                        d.containers.iter_mut().map(|c| &mut c.spec),
                        values,
                    )
                    .await;
                    Ok(d)
                }))
                .await
                .map_err(to_backend)?;
                ReadOutput::Pod(d)
            }
            ReadOp::ListEvents {
                connection_id,
                namespace,
                object,
            } => {
                let ns = namespace.clone();
                let object = object.map(|o| EventObject {
                    kind: o.kind,
                    name: o.name,
                });
                let l = Box::pin(st.read_in(&connection_id, &namespace, |c| async move {
                    list_events(&c.reader(), &ns, object.as_ref()).await
                }))
                .await
                .map_err(to_backend)?;
                ReadOutput::Events(m::EventList {
                    items: l.items.into_iter().map(|e| map::event(e, now)).collect(),
                    truncated: l.truncated,
                })
            }
            ReadOp::Quotas {
                connection_id,
                namespace,
            } => {
                let ns = namespace.clone();
                let q = Box::pin(st.read_in(&connection_id, &namespace, |c| async move {
                    Ok(crate::k8s::quotas::get_quotas(&c.reader(), &ns).await)
                }))
                .await
                .map_err(to_backend)?;
                ReadOutput::Quotas(map::quotas(q))
            }
            ReadOp::Network {
                connection_id,
                namespace,
            } => {
                let ns = namespace.clone();
                let n = Box::pin(st.read_in(&connection_id, &namespace, |c| async move {
                    Ok(
                        crate::k8s::network::get_network(&c.reader(), &ns, &c.info.api_groups)
                            .await,
                    )
                }))
                .await
                .map_err(to_backend)?;
                ReadOutput::Network(map::network(n, now))
            }
            ReadOp::WorkloadState {
                connection_id,
                namespace,
                workload,
            } => {
                let ns = namespace.clone();
                let s = Box::pin(st.read_in(&connection_id, &namespace, |c| async move {
                    let r = c.reader();
                    let kind = workload_kind(workload.kind);
                    let (row, selector) = get_row(&r, &ns, kind, &workload.name).await?;
                    // Без селектора поды workload'а не определить: пустой подошёл бы всем.
                    let pods = match selector.filter(|s| !s.is_empty()) {
                        Some(sel) => {
                            r.list::<Pod>(Some(&ns), &by_labels(Some(&sel)))
                                .await?
                                .items
                        }
                        None => Vec::new(),
                    };
                    let object = EventObject {
                        kind: format!("{kind:?}"),
                        name: workload.name.clone(),
                    };
                    let events = list_events(&r, &ns, Some(&object))
                        .await
                        .map(|l| l.items)
                        .unwrap_or_default();
                    Ok(map::workload_state(&row, &pods, events))
                }))
                .await
                .map_err(to_backend)?;
                ReadOutput::WorkloadState(s)
            }
            ReadOp::ResourceUsage {
                connection_id,
                namespace,
                workload,
                window_days,
            } => {
                let (st2, id, ns) = (st.clone(), connection_id.clone(), namespace.clone());
                let r = Box::pin(st.read_in(&connection_id, &namespace, |c| async move {
                    let sel = crate::metrics::select::for_connection(&st2, &id, &c, false).await?;
                    if sel.history.is_none() && !sel.status.pod_metrics {
                        return Ok(Err(sel.status.clone()));
                    }
                    let kind = workload_kind(workload.kind);
                    crate::resources::usage::workload_usage(
                        &c.reader(),
                        &sel,
                        &ns,
                        kind,
                        &workload.name,
                        window_days,
                        now,
                    )
                    .await
                    .map(Ok)
                }))
                .await
                .map_err(to_backend)?;
                let u = r.map_err(|s| super::resources::metrics_unavailable(&s))?;
                ReadOutput::ResourceUsage(super::resources::usage(&u, now))
            }
            ReadOp::RecommendResources {
                connection_id,
                namespace,
                workload,
                window_days,
            } => {
                let (st2, id, ns) = (st.clone(), connection_id.clone(), namespace.clone());
                let r = Box::pin(st.read_in(&connection_id, &namespace, |c| async move {
                    let sel = crate::metrics::select::for_connection(&st2, &id, &c, false).await?;
                    if sel.history.is_none() && !sel.status.pod_metrics {
                        return Ok(Err(sel.status.clone()));
                    }
                    let kind = workload_kind(workload.kind);
                    let nodes = c.info.capabilities.list_nodes.then_some(&c.nodes);
                    crate::resources::analyze(
                        &c.reader(),
                        &sel,
                        &ns,
                        kind,
                        &workload.name,
                        window_days,
                        now,
                        nodes,
                    )
                    .await
                    .map(|a| Ok((a, sel.status.clone())))
                }))
                .await
                .map_err(to_backend)?;
                let (a, status) = r.map_err(|s| super::resources::metrics_unavailable(&s))?;
                ReadOutput::Recommendation(super::resources::recommendation(
                    &a.recommendation,
                    &status,
                ))
            }
        };
        Ok(out)
    }
}

impl AppBackend {
    /// Автор замечаний и проверок: имя из диалога подтверждения.
    pub(super) fn author(&self, session: &SessionView) -> Author {
        Author {
            client: self.approvals.shown_name(&session.id),
            session_id: session.id.clone(),
        }
    }

    /// Объект замечания есть в кластере (защита от придуманных имён). Объекты проекта читаются
    /// через `read_in` (403 сбрасывает кэш прав проекта, как у вкладки); проект — по списку
    /// проектов пользователя (`project_exists`).
    async fn target_exists(&self, connection_id: &str, t: &Target) -> Result<(), BackendError> {
        let ns = t.namespace.clone().unwrap_or_default();
        let name = t.name.clone().unwrap_or_default();
        let found = match t.kind {
            TargetKind::Cluster => true,
            TargetKind::Project => self.project_exists(connection_id, &ns).await?,
            _ => {
                let node = t.kind == TargetKind::Node;
                let (target, in_ns) = (t.clone(), ns.clone());
                let read = |c: Arc<crate::k8s::pool::Connected>| async move {
                    exists(&c.reader(), &target, &in_ns, &name).await
                };
                let r = if node {
                    Box::pin(self.state.read(connection_id, read)).await
                } else {
                    Box::pin(self.state.read_in(connection_id, &ns, read)).await
                };
                match r {
                    Ok(found) => found,
                    Err(e) if e.code == ErrorCode::NotFound => false,
                    Err(e) => return Err(to_backend(e)),
                }
            }
        };
        if found {
            Ok(())
        } else {
            Err(BackendError::not_found(format!(
                "There is no {} {} in this cluster",
                t.kind.as_str(),
                [t.namespace.as_deref(), t.name.as_deref()]
                    .into_iter()
                    .flatten()
                    .collect::<Vec<_>>()
                    .join("/")
            )))
        }
    }

    /// Проект есть среди проектов пользователя (как в дереве). Без права на их список (403)
    /// проверить нельзя — тогда достаточно имени: так открывается проект из ручного списка.
    async fn project_exists(&self, connection_id: &str, ns: &str) -> Result<bool, BackendError> {
        if !is_valid_namespace(ns) {
            return Ok(false);
        }
        let profile = self.allowed(connection_id)?;
        let l = Box::pin(self.state.read(connection_id, |c| async move {
            crate::k8s::projects::list_projects(&c.reader(), c.info.api_groups.projects, &profile)
                .await
        }))
        .await
        .map_err(to_backend)?;
        Ok(l.error.is_some() || l.truncated || l.items.iter().any(|p| p.name == ns))
    }

    async fn health_impl(&self, op: mh::HealthOp) -> Result<mh::HealthOutput, BackendError> {
        // Поколение — до проверки доступа: запись в подключение, удалённое за это время, отклоняется.
        let generation = self.state.health.generation(op.connection_id());
        let profile = self.allowed(op.connection_id())?;
        let st = self.state.clone();
        match op {
            mh::HealthOp::Check {
                connection_id,
                max_age_ms,
                run_id,
                namespaces,
                session,
            } => {
                if let Some(list) = &namespaces {
                    for ns in list {
                        Self::in_scope(&profile, ns)?;
                    }
                }
                let (st2, cid) = (st.clone(), connection_id.clone());
                let (run, reused, ignored) = match run_id {
                    Some(id) => {
                        let file = blocking(move || st2.health.store.load(&cid, now_ms()))
                            .await
                            .map_err(health_to_backend)?;
                        match file.last_check {
                            Some(r) if r.run_id == id => (Arc::new(r), true, file.ignored),
                            _ => {
                                return Err(BackendError::not_found(
                                    "This check was replaced by a newer one; call health_check without runId",
                                ));
                            }
                        }
                    }
                    None => {
                        let trigger = Trigger::Mcp {
                            client: self.author(&session).client,
                        };
                        let svc = st.health.clone();
                        let (run, reused) = Box::pin(svc.latest_or_run(
                            &st,
                            &connection_id,
                            trigger,
                            max_age_ms,
                            namespaces,
                        ))
                        .await
                        .map_err(health_to_backend)?;
                        let ignored =
                            blocking(move || Ok(st2.health.store.load(&cid, now_ms())?.ignored))
                                .await
                                .map_err(health_to_backend)?;
                        (run, reused, ignored)
                    }
                };
                Ok(mh::HealthOutput::Run(map_run(&run, reused, |k| {
                    ignored.iter().any(|i| i.key == k)
                })))
            }
            mh::HealthOp::Annotate {
                connection_id,
                session,
                note,
            } => {
                check_note_texts(note.related_rule_id.as_deref(), &note.title)?;
                if let Some(ns) = &note.target.namespace {
                    Self::in_scope(&profile, ns)?;
                }
                let target: Target = reshape(&note.target)
                    .ok_or_else(|| BackendError::invalid_argument("target cannot be read"))?;
                Box::pin(self.target_exists(&connection_id, &target)).await?;
                let new = NewAnnotation {
                    target,
                    severity: reshape(&note.severity).ok_or_else(BackendError::unexpected)?,
                    title: note.title,
                    body: note.body,
                    related_rule_id: note.related_rule_id,
                    author: self.author(&session),
                };
                let id = connection_id.clone();
                let st2 = st.clone();
                let r = blocking(move || {
                    st2.health
                        .write(&id, generation, |s| s.annotate(&id, new, now_ms()))
                })
                .await
                .map_err(health_to_backend)?;
                let (annotation_id, created) = match r {
                    Annotated::Created(id) => (id, true),
                    Annotated::Updated(id) => (id, false),
                    Annotated::LimitReached => {
                        return Err(BackendError::new(
                            "limit_reached",
                            "This connection already has 200 open notes; ask the user to close some in Glafira",
                        ));
                    }
                };
                st.health.changed(&connection_id);
                Ok(mh::HealthOutput::Annotated {
                    annotation_id,
                    created,
                })
            }
            mh::HealthOp::ListAnnotations {
                connection_id,
                status,
            } => {
                let f = blocking(move || st.health.store.load(&connection_id, now_ms()))
                    .await
                    .map_err(health_to_backend)?;
                let items = f
                    .annotations
                    .into_iter()
                    .filter(|a| match status {
                        mh::StatusFilter::Open => a.status == AnnotationStatus::Open,
                        mh::StatusFilter::Closed => a.status != AnnotationStatus::Open,
                        mh::StatusFilter::All => true,
                    })
                    .filter_map(|a| {
                        Some(mh::AnnotationView {
                            target: reshape(&a.target)?,
                            severity: reshape(&a.severity)?,
                            status: serde_json::to_value(a.status).ok()?.as_str()?.to_string(),
                            id: a.id,
                            title: a.title,
                            body: a.body,
                            related_rule_id: a.related_rule_id,
                            client: a.client,
                            created_at_ms: a.created_at_ms,
                            updated_at_ms: a.updated_at_ms,
                        })
                    })
                    .collect();
                Ok(mh::HealthOutput::Annotations(items))
            }
            mh::HealthOp::RemoveAnnotation {
                connection_id,
                session,
                annotation_id,
            } => {
                let author = self.author(&session);
                let id = connection_id.clone();
                let st2 = st.clone();
                let r = blocking(move || {
                    st2.health.write(&id, generation, |s| {
                        s.remove_own(&id, &annotation_id, &author, now_ms())
                    })
                })
                .await
                .map_err(health_to_backend)?;
                match r {
                    Removed::Removed => {
                        st.health.changed(&connection_id);
                        Ok(mh::HealthOutput::Removed)
                    }
                    Removed::NotFound => Err(BackendError::not_found(
                        "The note was deleted or has expired",
                    )),
                    Removed::NotOwner => Err(BackendError::new(
                        "not_allowed",
                        "Only notes created by this client can be removed",
                    )),
                }
            }
        }
    }
}

/// Объект замечания в проекте: чтение по виду (`NOT_FOUND` — его нет).
async fn exists(r: &ReadClient, t: &Target, ns: &str, name: &str) -> AppResult<bool> {
    Ok(match t.kind {
        TargetKind::Node => {
            r.get_cluster::<Node>(name).await?;
            true
        }
        TargetKind::Workload => {
            let kind = t.workload_kind.unwrap_or(WorkloadKind::Deployment);
            get_row(r, ns, kind, name).await?;
            true
        }
        TargetKind::Pod => r.get_opt::<Pod>(ns, name).await?.is_some(),
        TargetKind::Service => r.get_opt::<Service>(ns, name).await?.is_some(),
        TargetKind::Ingress => r.get_opt::<Ingress>(ns, name).await?.is_some(),
        TargetKind::Pvc => r
            .get_opt::<PersistentVolumeClaim>(ns, name)
            .await?
            .is_some(),
        TargetKind::Hpa => r
            .get_opt::<HorizontalPodAutoscaler>(ns, name)
            .await?
            .is_some(),
        TargetKind::Quota => r.get_opt::<ResourceQuota>(ns, name).await?.is_some(),
        TargetKind::Route => {
            r.get_dynamic(&routes(), Some(ns), name).await?;
            true
        }
        TargetKind::Gateway => {
            r.get_dynamic(&gateways(), Some(ns), name).await?;
            true
        }
        TargetKind::Httproute => {
            r.get_dynamic(&http_routes(), Some(ns), name).await?;
            true
        }
        TargetKind::Grpcroute => {
            r.get_dynamic(&grpc_routes(), Some(ns), name).await?;
            true
        }
        TargetKind::Cluster | TargetKind::Project => true,
    })
}

/// Общие проверки текстов замечания: правило из каталога, непустой заголовок.
pub(super) fn check_note_texts(
    related_rule_id: Option<&str>,
    title: &str,
) -> Result<(), BackendError> {
    if let Some(r) = related_rule_id
        && !rules::rule_ids().any(|id| id == r)
    {
        return Err(BackendError::invalid_argument(format!(
            "relatedRuleId {r} is not a health_check rule"
        )));
    }
    // Заголовок из одних управляющих символов после очистки пуст — как пустой заголовок.
    if sanitize(title, MAX_TITLE_CHARS).is_empty() {
        return Err(BackendError::invalid_argument("title must not be empty"));
    }
    Ok(())
}

fn map_finding(f: &Finding, ignored: bool) -> Option<mh::HealthFinding> {
    let (title, details) = texts::english(f);
    Some(mh::HealthFinding {
        key: f.key.clone(),
        rule_id: f.rule_id.clone(),
        severity: reshape(&f.severity)?,
        target: reshape(&f.target)?,
        title,
        details,
        params: f.params.clone(),
        ignored,
    })
}

fn map_run(r: &CheckRun, reused: bool, ignored: impl Fn(&str) -> bool) -> mh::HealthRun {
    mh::HealthRun {
        run_id: r.run_id.clone(),
        started_at_ms: r.started_at_ms,
        duration_ms: r.duration_ms,
        trigger: serde_json::to_value(&r.trigger).unwrap_or_default(),
        environment: serde_json::to_value(r.environment)
            .ok()
            .and_then(|v| v.as_str().map(String::from))
            .unwrap_or_default(),
        incomplete: r.incomplete,
        reused,
        scope: mh::HealthScope {
            projects: r.scope.projects.clone(),
            projects_total: r.scope.projects_total,
            pods: r.scope.pods,
            nodes: r.scope.nodes,
        },
        findings: r
            .findings
            .iter()
            .filter_map(|f| map_finding(f, ignored(&f.key)))
            .collect(),
    }
}

impl Hooks for AppBackend {
    fn access(&self, session: &SessionView) -> SessionAccess {
        if !self.state.policy.status.mcp_enabled {
            return SessionAccess::Disabled;
        }
        let (access, new_pending) = self
            .approvals
            .access(session, glafira_mcp_core::audit::now_ms());
        if new_pending {
            (self.on_change)(true);
        }
        access
    }

    fn connection_name(&self, id: &str) -> Option<String> {
        self.state.connections.get(id).ok().map(|p| p.name)
    }

    fn session_closed(&self, session_id: &str) {
        self.approvals.closed(session_id);
    }

    fn sessions_changed(&self) {
        (self.on_change)(false);
    }
}

impl Backend for AppBackend {
    fn scope(&self, connection_id: &str) -> Option<Scope> {
        let p = self.allowed(connection_id).ok()?;
        let logs = match (p.llm_access, self.state.policy.status.mcp_logs_enabled) {
            (LlmAccess::Logs, true) => LogsAccess::Allowed,
            (LlmAccess::Logs, false) => LogsAccess::Disabled,
            _ => LogsAccess::Metadata,
        };
        Some(Scope {
            namespaces: p
                .llm_namespaces
                .map(|v| v.into_iter().collect::<BTreeSet<_>>()),
            // Свой список имён действует только на уровне `logs` (спец. §10.3).
            mask_fields: (logs == LogsAccess::Allowed)
                .then_some(p.llm_mask_fields)
                .flatten(),
            logs,
        })
    }

    fn read(&self, op: ReadOp) -> BoxFuture<'_, Result<ReadOutput, BackendError>> {
        Box::pin(self.read_impl(op))
    }

    fn health(&self, op: mh::HealthOp) -> BoxFuture<'_, Result<mh::HealthOutput, BackendError>> {
        Box::pin(self.health_impl(op))
    }

    fn proposals(
        &self,
        op: glafira_mcp_kubernetes::proposals::ProposalOp,
    ) -> BoxFuture<'_, Result<glafira_mcp_kubernetes::proposals::ProposalOutput, BackendError>>
    {
        Box::pin(self.proposals_impl(op))
    }

    fn logs(&self, op: LogsOp) -> BoxFuture<'_, Result<LogsOutput, BackendError>> {
        Box::pin(self.logs_impl(op))
    }
}

impl AppBackend {
    /// Логи подов (спец. §10.4): уровень `logs` и политика проверяются ещё раз — по профилю.
    async fn logs_impl(&self, op: LogsOp) -> Result<LogsOutput, BackendError> {
        let p = self.allowed(op.connection_id())?;
        Self::in_scope(&p, op.namespace())?;
        if p.llm_access != LlmAccess::Logs {
            return Err(m::logs_not_allowed());
        }
        if !self.state.policy.status.mcp_logs_enabled {
            return Err(m::logs_disabled());
        }
        let st = &self.state;
        match op {
            LogsOp::Pod {
                connection_id,
                namespace,
                pod,
                container,
                previous,
                since,
                tail_lines,
            } => {
                let since_time = super::logs::since_time(&since)?;
                let ns = namespace.clone();
                let log = Box::pin(st.read_in(&connection_id, &namespace, |c| async move {
                    super::logs::pod_log(
                        &c.reader(),
                        &ns,
                        &pod,
                        container,
                        previous,
                        since,
                        since_time,
                        tail_lines,
                    )
                    .await
                }))
                .await
                .map_err(to_backend)?;
                match log {
                    super::logs::PodLog::Log(log) => Ok(LogsOutput::Pod(log)),
                    super::logs::PodLog::Unavailable(why) => Err(m::log_unavailable(why)),
                }
            }
            LogsOp::Workload {
                connection_id,
                namespace,
                workload,
                since_seconds,
            } => {
                let ns = namespace.clone();
                let w = WorkloadRef {
                    kind: workload_kind(workload.kind),
                    name: workload.name,
                };
                let logs = Box::pin(st.read_in(&connection_id, &namespace, |c| async move {
                    super::logs::workload_logs(
                        &c.reader(),
                        &ns,
                        &w,
                        since_seconds,
                        super::logs::Limits::search(),
                    )
                    .await
                }))
                .await
                .map_err(to_backend)?;
                Ok(LogsOutput::Workload(logs))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::k8s::pods::fixtures::{crash_looping, running};
    use crate::k8s::pool::test_connected;
    use crate::k8s::testing::{Seen, status};
    use crate::secrets::MemoryStore;
    use crate::storage::profile::{LlmAccess, sample_profile};

    /// Подключения: `open` открыт для LLM в проекте team-a, `all` — во всех, `closed` — нет.
    fn backend() -> (tempfile::TempDir, AppBackend) {
        let dir = tempfile::tempdir().unwrap();
        let st = AppState::new(dir.path(), Arc::new(MemoryStore::default())).unwrap();
        let mut open = sample_profile("open");
        open.name = "dev".into();
        open.llm_access = LlmAccess::Metadata;
        open.llm_namespaces = Some(vec!["team-a".into()]);
        st.connections.upsert(open).unwrap();
        let mut all = sample_profile("all");
        all.llm_access = LlmAccess::Metadata;
        st.connections.upsert(all).unwrap();
        st.connections.upsert(sample_profile("closed")).unwrap();
        let approvals = Arc::new(Approvals::new(st.settings.clone()));
        (dir, AppBackend::new(st, approvals, Arc::new(|_| {})))
    }

    /// Мок кластера в пуле подключения: запросы чтения не уходят в сеть.
    fn pooled(
        b: &AppBackend,
        id: &str,
        handler: impl Fn(&Seen) -> (u16, serde_json::Value) + Send + 'static,
    ) {
        let pool = &b.state.pool;
        assert!(pool.insert_if_current(id, pool.generation(id), test_connected(handler)));
    }

    fn list(items: Vec<serde_json::Value>) -> (u16, serde_json::Value) {
        (
            200,
            serde_json::json!({"apiVersion": "v1", "kind": "List", "metadata": {}, "items": items}),
        )
    }

    fn view(id: &str, name: Option<&str>) -> SessionView {
        SessionView {
            id: id.into(),
            client: name.map(|n| glafira_mcp_core::ClientIdent {
                name: n.into(),
                version: "1".into(),
            }),
        }
    }

    #[tokio::test]
    async fn only_open_connections_are_listed_with_their_scope() {
        let (_d, b) = backend();
        let mut ids: Vec<String> = b.connections().into_iter().map(|c| c.id).collect();
        ids.sort();
        assert_eq!(ids, ["all", "open"]);
        let open = b
            .connections()
            .into_iter()
            .find(|c| c.id == "open")
            .unwrap();
        assert_eq!(
            (open.llm_access.as_str(), open.platform),
            ("metadata", None),
            "не подключено — платформы нет"
        );
        assert_eq!(b.connection_name("open").as_deref(), Some("dev"));
        assert_eq!(
            b.scope("open").unwrap().namespaces,
            Some(BTreeSet::from(["team-a".to_string()]))
        );
        assert_eq!(b.scope("all").unwrap().namespaces, None);
        assert!(b.scope("closed").is_none() && b.scope("missing").is_none());
        pooled(&b, "open", |_| list(vec![]));
        let open = b
            .connections()
            .into_iter()
            .find(|c| c.id == "open")
            .unwrap();
        assert_eq!(open.platform.unwrap().kind, m::PlatformKind::Kubernetes);
    }

    #[tokio::test]
    async fn closed_unknown_and_foreign_projects_are_not_allowed_before_network() {
        let (_d, b) = backend();
        for id in ["closed", "missing"] {
            let e = b
                .read(ReadOp::ClusterOverview {
                    connection_id: id.into(),
                })
                .await
                .unwrap_err();
            assert_eq!(e, not_allowed(), "{id}");
        }
        let e = b
            .read(ReadOp::ListPods {
                connection_id: "open".into(),
                namespace: "team-b".into(),
                workload: None,
            })
            .await
            .unwrap_err();
        assert_eq!(e, not_allowed());
    }

    #[tokio::test]
    async fn reads_use_the_pooled_client_and_map_for_the_llm() {
        let (_d, b) = backend();
        pooled(&b, "open", |s| match s.path() {
            "/api/v1/namespaces/team-a/pods" => {
                list(vec![crash_looping("web-1"), running("web-2")])
            }
            _ => list(vec![]),
        });
        let ReadOutput::Pods(p) = b
            .read(ReadOp::ListPods {
                connection_id: "open".into(),
                namespace: "team-a".into(),
                workload: None,
            })
            .await
            .unwrap()
        else {
            panic!("pods")
        };
        assert_eq!(p.items.len(), 2);
        assert_eq!(p.items[0].problem.as_deref(), Some("crashLoop"));
        assert_eq!(p.items[0].owner.as_ref().unwrap().name, "web");
    }

    #[test]
    fn policy_disables_every_session() {
        let (_d, b) = backend();
        let mut st = b.state.clone();
        let mut policy = (*st.policy).clone();
        policy.status.mcp_enabled = false;
        st.policy = Arc::new(policy);
        let b = AppBackend::new(st, b.approvals.clone(), Arc::new(|_| {}));
        assert_eq!(b.access(&view("s1", None)), SessionAccess::Disabled);
    }

    /// Уровень `logs`: доступ к логам по политике, свой список имён только на этом уровне.
    #[test]
    fn logs_level_and_mask_fields_reach_the_scope() {
        let (_d, b) = backend();
        let mut logs = sample_profile("logs");
        logs.llm_access = LlmAccess::Logs;
        logs.llm_mask_fields = Some(vec!["inn".into()]);
        b.state.connections.upsert(logs).unwrap();
        let mut meta = sample_profile("meta");
        meta.llm_access = LlmAccess::Metadata;
        meta.llm_mask_fields = Some(vec!["inn".into()]);
        b.state.connections.upsert(meta).unwrap();

        let s = b.scope("logs").unwrap();
        assert_eq!(
            (s.logs, s.mask_fields.as_deref()),
            (LogsAccess::Allowed, Some(&["inn".to_string()][..]))
        );
        let s = b.scope("meta").unwrap();
        assert_eq!(
            (s.logs, s.mask_fields),
            (LogsAccess::Metadata, None),
            "свой список — только на уровне logs"
        );
        let level = |b: &AppBackend, id: &str| {
            b.connections()
                .into_iter()
                .find(|c| c.id == id)
                .map(|c| c.llm_access)
        };
        assert_eq!(level(&b, "logs").as_deref(), Some("logs"));

        let mut st = b.state.clone();
        let mut policy = (*st.policy).clone();
        policy.status.mcp_logs_enabled = false;
        st.policy = Arc::new(policy);
        let b = AppBackend::new(st, b.approvals.clone(), Arc::new(|_| {}));
        let s = b.scope("logs").unwrap();
        assert_eq!((s.logs, s.mask_fields), (LogsAccess::Disabled, None));
        assert_eq!(
            level(&b, "logs").as_deref(),
            Some("metadata"),
            "при запрете логов уровень для модели — metadata"
        );
    }

    /// Под с литералом и ключами ConfigMap в env: прочитанный, без ключа, без прав.
    fn pod_with_env() -> serde_json::Value {
        let mut p = running("web-1");
        p["spec"]["containers"][0]["env"] = serde_json::json!([
            {"name": "LOG_LEVEL", "value": "debug"},
            {"name": "MODE", "valueFrom": {"configMapKeyRef": {"name": "app-config", "key": "mode"}}},
            {"name": "COLOR", "valueFrom": {"configMapKeyRef": {"name": "app-config", "key": "color"}}},
            {"name": "GONE", "valueFrom": {"configMapKeyRef": {"name": "app-config", "key": "nope"}}},
            {"name": "OTHER", "valueFrom": {"configMapKeyRef": {"name": "locked", "key": "x"}}},
            {"name": "DB_PASSWORD", "valueFrom": {"secretKeyRef": {"name": "db", "key": "password"}}},
        ]);
        p
    }

    #[tokio::test]
    async fn env_values_only_on_the_logs_level() {
        let (_d, b) = backend();
        let mut logs = sample_profile("logs");
        logs.llm_access = LlmAccess::Logs;
        b.state.connections.upsert(logs).unwrap();
        let config_reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        for id in ["logs", "all"] {
            let reads = config_reads.clone();
            pooled(&b, id, move |s| match s.path() {
                "/api/v1/namespaces/team-a/pods/web-1" => (200, pod_with_env()),
                "/api/v1/namespaces/team-a/configmaps/app-config" => {
                    reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    (
                        200,
                        serde_json::json!({"apiVersion": "v1", "kind": "ConfigMap",
                            "metadata": {"name": "app-config"}, "data": {"mode": "blue"},
                            "binaryData": {"color": "AAEC"}}),
                    )
                }
                "/api/v1/namespaces/team-a/configmaps/locked" => {
                    status(403, "Forbidden", "configmaps \"locked\" is forbidden")
                }
                "/api/v1/namespaces/team-a/secrets/db" => panic!("Secret не читается"),
                _ => list(vec![]),
            });
        }
        let describe = |id: &str| ReadOp::DescribePod {
            connection_id: id.into(),
            namespace: "team-a".into(),
            name: "web-1".into(),
        };
        let ReadOutput::Pod(p) = b.read(describe("logs")).await.unwrap() else {
            panic!("не под")
        };
        let env: Vec<(&str, Option<&str>, Option<&str>)> = p.containers[0]
            .spec
            .env
            .iter()
            .map(|e| {
                (
                    e.name.as_str(),
                    e.value.as_deref(),
                    e.value_error.as_deref(),
                )
            })
            .collect();
        assert_eq!(env[0], ("LOG_LEVEL", Some("debug"), None));
        assert_eq!(env[1], ("MODE", Some("blue"), None));
        assert_eq!(env[2], ("COLOR", None, Some("binary data, 3 bytes")));
        assert_eq!(
            env[3],
            ("GONE", None, Some("no key nope in ConfigMap app-config"))
        );
        assert!(env[4].2.unwrap().starts_with("FORBIDDEN"), "{:?}", env[4]);
        assert_eq!(
            env[5],
            ("DB_PASSWORD", None, None),
            "значения Secret — никогда"
        );
        assert_eq!(
            config_reads.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "ConfigMap читается один раз"
        );

        let ReadOutput::Pod(p) = b.read(describe("all")).await.unwrap() else {
            panic!("не под")
        };
        assert!(
            p.containers[0]
                .spec
                .env
                .iter()
                .all(|e| e.value.is_none() && e.value_error.is_none()),
            "уровень metadata — без значений"
        );
        assert_eq!(config_reads.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn logs_need_the_logs_level_and_the_policy_by_profile() {
        let (_d, b) = backend();
        let mut logs = sample_profile("logs");
        logs.llm_access = LlmAccess::Logs;
        logs.llm_namespaces = Some(vec!["team-a".into()]);
        b.state.connections.upsert(logs).unwrap();
        let op = |id: &str, ns: &str| LogsOp::Pod {
            connection_id: id.into(),
            namespace: ns.into(),
            pod: "web-1".into(),
            container: Some("web".into()),
            previous: false,
            since: None,
            tail_lines: Some(10),
        };
        let code = |r: Result<LogsOutput, BackendError>| r.unwrap_err().code;
        assert_eq!(code(b.logs(op("all", "team-a")).await), "logs_not_allowed");
        assert_eq!(code(b.logs(op("closed", "team-a")).await), "not_allowed");
        assert_eq!(code(b.logs(op("logs", "team-b")).await), "not_allowed");
        let mut bad = op("logs", "team-a");
        if let LogsOp::Pod { since, .. } = &mut bad {
            *since = Some(glafira_mcp_kubernetes::logs::Since::Time("noon".into()));
        }
        assert_eq!(code(b.logs(bad).await), "invalid_argument");
        // Контейнер не запускался: не ошибка аргументов, а `log_unavailable` с причиной.
        pooled(&b, "logs", |_| {
            (
                400,
                serde_json::json!({"kind": "Status", "apiVersion": "v1", "status": "Failure",
                    "reason": "BadRequest", "code": 400,
                    "message": "container \"web\" in pod \"web-1\" is waiting to start: ContainerCreating"}),
            )
        });
        let e = b.logs(op("logs", "team-a")).await.unwrap_err();
        assert_eq!(e.code, "log_unavailable");
        assert!(
            e.message.contains("waiting to start (ContainerCreating)"),
            "{}",
            e.message
        );

        let mut st = b.state.clone();
        let mut policy = (*st.policy).clone();
        policy.status.mcp_logs_enabled = false;
        st.policy = Arc::new(policy);
        let b = AppBackend::new(st, b.approvals.clone(), Arc::new(|_| {}));
        assert_eq!(code(b.logs(op("logs", "team-a")).await), "logs_disabled");
    }

    #[tokio::test]
    async fn check_runs_over_the_llm_projects() {
        let (_d, b) = backend();
        pooled(&b, "open", |s| match s.path() {
            "/api/v1/namespaces/team-a/pods" => list(vec![crash_looping("web-1")]),
            _ => list(vec![]),
        });
        let mh::HealthOutput::Run(run) = b
            .health(mh::HealthOp::Check {
                connection_id: "open".into(),
                max_age_ms: 300_000,
                run_id: None,
                namespaces: Some(vec!["team-a".into()]),
                session: view("s1", Some("claude-ai")),
            })
            .await
            .unwrap()
        else {
            panic!("run")
        };
        assert!(!run.reused);
        assert_eq!(run.scope.projects, ["team-a"]);
        let f = run
            .findings
            .iter()
            .find(|f| f.rule_id == "pod.crashLoop")
            .unwrap();
        assert_eq!(f.target.name.as_deref(), Some("web"));
        assert!(f.title.contains("restart in a loop"), "{}", f.title);
        // Повтор по тем же проектам — прошлая проверка.
        let mh::HealthOutput::Run(again) = b
            .health(mh::HealthOp::Check {
                connection_id: "open".into(),
                max_age_ms: 300_000,
                run_id: None,
                namespaces: Some(vec!["team-a".into()]),
                session: view("s1", Some("claude-ai")),
            })
            .await
            .unwrap()
        else {
            panic!("run")
        };
        assert_eq!((again.reused, again.run_id), (true, run.run_id));
        let e = b
            .health(mh::HealthOp::Check {
                connection_id: "open".into(),
                max_age_ms: 0,
                run_id: None,
                namespaces: Some(vec!["team-b".into()]),
                session: view("s1", None),
            })
            .await
            .unwrap_err();
        assert_eq!(e, not_allowed());
    }

    fn note(target: Target, title: &str) -> mh::NewNote {
        mh::NewNote {
            target: reshape(&target).unwrap(),
            severity: mh::Severity::Warning,
            title: title.into(),
            body: "body".into(),
            related_rule_id: Some("pod.crashLoop".into()),
        }
    }

    #[tokio::test]
    async fn notes_are_checked_against_the_cluster_and_removed_by_their_author() {
        let (_d, b) = backend();
        pooled(&b, "open", |s| match s.path() {
            "/api/v1/namespaces/team-a/pods/web-1" => (200, running("web-1")),
            _ => status(404, "NotFound", "not found"),
        });
        let session = view("s1", Some("claude-ai"));
        b.access(&session);
        let annotate = |target: Target, title: &str| {
            b.health(mh::HealthOp::Annotate {
                connection_id: "open".into(),
                session: session.clone(),
                note: note(target, title),
            })
        };
        let pod = |name: &str| Target::object(TargetKind::Pod, "team-a", name);
        let e = annotate(pod("ghost"), "t").await.unwrap_err();
        assert_eq!(e.code, "not_found", "{e:?}");
        let e = annotate(Target::object(TargetKind::Pod, "team-b", "x"), "t")
            .await
            .unwrap_err();
        assert_eq!(e, not_allowed());
        let e = annotate(pod("web-1"), "\u{7}").await.unwrap_err();
        assert_eq!(e.code, "invalid_argument");
        let mh::HealthOutput::Annotated {
            annotation_id,
            created,
        } = annotate(pod("web-1"), "Restarts on OOM").await.unwrap()
        else {
            panic!("annotated")
        };
        assert!(created);
        assert!(matches!(
            annotate(Target::cluster(), "Cluster note").await.unwrap(),
            mh::HealthOutput::Annotated { created: true, .. }
        ));
        let mh::HealthOutput::Annotations(items) = b
            .health(mh::HealthOp::ListAnnotations {
                connection_id: "open".into(),
                status: mh::StatusFilter::Open,
            })
            .await
            .unwrap()
        else {
            panic!("list")
        };
        assert_eq!(items.len(), 2);
        let n = items.iter().find(|n| n.id == annotation_id).unwrap();
        assert_eq!(
            (n.client.as_deref(), n.status.as_str()),
            (Some("claude-ai"), "open")
        );
        assert_eq!(n.target.namespace.as_deref(), Some("team-a"));
        let foreign = view("s2", Some("cursor"));
        b.access(&foreign);
        let e = b
            .health(mh::HealthOp::RemoveAnnotation {
                connection_id: "open".into(),
                session: foreign,
                annotation_id: annotation_id.clone(),
            })
            .await
            .unwrap_err();
        assert_eq!(e.code, "not_allowed");
        assert!(matches!(
            b.health(mh::HealthOp::RemoveAnnotation {
                connection_id: "open".into(),
                session: session.clone(),
                annotation_id,
            })
            .await
            .unwrap(),
            mh::HealthOutput::Removed
        ));
    }

    #[tokio::test]
    async fn project_notes_need_a_project_of_the_user() {
        let (_d, b) = backend();
        let ns = |name: &str| serde_json::json!({"metadata": {"name": name}});
        pooled(&b, "all", move |s| match s.path() {
            "/api/v1/namespaces" => list(vec![ns("team-a")]),
            _ => status(404, "NotFound", "not found"),
        });
        let session = view("s1", Some("claude-ai"));
        b.access(&session);
        let annotate = |project: &str| {
            b.health(mh::HealthOp::Annotate {
                connection_id: "all".into(),
                session: session.clone(),
                note: note(Target::project(project), "Project note"),
            })
        };
        assert!(annotate("team-a").await.is_ok());
        let e = annotate("team-b").await.unwrap_err();
        assert_eq!(e.code, "not_found", "{e:?}");
        // Без права на список проектов проверить нельзя: достаточно имени.
        pooled(&b, "all", |_| {
            status(403, "Forbidden", "namespaces is forbidden")
        });
        assert!(annotate("team-b").await.is_ok());
    }

    #[tokio::test]
    async fn health_ops_need_llm_access() {
        let (_d, b) = backend();
        let e = b
            .health(mh::HealthOp::ListAnnotations {
                connection_id: "closed".into(),
                status: mh::StatusFilter::All,
            })
            .await
            .unwrap_err();
        assert_eq!(e, not_allowed());
    }

    #[test]
    fn file_errors_hide_the_path() {
        let e = AppError::from(std::io::Error::other("/secret/path/health/c1.json"));
        let b = health_to_backend(e);
        assert_eq!(b.code, "internal");
        assert!(!b.message.contains("/secret"), "{b:?}");
        let e = crate::k8s::errors::not_allowed("get", "pods", Some("team-a"));
        assert_eq!(health_to_backend(e).code, "FORBIDDEN");
    }

    const HOST: &str = "marker-host.example";
    const CMD: &str = "/opt/marker-cmd";
    const USER: &str = "marker-user";

    /// Ошибки подключения, у которых в тексте может оказаться адрес, команда плагина или имя
    /// пользователя профиля.
    fn profile_errors() -> Vec<AppError> {
        let mut errors = vec![
            app_err!(
                ErrorCode::AuthFailed,
                "errors.exec.start",
                "Cannot start the exec plugin {{command}}: {{error}}",
                command = CMD,
                error = format!("{USER} at {HOST}")
            ),
            app_err!(
                ErrorCode::Timeout,
                "errors.exec.timeout",
                "The exec plugin {{command}} did not finish in {{seconds}} s",
                command = CMD,
                seconds = 30
            ),
            app_err!(
                ErrorCode::AuthFailed,
                "errors.exec.failed",
                "The exec plugin {{command}} failed (exit code {{code}})",
                command = CMD,
                code = 1
            ),
            app_err!(
                ErrorCode::AuthFailed,
                "errors.exec.badOutput",
                "The exec plugin {{command}} returned no credentials",
                command = CMD
            ),
            app_err!(
                ErrorCode::AuthFailed,
                "errors.exec.commandNotAbsolute",
                "Specify the full path to “{{command}}” — use “Find”",
                command = CMD
            ),
            app_err!(
                ErrorCode::AuthFailed,
                "errors.exec.argsNotAllowed",
                "The exec plugin “{{command}}” may run only as “{{expected}} …”",
                command = CMD,
                expected = format!("{CMD} --user={USER}")
            ),
            app_err!(
                ErrorCode::InvalidInput,
                "errors.connections.badApiUrl",
                "Invalid API server address “{{url}}”",
                url = format!("https://{HOST}:6443")
            ),
            app_err!(
                ErrorCode::InvalidInput,
                "errors.connections.badProxyUrl",
                "Invalid proxy address “{{url}}”: use http://host:port",
                url = format!("socks5://{HOST}:1080")
            ),
            app_err!(
                ErrorCode::AuthFailed,
                "errors.oauth.rejected",
                "The OAuth server rejected the sign-in: {{reason}}",
                reason = format!("no such user {USER} at {HOST}")
            ),
            app_err!(
                ErrorCode::AuthFailed,
                "errors.oauth.denied",
                "Sign-in was not completed: {{error}}",
                error = format!("access_denied for {USER}")
            ),
            app_err!(
                ErrorCode::AuthFailed,
                "errors.k8s.credentials",
                "The credentials of this connection cannot be used: {{message}}",
                message = format!("auth error for {USER}@{HOST}")
            ),
        ];
        // Запасные тексты сети и 403 строит сам код приложения, как и при настоящем сбое.
        errors.push(crate::k8s::errors::from_transport_text(&format!(
            "error sending request for {HOST}: odd failure"
        )));
        errors.push(crate::k8s::errors::forbidden(&format!(
            "User \"{USER}\" may not do this at {HOST}"
        )));
        errors
    }

    #[test]
    fn connection_errors_hide_the_profile() {
        let errors = profile_errors();
        assert_eq!(errors.len(), 13);
        for e in errors {
            let expected_code = code(&e);
            let shown = [error_text(&e), {
                let b = to_backend(e.clone());
                assert_eq!(b.code, expected_code, "{}", e.key);
                format!("{}: {}", b.code, b.message)
            }];
            for text in shown {
                for marker in [HOST, CMD, USER] {
                    assert!(!text.contains(marker), "{}: {text}", e.key);
                }
                assert!(
                    text.starts_with(&format!("{expected_code}: ")),
                    "{}: {text}",
                    e.key
                );
                assert!(text.len() > expected_code.len() + 2, "{}: {text}", e.key);
            }
        }
    }

    #[test]
    fn errors_with_names_the_model_asked_for_keep_their_text() {
        let e = crate::k8s::errors::not_allowed("get", "pods", Some("team-a"));
        assert_eq!(to_backend(e.clone()).message, e.message);
        assert!(error_text(&e).contains("team-a"));
        let e = crate::k8s::errors::from_status(
            &kube::core::Status::failure("pods \"web-1\" not found", "NotFound").with_code(404),
        );
        assert_eq!(to_backend(e.clone()).message, e.message);
        assert!(error_text(&e).contains("web-1"));
        let e = crate::k8s::read::check_namespace("Bad_NS").unwrap_err();
        assert!(to_backend(e).message.contains("Bad_NS"));
        let e = crate::health::store::connection_not_found("c-9");
        assert!(error_text(&e).contains("c-9"));
        let e = crate::app_err!(
            ErrorCode::AuthFailed,
            "errors.oauth.cancelled",
            "Sign-in was cancelled"
        );
        assert_eq!(to_backend(e.clone()).message, e.message);
        let e = crate::k8s::errors::from_transport_text("tls handshake eof");
        assert_eq!(to_backend(e.clone()).message, e.message);
    }

    fn workload_ref() -> m::WorkloadRef {
        m::WorkloadRef {
            kind: m::WorkloadKind::Deployment,
            name: "web".into(),
        }
    }

    #[tokio::test]
    async fn resources_come_from_the_metrics_source_without_the_own_address() {
        let (_d, b) = backend();
        pooled(&b, "open", crate::resources::usage::tests::cluster);
        let usage = || ReadOp::ResourceUsage {
            connection_id: "open".into(),
            namespace: "team-a".into(),
            workload: workload_ref(),
            window_days: 7,
        };
        // Prometheus через прокси нет (404), metrics.k8s.io есть — одна точка без истории.
        let ReadOutput::ResourceUsage(u) = b.read(usage()).await.unwrap() else {
            panic!("usage")
        };
        assert_eq!(u.source.kind, "metricsServer");
        assert_eq!(u.containers[0].stats.cpu_max.as_deref(), Some("120m"));
        assert_eq!(u.containers[0].requests.cpu.as_deref(), Some("500m"));
        assert!(u.series.is_none());
        assert_eq!(u.rejected.len(), 4, "кандидаты Prometheus с причинами");
        let ReadOutput::Recommendation(r) = b
            .read(ReadOp::RecommendResources {
                connection_id: "open".into(),
                namespace: "team-a".into(),
                workload: workload_ref(),
                window_days: 7,
            })
            .await
            .unwrap()
        else {
            panic!("recommendation")
        };
        assert_eq!(r.confidence, "low");
        assert_eq!(
            r.containers[0]
                .recommended_requests
                .as_ref()
                .unwrap()
                .cpu
                .as_deref(),
            Some("140m")
        );
        // Свой источник без токена: модель не видит его адреса.
        let mut p = b.state.connections.get("open").unwrap();
        p.metrics = crate::storage::profile::MetricsConfig::Prometheus(
            crate::storage::profile::MetricsEndpoint {
                url: "https://secret-prometheus.corp.example".into(),
                auth: crate::storage::profile::MetricsAuth::Bearer,
                ca: crate::storage::profile::MetricsCa::Connection,
            },
        );
        b.state.connections.upsert(p).unwrap();
        pooled(&b, "open", crate::resources::usage::tests::cluster);
        let e = b.read(usage()).await.unwrap_err();
        assert_eq!(e.code, "metrics_unavailable");
        assert!(!e.message.contains("secret-prometheus"), "{}", e.message);
        assert!(e.message.contains("tokenMissing"), "{}", e.message);
    }

    /// Свой источник метрик работает и выбран: его адрес не попадает в ответы модели — ни в
    /// `source`, ни где-либо ещё в `get_resource_usage` и `recommend_resources`.
    #[tokio::test]
    async fn a_working_own_source_is_used_without_its_address() {
        let (base, seen) = crate::metrics::http::fake::server(|line| {
            let body = if line.contains("vector%281%29") || line.contains("vector(1)&") {
                r#"{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1,"1"]}]}}"#
            } else if line.contains("/query_range") {
                r#"{"status":"success","data":{"resultType":"matrix","result":[]}}"#
            } else {
                r#"{"status":"success","data":{"resultType":"vector","result":[]}}"#
            };
            (200, body.to_string())
        })
        .await;
        let (_d, b) = backend();
        let mut p = b.state.connections.get("open").unwrap();
        p.proxy = crate::storage::profile::ProxyConfig::None;
        p.metrics = crate::storage::profile::MetricsConfig::Prometheus(
            crate::storage::profile::MetricsEndpoint {
                url: base.to_string(),
                auth: crate::storage::profile::MetricsAuth::None,
                ca: crate::storage::profile::MetricsCa::Connection,
            },
        );
        b.state.connections.upsert(p).unwrap();
        pooled(&b, "open", crate::resources::usage::tests::cluster);
        let address = format!("{}:{}", base.host_str().unwrap(), base.port().unwrap());
        let ReadOutput::ResourceUsage(u) = b
            .read(ReadOp::ResourceUsage {
                connection_id: "open".into(),
                namespace: "team-a".into(),
                workload: workload_ref(),
                window_days: 7,
            })
            .await
            .unwrap()
        else {
            panic!("usage")
        };
        assert_eq!(
            (u.source.kind.as_str(), u.source.origin.as_str()),
            ("prometheus", "profile")
        );
        assert_eq!(u.source.location, None);
        assert!(u.rejected.is_empty());
        let text = serde_json::to_string(&u).unwrap();
        assert!(
            !text.contains(&address) && !text.contains("/prefix"),
            "{text}"
        );
        let ReadOutput::Recommendation(r) = b
            .read(ReadOp::RecommendResources {
                connection_id: "open".into(),
                namespace: "team-a".into(),
                workload: workload_ref(),
                window_days: 7,
            })
            .await
            .unwrap()
        else {
            panic!("recommendation")
        };
        assert_eq!(r.source.location, None);
        let text = serde_json::to_string(&r).unwrap();
        assert!(
            !text.contains(&address) && !text.contains("/prefix"),
            "{text}"
        );
        assert!(
            seen.lock().unwrap().len() > 1,
            "источник действительно опрашивался"
        );
    }

    /// Подключение только для чтения: предложение создаётся (применит человек, сняв флажок), ответ
    /// говорит `readOnly: true`. Список HPA не прочитался (500): `scale` — не отказ, а предложение с
    /// предупреждением; форма перепроверит HPA при применении (решение контроллера).
    #[tokio::test]
    async fn read_only_connection_and_an_unread_hpa_list_still_give_a_proposal() {
        use glafira_mcp_kubernetes::proposals::{ProposalOp, ProposalOutput};
        let (_d, b) = backend();
        let mut p = b.state.connections.get("open").unwrap();
        p.read_only = true;
        b.state.connections.upsert(p).unwrap();
        pooled(&b, "open", |s| match s.path() {
            "/apis/autoscaling/v2/namespaces/team-a/horizontalpodautoscalers" => {
                status(500, "InternalError", "boom")
            }
            p if p.ends_with("/limitranges") || p.ends_with("/resourcequotas") => list(vec![]),
            _ => crate::resources::usage::tests::cluster(s),
        });
        let session = view("s1", Some("claude-ai"));
        b.access(&session);
        let out = b
            .proposals(ProposalOp::Propose {
                connection_id: "open".into(),
                session,
                proposal: proposal(serde_json::json!({"kind": "scale", "namespace": "team-a",
                    "workload": {"kind": "Deployment", "name": "web"}, "replicas": 3})),
            })
            .await
            .unwrap();
        let ProposalOutput::Proposed {
            created,
            read_only,
            warnings,
            ..
        } = out
        else {
            panic!("proposed")
        };
        assert!(created && read_only);
        assert_eq!(
            warnings,
            [
                "The HorizontalPodAutoscalers of the project could not be read, so the change was not checked against them"
            ]
        );
    }

    fn proposal(change: serde_json::Value) -> glafira_mcp_kubernetes::proposals::NewProposal {
        glafira_mcp_kubernetes::proposals::NewProposal {
            severity: mh::Severity::Warning,
            title: "Fix".into(),
            rationale: "r".into(),
            change: serde_json::from_value(change).unwrap(),
            related_rule_id: None,
            related_finding_key: None,
        }
    }

    #[tokio::test]
    async fn proposals_are_checked_against_the_cluster_and_kept_in_glafira() {
        use glafira_mcp_kubernetes::proposals::{ProposalOp, ProposalOutput};
        let (_d, b) = backend();
        pooled(&b, "open", |s| match s.path() {
            "/apis/autoscaling/v2/namespaces/team-a/horizontalpodautoscalers" => list(vec![
                serde_json::json!({"metadata": {"name": "web-hpa"}, "spec": {"maxReplicas": 5,
                    "scaleTargetRef": {"kind": "Deployment", "name": "web"}}}),
            ]),
            "/api/v1/namespaces/team-a/limitranges" => list(vec![serde_json::json!({
                "metadata": {"name": "lr"},
                "spec": {"limits": [{"type": "Container", "min": {"cpu": "100m"}}]}})]),
            p if p.ends_with("/resourcequotas") => list(vec![]),
            other => crate::resources::usage::tests::cluster(&Seen {
                method: s.method.clone(),
                uri: other.to_string(),
                body: s.body.clone(),
            }),
        });
        let session = view("s1", Some("claude-ai"));
        b.access(&session);
        let propose = |change: serde_json::Value| {
            b.proposals(ProposalOp::Propose {
                connection_id: "open".into(),
                session: session.clone(),
                proposal: proposal(change),
            })
        };
        let w = serde_json::json!({"kind": "Deployment", "name": "web"});
        let e = propose(serde_json::json!({"kind": "scale", "namespace": "team-a", "workload": w, "replicas": 3}))
            .await
            .unwrap_err();
        assert_eq!(e.code, "invalid_argument");
        assert!(e.message.contains("setHpaBounds"), "{}", e.message);
        let e = propose(
            serde_json::json!({"kind": "setResources", "namespace": "team-a", "workload": w,
            "containers": [{"name": "ghost", "requests": {"cpu": "1"}}]}),
        )
        .await
        .unwrap_err();
        assert_eq!(e.code, "invalid_argument");
        assert!(e.message.contains("ghost"), "{}", e.message);
        let e = propose(
            serde_json::json!({"kind": "rolloutRestart", "namespace": "team-b", "workload": w}),
        )
        .await
        .unwrap_err();
        assert_eq!(e, not_allowed());
        // Значение ниже минимума LimitRange — ошибка, а не предупреждение: поды новой ревизии
        // отклонят позже, и выкладка встанет.
        let e = propose(
            serde_json::json!({"kind": "setResources", "namespace": "team-a", "workload": w,
            "containers": [{"name": "app", "requests": {"cpu": "50m"}}]}),
        )
        .await
        .unwrap_err();
        assert_eq!(e.code, "invalid_argument");
        assert!(e.message.contains("LimitRange"), "{}", e.message);
        assert!(e.message.contains("100m"), "{}", e.message);
        let out = propose(
            serde_json::json!({"kind": "setResources", "namespace": "team-a", "workload": w,
            "containers": [{"name": "app", "requests": {"cpu": "150m"}}]}),
        )
        .await
        .unwrap();
        let ProposalOutput::Proposed {
            proposal_id,
            created,
            read_only,
            warnings,
        } = out
        else {
            panic!("proposed")
        };
        assert!(created && !read_only);
        assert!(warnings.is_empty(), "{warnings:?}");
        let ProposalOutput::Proposals(items) = b
            .proposals(ProposalOp::List {
                connection_id: "open".into(),
                status: mh::StatusFilter::Open,
            })
            .await
            .unwrap()
        else {
            panic!("list")
        };
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].change.kind_name(), "setResources");
        assert_eq!(
            items[0].target.workload_kind,
            Some(m::WorkloadKind::Deployment)
        );
        let other = view("s2", Some("cursor"));
        b.access(&other);
        let e = b
            .proposals(ProposalOp::Withdraw {
                connection_id: "open".into(),
                session: other,
                proposal_id: proposal_id.clone(),
            })
            .await
            .unwrap_err();
        assert_eq!(e.code, "not_allowed");
        assert!(matches!(
            b.proposals(ProposalOp::Withdraw {
                connection_id: "open".into(),
                session,
                proposal_id,
            })
            .await
            .unwrap(),
            ProposalOutput::Withdrawn
        ));
        let e = b
            .proposals(ProposalOp::List {
                connection_id: "closed".into(),
                status: mh::StatusFilter::All,
            })
            .await
            .unwrap_err();
        assert_eq!(e, not_allowed());
    }
}
