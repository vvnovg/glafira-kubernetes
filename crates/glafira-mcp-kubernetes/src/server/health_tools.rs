//! Инструменты проверки кластера и замечаний (спец. §8, §10.4). Кластер они не меняют: проверка
//! только читает, замечания пишутся в Glafira и видны человеку рядом с объектами. Находки и
//! замечания вне области LLM (проекты не из `llmNamespaces`) в ответ не попадают.

use glafira_mcp_core::fit::{self, Page, TRUNCATED_HINT};
use glafira_mcp_core::{BackendError, to_value};
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::CallToolResult;
use rmcp::service::RequestContext;
use rmcp::{ErrorData, RoleServer, tool, tool_router};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::KubeServer;
use crate::health::*;
use crate::model::not_allowed;

pub const MAX_AGE_SECONDS_DEFAULT: u32 = 300;
pub const MAX_AGE_SECONDS_MAX: u32 = 3600;
pub const FINDINGS_LIMIT_DEFAULT: u32 = 100;
pub const FINDINGS_LIMIT_MAX: u32 = 200;
/// Сколько проектов можно назвать в `namespaces`: столько проверка и охватывает.
pub const MAX_CHECK_NAMESPACES: usize = 50;

/// У `list_annotations` нет страниц: при обрезке остаются самые свежие, остальное — сужением по status.
const NOTES_TRUNCATED_HINT: &str = "The response was truncated to 100 KB: only the most recently updated notes are shown; narrow with status";

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct HealthCheckParams {
    /// Connection id from list_connections.
    pub connection_id: String,
    /// Check only these projects (up to 50, from list_projects). Default: all projects you can see, up to 50.
    #[serde(default)]
    pub namespaces: Option<Vec<String>>,
    /// Reuse the last check if it is younger than this and covers the projects, 0..=3600 seconds. Default: 300; 0 forces a new check.
    #[serde(default)]
    pub max_age_seconds: Option<u32>,
    /// Page through this check (runId from a previous response) instead of running a new one.
    #[serde(default)]
    pub run_id: Option<String>,
    /// Only these rules (ruleId values from summary).
    #[serde(default)]
    pub rule_ids: Option<Vec<String>>,
    /// Only findings at least this severe. Default: info.
    #[serde(default)]
    pub min_severity: Option<Severity>,
    /// Include findings the user chose to ignore (marked ignored: true). Default: true.
    #[serde(default)]
    pub include_ignored: Option<bool>,
    /// Skip this many findings. Default: 0.
    #[serde(default)]
    pub offset: Option<u32>,
    /// Page size, 1..=200. Default: 100.
    #[serde(default)]
    pub limit: Option<u32>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AnnotateParams {
    /// Connection id from list_connections.
    pub connection_id: String,
    /// Object of the note: cluster, a node (name), a project (namespace) or an object in a project (namespace and name; workloadKind for a workload).
    pub target: Target,
    /// info, warning or critical.
    pub severity: Severity,
    /// Short summary, up to 120 characters; plain text.
    pub title: String,
    /// Explanation and what to do, up to 2000 characters; plain text, no markup or links.
    pub body: String,
    /// ruleId of the health_check finding this note explains, if any.
    #[serde(default)]
    pub related_rule_id: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ListAnnotationsParams {
    /// Connection id from list_connections.
    pub connection_id: String,
    /// open (default), closed (resolved or dismissed by the user) or all.
    #[serde(default)]
    pub status: Option<StatusFilter>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RemoveAnnotationParams {
    /// Connection id from list_connections.
    pub connection_id: String,
    /// Note id from annotate or list_annotations.
    pub annotation_id: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CheckBody {
    run_id: String,
    started_at_ms: i64,
    duration_ms: i64,
    trigger: Value,
    environment: String,
    incomplete: bool,
    reused: bool,
    scope: HealthScope,
    summary: Vec<RuleCount>,
    findings: Page<HealthFinding>,
    truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    hint: Option<&'static str>,
}

fn findings_of(b: &mut CheckBody) -> &mut Vec<HealthFinding> {
    &mut b.findings.items
}

#[derive(Serialize)]
struct NotesBody {
    items: Vec<AnnotationView>,
    truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    hint: Option<&'static str>,
}

fn notes_of(b: &mut NotesBody) -> &mut Vec<AnnotationView> {
    &mut b.items
}

/// Порядок списка без страниц: сначала недавно обновлённые, при равенстве — созданные позже,
/// затем по id. Обрезка до 100 КБ оставляет голову.
fn newest_first(items: &mut [AnnotationView]) {
    items.sort_by(|a, b| {
        b.updated_at_ms
            .cmp(&a.updated_at_ms)
            .then(b.created_at_ms.cmp(&a.created_at_ms))
            .then_with(|| a.id.cmp(&b.id))
    });
}

/// Аргументы вызова для журнала.
fn args<T: Serialize>(p: &T) -> Value {
    serde_json::to_value(p).unwrap_or(Value::Null)
}

impl KubeServer {
    /// Операция проверки и замечаний. `cluster` — она ходит в кластер (проверка, сверка объекта
    /// замечания): тогда занимает слоты параллельности, как чтение.
    async fn health(&self, op: HealthOp, cluster: bool) -> Result<HealthOutput, BackendError> {
        let _slots = match cluster {
            true => Some(self.session.slots().await?),
            false => None,
        };
        self.backend.health(op).await
    }

    async fn check(&self, p: &HealthCheckParams) -> Result<Value, BackendError> {
        let max_age = p.max_age_seconds.unwrap_or(MAX_AGE_SECONDS_DEFAULT);
        if max_age > MAX_AGE_SECONDS_MAX {
            return Err(BackendError::invalid_argument(format!(
                "maxAgeSeconds must be between 0 and {MAX_AGE_SECONDS_MAX}, got {max_age}"
            )));
        }
        let limit = p.limit.unwrap_or(FINDINGS_LIMIT_DEFAULT);
        if !(1..=FINDINGS_LIMIT_MAX).contains(&limit) {
            return Err(BackendError::invalid_argument(format!(
                "limit must be between 1 and {FINDINGS_LIMIT_MAX}, got {limit}"
            )));
        }
        if p.namespaces
            .as_ref()
            .is_some_and(|n| n.is_empty() || n.len() > MAX_CHECK_NAMESPACES)
        {
            return Err(BackendError::invalid_argument(format!(
                "namespaces: pass 1..={MAX_CHECK_NAMESPACES} projects or omit it"
            )));
        }
        let scope = self.scope(&p.connection_id, None)?;
        if p.namespaces.iter().flatten().any(|ns| !scope.allows(ns)) {
            return Err(not_allowed());
        }
        // Без своего списка проверка охватывает то, что видит LLM: проекты области или все.
        let namespaces = p.namespaces.clone().or_else(|| {
            scope
                .namespaces
                .as_ref()
                .map(|ns| ns.iter().cloned().collect())
        });
        let op = HealthOp::Check {
            connection_id: p.connection_id.clone(),
            max_age_ms: i64::from(max_age) * 1000,
            run_id: p.run_id.clone(),
            namespaces,
            session: self.session.identify(None),
        };
        let wait = self.options.check_wait;
        // Страница проверки по `runId` читается из файла Glafira — без слотов кластера.
        let out = tokio::time::timeout(wait, self.health(op, p.run_id.is_none()))
            .await
            .map_err(|_| {
                BackendError::new(
                    "check_in_progress_timeout",
                    "The cluster check takes longer than expected; retry in a minute",
                )
            })??;
        let HealthOutput::Run(mut run) = out else {
            return Err(BackendError::unexpected());
        };
        // Проверка, которую запустил человек, могла охватить и проекты вне области LLM.
        run.findings.retain(|f| in_scope(&f.target, &scope));
        run.scope.projects.retain(|ns| scope.allows(ns));
        let filter = FindingFilter {
            rule_ids: p.rule_ids.clone(),
            min_severity: p.min_severity,
            include_ignored: p.include_ignored.unwrap_or(true),
        };
        let (summary, findings) = select(
            run.findings,
            &filter,
            p.offset.unwrap_or(0) as usize,
            limit as usize,
        );
        let mut body = CheckBody {
            run_id: run.run_id,
            started_at_ms: run.started_at_ms,
            duration_ms: run.duration_ms,
            trigger: run.trigger,
            environment: run.environment,
            incomplete: run.incomplete,
            reused: run.reused,
            scope: run.scope,
            summary,
            findings,
            truncated: false,
            hint: None,
        };
        if fit::fit(&mut body, findings_of) {
            body.truncated = true;
            body.hint = Some(TRUNCATED_HINT);
            // Страница стала короче: следующая начинается сразу за последней находкой.
            let end = p.offset.unwrap_or(0) as usize + body.findings.items.len();
            body.findings.next_offset = (end < body.findings.total).then_some(end);
        }
        to_value(&body)
    }
}

#[tool_router(router = health_router, vis = "pub(crate)")]
impl KubeServer {
    #[tool(
        description = "Run Glafira's built-in cluster check (deterministic rules) or reuse the last one: API server readiness, OpenShift operators and updates, node readiness and pressure, crash-looping, image-pull, pending and OOMKilled pods, frequent restarts, unavailable workloads and stuck rollouts, single replicas, containers without requests, memory limits or readiness probes, latest image tags, quotas near the limit, pending PVCs, HPAs at max or unable to scale, services without endpoints, Routes, Ingresses and Gateway API routes with problems. Severity depends on the connection environment; pod findings are grouped by workload. Returns a summary by rule and a page of findings with ruleId, target, title, details and params. Start a review with this tool: explain and prioritize these facts rather than re-deriving them. Findings with ignored: true were dismissed by the user for that object.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            open_world_hint = false
        )
    )]
    async fn health_check(
        &self,
        Parameters(p): Parameters<HealthCheckParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        Ok(self
            .session
            .call(
                &ctx,
                "health_check",
                args(&p),
                Some(&p.connection_id),
                self.check(&p),
            )
            .await)
    }

    #[tool(
        description = "Leave a note about the cluster, a node, a project or an object in a project (workload, pod, service, route, ingress, httproute, grpcroute, gateway, pvc, hpa, quota) in Glafira: the user sees it next to the object, with your client name. Use it for conclusions that add to health_check findings (link them with relatedRuleId) — do not repeat findings. A note with the same target and title replaces your previous one. Notes expire after 7 days; at most 200 open notes per connection. Plain text only. Does not change the cluster.",
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn annotate(
        &self,
        Parameters(p): Parameters<AnnotateParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        // В журнал — без заголовка и текста замечания.
        let args = json!({
            "connectionId": p.connection_id,
            "target": p.target,
            "severity": p.severity,
            "relatedRuleId": p.related_rule_id,
        });
        Ok(self
            .session
            .call(&ctx, "annotate", args, Some(&p.connection_id), async {
                p.target.check().map_err(BackendError::invalid_argument)?;
                if p.title.trim().is_empty() {
                    return Err(BackendError::invalid_argument("title must not be empty"));
                }
                self.scope(&p.connection_id, p.target.namespace.as_deref())?;
                let op = HealthOp::Annotate {
                    connection_id: p.connection_id.clone(),
                    session: self.session.identify(None),
                    note: NewNote {
                        target: p.target.clone(),
                        severity: p.severity,
                        title: p.title.clone(),
                        body: p.body.clone(),
                        related_rule_id: p.related_rule_id.clone(),
                    },
                };
                match self.health(op, true).await? {
                    HealthOutput::Annotated {
                        annotation_id,
                        created,
                    } => to_value(&json!({ "annotationId": annotation_id, "created": created })),
                    _ => Err(BackendError::unexpected()),
                }
            })
            .await)
    }

    #[tool(
        description = "List notes left in Glafira for a connection by any client, with their status: open, resolved or dismissed (closed by the user). Use it to see what the user already handled before adding notes.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            open_world_hint = false
        )
    )]
    async fn list_annotations(
        &self,
        Parameters(p): Parameters<ListAnnotationsParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        Ok(self
            .session
            .call(
                &ctx,
                "list_annotations",
                args(&p),
                Some(&p.connection_id),
                async {
                    let scope = self.scope(&p.connection_id, None)?;
                    let op = HealthOp::ListAnnotations {
                        connection_id: p.connection_id.clone(),
                        status: p.status.unwrap_or_default(),
                    };
                    match self.health(op, false).await? {
                        HealthOutput::Annotations(mut items) => {
                            items.retain(|n| in_scope(&n.target, &scope));
                            newest_first(&mut items);
                            let mut body = NotesBody {
                                items,
                                truncated: false,
                                hint: None,
                            };
                            if fit::fit(&mut body, notes_of) {
                                body.truncated = true;
                                body.hint = Some(NOTES_TRUNCATED_HINT);
                            }
                            to_value(&body)
                        }
                        _ => Err(BackendError::unexpected()),
                    }
                },
            )
            .await)
    }

    #[tool(
        description = "Remove one of your own notes from Glafira (notes of other clients cannot be removed).",
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn remove_annotation(
        &self,
        Parameters(p): Parameters<RemoveAnnotationParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        Ok(self
            .session
            .call(
                &ctx,
                "remove_annotation",
                args(&p),
                Some(&p.connection_id),
                async {
                    self.scope(&p.connection_id, None)?;
                    let op = HealthOp::RemoveAnnotation {
                        connection_id: p.connection_id.clone(),
                        session: self.session.identify(None),
                        annotation_id: p.annotation_id.clone(),
                    };
                    match self.health(op, false).await? {
                        HealthOutput::Removed => to_value(&json!({ "removed": true })),
                        _ => Err(BackendError::unexpected()),
                    }
                },
            )
            .await)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn note(id: &str, created: i64, updated: i64) -> AnnotationView {
        AnnotationView {
            id: id.into(),
            target: Target {
                kind: TargetKind::Cluster,
                namespace: None,
                name: None,
                workload_kind: None,
            },
            severity: Severity::Info,
            title: "t".into(),
            body: "b".into(),
            related_rule_id: None,
            client: None,
            status: "open".into(),
            created_at_ms: created,
            updated_at_ms: updated,
        }
    }

    #[test]
    fn notes_are_sorted_newest_first_with_stable_ties() {
        let mut v = vec![
            note("b", 1, 5),
            note("a", 1, 5),
            note("old", 0, 1),
            note("later-created", 3, 5),
            note("fresh", 0, 9),
        ];
        newest_first(&mut v);
        let ids: Vec<_> = v.iter().map(|n| n.id.as_str()).collect();
        assert_eq!(ids, ["fresh", "later-created", "a", "b", "old"]);
    }
}
