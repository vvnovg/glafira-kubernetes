//! Инструменты проверки кластера и замечаний (спец. §8, §10.4). Кластер они не меняют: проверка
//! только читает, замечания пишутся в Glafira и видны человеку рядом с объектами. Находки и
//! замечания вне области LLM (проекты не из `llmNamespaces`) в ответ не попадают, текст находок из
//! кластера проходит встроенные шаблоны маскировки §10.3.

use std::collections::BTreeSet;

use glafira_mcp_core::fit::{self, Page, TRUNCATED_HINT};
use glafira_mcp_core::{BackendError, to_value};
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::CallToolResult;
use rmcp::service::RequestContext;
use rmcp::{ErrorData, RoleServer, tool, tool_router};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use super::{KubeServer, mask_text, text_masker};
use crate::health::*;
use crate::mask::Masker;
use crate::model::{Scope, not_allowed};

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

/// Строки внутри JSON встроенными шаблонами §10.3: массивы и объекты — рекурсивно, ключи
/// остаются как есть.
fn mask_json(m: &Masker, v: &mut Value) {
    match v {
        Value::String(s) => {
            mask_text(m, s);
        }
        Value::Array(items) => items.iter_mut().for_each(|x| mask_json(m, x)),
        Value::Object(map) => map.values_mut().for_each(|x| mask_json(m, x)),
        _ => {}
    }
}

/// `title`, `details` и строки в `params` находок несут текст из кластера (причины и сообщения
/// условий и состояний контейнеров, сообщения Route, Gateway и ClusterOperator, образы): через
/// встроенные шаблоны §10.3, как в остальных инструментах чтения. Тихо, без счётчика.
fn mask_findings(m: &Masker, findings: &mut [HealthFinding]) {
    for f in findings {
        mask_text(m, &mut f.title);
        mask_text(m, &mut f.details);
        f.params.values_mut().for_each(|v| mask_json(m, v));
    }
}

/// Находка о неполной проверке.
const CHECK_PARTIAL: &str = "check.partial";

/// Сколько пробелов `check.partial` видит модель; столько же показывает приложение.
const MAX_LISTED_GAPS: usize = 20;

/// Проверку мог запустить человек по большему числу проектов, чем видит LLM, и приложение
/// отдаёт её целиком. Здесь она сужается до области подключения: проекты вне её неотличимы от
/// отсутствующих (спец. §10.2). Область «все проекты» проверку не сужает.
///
/// Находки вне области и чужие проекты в `scope.projects` отбрасываются. `scope.projects_total`
/// считает проекты области: проверенные и не проверенные из-за предела, но не больше самой
/// области. `scope.pods` и `scope.nodes` — итог всей проверки, их число проекта не называет и
/// остаётся прежним. `incomplete` и находка `check.partial` в приложении возникают по одному
/// условию: если находка была и после сужения её не осталось, проверка для LLM полная.
/// В любой области пробелы `check.partial` сужаются по полному списку и лишь затем обрезаются
/// до `MAX_LISTED_GAPS` (`limit_partial`).
fn limit_to_scope(run: &mut HealthRun, scope: &Scope) {
    let Some(allowed) = &scope.namespaces else {
        for f in &mut run.findings {
            if f.rule_id == CHECK_PARTIAL {
                limit_partial(f, None, None);
            }
        }
        return;
    };
    let had_partial = run.findings.iter().any(|f| f.rule_id == CHECK_PARTIAL);
    run.findings.retain(|f| in_scope(&f.target, scope));
    let before = run.scope.projects.len();
    run.scope.projects.retain(|ns| allowed.contains(ns));
    let checked = run.scope.projects.len();
    let skipped = run.scope.projects_total.saturating_sub(before);
    let total = (checked + skipped).min(allowed.len()).max(checked);
    let foreign = before != checked || total != run.scope.projects_total;
    run.scope.projects_total = total;
    // Числа в `check.partial` пересчитываются, только если проверка охватила чужие проекты.
    let counts = foreign.then_some((checked, total));
    run.findings
        .retain_mut(|f| f.rule_id != CHECK_PARTIAL || limit_partial(f, Some(allowed), counts));
    if had_partial && !run.findings.iter().any(|f| f.rule_id == CHECK_PARTIAL) {
        run.incomplete = false;
    }
}

/// `check.partial` для модели. В `params` приложение кладёт список пробелов целиком
/// (`<проект>/<часть>: <код>` или `<часть>: <код>`). Если задана область `allowed`, пробелы чужих
/// проектов убираются из всего списка, и `gapCount` считается заново по оставшимся — иначе при
/// длинном списке чужие пробелы, попавшие в первые 20, вытеснили бы пробелы области, а их число
/// выдало бы чужие проекты. Только потом список обрезается до `MAX_LISTED_GAPS`; `gapCount`
/// остаётся полным числом пробелов. `projectsChecked`, `projectsTotal` и `projectLimit` пересчитываются
/// по проектам области (`counts`). Если `params` изменились, `details` собирается заново.
/// `false` — находка осталась без причины (ни предела, ни дедлайна, ни пробелов) и отбрасывается:
/// у проверки только по проектам области её не было бы.
fn limit_partial(
    f: &mut HealthFinding,
    allowed: Option<&BTreeSet<String>>,
    counts: Option<(usize, usize)>,
) -> bool {
    let before = f.params.clone();
    let mut narrowed = None;
    if let Some(Value::Array(gaps)) = f.params.get_mut("gaps") {
        if let Some(allowed) = allowed {
            gaps.retain(|g| {
                g.as_str()
                    .and_then(|g| g.split_once('/'))
                    .is_none_or(|(ns, _)| allowed.contains(ns))
            });
            narrowed = Some(gaps.len());
        }
        gaps.truncate(MAX_LISTED_GAPS);
    }
    if let Some(n) = narrowed
        && f.params.contains_key("gapCount")
    {
        f.params.insert("gapCount".into(), json!(n));
    }
    if let Some((checked, total)) = counts {
        for (key, value) in [
            ("projectsChecked", json!(checked)),
            ("projectsTotal", json!(total)),
            ("projectLimit", json!(checked < total)),
        ] {
            if let Some(v) = f.params.get_mut(key) {
                *v = value;
            }
        }
    }
    if f.params == before {
        return true;
    }
    let flag = |k: &str| f.params.get(k).and_then(Value::as_bool) == Some(true);
    let counted = f
        .params
        .get("gapCount")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let listed = f
        .params
        .get("gaps")
        .and_then(Value::as_array)
        .map_or(0, Vec::len);
    let gaps = counted.max(listed as u64);
    if !flag("projectLimit") && !flag("podLimit") && !flag("timedOut") && gaps == 0 {
        return false;
    }
    f.details = partial_details(&f.params);
    true
}

/// Пояснение `check.partial` теми же словами, что собирает приложение (`texts::english`): после
/// сужения до области прежний текст называл бы чужие проекты.
fn partial_details(p: &Map<String, Value>) -> String {
    let flag = |k: &str| p.get(k).and_then(Value::as_bool) == Some(true);
    let show = |k: &str| match p.get(k) {
        Some(Value::String(s)) => s.clone(),
        Some(v) => v.to_string(),
        None => String::new(),
    };
    let mut parts = Vec::new();
    if flag("projectLimit") {
        parts.push(format!(
            "only {} of {} projects were checked (limit 50)",
            show("projectsChecked"),
            show("projectsTotal")
        ));
    }
    if flag("podLimit") {
        parts.push("only the first 5000 pods were checked".to_string());
    }
    if flag("timedOut") {
        parts.push("the check stopped at its 45 s deadline".to_string());
    }
    let counted = p.get("gapCount").and_then(Value::as_u64).unwrap_or(0);
    if counted > 0 {
        let (mut gaps, shown) = match p.get("gaps") {
            Some(Value::Array(items)) => (
                items
                    .iter()
                    .map(|g| match g {
                        Value::String(s) => s.clone(),
                        v => v.to_string(),
                    })
                    .collect::<Vec<_>>()
                    .join(", "),
                items.len() as u64,
            ),
            _ => (String::new(), 0),
        };
        // Показано меньше, чем пробелов (список обрезан до 20): как в тексте приложения, «…».
        if shown > 0 && counted > shown {
            gaps.push_str(", …");
        }
        parts.push(format!("not read: {gaps}"));
    }
    match parts.is_empty() {
        true => "Some objects were not checked: findings may be missing for them.".to_string(),
        false => format!(
            "Some objects were not checked: {}. Findings may be missing for them.",
            parts.join("; ")
        ),
    }
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
        // Проверка шла до 50 с: пользователь мог за это время сузить `llmNamespaces` или снять
        // доступ LLM. Область читается заново — по прежней ответ раскрыл бы то, что уже закрыто
        // (доступ снят — `not_allowed`, как у любого вызова без доступа).
        let scope = self.scope(&p.connection_id, None)?;
        // Проверка, которую запустил человек, могла охватить и проекты вне области LLM.
        limit_to_scope(&mut run, &scope);
        // Текст находок — из кластера: маскировка до сводки, отбора и обрезки под предел размера,
        // чтобы итоги и размер считались по тому, что уйдёт модели.
        mask_findings(&text_masker(), &mut run.findings);
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

    fn scope_of(names: &[&str]) -> Scope {
        Scope {
            namespaces: Some(names.iter().map(|n| n.to_string()).collect()),
        }
    }

    fn partial(params: Value, details: &str) -> HealthFinding {
        HealthFinding {
            key: "check.partial:cluster".into(),
            rule_id: "check.partial".into(),
            severity: Severity::Info,
            target: Target {
                kind: TargetKind::Cluster,
                namespace: None,
                name: None,
                workload_kind: None,
            },
            title: "The check is incomplete".into(),
            details: details.into(),
            params: params.as_object().cloned().unwrap(),
            ignored: false,
        }
    }

    fn run_of(projects: &[&str], total: usize, finding: HealthFinding) -> HealthRun {
        HealthRun {
            run_id: "r".into(),
            started_at_ms: 0,
            duration_ms: 0,
            trigger: json!({ "kind": "ui" }),
            environment: "prod".into(),
            incomplete: false,
            reused: false,
            scope: HealthScope {
                projects: projects.iter().map(|p| p.to_string()).collect(),
                projects_total: total,
                pods: 7,
                nodes: None,
            },
            findings: vec![finding],
        }
    }

    #[test]
    fn partial_details_follow_the_wording_of_the_app() {
        let all = json!({
            "projectLimit": true, "projectsChecked": 2, "projectsTotal": 3,
            "podLimit": true, "timedOut": true,
            "gaps": ["nodes: FORBIDDEN", "a/secrets: FORBIDDEN"], "gapCount": 2,
        });
        assert_eq!(
            partial_details(all.as_object().unwrap()),
            "Some objects were not checked: only 2 of 3 projects were checked (limit 50); only the first 5000 pods were checked; the check stopped at its 45 s deadline; not read: nodes: FORBIDDEN, a/secrets: FORBIDDEN. Findings may be missing for them."
        );
        assert_eq!(
            partial_details(&Map::new()),
            "Some objects were not checked: findings may be missing for them."
        );
        // Показано меньше, чем пробелов: список заканчивается многоточием.
        let more = json!({"gaps": ["a: X", "b: X"], "gapCount": 3});
        assert_eq!(
            partial_details(more.as_object().unwrap()),
            "Some objects were not checked: not read: a: X, b: X, …. Findings may be missing for them."
        );
    }

    #[test]
    fn a_project_limit_keeps_the_unchecked_projects_of_the_scope() {
        // Человек проверил 3 проекта из 80; в области LLM их три, проверены два.
        let f = partial(
            json!({"projectLimit": true, "projectsChecked": 3, "projectsTotal": 80,
                   "podLimit": false, "timedOut": false, "gaps": [], "gapCount": 0}),
            "Some objects were not checked: only 3 of 80 projects were checked (limit 50). Findings may be missing for them.",
        );
        let mut run = run_of(&["a", "b", "x"], 80, f);
        limit_to_scope(&mut run, &scope_of(&["a", "b", "c"]));
        assert_eq!(run.scope.projects, ["a", "b"]);
        assert_eq!((run.scope.projects_total, run.scope.pods), (3, 7));
        let f = &run.findings[0];
        assert_eq!(
            (
                f.params["projectsChecked"].as_u64(),
                f.params["projectsTotal"].as_u64()
            ),
            (Some(2), Some(3))
        );
        assert_eq!(f.params["projectLimit"], true);
        assert_eq!(
            f.details,
            "Some objects were not checked: only 2 of 3 projects were checked (limit 50). Findings may be missing for them."
        );
    }

    #[test]
    fn a_run_over_the_scope_only_is_not_touched() {
        let params = json!({"projectLimit": false, "projectsChecked": 1, "projectsTotal": 1,
                            "podLimit": false, "timedOut": false,
                            "gaps": ["a/secrets: FORBIDDEN"], "gapCount": 1});
        let f = partial(params.clone(), "text of the app");
        let mut run = run_of(&["a"], 1, f.clone());
        limit_to_scope(&mut run, &scope_of(&["a"]));
        assert_eq!(run.findings, [f]);
        assert_eq!(run.scope.projects_total, 1);
        let mut run = run_of(&["a", "b"], 2, partial(params, "text of the app"));
        limit_to_scope(&mut run, &Scope::default());
        assert_eq!((run.scope.projects.len(), run.scope.projects_total), (2, 2));
        assert_eq!(run.findings[0].details, "text of the app");
    }

    #[test]
    fn incomplete_is_cleared_only_when_the_partial_finding_leaves() {
        let foreign_only = || {
            partial(
                json!({"projectLimit": false, "projectsChecked": 2, "projectsTotal": 2,
                       "podLimit": false, "timedOut": false,
                       "gaps": ["b/secrets: FORBIDDEN"], "gapCount": 1}),
                "text of the app",
            )
        };
        let mut run = run_of(&["a", "b"], 2, foreign_only());
        run.incomplete = true;
        limit_to_scope(&mut run, &scope_of(&["a"]));
        assert!(run.findings.is_empty() && !run.incomplete);
        // Находки о неполноте не было вовсе: флаг не наш, остаётся как есть.
        let mut run = run_of(&["a", "b"], 2, foreign_only());
        run.findings.clear();
        run.incomplete = true;
        limit_to_scope(&mut run, &scope_of(&["a"]));
        assert!(run.incomplete);
        // Область «все проекты» проверку не меняет.
        let mut run = run_of(&["a", "b"], 2, foreign_only());
        run.incomplete = true;
        limit_to_scope(&mut run, &Scope::default());
        assert!(run.incomplete && run.findings.len() == 1);
    }

    /// Чужие пробелы идут первыми: 30 чужих, затем 25 пробелов проекта `a` и пробел без проекта.
    fn many_gaps() -> Vec<String> {
        let mut gaps: Vec<String> = (0..30)
            .map(|i| format!("b/part{i:02}: FORBIDDEN"))
            .collect();
        gaps.extend((0..25).map(|i| format!("a/part{i:02}: FORBIDDEN")));
        gaps.push("nodes: FORBIDDEN".into());
        gaps
    }

    fn many_gaps_run() -> HealthRun {
        let gaps = many_gaps();
        let f = partial(
            json!({"projectLimit": false, "projectsChecked": 2, "projectsTotal": 2,
                   "podLimit": false, "timedOut": false,
                   "gaps": gaps, "gapCount": gaps.len()}),
            "text of the app",
        );
        run_of(&["a", "b"], 2, f)
    }

    #[test]
    fn gaps_are_narrowed_on_the_full_list_and_then_cut_for_the_model() {
        let mut run = many_gaps_run();
        limit_to_scope(&mut run, &scope_of(&["a"]));
        let f = &run.findings[0];
        // В области 26 пробелов (25 проекта и один без проекта); модель видит первые 20 из них.
        assert_eq!(f.params["gapCount"], 26);
        let listed: Vec<_> = f.params["gaps"]
            .as_array()
            .unwrap()
            .iter()
            .map(|g| g.as_str().unwrap().to_string())
            .collect();
        let expected: Vec<_> = (0..20)
            .map(|i| format!("a/part{i:02}: FORBIDDEN"))
            .collect();
        assert_eq!(listed, expected);
        assert!(
            f.details.contains("a/part19: FORBIDDEN, …. Findings"),
            "{}",
            f.details
        );
        assert!(!f.details.contains("a/part20"), "{}", f.details);
        let text = serde_json::to_string(&run.findings).unwrap();
        assert!(
            !text.contains("b/part") && !text.contains("\"b\""),
            "{text}"
        );
    }

    #[test]
    fn foreign_gaps_filling_the_first_twenty_do_not_hide_the_ones_in_the_scope() {
        // Область из одного пробела после 30 чужих: прежде список из 20 был бы целиком чужим.
        let mut gaps: Vec<String> = (0..30)
            .map(|i| format!("b/part{i:02}: FORBIDDEN"))
            .collect();
        gaps.push("a/secrets: FORBIDDEN".into());
        let f = partial(
            json!({"projectLimit": false, "projectsChecked": 2, "projectsTotal": 2,
                   "podLimit": false, "timedOut": false,
                   "gaps": gaps, "gapCount": 31}),
            "text of the app",
        );
        let mut run = run_of(&["a", "b"], 2, f);
        limit_to_scope(&mut run, &scope_of(&["a"]));
        let f = &run.findings[0];
        assert_eq!(f.params["gapCount"], 1);
        assert_eq!(f.params["gaps"], json!(["a/secrets: FORBIDDEN"]));
        assert_eq!(
            f.details,
            "Some objects were not checked: not read: a/secrets: FORBIDDEN. Findings may be missing for them."
        );
    }

    #[test]
    fn the_whole_scope_gets_the_first_twenty_gaps_and_the_full_count() {
        let mut run = many_gaps_run();
        limit_to_scope(&mut run, &Scope::default());
        let f = &run.findings[0];
        assert_eq!(f.params["gapCount"], 56);
        let gaps = f.params["gaps"].as_array().unwrap();
        assert_eq!(gaps.len(), 20);
        assert_eq!(gaps[0], "b/part00: FORBIDDEN");
        assert!(
            f.details.contains("b/part19: FORBIDDEN, …. Findings"),
            "{}",
            f.details
        );
    }

    #[test]
    fn strings_are_masked_at_any_depth_and_keys_stay() {
        let mut v = json!({
            "a": "Authorization: Bearer abc.def-123",
            "b": ["https://ci:hunter2@host/x", {"c": ["Bearer xyz"]}],
            "n": 1, "t": true, "z": null,
        });
        mask_json(&text_masker(), &mut v);
        assert_eq!(
            v,
            json!({
                "a": "Authorization: Bearer ***",
                "b": ["https://ci:***@host/x", {"c": ["Bearer ***"]}],
                "n": 1, "t": true, "z": null,
            })
        );
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
