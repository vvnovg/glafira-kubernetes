//! Инструменты предложений (спец. §10.4, §10.5, как в Kafka G3). Кластер они не меняют:
//! предложение пишется в Glafira и показывается человеку рядом с объектом; применяет его человек
//! формой. Предложения вне области LLM (проекты не из `llmNamespaces`) в ответ не попадают.

use glafira_mcp_core::fit;
use glafira_mcp_core::{BackendError, to_value};
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::CallToolResult;
use rmcp::service::RequestContext;
use rmcp::{ErrorData, RoleServer, tool, tool_router};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::KubeServer;
use crate::health::{Severity, StatusFilter, in_scope};
use crate::proposals::*;

/// У `list_proposals` нет страниц: при обрезке остаются самые свежие, остальное — сужением по status.
const PROPOSALS_TRUNCATED_HINT: &str = "The response was truncated to the size limit: only the most recently updated proposals are shown; narrow with status";

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProposeChangeParams {
    /// Connection id from list_connections.
    pub connection_id: String,
    /// info, warning or critical: how urgent the change is.
    pub severity: Severity,
    /// Short summary for the user, up to 120 characters; plain text.
    pub title: String,
    /// Why this change and what it does, up to 2000 characters; plain text, no markup or links.
    pub rationale: String,
    /// The change itself; kind selects the form the user will see.
    pub change: ChangeSpec,
    /// ruleId of the health_check finding this change fixes, if any.
    #[serde(default)]
    pub related_rule_id: Option<String>,
    /// key of the health_check finding this change fixes, if any.
    #[serde(default)]
    pub related_finding_key: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ListProposalsParams {
    /// Connection id from list_connections.
    pub connection_id: String,
    /// open (default), closed (applied, rejected or withdrawn) or all.
    #[serde(default)]
    pub status: Option<StatusFilter>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WithdrawProposalParams {
    /// Connection id from list_connections.
    pub connection_id: String,
    /// Proposal id from propose_change or list_proposals.
    pub proposal_id: String,
}

#[derive(Serialize)]
struct ProposalsBody {
    items: Vec<ProposalView>,
    truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    hint: Option<&'static str>,
}

fn proposals_of(b: &mut ProposalsBody) -> &mut Vec<ProposalView> {
    &mut b.items
}

/// Сначала недавно обновлённые, при равенстве — созданные позже, затем по id: обрезка до 100 КБ
/// оставляет голову.
fn newest_first(items: &mut [ProposalView]) {
    items.sort_by(|a, b| {
        b.updated_at_ms
            .cmp(&a.updated_at_ms)
            .then(b.created_at_ms.cmp(&a.created_at_ms))
            .then_with(|| a.id.cmp(&b.id))
    });
}

impl KubeServer {
    /// Операция предложений. `cluster` — она читает кластер (проверка предложения): тогда
    /// занимает слоты параллельности, как чтение.
    async fn proposals(
        &self,
        op: ProposalOp,
        cluster: bool,
    ) -> Result<ProposalOutput, BackendError> {
        let _slots = match cluster {
            true => Some(self.session.slots().await?),
            false => None,
        };
        self.backend.proposals(op).await
    }
}

#[tool_router(router = proposal_router, vis = "pub(crate)")]
impl KubeServer {
    #[tool(
        description = "Propose a change for the user to apply in Glafira. Does not change the cluster: the proposal appears next to the object with your title and rationale, and the user opens it as a pre-filled form, may edit it, reviews a server-side dry-run and confirms (or rejects it). Kinds: setResources (requests and limits of containers), scale (replicas), rolloutRestart, setHpaBounds. Deleting objects or other edits cannot be proposed — describe them in the chat instead. One problem — one proposal; a proposal with the same kind and object as your open one replaces it. The proposal is checked against the cluster (unknown workload, container or HPA, limits below requests, scaling a workload managed by an HPA); a value outside a LimitRange makes it invalid; quota and other warnings come back in warnings and do not block it. Then check the user's decision with list_proposals. readOnly: true in the response means the connection is read-only in Glafira and the user must lift that first. A proposal expires after 7 days; at most 50 open per connection.",
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn propose_change(
        &self,
        Parameters(p): Parameters<ProposeChangeParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        // В журнал — вид и объект, без заголовка, обоснования и значений изменения.
        let args = json!({
            "connectionId": p.connection_id,
            "kind": p.change.kind_name(),
            "namespace": p.change.namespace(),
            "target": p.change.target_name(),
            "severity": p.severity,
            "relatedRuleId": p.related_rule_id,
        });
        Ok(self
            .session
            .call(
                &ctx,
                "propose_change",
                args,
                Some(&p.connection_id),
                async {
                    if p.title.trim().is_empty() {
                        return Err(BackendError::invalid_argument("title must not be empty"));
                    }
                    if p.rationale.trim().is_empty() {
                        return Err(BackendError::invalid_argument(
                            "rationale must not be empty: explain the change to the user",
                        ));
                    }
                    if p.related_finding_key
                        .as_deref()
                        .is_some_and(|k| k.chars().count() > MAX_RELATED_KEY_CHARS)
                    {
                        return Err(BackendError::invalid_argument(format!(
                            "relatedFindingKey is longer than {MAX_RELATED_KEY_CHARS} characters"
                        )));
                    }
                    self.scope(&p.connection_id, Some(p.change.namespace().trim()))?;
                    let op = ProposalOp::Propose {
                        connection_id: p.connection_id.clone(),
                        session: self.session.identify(None),
                        proposal: NewProposal {
                            severity: p.severity,
                            title: p.title.clone(),
                            rationale: p.rationale.clone(),
                            change: p.change.clone(),
                            related_rule_id: p.related_rule_id.clone(),
                            related_finding_key: p.related_finding_key.clone(),
                        },
                    };
                    match self.proposals(op, true).await? {
                        ProposalOutput::Proposed {
                            proposal_id,
                            created,
                            read_only,
                            warnings,
                        } => Ok(json!({
                            "proposalId": proposal_id,
                            "created": created,
                            "readOnly": read_only,
                            "warnings": warnings,
                        })),
                        _ => Err(BackendError::unexpected()),
                    }
                },
            )
            .await)
    }

    #[tool(
        description = "List the change proposals left in Glafira for a connection by any client, with the user's decision: open, applied (appliedAsProposed tells whether the user applied it unchanged), rejected (with rejectReason when the user gave one) or withdrawn. When the user applied it with edits, appliedChange holds what was actually applied, in the same shape as change. Check it after proposing instead of asking the user in the chat.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            open_world_hint = false
        )
    )]
    async fn list_proposals(
        &self,
        Parameters(p): Parameters<ListProposalsParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let args = serde_json::to_value(&p).unwrap_or(Value::Null);
        Ok(self
            .session
            .call(
                &ctx,
                "list_proposals",
                args,
                Some(&p.connection_id),
                async {
                    let scope = self.scope(&p.connection_id, None)?;
                    let op = ProposalOp::List {
                        connection_id: p.connection_id.clone(),
                        status: p.status.unwrap_or_default(),
                    };
                    match self.proposals(op, false).await? {
                        ProposalOutput::Proposals(mut items) => {
                            items.retain(|x| in_scope(&x.target, &scope));
                            newest_first(&mut items);
                            let mut body = ProposalsBody {
                                items,
                                truncated: false,
                                hint: None,
                            };
                            if fit::fit(&mut body, proposals_of) {
                                body.truncated = true;
                                body.hint = Some(PROPOSALS_TRUNCATED_HINT);
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
        description = "Withdraw one of your own open proposals from Glafira. A proposal of another client, or one the user already applied or rejected, cannot be withdrawn.",
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn withdraw_proposal(
        &self,
        Parameters(p): Parameters<WithdrawProposalParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let args = serde_json::to_value(&p).unwrap_or(Value::Null);
        Ok(self
            .session
            .call(
                &ctx,
                "withdraw_proposal",
                args,
                Some(&p.connection_id),
                async {
                    self.scope(&p.connection_id, None)?;
                    let op = ProposalOp::Withdraw {
                        connection_id: p.connection_id.clone(),
                        session: self.session.identify(None),
                        proposal_id: p.proposal_id.clone(),
                    };
                    match self.proposals(op, false).await? {
                        ProposalOutput::Withdrawn => Ok(json!({ "withdrawn": true })),
                        _ => Err(BackendError::unexpected()),
                    }
                },
            )
            .await)
    }
}
