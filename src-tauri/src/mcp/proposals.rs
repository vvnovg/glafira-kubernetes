//! Предложения LLM в приложении (спец. §10.5): проверка при создании против кластера — только
//! чтением (`change::facts`: workload, HPA и контейнеры есть, quantity разбираются, `limits ≥
//! requests`; `scale` workload'а под HPA — `invalid_argument` с советом `setHpaBounds`;
//! нарушение `LimitRange` — `invalid_argument`, а квота и прочее — в `warnings`), запись в файл
//! Glafira. Server-side dry-run при создании не выполняется: у шлюза нет запросов записи даже
//! с `dryRun`.

use glafira_mcp_core::BackendError;
use glafira_mcp_kubernetes::health as mh;
use glafira_mcp_kubernetes::proposals as mp;

use super::backend::{AppBackend, health_to_backend, to_backend};
use super::map::reshape;
use crate::change::facts::{Warning, check, read_facts};
use crate::change::{Change, validate_form};
use crate::commands::blocking;
use crate::health::proposals::{Proposal, ProposalStatus};
use crate::health::service::now_ms;
use crate::health::store::{NewProposal, Proposed, Withdrawn};

/// Ошибка проверки изменения — `invalid_argument` с английским текстом (ключи `errors.change.*`
/// и `errors.k8s.badName` несут только то, что прислала модель); объекта нет — `not_found`.
fn invalid(e: crate::error::AppError) -> BackendError {
    match e.code {
        crate::error::ErrorCode::InvalidInput => BackendError::invalid_argument(e.message),
        _ => to_backend(e),
    }
}

fn view(p: Proposal) -> Option<mp::ProposalView> {
    Some(mp::ProposalView {
        id: p.id,
        severity: reshape(&p.severity)?,
        title: p.title,
        rationale: p.rationale,
        target: reshape(&p.target)?,
        // Удаление пода модель предложить не может: такой записи в её ответе нет.
        change: reshape(&p.change)?,
        related_rule_id: p.related_rule_id,
        related_finding_key: p.related_finding_key,
        client: p.client,
        status: p.status.as_str().to_string(),
        created_at_ms: p.created_at_ms,
        updated_at_ms: p.updated_at_ms,
        closed_at_ms: p.closed_at_ms,
        applied_as_proposed: p.applied_as_proposed,
        applied_change: p.applied_change.as_ref().and_then(reshape),
        reject_reason: p.reject_reason,
    })
}

impl AppBackend {
    pub(super) async fn proposals_impl(
        &self,
        op: mp::ProposalOp,
    ) -> Result<mp::ProposalOutput, BackendError> {
        // Поколение — до проверки доступа: запись в подключение, удалённое за это время, отклоняется.
        let generation = self.state.health.generation(op.connection_id());
        let profile = self.allowed(op.connection_id())?;
        let st = self.state.clone();
        match op {
            mp::ProposalOp::Propose {
                connection_id,
                session,
                proposal,
            } => {
                super::backend::check_note_texts(
                    proposal.related_rule_id.as_deref(),
                    &proposal.title,
                )?;
                let change: Change = reshape(&proposal.change)
                    .ok_or_else(|| BackendError::invalid_argument("change cannot be read"))?;
                let change = change.trimmed();
                Self::in_scope(&profile, change.namespace())?;
                validate_form(&change).map_err(invalid)?;
                let ns = change.namespace().to_string();
                let c2 = change.clone();
                let facts = Box::pin(st.read_in(&connection_id, &ns, |c| async move {
                    read_facts(&c.reader(), &c2).await
                }))
                .await
                .map_err(to_backend)?;
                let warnings = check(&change, &facts).map_err(invalid)?;
                if let (Change::Scale { .. }, Some(Warning::HpaManaged { hpa })) = (
                    &change,
                    warnings
                        .iter()
                        .find(|w| matches!(w, Warning::HpaManaged { .. })),
                ) {
                    return Err(BackendError::invalid_argument(format!(
                        "HPA {hpa} scales this workload and would overwrite the replicas: propose setHpaBounds instead"
                    )));
                }
                let new = NewProposal {
                    severity: reshape(&proposal.severity).ok_or_else(BackendError::unexpected)?,
                    title: proposal.title,
                    rationale: proposal.rationale,
                    change,
                    related_rule_id: proposal.related_rule_id,
                    related_finding_key: proposal.related_finding_key,
                    author: self.author(&session),
                };
                let id = connection_id.clone();
                let st2 = st.clone();
                let r = blocking(move || {
                    st2.health
                        .write(&id, generation, |s| s.propose(&id, new, now_ms()))
                })
                .await
                .map_err(health_to_backend)?;
                let (proposal_id, created) = match r {
                    Proposed::Created(id) => (id, true),
                    Proposed::Updated(id) => (id, false),
                    Proposed::LimitReached => {
                        return Err(BackendError::new(
                            "limit_reached",
                            "This connection already has 50 open proposals; ask the user to apply or reject some in Glafira",
                        ));
                    }
                };
                st.health.changed(&connection_id);
                Ok(mp::ProposalOutput::Proposed {
                    proposal_id,
                    created,
                    read_only: profile.read_only,
                    warnings: warnings.iter().map(super::resources::warning).collect(),
                })
            }
            mp::ProposalOp::List {
                connection_id,
                status,
            } => {
                let f = blocking(move || st.health.store.load(&connection_id, now_ms()))
                    .await
                    .map_err(health_to_backend)?;
                Ok(mp::ProposalOutput::Proposals(
                    f.proposals
                        .into_iter()
                        .filter(|p| match status {
                            mh::StatusFilter::Open => p.status == ProposalStatus::Open,
                            mh::StatusFilter::Closed => p.status != ProposalStatus::Open,
                            mh::StatusFilter::All => true,
                        })
                        .filter_map(view)
                        .collect(),
                ))
            }
            mp::ProposalOp::Withdraw {
                connection_id,
                session,
                proposal_id,
            } => {
                let author = self.author(&session);
                let id = connection_id.clone();
                let st2 = st.clone();
                let r = blocking(move || {
                    st2.health.write(&id, generation, |s| {
                        s.withdraw_own(&id, &proposal_id, &author, now_ms())
                    })
                })
                .await
                .map_err(health_to_backend)?;
                match r {
                    Withdrawn::Withdrawn => {
                        st.health.changed(&connection_id);
                        Ok(mp::ProposalOutput::Withdrawn)
                    }
                    Withdrawn::NotFound => Err(BackendError::not_found(
                        "There is no such proposal: it was deleted or has expired",
                    )),
                    Withdrawn::NotOwner => Err(BackendError::new(
                        "not_allowed",
                        "This proposal belongs to another client",
                    )),
                    Withdrawn::Closed(s) => Err(BackendError::invalid_argument(format!(
                        "The proposal is already {}",
                        s.as_str()
                    ))),
                }
            }
        }
    }
}
