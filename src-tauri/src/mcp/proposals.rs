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
use crate::health::proposals::{MAX_RELATED_KEY_CHARS, Proposal, ProposalStatus};
use crate::health::service::now_ms;
use crate::health::store::{MAX_BODY_CHARS, NewProposal, Proposed, Withdrawn, sanitize};

/// Ошибка проверки изменения — `invalid_argument` с английским текстом; объекта нет —
/// `not_found`. Текст проходит тот же фильтр, что у остальных ошибок (`llm_message`): как есть —
/// только ключи `LLM_TEXT_KEYS` (`errors.change.*` и `errors.k8s.badName` несут то, что прислала
/// модель) не длиннее `MAX_LLM_ERROR_CHARS`, любой другой ключ — фиксированная фраза.
fn invalid(e: crate::error::AppError) -> BackendError {
    match e.code {
        crate::error::ErrorCode::InvalidInput => {
            BackendError::invalid_argument(super::map::llm_message(&e))
        }
        _ => to_backend(e),
    }
}

/// Тексты предложения сверх общих (`check_note_texts`): обоснование после очистки не пусто (из
/// одних управляющих символов — как пустое), ключ находки не длиннее `MAX_RELATED_KEY_CHARS`
/// приложения — сам файл Glafira не полагается на проверку крейта.
fn check_proposal_texts(
    rationale: &str,
    related_finding_key: Option<&str>,
) -> Result<(), BackendError> {
    if sanitize(rationale, MAX_BODY_CHARS).is_empty() {
        return Err(BackendError::invalid_argument(
            "rationale must not be empty: explain the change to the user",
        ));
    }
    if related_finding_key.is_some_and(|k| k.chars().count() > MAX_RELATED_KEY_CHARS) {
        return Err(BackendError::invalid_argument(format!(
            "relatedFindingKey is longer than {MAX_RELATED_KEY_CHARS} characters"
        )));
    }
    Ok(())
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
                check_proposal_texts(&proposal.rationale, proposal.related_finding_key.as_deref())?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{AppError, ErrorCode};

    #[test]
    fn rationale_of_control_characters_and_a_long_finding_key_are_refused() {
        assert!(check_proposal_texts("Two replicas are safer", Some("k")).is_ok());
        let e = check_proposal_texts("\u{7}\u{1b}\t\r", None).unwrap_err();
        assert_eq!(e.code, "invalid_argument");
        assert!(e.message.contains("rationale"), "{}", e.message);
        let key = "k".repeat(MAX_RELATED_KEY_CHARS);
        assert!(check_proposal_texts("r", Some(&key)).is_ok());
        let e = check_proposal_texts("r", Some(&format!("{key}k"))).unwrap_err();
        assert_eq!(e.code, "invalid_argument");
        assert!(e.message.contains("relatedFindingKey"), "{}", e.message);
    }

    /// Ошибка проверки с ключом не из `LLM_TEXT_KEYS` (в ней могли бы оказаться данные профиля)
    /// уходит модели фиксированной фразой, длинный текст разрешённого ключа — урезанным.
    #[test]
    fn invalid_input_reaches_the_model_through_the_llm_filter() {
        let hidden = AppError::localized(
            ErrorCode::InvalidInput,
            crate::i18n::LocalizedText::raw("https://user:secret@api.corp.example:6443"),
        );
        let e = invalid(hidden);
        assert_eq!(e.code, "invalid_argument");
        assert!(!e.message.contains("secret"), "{}", e.message);
        assert_eq!(
            e.message,
            "The operation failed; the user can see the details in Glafira"
        );
        let long = AppError::localized(
            ErrorCode::InvalidInput,
            crate::ltext!(
                "errors.change.noContainer",
                "There is no container {{container}} in the pod template",
                container = "x".repeat(2000)
            ),
        );
        let e = invalid(long);
        assert_eq!(e.code, "invalid_argument");
        assert!(
            e.message.starts_with("There is no container xxx"),
            "{}",
            e.message
        );
        assert_eq!(
            e.message.chars().count(),
            crate::mcp::map::MAX_LLM_ERROR_CHARS
        );
    }
}
