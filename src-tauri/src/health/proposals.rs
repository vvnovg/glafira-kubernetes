//! Предложения LLM (спец. §10.5, как в Kafka G3): запись в `health/<connectionId>.json`, статусы
//! и сроки. Вид изменения — `change::Change`; предложение кластер не меняет: применяет его
//! человек формой изменения с обычными стражами, а «применено» ставит Rust после успеха.

use serde::{Deserialize, Serialize};

use super::model::{Severity, Target};
use crate::change::Change;

/// Срок предложения — 7 дней с создания, как у замечаний.
pub const PROPOSAL_TTL_MS: i64 = 7 * 24 * 60 * 60 * 1000;
/// Открытых предложений на подключение.
pub const MAX_OPEN_PROPOSALS: usize = 50;
pub const MAX_REJECT_REASON_CHARS: usize = 300;
pub const MAX_RELATED_KEY_CHARS: usize = 200;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProposalStatus {
    Open,
    /// Человек применил формой (ставит только Rust после успеха операции).
    Applied,
    /// Человек отклонил; «Открыть снова» возвращает в `Open`.
    Rejected,
    /// LLM отозвала своё.
    Withdrawn,
}

impl ProposalStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            ProposalStatus::Open => "open",
            ProposalStatus::Applied => "applied",
            ProposalStatus::Rejected => "rejected",
            ProposalStatus::Withdrawn => "withdrawn",
        }
    }
}

/// Предложение в файле `health/<connectionId>.json`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Proposal {
    pub id: String,
    pub severity: Severity,
    pub title: String,
    pub rationale: String,
    pub target: Target,
    pub change: Change,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub related_rule_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub related_finding_key: Option<String>,
    /// Имя клиента из диалога подтверждения; `None` — безымянный.
    #[serde(default)]
    pub client: Option<String>,
    /// Сессия безымянного клиента: отозвать можно только из неё.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub status: ProposalStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub closed_at_ms: Option<i64>,
    /// Только у `applied`: человек применил без правок (`true`) или с правками.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub applied_as_proposed: Option<bool>,
    /// Только у `applied` с правками: что применено фактически.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub applied_change: Option<Change>,
    /// Только у `rejected`: необязательный комментарий человека.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reject_reason: Option<String>,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn json_shape_of_a_stored_proposal() {
        let p: Proposal = serde_json::from_value(json!({
            "id": "p1", "severity": "warning", "title": "t", "rationale": "r",
            "target": {"kind": "workload", "namespace": "team-a", "name": "web", "workloadKind": "Deployment"},
            "change": {"kind": "scale", "namespace": "team-a",
                "workload": {"kind": "Deployment", "name": "web"}, "replicas": 3},
            "client": "claude-ai", "createdAtMs": 1, "updatedAtMs": 2, "status": "open"
        }))
        .unwrap();
        assert_eq!(p.status, ProposalStatus::Open);
        assert_eq!(p.change.kind_name(), "scale");
        let v = serde_json::to_value(&p).unwrap();
        for absent in [
            "closedAtMs",
            "appliedAsProposed",
            "appliedChange",
            "rejectReason",
            "sessionId",
        ] {
            assert!(v.get(absent).is_none(), "{absent}: {v}");
        }
        assert_eq!(ProposalStatus::Withdrawn.as_str(), "withdrawn");
    }
}
