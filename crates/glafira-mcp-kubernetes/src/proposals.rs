//! Предложения LLM (спец. §10.5, как в Kafka G3): виды изменения (их схема уходит LLM),
//! операции [`ProposalOp`] и ответы. Ни одна операция не меняет кластер: предложение пишется в
//! Glafira, применяет его человек заполненной формой приложения.

use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize};

use glafira_mcp_core::SessionView;

use crate::health::{Severity, StatusFilter, Target};
use crate::model::WorkloadRef;

/// Длина `relatedFindingKey`.
pub const MAX_RELATED_KEY_CHARS: usize = 200;

/// Нет поля — `None`, `null` — `Some(None)`, строка — `Some(Some(…))`.
fn present<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Option<String>>, D::Error> {
    Option::<String>::deserialize(d).map(Some)
}

/// CPU and memory of requests or limits: quantity strings like "250m" or "512Mi"; null removes the value; omit a field to keep the current value.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ResourceValuesSpec {
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub cpu: Option<Option<String>>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub memory: Option<Option<String>>,
}

/// New requests and limits of one container of the pod template.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ContainerResourcesSpec {
    /// Container name from describe_workload.
    pub name: String,
    #[serde(default)]
    pub requests: ResourceValuesSpec,
    #[serde(default)]
    pub limits: ResourceValuesSpec,
}

/// The change an LLM proposes (`kind`): the same JSON shape Glafira stores and shows in its forms.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum ChangeSpec {
    /// Set requests and limits of containers in the pod template (not of a Job); limits must not be below requests. The user reviews the change and a server-side dry-run before applying.
    SetResources {
        namespace: String,
        workload: WorkloadRef,
        /// 1 to 50 containers.
        containers: Vec<ContainerResourcesSpec>,
    },
    /// Set the number of replicas of a Deployment, StatefulSet or DeploymentConfig, 0..=1000. Not for a workload scaled by an HPA: propose setHpaBounds instead.
    Scale {
        namespace: String,
        workload: WorkloadRef,
        replicas: i32,
    },
    /// Restart the pods of a workload with a rolling update (not a Job or CronJob).
    RolloutRestart {
        namespace: String,
        workload: WorkloadRef,
    },
    /// Set minReplicas (optional) and maxReplicas of a HorizontalPodAutoscaler: 1 <= min <= max <= 1000.
    #[serde(rename_all = "camelCase")]
    SetHpaBounds {
        namespace: String,
        hpa: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        min_replicas: Option<i32>,
        max_replicas: i32,
    },
}

impl ChangeSpec {
    /// Имя вида — как `kind` в JSON (журнал).
    pub fn kind_name(&self) -> &'static str {
        match self {
            ChangeSpec::SetResources { .. } => "setResources",
            ChangeSpec::Scale { .. } => "scale",
            ChangeSpec::RolloutRestart { .. } => "rolloutRestart",
            ChangeSpec::SetHpaBounds { .. } => "setHpaBounds",
        }
    }

    pub fn namespace(&self) -> &str {
        match self {
            ChangeSpec::SetResources { namespace, .. }
            | ChangeSpec::Scale { namespace, .. }
            | ChangeSpec::RolloutRestart { namespace, .. }
            | ChangeSpec::SetHpaBounds { namespace, .. } => namespace,
        }
    }

    /// Имя объекта: workload или HPA (журнал).
    pub fn target_name(&self) -> &str {
        match self {
            ChangeSpec::SetResources { workload, .. }
            | ChangeSpec::Scale { workload, .. }
            | ChangeSpec::RolloutRestart { workload, .. } => &workload.name,
            ChangeSpec::SetHpaBounds { hpa, .. } => hpa,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewProposal {
    pub severity: Severity,
    pub title: String,
    pub rationale: String,
    pub change: ChangeSpec,
    pub related_rule_id: Option<String>,
    pub related_finding_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProposalOp {
    Propose {
        connection_id: String,
        session: SessionView,
        proposal: NewProposal,
    },
    List {
        connection_id: String,
        status: StatusFilter,
    },
    Withdraw {
        connection_id: String,
        session: SessionView,
        proposal_id: String,
    },
}

impl ProposalOp {
    pub fn connection_id(&self) -> &str {
        match self {
            ProposalOp::Propose { connection_id, .. }
            | ProposalOp::List { connection_id, .. }
            | ProposalOp::Withdraw { connection_id, .. } => connection_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProposalView {
    pub id: String,
    pub severity: Severity,
    pub title: String,
    pub rationale: String,
    pub target: Target,
    pub change: ChangeSpec,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub related_rule_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub related_finding_key: Option<String>,
    /// Клиент, оставивший предложение; `None` — безымянный.
    pub client: Option<String>,
    /// open, applied (человек применил), rejected (человек отклонил), withdrawn (LLM отозвала).
    pub status: String,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub closed_at_ms: Option<i64>,
    /// Только у applied: применено без правок (`true`) или с правками.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub applied_as_proposed: Option<bool>,
    /// Что применено фактически, если оно отличается от предложенного.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub applied_change: Option<ChangeSpec>,
    /// Только у rejected: комментарий человека, если он его оставил.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reject_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ProposalOutput {
    Proposed {
        proposal_id: String,
        created: bool,
        /// Подключение только для чтения: применить сможет человек, сняв флажок.
        read_only: bool,
        /// Предупреждения словами (квота, HPA, масштаб в 0 и прочее): предложение создано.
        /// Нарушение `LimitRange` — не предупреждение, а `invalid_argument`.
        warnings: Vec<String>,
    },
    Proposals(Vec<ProposalView>),
    Withdrawn,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn change_json_matches_the_stored_form() {
        let c: ChangeSpec = serde_json::from_value(json!({
            "kind": "setResources", "namespace": "team-a",
            "workload": {"kind": "Deployment", "name": "web"},
            "containers": [{"name": "app", "requests": {"cpu": "250m", "memory": null}}]
        }))
        .unwrap();
        assert_eq!(
            (c.kind_name(), c.target_name(), c.namespace()),
            ("setResources", "web", "team-a")
        );
        assert_eq!(
            serde_json::to_value(&c).unwrap()["containers"][0],
            json!({"name": "app", "requests": {"cpu": "250m", "memory": null}, "limits": {}})
        );
        let h: ChangeSpec = serde_json::from_value(json!({
            "kind": "setHpaBounds", "namespace": "team-a", "hpa": "web", "maxReplicas": 5
        }))
        .unwrap();
        assert_eq!(h.target_name(), "web");
        assert!(
            serde_json::from_value::<ChangeSpec>(
                json!({"kind": "deletePod", "namespace": "a", "pod": "x"})
            )
            .is_err(),
            "удаление пода предложить нельзя"
        );
    }

    #[test]
    fn schema_uses_camel_case_field_names() {
        let schema = serde_json::to_value(schemars::schema_for!(ChangeSpec)).unwrap();
        let text = schema.to_string();
        for name in [
            "minReplicas",
            "maxReplicas",
            "setHpaBounds",
            "rolloutRestart",
        ] {
            assert!(text.contains(name), "{name}: {text}");
        }
        assert!(!text.contains("max_replicas"), "{text}");
    }
}
