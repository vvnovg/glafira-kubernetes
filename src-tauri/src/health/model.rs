//! Находки проверки: строгость, объект, ключ (спец. §8.1).

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::k8s::workloads::WorkloadKind;
use crate::storage::profile::Environment;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Info,
    Warning,
    Critical,
}

/// Вид объекта находки или замечания (`target.kind`, спец. §8.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TargetKind {
    Cluster,
    Node,
    Project,
    Workload,
    Pod,
    Service,
    Route,
    Ingress,
    Httproute,
    Grpcroute,
    Gateway,
    Pvc,
    Hpa,
    Quota,
}

impl TargetKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Cluster => "cluster",
            Self::Node => "node",
            Self::Project => "project",
            Self::Workload => "workload",
            Self::Pod => "pod",
            Self::Service => "service",
            Self::Route => "route",
            Self::Ingress => "ingress",
            Self::Httproute => "httproute",
            Self::Grpcroute => "grpcroute",
            Self::Gateway => "gateway",
            Self::Pvc => "pvc",
            Self::Hpa => "hpa",
            Self::Quota => "quota",
        }
    }
}

/// Объект находки или замечания: у кластера ни проекта, ни имени; у ноды — имя; у проекта —
/// namespace; у остальных — namespace и имя, у workload'а ещё и его вид.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Target {
    pub kind: TargetKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workload_kind: Option<WorkloadKind>,
}

impl Target {
    pub fn cluster() -> Self {
        Self {
            kind: TargetKind::Cluster,
            namespace: None,
            name: None,
            workload_kind: None,
        }
    }

    pub fn node(name: &str) -> Self {
        Self {
            kind: TargetKind::Node,
            namespace: None,
            name: Some(name.to_string()),
            workload_kind: None,
        }
    }

    pub fn project(namespace: &str) -> Self {
        Self {
            kind: TargetKind::Project,
            namespace: Some(namespace.to_string()),
            name: None,
            workload_kind: None,
        }
    }

    /// Объект в проекте (кроме workload'а).
    pub fn object(kind: TargetKind, namespace: &str, name: &str) -> Self {
        Self {
            kind,
            namespace: Some(namespace.to_string()),
            name: Some(name.to_string()),
            workload_kind: None,
        }
    }

    pub fn workload(namespace: &str, kind: WorkloadKind, name: &str) -> Self {
        Self {
            kind: TargetKind::Workload,
            namespace: Some(namespace.to_string()),
            name: Some(name.to_string()),
            workload_kind: Some(kind),
        }
    }
}

/// Строгость правила по окружению подключения: prod, stage и остальные (test, dev, local).
/// `None` — в этом окружении правило не срабатывает.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Levels {
    pub prod: Option<Severity>,
    pub stage: Option<Severity>,
    pub other: Option<Severity>,
}

impl Levels {
    pub const fn new(
        prod: Option<Severity>,
        stage: Option<Severity>,
        other: Option<Severity>,
    ) -> Self {
        Self { prod, stage, other }
    }

    pub fn for_env(&self, env: Environment) -> Option<Severity> {
        match env {
            Environment::Prod => self.prod,
            Environment::Stage => self.stage,
            Environment::Test | Environment::Dev | Environment::Local => self.other,
        }
    }
}

/// Находка правила. `key` — правило, объект и уточнение: по нему работают скрытие и значки.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Finding {
    pub key: String,
    pub rule_id: String,
    pub severity: Severity,
    pub target: Target,
    #[serde(default)]
    pub params: Map<String, Value>,
}

/// `pod.crashLoop:workload:team-a:Deployment/web`, у ноды — `node.notReady:node::w1`.
pub fn finding_key(rule_id: &str, target: &Target, qualifier: Option<&str>) -> String {
    let name = target.name.as_deref().unwrap_or("");
    let object = match target.workload_kind {
        Some(k) => format!("{k:?}/{name}"),
        None => name.to_string(),
    };
    let mut key = format!(
        "{rule_id}:{}:{}:{object}",
        target.kind.as_str(),
        target.namespace.as_deref().unwrap_or("")
    );
    if let Some(q) = qualifier {
        key.push(':');
        key.push_str(q);
    }
    key
}

impl Finding {
    pub fn new(
        rule_id: &str,
        severity: Severity,
        target: Target,
        qualifier: Option<&str>,
        params: Value,
    ) -> Self {
        Self {
            key: finding_key(rule_id, &target, qualifier),
            rule_id: rule_id.to_string(),
            severity,
            target,
            params: match params {
                Value::Object(m) => m,
                _ => Map::new(),
            },
        }
    }

    pub fn param_i64(&self, name: &str) -> Option<i64> {
        self.params.get(name).and_then(Value::as_i64)
    }

    pub fn param_str(&self, name: &str) -> Option<&str> {
        self.params.get(name).and_then(Value::as_str)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_has_rule_target_and_qualifier() {
        assert_eq!(
            finding_key(
                "pod.crashLoop",
                &Target::workload("team-a", WorkloadKind::Deployment, "web"),
                None
            ),
            "pod.crashLoop:workload:team-a:Deployment/web"
        );
        assert_eq!(
            finding_key("node.notReady", &Target::node("w1"), None),
            "node.notReady:node::w1"
        );
        assert_eq!(
            finding_key("check.partial", &Target::cluster(), Some("x")),
            "check.partial:cluster:::x"
        );
    }

    #[test]
    fn target_serializes_only_its_fields() {
        assert_eq!(
            serde_json::to_value(Target::cluster()).unwrap(),
            serde_json::json!({ "kind": "cluster" })
        );
        assert_eq!(
            serde_json::to_value(Target::workload("a", WorkloadKind::StatefulSet, "db")).unwrap(),
            serde_json::json!({ "kind": "workload", "namespace": "a", "name": "db",
                "workloadKind": "StatefulSet" })
        );
        assert_eq!(
            serde_json::to_value(Target::project("a")).unwrap(),
            serde_json::json!({ "kind": "project", "namespace": "a" })
        );
    }

    #[test]
    fn levels_map_environments() {
        let l = Levels::new(Some(Severity::Critical), Some(Severity::Warning), None);
        assert_eq!(l.for_env(Environment::Prod), Some(Severity::Critical));
        assert_eq!(l.for_env(Environment::Stage), Some(Severity::Warning));
        for env in [Environment::Test, Environment::Dev, Environment::Local] {
            assert_eq!(l.for_env(env), None);
        }
    }
}
