//! Проверка кластера и замечания для LLM (спец. §8, §10.4): операции [`HealthOp`] и ответы.
//! Ни одна операция не меняет кластер: проверка только читает его, замечания пишутся в Glafira.

use std::collections::BTreeMap;

use glafira_mcp_core::SessionView;
use glafira_mcp_core::fit::{Page, page};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::model::{Scope, WorkloadKind};

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Info,
    Warning,
    Critical,
}

/// Kind of the object of a finding or note (spec §8.1).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
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

/// Object of a finding or note.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Target {
    /// cluster, node, project, workload, pod, service, route, ingress, httproute, grpcroute, gateway, pvc, hpa or quota.
    pub kind: TargetKind,
    /// Project (namespace) of the object; omit for cluster and node, the project itself for project.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    /// Object name; omit for cluster and project.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Workload kind for kind = workload: Deployment, StatefulSet, DaemonSet, DeploymentConfig, Job or CronJob.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workload_kind: Option<WorkloadKind>,
}

fn given(s: &Option<String>) -> bool {
    s.as_deref().is_some_and(|v| !v.trim().is_empty())
}

impl Target {
    /// Какие поля нужны виду: у кластера — никаких, у ноды — имя, у проекта — namespace, у
    /// остальных — namespace и имя, у workload'а — ещё и его вид.
    pub fn check(&self) -> Result<(), String> {
        let (ns, name) = (given(&self.namespace), given(&self.name));
        let ok = match self.kind {
            TargetKind::Cluster => !ns && !name,
            TargetKind::Node => !ns && name,
            TargetKind::Project => ns && !name,
            TargetKind::Workload => ns && name && self.workload_kind.is_some(),
            _ => ns && name,
        };
        if ok && (self.kind == TargetKind::Workload || self.workload_kind.is_none()) {
            return Ok(());
        }
        Err(match self.kind {
            TargetKind::Cluster => "target of kind cluster takes no namespace or name".into(),
            TargetKind::Node => "target of kind node needs name and no namespace".into(),
            TargetKind::Project => "target of kind project needs namespace and no name".into(),
            TargetKind::Workload => {
                "target of kind workload needs namespace, name and workloadKind".into()
            }
            _ => "target needs namespace and name; workloadKind is only for workload".into(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthFinding {
    pub key: String,
    pub rule_id: String,
    pub severity: Severity,
    pub target: Target,
    pub title: String,
    pub details: String,
    pub params: Map<String, Value>,
    /// Человек скрыл находку для этого объекта.
    pub ignored: bool,
}

/// Что проверено: проекты (не больше 50), поды (не больше 5000), ноды.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthScope {
    /// Проверенные проекты.
    pub projects: Vec<String>,
    /// Сколько проектов было в охвате до предела.
    pub projects_total: usize,
    pub pods: usize,
    /// `None` — ноды не читаются.
    pub nodes: Option<usize>,
}

/// Проверка целиком (все находки); фильтры и страницы — в инструменте.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthRun {
    pub run_id: String,
    pub started_at_ms: i64,
    pub duration_ms: i64,
    /// `{ "kind": "ui" }` или `{ "kind": "mcp", "client": "…" }`.
    pub trigger: Value,
    pub environment: String,
    pub incomplete: bool,
    /// Использована прошлая проверка (моложе `maxAgeSeconds`).
    pub reused: bool,
    pub scope: HealthScope,
    pub findings: Vec<HealthFinding>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum StatusFilter {
    #[default]
    Open,
    Closed,
    All,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AnnotationView {
    pub id: String,
    pub target: Target,
    pub severity: Severity,
    pub title: String,
    pub body: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub related_rule_id: Option<String>,
    /// Клиент, оставивший замечание; `None` — безымянный.
    pub client: Option<String>,
    /// open, resolved (человек: «решено»), dismissed (человек: «не актуально»).
    pub status: String,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewNote {
    pub target: Target,
    pub severity: Severity,
    pub title: String,
    pub body: String,
    pub related_rule_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HealthOp {
    /// Последняя проверка, если ей меньше `max_age_ms` и она охватывает `namespaces`, иначе
    /// новая по этим проектам (`None` — по всем, что видит LLM); с `run_id` — только эта
    /// проверка (`not_found`, если её уже заменила новая).
    Check {
        connection_id: String,
        max_age_ms: i64,
        run_id: Option<String>,
        namespaces: Option<Vec<String>>,
        session: SessionView,
    },
    Annotate {
        connection_id: String,
        session: SessionView,
        note: NewNote,
    },
    ListAnnotations {
        connection_id: String,
        status: StatusFilter,
    },
    RemoveAnnotation {
        connection_id: String,
        session: SessionView,
        annotation_id: String,
    },
}

impl HealthOp {
    pub fn connection_id(&self) -> &str {
        match self {
            HealthOp::Check { connection_id, .. }
            | HealthOp::Annotate { connection_id, .. }
            | HealthOp::ListAnnotations { connection_id, .. }
            | HealthOp::RemoveAnnotation { connection_id, .. } => connection_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum HealthOutput {
    Run(HealthRun),
    Annotated {
        annotation_id: String,
        created: bool,
    },
    Annotations(Vec<AnnotationView>),
    Removed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuleCount {
    pub rule_id: String,
    pub severity: Severity,
    pub count: usize,
}

/// Фильтры `health_check`.
#[derive(Debug, Clone, Default)]
pub struct FindingFilter {
    pub rule_ids: Option<Vec<String>>,
    pub min_severity: Option<Severity>,
    pub include_ignored: bool,
}

/// Объект в области LLM: у кластера и ноды проекта нет, остальные — в проектах области.
pub fn in_scope(target: &Target, scope: &Scope) -> bool {
    target
        .namespace
        .as_deref()
        .is_none_or(|ns| scope.allows(ns))
}

/// Сводка по всем находкам проверки и отфильтрованная страница.
pub fn select(
    findings: Vec<HealthFinding>,
    filter: &FindingFilter,
    offset: usize,
    limit: usize,
) -> (Vec<RuleCount>, Page<HealthFinding>) {
    let mut counts: BTreeMap<(String, Severity), usize> = BTreeMap::new();
    for f in &findings {
        *counts.entry((f.rule_id.clone(), f.severity)).or_default() += 1;
    }
    let mut summary: Vec<RuleCount> = counts
        .into_iter()
        .map(|((rule_id, severity), count)| RuleCount {
            rule_id,
            severity,
            count,
        })
        .collect();
    summary.sort_by(|a, b| {
        b.severity
            .cmp(&a.severity)
            .then_with(|| a.rule_id.cmp(&b.rule_id))
    });
    let kept: Vec<HealthFinding> = findings
        .into_iter()
        .filter(|f| filter.include_ignored || !f.ignored)
        .filter(|f| filter.min_severity.is_none_or(|m| f.severity >= m))
        .filter(|f| {
            filter
                .rule_ids
                .as_ref()
                .is_none_or(|ids| ids.iter().any(|id| id == &f.rule_id))
        })
        .collect();
    (summary, page(kept, offset, limit))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    fn target(kind: TargetKind, ns: Option<&str>, name: Option<&str>) -> Target {
        Target {
            kind,
            namespace: ns.map(String::from),
            name: name.map(String::from),
            workload_kind: None,
        }
    }

    fn finding(rule: &str, sev: Severity, ns: &str, name: &str, ignored: bool) -> HealthFinding {
        HealthFinding {
            key: format!("{rule}:pod:{ns}/{name}"),
            rule_id: rule.into(),
            severity: sev,
            target: target(TargetKind::Pod, Some(ns), Some(name)),
            title: "t".into(),
            details: "d".into(),
            params: Map::new(),
            ignored,
        }
    }

    fn sample() -> Vec<HealthFinding> {
        vec![
            finding("pod.crashLoop", Severity::Critical, "a", "web-1", false),
            finding("pod.crashLoop", Severity::Critical, "a", "web-2", true),
            finding("pod.pending", Severity::Warning, "b", "job-1", false),
            finding("container.latestTag", Severity::Info, "b", "api-1", false),
        ]
    }

    #[test]
    fn summary_counts_all_findings() {
        let (summary, page) = select(sample(), &FindingFilter::default(), 0, 100);
        assert_eq!(summary[0].rule_id, "pod.crashLoop");
        assert_eq!(summary[0].count, 2);
        assert_eq!(summary.len(), 3);
        // Без includeIgnored скрытая не попадает в страницу, но считается в сводке.
        assert_eq!(page.total, 3);
    }

    #[test]
    fn filters_and_paging() {
        let f = FindingFilter {
            min_severity: Some(Severity::Warning),
            include_ignored: true,
            ..Default::default()
        };
        let (_, p) = select(sample(), &f, 1, 1);
        assert_eq!(p.total, 3);
        assert_eq!(p.items[0].target.name.as_deref(), Some("web-2"));
        assert_eq!(p.next_offset, Some(2));
        let f = FindingFilter {
            rule_ids: Some(vec!["pod.pending".into()]),
            include_ignored: true,
            ..Default::default()
        };
        let (_, p) = select(sample(), &f, 0, 10);
        assert_eq!(p.items.len(), 1);
    }

    #[test]
    fn targets_need_the_fields_of_their_kind() {
        use TargetKind::*;
        assert!(target(Cluster, None, None).check().is_ok());
        assert!(target(Cluster, Some("a"), None).check().is_err());
        assert!(target(Node, None, Some("w1")).check().is_ok());
        assert!(target(Node, Some("a"), Some("w1")).check().is_err());
        assert!(target(Project, Some("a"), None).check().is_ok());
        assert!(target(Project, None, Some("a")).check().is_err());
        assert!(target(Pod, Some("a"), Some("web-1")).check().is_ok());
        assert!(target(Service, Some("a"), Some(" ")).check().is_err());
        assert!(target(Workload, Some("a"), Some("web")).check().is_err());
        let mut w = target(Workload, Some("a"), Some("web"));
        w.workload_kind = Some(WorkloadKind::Deployment);
        assert!(w.check().is_ok());
        let mut p = target(Pod, Some("a"), Some("web-1"));
        p.workload_kind = Some(WorkloadKind::Deployment);
        assert!(p.check().is_err(), "вид workload'а — только у workload");
    }

    #[test]
    fn scope_keeps_cluster_objects_and_allowed_projects() {
        let scope = Scope {
            namespaces: Some(BTreeSet::from(["a".to_string()])),
        };
        assert!(in_scope(&target(TargetKind::Cluster, None, None), &scope));
        assert!(in_scope(
            &target(TargetKind::Node, None, Some("w1")),
            &scope
        ));
        assert!(in_scope(
            &target(TargetKind::Pod, Some("a"), Some("x")),
            &scope
        ));
        assert!(!in_scope(
            &target(TargetKind::Pod, Some("b"), Some("x")),
            &scope
        ));
        assert!(in_scope(
            &target(TargetKind::Pod, Some("b"), Some("x")),
            &Scope::default()
        ));
    }
}
