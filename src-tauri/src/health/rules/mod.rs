//! Каталог правил и их строгость по окружению (спец. §8.2). Правила — чистые функции над
//! `ClusterSnapshot`: сеть и файлы здесь не трогаются.

mod cluster;
mod network;
pub mod pods;
mod project;
mod resources;
mod workloads;

use serde_json::Value;

use super::model::{Finding, Levels, Severity, Target};
use super::snapshot::ClusterSnapshot;
use crate::storage::profile::Environment;

const C: Option<Severity> = Some(Severity::Critical);
const W: Option<Severity> = Some(Severity::Warning);
const I: Option<Severity> = Some(Severity::Info);
const N: Option<Severity> = None;

/// Пять минут: столько ждут `pod.pending`, `workload.unavailable`, `pvc.pending`, Gateway API и
/// Secret TLS у Ingress.
pub const GRACE_S: i64 = 5 * 60;
/// В списках `params` — не больше стольких элементов.
pub const MAX_LISTED: usize = 20;

pub struct RuleDef {
    pub id: &'static str,
    pub levels: Levels,
}

const fn rule(
    id: &'static str,
    prod: Option<Severity>,
    stage: Option<Severity>,
    other: Option<Severity>,
) -> RuleDef {
    RuleDef {
        id,
        levels: Levels::new(prod, stage, other),
    }
}

/// Таблица §8.2 спецификации: prod, stage, остальные окружения.
pub const RULES: &[RuleDef] = &[
    rule("cluster.apiNotReady", C, C, W),
    rule("cluster.operatorDegraded", C, C, W),
    rule("cluster.updateFailing", C, C, W),
    rule("cluster.mcpDegraded", C, C, W),
    rule("node.notReady", C, C, W),
    rule("node.pressure", C, W, W),
    rule("node.requestsHigh", W, W, I),
    rule("node.unschedulable", I, I, I),
    rule("pod.crashLoop", C, W, W),
    rule("pod.imagePull", C, W, W),
    rule("pod.pending", C, W, W),
    rule("pod.oomKilled", C, W, I),
    rule("pod.restartsFrequent", W, W, I),
    rule("workload.unavailable", C, W, I),
    rule("workload.rolloutStuck", C, W, W),
    rule("workload.singleReplica", W, I, N),
    rule("container.noRequests", W, W, I),
    rule("container.noMemoryLimit", W, I, N),
    rule("container.noReadinessProbe", W, I, N),
    rule("container.latestTag", W, I, N),
    rule("quota.nearLimit", W, W, I),
    rule("pvc.pending", C, W, W),
    rule("hpa.atMax", W, W, I),
    rule("hpa.unableToScale", W, W, I),
    rule("service.noEndpoints", W, W, I),
    rule("route.notAdmitted", C, W, W),
    rule("route.certExpiring", W, W, I),
    rule("ingress.classMissing", C, W, W),
    rule("ingress.backendMissing", C, W, W),
    rule("ingress.tlsSecretMissing", C, W, W),
    rule("gatewayRoute.notAccepted", C, W, W),
    rule("gatewayRoute.refsUnresolved", C, W, W),
    rule("gateway.notProgrammed", C, C, W),
    rule("resources.cpuThrottled", W, W, I),
    rule("resources.memoryNearLimit", W, W, I),
    rule("resources.overProvisioned", I, I, I),
    rule("check.partial", I, I, I),
];

/// Истёкший встроенный сертификат Route строже, чем истекающий (спец. §8.2).
pub const CERT_EXPIRED: Levels = Levels::new(C, C, W);

pub fn rule_def(id: &str) -> Option<&'static RuleDef> {
    RULES.iter().find(|r| r.id == id)
}

/// Идентификаторы всех правил (для `relatedRuleId`, словарей и MCP).
pub fn rule_ids() -> impl Iterator<Item = &'static str> {
    RULES.iter().map(|r| r.id)
}

/// Накопитель находок: строгость берётся из таблицы по окружению, «—» отбрасывается.
pub struct Findings {
    env: Environment,
    items: Vec<Finding>,
}

impl Findings {
    pub fn new(env: Environment) -> Self {
        Self {
            env,
            items: Vec::new(),
        }
    }

    pub fn emit(
        &mut self,
        rule_id: &'static str,
        target: Target,
        qualifier: Option<&str>,
        params: Value,
    ) {
        let def = rule_def(rule_id);
        debug_assert!(def.is_some(), "unknown rule {rule_id}");
        if let Some(d) = def {
            self.emit_at(rule_id, d.levels, target, qualifier, params);
        }
    }

    /// Находка со своей строгостью (истёкший сертификат Route).
    pub fn emit_at(
        &mut self,
        rule_id: &'static str,
        levels: Levels,
        target: Target,
        qualifier: Option<&str>,
        params: Value,
    ) {
        if let Some(severity) = levels.for_env(self.env) {
            self.items
                .push(Finding::new(rule_id, severity, target, qualifier, params));
        }
    }

    /// Сначала строгие, затем по порядку правил в таблице и по объекту.
    pub fn into_sorted(mut self) -> Vec<Finding> {
        let order = |id: &str| RULES.iter().position(|r| r.id == id).unwrap_or(usize::MAX);
        self.items.sort_by(|a, b| {
            b.severity
                .cmp(&a.severity)
                .then_with(|| order(&a.rule_id).cmp(&order(&b.rule_id)))
                .then_with(|| a.target.cmp(&b.target))
                .then_with(|| a.key.cmp(&b.key))
        });
        self.items
    }
}

/// Первые `MAX_LISTED` элементов списка — для `params`.
pub fn listed<T: Clone>(items: &[T]) -> Vec<T> {
    items.iter().take(MAX_LISTED).cloned().collect()
}

/// Все правила над снимком.
pub fn evaluate(s: &ClusterSnapshot, env: Environment) -> Vec<Finding> {
    let mut out = Findings::new(env);
    cluster::check(s, &mut out);
    pods::check(s, &mut out);
    workloads::check(s, &mut out);
    project::check(s, &mut out);
    network::check(s, &mut out);
    resources::check(s, &mut out);
    cluster::partial(s, &mut out);
    out.into_sorted()
}

#[cfg(test)]
pub(crate) mod fixtures {
    use crate::storage::profile::Environment;

    /// 2026-10-26T12:00:00Z.
    pub const NOW: i64 = 1_793_016_000;

    pub const ENVS: [Environment; 5] = {
        use Environment::*;
        [Prod, Stage, Test, Dev, Local]
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rule_ids_are_unique() {
        let mut ids: Vec<_> = rule_ids().collect();
        let n = ids.len();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), n);
        assert_eq!(n, 37, "все правила §8.2");
    }

    #[test]
    fn severity_matrix_matches_spec() {
        // Строки таблицы §8.2: правило, prod, stage, прочие («К», «П», «И», «—»).
        let spec = [
            ("cluster.apiNotReady", "ККП"),
            ("cluster.operatorDegraded", "ККП"),
            ("cluster.updateFailing", "ККП"),
            ("cluster.mcpDegraded", "ККП"),
            ("node.notReady", "ККП"),
            ("node.pressure", "КПП"),
            ("node.requestsHigh", "ППИ"),
            ("node.unschedulable", "ИИИ"),
            ("pod.crashLoop", "КПП"),
            ("pod.imagePull", "КПП"),
            ("pod.pending", "КПП"),
            ("pod.oomKilled", "КПИ"),
            ("pod.restartsFrequent", "ППИ"),
            ("workload.unavailable", "КПИ"),
            ("workload.rolloutStuck", "КПП"),
            ("workload.singleReplica", "ПИ—"),
            ("container.noRequests", "ППИ"),
            ("container.noMemoryLimit", "ПИ—"),
            ("container.noReadinessProbe", "ПИ—"),
            ("container.latestTag", "ПИ—"),
            ("quota.nearLimit", "ППИ"),
            ("pvc.pending", "КПП"),
            ("hpa.atMax", "ППИ"),
            ("hpa.unableToScale", "ППИ"),
            ("service.noEndpoints", "ППИ"),
            ("route.notAdmitted", "КПП"),
            ("route.certExpiring", "ППИ"),
            ("ingress.classMissing", "КПП"),
            ("ingress.backendMissing", "КПП"),
            ("ingress.tlsSecretMissing", "КПП"),
            ("gatewayRoute.notAccepted", "КПП"),
            ("gatewayRoute.refsUnresolved", "КПП"),
            ("gateway.notProgrammed", "ККП"),
            ("resources.cpuThrottled", "ППИ"),
            ("resources.memoryNearLimit", "ППИ"),
            ("resources.overProvisioned", "ИИИ"),
            ("check.partial", "ИИИ"),
        ];
        let sym = |s: Option<Severity>| match s {
            Some(Severity::Critical) => 'К',
            Some(Severity::Warning) => 'П',
            Some(Severity::Info) => 'И',
            None => '—',
        };
        let row =
            |l: Levels| -> String { [l.prod, l.stage, l.other].into_iter().map(sym).collect() };
        assert_eq!(spec.len(), RULES.len());
        for (id, expected) in spec {
            assert_eq!(row(rule_def(id).unwrap().levels), expected, "{id}");
        }
        assert_eq!(row(CERT_EXPIRED), "ККП", "истёкший сертификат Route");
    }

    #[test]
    fn every_rule_has_interface_texts() {
        for dict in [
            include_str!("../../../../src/locales/en.json"),
            include_str!("../../../../src/locales/ru.json"),
        ] {
            let v: serde_json::Value = serde_json::from_str(dict).unwrap();
            for id in rule_ids() {
                for part in ["title", "details", "fix"] {
                    assert!(
                        v["health"]["rules"][id][part].is_string(),
                        "health.rules.{id}.{part}"
                    );
                }
                assert!(
                    v["health"]["ruleNames"][id].is_string(),
                    "health.ruleNames.{id}"
                );
            }
        }
    }

    #[test]
    fn findings_drop_hidden_levels_and_sort_by_severity() {
        let mut f = Findings::new(Environment::Local);
        f.emit(
            "workload.singleReplica",
            Target::workload("a", crate::k8s::workloads::WorkloadKind::Deployment, "web"),
            None,
            Value::Null,
        );
        f.emit("node.unschedulable", Target::node("w2"), None, Value::Null);
        f.emit("node.notReady", Target::node("w1"), None, Value::Null);
        let v = f.into_sorted();
        let ids: Vec<_> = v.iter().map(|x| x.rule_id.as_str()).collect();
        assert_eq!(ids, ["node.notReady", "node.unschedulable"]);
        assert_eq!(v[0].severity, Severity::Warning);
        assert_eq!(v[1].severity, Severity::Info);
    }
}
