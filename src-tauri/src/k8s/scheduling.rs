//! Ограничения размещения пода или шаблона подов — для MCP (спец. §10.4): по ним модель
//! объясняет `FailedScheduling`. Термы affinity и толерантности — короткими строками, а не
//! вложенным JSON; интерфейс их не показывает (у него вкладка YAML).

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::{
    NodeSelectorRequirement, NodeSelectorTerm, PodAffinityTerm, PodSpec, Toleration,
    WeightedPodAffinityTerm,
};

use crate::k8s::selectors::selector_string;

/// Планировщик по умолчанию: его имя модели не нужно.
const DEFAULT_SCHEDULER: &str = "default-scheduler";

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SchedulingView {
    pub node_selector: BTreeMap<String, String>,
    /// `required: kubernetes.io/arch In [amd64], disktype Exists`, `preferred (weight 50): …`.
    pub node_affinity: Vec<String>,
    /// `required: app=db; topologyKey kubernetes.io/hostname`.
    pub pod_affinity: Vec<String>,
    pub pod_anti_affinity: Vec<String>,
    /// Как у `kubectl describe`: `key=value:NoSchedule`, `key:NoExecute op=Exists for 300s`.
    pub tolerations: Vec<String>,
    pub node_name: Option<String>,
    pub priority_class_name: Option<String>,
    /// Только если не `default-scheduler`.
    pub scheduler_name: Option<String>,
}

pub fn scheduling_of(spec: &PodSpec) -> SchedulingView {
    let affinity = spec.affinity.as_ref();
    let node = affinity.and_then(|a| a.node_affinity.as_ref());
    let mut node_affinity: Vec<String> = node
        .and_then(|n| {
            n.required_during_scheduling_ignored_during_execution
                .as_ref()
        })
        .map(|r| {
            r.node_selector_terms
                .iter()
                .map(|t| format!("required: {}", node_term(t)))
                .collect()
        })
        .unwrap_or_default();
    node_affinity.extend(
        node.and_then(|n| {
            n.preferred_during_scheduling_ignored_during_execution
                .as_ref()
        })
        .into_iter()
        .flatten()
        .map(|p| {
            format!(
                "preferred (weight {}): {}",
                p.weight,
                node_term(&p.preference)
            )
        }),
    );
    let pod_terms = |required: Option<&Vec<PodAffinityTerm>>,
                     preferred: Option<&Vec<WeightedPodAffinityTerm>>| {
        required
            .into_iter()
            .flatten()
            .map(|t| format!("required: {}", pod_term(t)))
            .chain(preferred.into_iter().flatten().map(|w| {
                format!(
                    "preferred (weight {}): {}",
                    w.weight,
                    pod_term(&w.pod_affinity_term)
                )
            }))
            .collect::<Vec<_>>()
    };
    let pod = affinity.and_then(|a| a.pod_affinity.as_ref());
    let anti = affinity.and_then(|a| a.pod_anti_affinity.as_ref());
    SchedulingView {
        node_selector: spec.node_selector.clone().unwrap_or_default(),
        node_affinity,
        pod_affinity: pod_terms(
            pod.and_then(|p| {
                p.required_during_scheduling_ignored_during_execution
                    .as_ref()
            }),
            pod.and_then(|p| {
                p.preferred_during_scheduling_ignored_during_execution
                    .as_ref()
            }),
        ),
        pod_anti_affinity: pod_terms(
            anti.and_then(|p| {
                p.required_during_scheduling_ignored_during_execution
                    .as_ref()
            }),
            anti.and_then(|p| {
                p.preferred_during_scheduling_ignored_during_execution
                    .as_ref()
            }),
        ),
        tolerations: spec.tolerations.iter().flatten().map(toleration).collect(),
        node_name: spec.node_name.clone().filter(|n| !n.is_empty()),
        priority_class_name: spec.priority_class_name.clone().filter(|n| !n.is_empty()),
        scheduler_name: spec
            .scheduler_name
            .clone()
            .filter(|n| !n.is_empty() && n != DEFAULT_SCHEDULER),
    }
}

/// `kubernetes.io/arch In [amd64]`, `disktype Exists`.
fn requirement(r: &NodeSelectorRequirement) -> String {
    match r.values.as_deref() {
        Some(v) if !v.is_empty() => format!("{} {} [{}]", r.key, r.operator, v.join(", ")),
        _ => format!("{} {}", r.key, r.operator),
    }
}

/// Выражения и поля терма через запятую (все должны выполняться).
fn node_term(t: &NodeSelectorTerm) -> String {
    let parts: Vec<String> = t
        .match_expressions
        .iter()
        .chain(t.match_fields.iter())
        .flatten()
        .map(requirement)
        .collect();
    if parts.is_empty() {
        "no expressions".into()
    } else {
        parts.join(", ")
    }
}

/// Селектор подов, проекты и `topologyKey`: `app=db; namespaces [a, b]; topologyKey zone`.
fn pod_term(t: &PodAffinityTerm) -> String {
    let pods = match &t.label_selector {
        None => "no pods".to_string(),
        Some(s) => match selector_string(s) {
            s if s.is_empty() => "all pods".to_string(),
            s => s,
        },
    };
    let mut out = pods;
    if let Some(ns) = t.namespaces.as_deref().filter(|n| !n.is_empty()) {
        out.push_str(&format!("; namespaces [{}]", ns.join(", ")));
    }
    if let Some(s) = &t.namespace_selector {
        match selector_string(s) {
            s if s.is_empty() => out.push_str("; all namespaces"),
            s => out.push_str(&format!("; namespaceSelector {s}")),
        }
    }
    out.push_str(&format!("; topologyKey {}", t.topology_key));
    out
}

/// Как у `kubectl describe pod`: `key=value:NoSchedule`, `key:NoExecute op=Exists for 300s`,
/// `op=Exists` (терпит всё).
fn toleration(t: &Toleration) -> String {
    let mut s = t.key.clone().unwrap_or_default();
    if let Some(v) = t.value.as_deref().filter(|v| !v.is_empty()) {
        s.push('=');
        s.push_str(v);
    }
    if let Some(e) = t.effect.as_deref().filter(|e| !e.is_empty()) {
        s.push(':');
        s.push_str(e);
    }
    if t.operator.as_deref() == Some("Exists") {
        if !s.is_empty() {
            s.push(' ');
        }
        s.push_str("op=Exists");
    }
    if let Some(secs) = t.toleration_seconds {
        s.push_str(&format!(" for {secs}s"));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(v: serde_json::Value) -> PodSpec {
        serde_json::from_value(v).unwrap()
    }

    #[test]
    fn constraints_are_compact_strings() {
        let s = scheduling_of(&spec(serde_json::json!({
            "containers": [{"name": "app"}],
            "nodeSelector": {"glafira/none": "true"},
            "nodeName": "w1",
            "priorityClassName": "high",
            "schedulerName": "my-scheduler",
            "affinity": {
                "nodeAffinity": {
                    "requiredDuringSchedulingIgnoredDuringExecution": {"nodeSelectorTerms": [
                        {"matchExpressions": [
                            {"key": "kubernetes.io/arch", "operator": "In", "values": ["amd64", "arm64"]},
                            {"key": "disktype", "operator": "Exists"}]},
                        {"matchFields": [{"key": "metadata.name", "operator": "NotIn", "values": ["w2"]}]}]},
                    "preferredDuringSchedulingIgnoredDuringExecution": [
                        {"weight": 50, "preference": {"matchExpressions": [
                            {"key": "zone", "operator": "In", "values": ["a"]}]}}]},
                "podAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": [
                    {"labelSelector": {"matchLabels": {"app": "db"}},
                     "namespaces": ["data"], "topologyKey": "kubernetes.io/hostname"}]},
                "podAntiAffinity": {"preferredDuringSchedulingIgnoredDuringExecution": [
                    {"weight": 100, "podAffinityTerm": {
                        "labelSelector": {"matchExpressions": [{"key": "app", "operator": "In", "values": ["web"]}]},
                        "namespaceSelector": {}, "topologyKey": "topology.kubernetes.io/zone"}}]}
            },
            "tolerations": [
                {"key": "dedicated", "operator": "Equal", "value": "gpu", "effect": "NoSchedule"},
                {"key": "node.kubernetes.io/not-ready", "operator": "Exists", "effect": "NoExecute", "tolerationSeconds": 300},
                {"key": "spot", "operator": "Exists"},
                {"operator": "Exists"}
            ]
        })));
        assert_eq!(
            s,
            SchedulingView {
                node_selector: BTreeMap::from([("glafira/none".into(), "true".into())]),
                node_affinity: vec![
                    "required: kubernetes.io/arch In [amd64, arm64], disktype Exists".into(),
                    "required: metadata.name NotIn [w2]".into(),
                    "preferred (weight 50): zone In [a]".into(),
                ],
                pod_affinity: vec![
                    "required: app=db; namespaces [data]; topologyKey kubernetes.io/hostname"
                        .into()
                ],
                pod_anti_affinity: vec![
                    "preferred (weight 100): app in (web); all namespaces; topologyKey topology.kubernetes.io/zone"
                        .into()
                ],
                tolerations: vec![
                    "dedicated=gpu:NoSchedule".into(),
                    "node.kubernetes.io/not-ready:NoExecute op=Exists for 300s".into(),
                    "spot op=Exists".into(),
                    "op=Exists".into(),
                ],
                node_name: Some("w1".into()),
                priority_class_name: Some("high".into()),
                scheduler_name: Some("my-scheduler".into()),
            }
        );
    }

    #[test]
    fn nothing_set_is_empty_and_the_default_scheduler_is_dropped() {
        let s = scheduling_of(&spec(serde_json::json!({
            "containers": [{"name": "app"}],
            "schedulerName": "default-scheduler"
        })));
        assert_eq!(s, SchedulingView::default());
    }
}
