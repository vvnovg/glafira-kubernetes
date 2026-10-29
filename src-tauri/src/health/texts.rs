//! Английские тексты находок для LLM (MCP, спец. §8.2). Интерфейс берёт свои из словарей
//! (`health.rules.<ruleId>.*`), здесь — короткие факты без перевода.

use serde_json::Value;

use super::model::{Finding, TargetKind};
use super::rules::MAX_LISTED;

fn text(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn list(f: &Finding, name: &str) -> String {
    match f.params.get(name) {
        Some(Value::Array(items)) => items.iter().map(text).collect::<Vec<_>>().join(", "),
        _ => String::new(),
    }
}

/// Пробелы `check.partial`: в `params` их список целиком, показываются первые `MAX_LISTED`, дальше «…».
fn gaps(f: &Finding) -> String {
    let Some(Value::Array(items)) = f.params.get("gaps") else {
        return String::new();
    };
    let mut shown = items
        .iter()
        .take(MAX_LISTED)
        .map(text)
        .collect::<Vec<_>>()
        .join(", ");
    if items.len() > MAX_LISTED {
        shown.push_str(", …");
    }
    shown
}

fn param(f: &Finding, name: &str) -> String {
    f.params.get(name).map(text).unwrap_or_default()
}

/// « (reason: message)» из параметров, если они есть. Точку в конце сообщения Kubernetes
/// ставит сам текст находки, иначе выходит «….).».
fn because(f: &Finding) -> String {
    let message = f
        .param_str("message")
        .map(|m| m.trim_end_matches(|c: char| c == '.' || c.is_whitespace()))
        .filter(|m| !m.is_empty());
    match (f.param_str("reason"), message) {
        (Some(r), Some(m)) => format!(" ({r}: {m})"),
        (Some(r), None) => format!(" ({r})"),
        (None, Some(m)) => format!(" ({m})"),
        (None, None) => String::new(),
    }
}

/// Объект находки словами: «Deployment team-a/web», «pod team-a/web-1», «node w1».
fn object(f: &Finding) -> String {
    let t = &f.target;
    let name = t.name.as_deref().unwrap_or("");
    let ns = t.namespace.as_deref().unwrap_or("");
    match t.kind {
        TargetKind::Cluster => "the cluster".into(),
        TargetKind::Node => format!("node {name}"),
        TargetKind::Project => format!("project {ns}"),
        TargetKind::Workload => {
            let kind = t
                .workload_kind
                .map(|k| format!("{k:?}"))
                .unwrap_or_default();
            format!("{kind} {ns}/{name}")
        }
        TargetKind::Pod => format!("pod {ns}/{name}"),
        TargetKind::Service => format!("Service {ns}/{name}"),
        TargetKind::Route => format!("Route {ns}/{name}"),
        TargetKind::Ingress => format!("Ingress {ns}/{name}"),
        TargetKind::Httproute => format!("HTTPRoute {ns}/{name}"),
        TargetKind::Grpcroute => format!("GRPCRoute {ns}/{name}"),
        TargetKind::Gateway => format!("Gateway {ns}/{name}"),
        TargetKind::Pvc => format!("PVC {ns}/{name}"),
        TargetKind::Hpa => format!("HPA {ns}/{name}"),
        TargetKind::Quota => format!("ResourceQuota {ns}/{name}"),
    }
}

/// «2 of 5 pods (web-1, web-2)» у находки по workload'у, «the pod» — по поду.
fn pods(f: &Finding) -> String {
    if f.target.kind == TargetKind::Pod {
        return "the pod".into();
    }
    format!(
        "{} of {} pods ({})",
        param(f, "count"),
        param(f, "total"),
        list(f, "pods")
    )
}

/// Заголовок и пояснение находки.
pub fn english(f: &Finding) -> (String, String) {
    let o = object(f);
    match f.rule_id.as_str() {
        "cluster.apiNotReady" => (
            "API server is not ready".into(),
            format!(
                "/readyz reports failed checks: {}. Requests to the cluster may fail or be slow until they pass.",
                list(f, "failed")
            ),
        ),
        "cluster.operatorDegraded" => (
            "ClusterOperators are degraded or unavailable".into(),
            format!(
                "{} ClusterOperator(s) report Degraded=True or Available=False: {}. Check them with get_cluster_overview.",
                param(f, "count"),
                list(f, "names")
            ),
        ),
        "cluster.updateFailing" => (
            "Cluster update is failing".into(),
            format!(
                "ClusterVersion reports Failing=True{}: the cluster cannot complete its update.",
                because(f)
            ),
        ),
        "cluster.mcpDegraded" => (
            "MachineConfigPools are degraded".into(),
            format!(
                "{} MachineConfigPool(s) report Degraded=True: {}. Nodes of these pools do not get configuration updates.",
                param(f, "count"),
                list(f, "pools")
            ),
        ),
        "node.notReady" => (
            format!("Node {} is not ready", f.target.name.as_deref().unwrap_or("")),
            format!(
                "The Ready condition is {}{}: pods on the node may be evicted and new pods are not scheduled there.",
                param(f, "status"),
                because(f)
            ),
        ),
        "node.pressure" => (
            format!("Node {} is under pressure", f.target.name.as_deref().unwrap_or("")),
            format!(
                "Conditions that are True: {}. The kubelet evicts pods and refuses new ones until the pressure goes away.",
                list(f, "conditions")
            ),
        ),
        "node.requestsHigh" => (
            format!("Node {} is almost fully requested", f.target.name.as_deref().unwrap_or("")),
            format!(
                "Pods on the node request {}% of its allocatable CPU and {}% of its memory (above 90%): new pods may not fit.",
                param(f, "cpuPercent"),
                param(f, "memoryPercent")
            ),
        ),
        "node.unschedulable" => (
            format!("Node {} is cordoned", f.target.name.as_deref().unwrap_or("")),
            "spec.unschedulable is set: no new pods are scheduled on the node (maintenance or drain).".into(),
        ),
        "pod.crashLoop" => (
            format!("{o}: containers restart in a loop"),
            format!(
                "Containers {} of {} keep crashing (last exit: {}, exit code {}, up to {} restarts). Check describe_pod and the events; the cause is usually in the container logs or its configuration.",
                list(f, "containers"),
                pods(f),
                param(f, "reason"),
                param(f, "exitCode"),
                param(f, "restarts")
            ),
        ),
        "pod.imagePull" => (
            format!("{o}: image cannot be pulled"),
            format!(
                "{} cannot pull image {}{}: check the image name and tag, the registry and the pull secret.",
                pods(f),
                param(f, "image"),
                because(f)
            ),
        ),
        "pod.pending" => (
            format!("{o}: pods are pending"),
            format!(
                "{} have been Pending for more than 5 minutes (up to {} min){}.",
                pods(f),
                param(f, "minutes"),
                because(f)
            ),
        ),
        "pod.oomKilled" => (
            format!("{o}: containers were OOMKilled"),
            format!(
                "Containers {} of {} were killed for exceeding their memory limit in the last 24 hours: raise the memory limit or reduce memory use.",
                list(f, "containers"),
                pods(f)
            ),
        ),
        "pod.restartsFrequent" => (
            format!("{o}: frequent restarts"),
            format!(
                "{} restarted at least 5 times (up to {}), the last time {} min ago.",
                pods(f),
                param(f, "restarts"),
                param(f, "lastRestartMinutes")
            ),
        ),
        "workload.unavailable" => (
            format!("{o} is unavailable"),
            format!(
                "Only {} of {} desired replicas are available for more than 5 minutes (about {} min).",
                param(f, "available"),
                param(f, "desired"),
                param(f, "minutes")
            ),
        ),
        "workload.rolloutStuck" => (
            format!("{o}: rollout is stuck"),
            format!(
                "Progressing=False with ProgressDeadlineExceeded{}: new pods did not become available in time.",
                because(f)
            ),
        ),
        "workload.singleReplica" => (
            format!("{o} runs a single replica"),
            match f.param_str("hpa") {
                Some(h) => format!(
                    "One replica and HPA {h} with minReplicas = 1: a restart, a node drain or an update makes it unavailable."
                ),
                None => "One replica and no HPA: a restart, a node drain or an update makes it unavailable.".into(),
            },
        ),
        "container.noRequests" => {
            // Пустой список не называется: «without a CPU request: ;» читается как сбой.
            let parts: Vec<String> = [("a CPU request", "cpu"), ("a memory request", "memory")]
                .into_iter()
                .filter_map(|(what, name)| {
                    let l = list(f, name);
                    (!l.is_empty()).then(|| format!("without {what}: {l}"))
                })
                .collect();
            (
                format!("{o}: containers without requests"),
                format!(
                    "Running containers {} (after LimitRange defaults). The scheduler cannot place such pods reliably and they are evicted first.",
                    parts.join("; ")
                ),
            )
        }
        "container.noMemoryLimit" => (
            format!("{o}: containers without a memory limit"),
            format!(
                "Running containers without a memory limit: {}. One leaking container can starve the whole node.",
                list(f, "containers")
            ),
        ),
        "container.noReadinessProbe" => (
            format!("{o}: containers without a readiness probe"),
            format!(
                "Containers without a readiness probe: {}. Traffic reaches pods before they are ready, also during rollouts.",
                list(f, "containers")
            ),
        ),
        "container.latestTag" => (
            format!("{o}: images with the latest tag"),
            format!(
                "Running containers use images tagged latest or without a tag: {}. Pods may run different versions after restarts.",
                list(f, "images")
            ),
        ),
        "quota.nearLimit" => (
            format!("{o} is almost used up"),
            format!(
                "Resources used at 90% or more: {}. New pods or objects will be rejected when the quota is exceeded.",
                list(f, "resources")
            ),
        ),
        "pvc.pending" => (
            format!("{o} is {}", param(f, "phase")),
            match f.param_str("phase") {
                Some("Lost") => "The bound volume is lost: pods that use it cannot start.".into(),
                _ => format!(
                    "The claim has been Pending for more than 5 minutes (about {} min) while pods {} wait for it (storage class {}).",
                    param(f, "minutes"),
                    list(f, "pods"),
                    param(f, "class")
                ),
            },
        ),
        "hpa.atMax" => (
            format!("{o} is at its maximum"),
            format!(
                "ScalingLimited=True with TooManyReplicas: {} needs more than maxReplicas = {}.",
                param(f, "target"),
                param(f, "maxReplicas")
            ),
        ),
        "hpa.unableToScale" => (
            format!("{o} cannot scale"),
            format!(
                "{}=False{}: the HPA does not change the replicas of {}.",
                param(f, "condition"),
                because(f),
                param(f, "target")
            ),
        ),
        "service.noEndpoints" => (
            format!("{o} has no ready endpoints"),
            match list(f, "referencedBy") {
                r if r.is_empty() => "The Service selects no ready pods: requests to it fail.".into(),
                r => format!("The Service selects no ready pods: requests to it fail. It is used by {r}."),
            },
        ),
        "route.notAdmitted" => (
            format!("{o} is not admitted"),
            match f.param_str("reason") {
                Some("noStatus") => "No router has admitted the Route (no status): it does not serve traffic.".into(),
                _ => format!("Admitted=False{}: the Route does not serve traffic.", because(f)),
            },
        ),
        "route.certExpiring" => (
            format!("{o}: certificate expires"),
            match f.params.get("expired").and_then(Value::as_bool) {
                Some(true) => format!(
                    "The certificate in spec.tls expired {} day(s) ago: clients reject the connection.",
                    -f.param_i64("days").unwrap_or(0)
                ),
                _ => format!(
                    "The certificate in spec.tls expires in {} day(s): renew it.",
                    param(f, "days")
                ),
            },
        ),
        "ingress.classMissing" => (
            format!("{o}: ingress class is missing"),
            match f.param_str("class") {
                Some(c) => format!("IngressClass {c} does not exist: no controller serves this Ingress."),
                None => "The Ingress names no class and there is no default IngressClass: no controller may serve it.".into(),
            },
        ),
        "ingress.backendMissing" => (
            format!("{o}: backend is missing"),
            format!(
                "Services that do not exist in the project: {}; ports missing on existing Services: {}. Requests to these paths fail.",
                list(f, "services"),
                list(f, "ports")
            ),
        ),
        "ingress.tlsSecretMissing" => (
            format!("{o}: TLS Secret is missing"),
            format!(
                "Secrets from spec.tls are not found for more than 5 minutes: {}. The controller serves its default certificate instead.",
                list(f, "secrets")
            ),
        ),
        "gatewayRoute.notAccepted" => (
            format!("{o} is not accepted"),
            format!(
                "Gateways {} did not accept the route{}: its rules do not apply there.",
                list(f, "parents"),
                because(f)
            ),
        ),
        "gatewayRoute.refsUnresolved" => (
            format!("{o}: references are unresolved"),
            format!(
                "ResolvedRefs=False{} on {}: some backends are missing or not permitted (ReferenceGrant).",
                because(f),
                list(f, "parents")
            ),
        ),
        "gateway.notProgrammed" => (
            format!("{o} is not programmed"),
            format!(
                "{}=False for more than 5 minutes{}: the Gateway does not serve traffic.",
                param(f, "condition"),
                because(f)
            ),
        ),
        "resources.cpuThrottled" => (
            format!("{o}: CPU is throttled"),
            format!(
                "Containers with a CPU limit were throttled on average over the last 24 h: {}. The limit cuts their CPU time, so responses slow down.",
                list(f, "containers")
            ),
        ),
        "resources.memoryNearLimit" => (
            format!("{o}: memory close to the limit"),
            format!(
                "Containers used 90% or more of their memory limit over the last 24 h (max / limit): {}. The next spike may end in OOMKilled.",
                list(f, "containers")
            ),
        ),
        "resources.overProvisioned" => {
            // Пустой список не называется, как у `container.noRequests`.
            let parts: Vec<String> = [("CPU (request / p95)", "cpu"), ("memory (request / max)", "memory")]
                .into_iter()
                .filter_map(|(what, name)| {
                    let l = list(f, name);
                    (!l.is_empty()).then(|| format!("{what}: {l}"))
                })
                .collect();
            (
                format!("{o}: requests far above the usage"),
                format!(
                    "Requests are much higher than the usage over the last 24 h — {}. The reserved capacity is unavailable to other pods.",
                    parts.join("; ")
                ),
            )
        }
        "check.partial" => (
            "The check is incomplete".into(),
            {
                let mut parts = Vec::new();
                if f.params.get("projectLimit").and_then(Value::as_bool) == Some(true) {
                    parts.push(format!(
                        "only {} of {} projects were checked (limit 50)",
                        param(f, "projectsChecked"),
                        param(f, "projectsTotal")
                    ));
                }
                if f.params.get("podLimit").and_then(Value::as_bool) == Some(true) {
                    parts.push("only the first 5000 pods were checked".into());
                }
                if f.params.get("timedOut").and_then(Value::as_bool) == Some(true) {
                    parts.push("the check stopped at its 45 s deadline".into());
                }
                if f.param_i64("gapCount").unwrap_or(0) > 0 {
                    parts.push(format!("not read: {}", gaps(f)));
                }
                match parts.is_empty() {
                    true => "Some objects were not checked: findings may be missing for them.".into(),
                    false => format!(
                        "Some objects were not checked: {}. Findings may be missing for them.",
                        parts.join("; ")
                    ),
                }
            },
        ),
        other => (other.to_string(), String::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::health::model::{Severity, Target};
    use crate::k8s::workloads::WorkloadKind;

    #[test]
    fn texts_use_params() {
        let f = Finding::new(
            "pod.crashLoop",
            Severity::Critical,
            Target::workload("team-a", WorkloadKind::Deployment, "web"),
            None,
            serde_json::json!({ "pods": ["web-1", "web-2"], "count": 2, "total": 3,
                "containers": ["web"], "reason": "Error", "exitCode": 1, "restarts": 7 }),
        );
        let (title, details) = english(&f);
        assert_eq!(title, "Deployment team-a/web: containers restart in a loop");
        assert!(
            details.starts_with("Containers web of 2 of 3 pods (web-1, web-2) keep crashing"),
            "{details}"
        );
        assert!(
            details.contains("exit code 1, up to 7 restarts"),
            "{details}"
        );
    }

    #[test]
    fn empty_lists_and_final_dots_do_not_reach_the_text() {
        let requests = Finding::new(
            "container.noRequests",
            Severity::Warning,
            Target::object(TargetKind::Pod, "kube-system", "kube-apiserver-n1"),
            None,
            serde_json::json!({ "cpu": [], "memory": ["kube-apiserver"] }),
        );
        assert!(
            english(&requests)
                .1
                .starts_with("Running containers without a memory request: kube-apiserver (after"),
            "{}",
            english(&requests).1
        );
        let pending = Finding::new(
            "pod.pending",
            Severity::Critical,
            Target::workload("team-a", WorkloadKind::Deployment, "web"),
            None,
            serde_json::json!({ "pods": ["web-1"], "count": 1, "total": 1, "minutes": 9,
                "reason": "Unschedulable", "message": "0/1 nodes are available. " }),
        );
        assert!(
            english(&pending)
                .1
                .ends_with("(Unschedulable: 0/1 nodes are available)."),
            "{}",
            english(&pending).1
        );
    }

    #[test]
    fn expired_certificate_counts_days_back() {
        let f = Finding::new(
            "route.certExpiring",
            Severity::Critical,
            Target::object(TargetKind::Route, "team-a", "web"),
            None,
            serde_json::json!({ "days": -3, "expired": true }),
        );
        assert!(english(&f).1.contains("expired 3 day(s) ago"));
    }

    #[test]
    fn every_rule_has_a_text() {
        for id in crate::health::rules::rule_ids() {
            let f = Finding::new(
                id,
                Severity::Info,
                Target::cluster(),
                None,
                serde_json::json!({}),
            );
            let (title, details) = english(&f);
            assert_ne!(title, id, "{id}");
            assert!(!details.is_empty(), "{id}");
        }
    }

    #[test]
    fn partial_text_names_every_reason() {
        let f = Finding::new(
            "check.partial",
            Severity::Info,
            Target::cluster(),
            None,
            serde_json::json!({ "projectLimit": true, "projectsChecked": 50, "projectsTotal": 70,
                "podLimit": false, "timedOut": true, "gaps": ["nodes: FORBIDDEN"], "gapCount": 1 }),
        );
        let (_, d) = english(&f);
        assert!(d.contains("only 50 of 70 projects"), "{d}");
        assert!(
            d.contains("45 s deadline") && d.contains("not read: nodes: FORBIDDEN"),
            "{d}"
        );
    }

    #[test]
    fn partial_text_shows_the_first_twenty_gaps_and_an_ellipsis() {
        let partial = |n: usize| {
            let gaps: Vec<String> = (0..n)
                .map(|i| format!("team-{i:02}/secrets: FORBIDDEN"))
                .collect();
            Finding::new(
                "check.partial",
                Severity::Info,
                Target::cluster(),
                None,
                serde_json::json!({ "projectLimit": false, "podLimit": false, "timedOut": false,
                    "gaps": gaps, "gapCount": n }),
            )
        };
        // Ровно 20 — без многоточия.
        let (_, d) = english(&partial(20));
        assert!(d.contains("team-19/secrets: FORBIDDEN. Findings"), "{d}");
        assert!(!d.contains('…'), "{d}");
        // Больше 20 — первые 20 и «…»; двадцать первого в тексте нет.
        let (_, d) = english(&partial(25));
        assert!(d.contains("team-19/secrets: FORBIDDEN, …. Findings"), "{d}");
        assert!(!d.contains("team-20"), "{d}");
    }
}
