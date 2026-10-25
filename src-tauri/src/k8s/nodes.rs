//! Ноды кластера (спец. §6.1–6.2): список, сводка готовых по ролям и сведения о ноде.

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::Node;
use kube::api::ListParams;
use serde::Serialize;

use crate::error::AppResult;
use crate::k8s::read::{Listed, ReadClient};
use crate::k8s::view::{ConditionView, conditions_of, created, name};

/// Условия ноды, которые при `True` означают проблему.
const PRESSURE: &[&str] = &[
    "MemoryPressure",
    "DiskPressure",
    "PIDPressure",
    "NetworkUnavailable",
];

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeRow {
    pub name: String,
    pub roles: Vec<String>,
    pub ready: bool,
    /// Условия-проблемы: давление на память, диск или PID, нет сети; `Ready` не `True`.
    pub problems: Vec<String>,
    pub unschedulable: bool,
    pub kubelet_version: Option<String>,
    pub internal_ip: Option<String>,
    /// Allocatable: CPU, память, поды.
    pub cpu: Option<String>,
    pub memory: Option<String>,
    pub pods: Option<String>,
    pub created: Option<i64>,
    pub taints: usize,
}

/// Роли ноды по меткам `node-role.kubernetes.io/<роль>` и `kubernetes.io/role`.
fn roles(n: &Node) -> Vec<String> {
    let mut roles: Vec<String> = n
        .metadata
        .labels
        .iter()
        .flatten()
        .filter_map(|(k, v)| match k.strip_prefix("node-role.kubernetes.io/") {
            Some(role) if !role.is_empty() => Some(role.to_string()),
            _ if k == "kubernetes.io/role" && !v.is_empty() => Some(v.clone()),
            _ => None,
        })
        .collect();
    roles.sort();
    roles.dedup();
    roles
}

pub fn node_row(n: &Node) -> NodeRow {
    let status = n.status.as_ref();
    let conds = conditions_of(&status.and_then(|s| s.conditions.clone()));
    let ready = conds.iter().any(|c| c.kind == "Ready" && c.is_true());
    let mut problems: Vec<String> = conds
        .iter()
        .filter(|c| PRESSURE.contains(&c.kind.as_str()) && c.is_true())
        .map(|c| c.kind.clone())
        .collect();
    if !ready {
        problems.insert(0, "NotReady".into());
    }
    let allocatable = |k: &str| {
        status
            .and_then(|s| s.allocatable.as_ref())
            .and_then(|a| a.get(k))
            .map(|q| q.0.clone())
    };
    NodeRow {
        name: name(&n.metadata),
        roles: roles(n),
        ready,
        problems,
        unschedulable: n.spec.as_ref().and_then(|s| s.unschedulable) == Some(true),
        kubelet_version: status
            .and_then(|s| s.node_info.as_ref())
            .map(|i| i.kubelet_version.clone()),
        internal_ip: status
            .and_then(|s| s.addresses.as_ref())
            .and_then(|a| a.iter().find(|a| a.type_ == "InternalIP"))
            .map(|a| a.address.clone()),
        cpu: allocatable("cpu"),
        memory: allocatable("memory"),
        pods: allocatable("pods"),
        created: created(&n.metadata),
        taints: n
            .spec
            .as_ref()
            .and_then(|s| s.taints.as_ref())
            .map_or(0, Vec::len),
    }
}

/// Готовые ноды из всех и по ролям (спец. §6.2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeSummary {
    pub total: usize,
    pub ready: usize,
    pub roles: Vec<RoleCount>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RoleCount {
    pub role: String,
    pub total: usize,
    pub ready: usize,
}

pub fn summary(rows: &[NodeRow]) -> NodeSummary {
    let mut by_role: BTreeMap<&str, (usize, usize)> = BTreeMap::new();
    for r in rows {
        for role in &r.roles {
            let e = by_role.entry(role).or_default();
            e.0 += 1;
            e.1 += usize::from(r.ready);
        }
    }
    NodeSummary {
        total: rows.len(),
        ready: rows.iter().filter(|r| r.ready).count(),
        roles: by_role
            .into_iter()
            .map(|(role, (total, ready))| RoleCount {
                role: role.to_string(),
                total,
                ready,
            })
            .collect(),
    }
}

/// Сведения о ноде.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeDetails {
    pub node: NodeRow,
    pub conditions: Vec<ConditionView>,
    /// `ключ=значение:эффект`.
    pub taints: Vec<String>,
    pub labels: BTreeMap<String, String>,
    pub capacity: BTreeMap<String, String>,
    pub allocatable: BTreeMap<String, String>,
    /// `тип: адрес`.
    pub addresses: Vec<String>,
    pub os_image: Option<String>,
    pub kernel_version: Option<String>,
    pub container_runtime: Option<String>,
    pub architecture: Option<String>,
}

pub fn node_details(n: &Node) -> NodeDetails {
    let status = n.status.as_ref();
    let info = status.and_then(|s| s.node_info.as_ref());
    let quantities =
        |m: Option<&BTreeMap<String, k8s_openapi::apimachinery::pkg::api::resource::Quantity>>| {
            m.map(|m| m.iter().map(|(k, q)| (k.clone(), q.0.clone())).collect())
                .unwrap_or_default()
        };
    NodeDetails {
        node: node_row(n),
        conditions: conditions_of(&status.and_then(|s| s.conditions.clone())),
        taints: n
            .spec
            .as_ref()
            .and_then(|s| s.taints.as_ref())
            .map(|ts| {
                ts.iter()
                    .map(|t| match &t.value {
                        Some(v) if !v.is_empty() => format!("{}={}:{}", t.key, v, t.effect),
                        _ => format!("{}:{}", t.key, t.effect),
                    })
                    .collect()
            })
            .unwrap_or_default(),
        labels: n.metadata.labels.clone().unwrap_or_default(),
        capacity: quantities(status.and_then(|s| s.capacity.as_ref())),
        allocatable: quantities(status.and_then(|s| s.allocatable.as_ref())),
        addresses: status
            .and_then(|s| s.addresses.as_ref())
            .map(|a| {
                a.iter()
                    .map(|a| format!("{}: {}", a.type_, a.address))
                    .collect()
            })
            .unwrap_or_default(),
        os_image: info.map(|i| i.os_image.clone()),
        kernel_version: info.map(|i| i.kernel_version.clone()),
        container_runtime: info.map(|i| i.container_runtime_version.clone()),
        architecture: info.map(|i| i.architecture.clone()),
    }
}

pub async fn list_nodes(r: &ReadClient) -> AppResult<Listed<NodeRow>> {
    let nodes: Listed<Node> = r.list_cluster(&ListParams::default()).await?;
    Ok(nodes.map(|n| node_row(&n)))
}

pub async fn get_node(r: &ReadClient, node: &str) -> AppResult<NodeDetails> {
    let n: Node = r.get_cluster(node).await?;
    Ok(node_details(&n))
}

#[cfg(test)]
pub(crate) mod fixtures {
    pub fn node(name: &str, roles: &[&str], ready: &str, pressure: bool) -> serde_json::Value {
        let mut labels = serde_json::Map::new();
        for r in roles {
            labels.insert(format!("node-role.kubernetes.io/{r}"), "".into());
        }
        labels.insert("kubernetes.io/os".into(), "linux".into());
        serde_json::json!({"metadata": {"name": name, "labels": labels,
                "creationTimestamp": "2026-10-25T10:00:00Z"},
            "spec": {"taints": [{"key": "node-role.kubernetes.io/master", "effect": "NoSchedule"},
                {"key": "dedicated", "value": "db", "effect": "NoExecute"}]},
            "status": {
                "conditions": [
                    {"type": "Ready", "status": ready},
                    {"type": "MemoryPressure", "status": if pressure { "True" } else { "False" }}
                ],
                "addresses": [{"type": "InternalIP", "address": "10.0.0.1"},
                    {"type": "Hostname", "address": name}],
                "allocatable": {"cpu": "3500m", "memory": "15Gi", "pods": "250"},
                "capacity": {"cpu": "4", "memory": "16Gi", "pods": "250"},
                "nodeInfo": {"kubeletVersion": "v1.32.5", "osImage": "RHCOS", "kernelVersion": "5.14",
                    "containerRuntimeVersion": "cri-o://1.32", "architecture": "amd64",
                    "bootID": "", "kubeProxyVersion": "", "machineID": "", "operatingSystem": "linux",
                    "systemUUID": ""}}})
    }

    pub fn node_list() -> serde_json::Value {
        serde_json::json!({"apiVersion": "v1", "kind": "NodeList", "metadata": {}, "items": [
            node("m1", &["control-plane", "master"], "True", false),
            node("w1", &["worker"], "True", true),
            node("w2", &["worker"], "Unknown", false),
            node("x1", &[], "True", false)
        ]})
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::k8s::testing::mock_client;

    fn nodes() -> Vec<Node> {
        let list: kube::api::ObjectList<Node> =
            serde_json::from_value(fixtures::node_list()).unwrap();
        list.items
    }

    #[test]
    fn rows_roles_problems_and_summary() {
        let rows: Vec<NodeRow> = nodes().iter().map(node_row).collect();
        assert_eq!(rows[0].roles, ["control-plane", "master"]);
        assert!(rows[0].ready && rows[0].problems.is_empty());
        assert_eq!(rows[0].internal_ip.as_deref(), Some("10.0.0.1"));
        assert_eq!(rows[0].cpu.as_deref(), Some("3500m"));
        assert_eq!(rows[0].taints, 2);
        assert_eq!(rows[0].created, Some(1_792_922_400));
        assert_eq!(rows[1].problems, ["MemoryPressure"]);
        assert_eq!(rows[2].problems, ["NotReady"]);
        assert!(rows[3].roles.is_empty());
        let s = summary(&rows);
        assert_eq!((s.total, s.ready), (4, 3));
        assert_eq!(
            s.roles,
            [
                RoleCount {
                    role: "control-plane".into(),
                    total: 1,
                    ready: 1
                },
                RoleCount {
                    role: "master".into(),
                    total: 1,
                    ready: 1
                },
                RoleCount {
                    role: "worker".into(),
                    total: 2,
                    ready: 1
                },
            ]
        );
    }

    #[test]
    fn details_of_a_node() {
        let d = node_details(&nodes()[1]);
        assert_eq!(d.node.name, "w1");
        assert_eq!(
            d.taints,
            [
                "node-role.kubernetes.io/master:NoSchedule",
                "dedicated=db:NoExecute"
            ]
        );
        assert_eq!(d.capacity["memory"], "16Gi");
        assert_eq!(d.addresses, ["InternalIP: 10.0.0.1", "Hostname: w1"]);
        assert_eq!(d.container_runtime.as_deref(), Some("cri-o://1.32"));
        assert_eq!(d.conditions.len(), 2);
    }

    #[tokio::test]
    async fn list_and_get_read_the_cluster() {
        let (client, server) = mock_client(|s| match s.path() {
            "/api/v1/nodes" => (200, fixtures::node_list()),
            _ => (200, fixtures::node("w1", &["worker"], "True", false)),
        });
        let r = ReadClient::new(client, std::time::Duration::from_secs(5));
        assert_eq!(list_nodes(&r).await.unwrap().items.len(), 4);
        assert_eq!(get_node(&r, "w1").await.unwrap().node.roles, ["worker"]);
        drop(r);
        let seen = server.await.unwrap();
        assert_eq!(seen[1].path(), "/api/v1/nodes/w1");
    }
}
