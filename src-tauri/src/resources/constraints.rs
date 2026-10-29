//! Ограничения рекомендации из кластера (спец. §7.3): `LimitRange` и `ResourceQuota` проекта,
//! allocatable самой крупной ноды (если ноды читаются), HPA workload'а по utilization. Что не
//! прочиталось, то не проверяется: рекомендация без него остаётся.

use k8s_openapi::api::autoscaling::v2::HorizontalPodAutoscaler;
use k8s_openapi::api::core::v1::{LimitRange, Node, ResourceQuota};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use kube::api::ListParams;

use super::current::Amounts;
use super::recommend::{Constraints, HpaTarget, LimitRangeBounds, QuotaRoom};
use crate::k8s::quantity::parse_quantity;
use crate::k8s::read::ReadClient;
use crate::k8s::workloads::WorkloadKind;

type Quantities = std::collections::BTreeMap<String, Quantity>;

fn amounts(m: Option<&Quantities>) -> Amounts {
    let q = |k: &str| m.and_then(|m| m.get(k)).and_then(|v| parse_quantity(&v.0));
    Amounts {
        cpu: q("cpu"),
        memory: q("memory"),
    }
}

/// Пара значений: из всех `LimitRange` — самое строгое (`pick`) или первое заданное.
fn merge(a: Amounts, b: Amounts, pick: fn(f64, f64) -> f64) -> Amounts {
    let one = |x: Option<f64>, y: Option<f64>| match (x, y) {
        (Some(x), Some(y)) => Some(pick(x, y)),
        (x, y) => x.or(y),
    };
    Amounts {
        cpu: one(a.cpu, b.cpu),
        memory: one(a.memory, b.memory),
    }
}

/// Границы контейнера (`type: Container`) по всем `LimitRange` проекта: наибольший минимум,
/// наименьший максимум и предел отношения, значения по умолчанию — первые заданные.
pub fn limit_range_bounds(list: &[LimitRange]) -> Option<LimitRangeBounds> {
    let items: Vec<_> = list
        .iter()
        .filter_map(|l| l.spec.as_ref())
        .flat_map(|s| &s.limits)
        .filter(|i| i.type_ == "Container")
        .collect();
    if items.is_empty() {
        return None;
    }
    let mut b = LimitRangeBounds::default();
    for i in items {
        b.min = merge(b.min, amounts(i.min.as_ref()), f64::max);
        b.max = merge(b.max, amounts(i.max.as_ref()), f64::min);
        b.max_ratio = merge(
            b.max_ratio,
            amounts(i.max_limit_request_ratio.as_ref()),
            f64::min,
        );
        b.default_limit = merge(b.default_limit, amounts(i.default.as_ref()), |x, _| x);
        b.default_request = merge(
            b.default_request,
            amounts(i.default_request.as_ref()),
            |x, _| x,
        );
    }
    Some(b)
}

/// Ресурсы квот, которые меняет рекомендация, с лимитом и занятым.
pub fn quota_rooms(list: &[ResourceQuota]) -> Vec<QuotaRoom> {
    const KEYS: [&str; 6] = [
        "requests.cpu",
        "cpu",
        "requests.memory",
        "memory",
        "limits.cpu",
        "limits.memory",
    ];
    let mut out = Vec::new();
    for q in list {
        let status = q.status.as_ref();
        let hard = status
            .and_then(|s| s.hard.as_ref())
            .or_else(|| q.spec.as_ref().and_then(|s| s.hard.as_ref()));
        let used = status.and_then(|s| s.used.as_ref());
        for key in KEYS {
            let h = hard
                .and_then(|h| h.get(key))
                .and_then(|v| parse_quantity(&v.0));
            if let Some(hard) = h {
                out.push(QuotaRoom {
                    quota: q.metadata.name.clone().unwrap_or_default(),
                    resource: key.to_string(),
                    hard,
                    used: used
                        .and_then(|u| u.get(key))
                        .and_then(|v| parse_quantity(&v.0))
                        .unwrap_or(0.0),
                });
            }
        }
    }
    out
}

/// Allocatable самой крупной ноды: CPU и память — каждый по своей самой крупной.
pub fn largest_node(list: &[Node]) -> Option<Amounts> {
    list.iter()
        .map(|n| amounts(n.status.as_ref().and_then(|s| s.allocatable.as_ref())))
        .reduce(|a, b| merge(a, b, f64::max))
}

/// HPA workload'а по utilization CPU и памяти.
pub fn hpa_targets(
    list: &[HorizontalPodAutoscaler],
    kind: WorkloadKind,
    name: &str,
) -> Vec<HpaTarget> {
    let kind = format!("{kind:?}");
    list.iter()
        .filter_map(|h| Some((h.metadata.name.clone()?, h.spec.as_ref()?)))
        .filter(|(_, s)| s.scale_target_ref.kind == kind && s.scale_target_ref.name == name)
        .flat_map(|(hpa, s)| {
            s.metrics.iter().flatten().filter_map(move |m| {
                let r = m.resource.as_ref()?;
                let target = r.target.average_utilization?;
                Some(HpaTarget {
                    hpa: hpa.clone(),
                    resource: r.name.clone(),
                    target,
                })
            })
        })
        .collect()
}

/// Ограничения из кластера. Ноды читаются, только если `nodes` (право есть); их отказ, как и
/// отказы проекта, ограничение не даёт. Чтение нод — вне проекта: его 403 не сбрасывает кэш прав
/// проекта (`observe`).
pub async fn read_constraints(
    r: &ReadClient,
    namespace: &str,
    kind: WorkloadKind,
    name: &str,
    nodes: bool,
) -> Constraints {
    let ns = Some(namespace);
    let all = ListParams::default();
    let (lr, quotas, hpa, node_list) = tokio::join!(
        r.list::<LimitRange>(ns, &all),
        r.list::<ResourceQuota>(ns, &all),
        r.list::<HorizontalPodAutoscaler>(ns, &all),
        async {
            if !nodes {
                return None;
            }
            crate::k8s::read::observe(r.list_cluster::<Node>(&all))
                .await
                .0
                .ok()
        },
    );
    Constraints {
        limit_range: lr.ok().and_then(|l| limit_range_bounds(&l.items)),
        quotas: quotas.map(|l| quota_rooms(&l.items)).unwrap_or_default(),
        largest_node: node_list.and_then(|l| largest_node(&l.items)),
        hpa: hpa
            .map(|l| hpa_targets(&l.items, kind, name))
            .unwrap_or_default(),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::k8s::testing::{mock_client, status};

    const MI: f64 = 1024.0 * 1024.0;

    fn list(kind: &str, items: serde_json::Value) -> (u16, serde_json::Value) {
        (
            200,
            serde_json::json!({"apiVersion": "v1", "kind": kind, "metadata": {}, "items": items}),
        )
    }

    #[tokio::test]
    async fn constraints_come_from_limit_ranges_quotas_nodes_and_the_hpa() {
        let (client, _) = mock_client(|s| match s.path() {
            "/api/v1/namespaces/team-a/limitranges" => list(
                "LimitRangeList",
                serde_json::json!([
                    {"metadata": {"name": "a"}, "spec": {"limits": [
                        {"type": "Container", "min": {"cpu": "10m"}, "max": {"memory": "2Gi"},
                         "default": {"cpu": "500m"}, "maxLimitRequestRatio": {"cpu": "4"}},
                        {"type": "Pod", "max": {"cpu": "1"}}]}},
                    {"metadata": {"name": "b"}, "spec": {"limits": [
                        {"type": "Container", "min": {"cpu": "50m"}, "max": {"memory": "1Gi"},
                         "default": {"cpu": "1"}}]}}
                ]),
            ),
            "/api/v1/namespaces/team-a/resourcequotas" => list(
                "ResourceQuotaList",
                serde_json::json!([{"metadata": {"name": "compute"},
                    "spec": {"hard": {"requests.cpu": "4"}},
                    "status": {"hard": {"requests.cpu": "4", "limits.memory": "8Gi", "pods": "10"},
                               "used": {"requests.cpu": "3500m"}}}]),
            ),
            "/apis/autoscaling/v2/namespaces/team-a/horizontalpodautoscalers" => list(
                "HorizontalPodAutoscalerList",
                serde_json::json!([
                    {"metadata": {"name": "web"}, "spec": {"maxReplicas": 5,
                        "scaleTargetRef": {"kind": "Deployment", "name": "web", "apiVersion": "apps/v1"},
                        "metrics": [{"type": "Resource", "resource": {"name": "cpu",
                            "target": {"type": "Utilization", "averageUtilization": 80}}},
                            {"type": "Pods", "pods": {"metric": {"name": "rps"},
                            "target": {"type": "AverageValue", "averageValue": "10"}}}]}},
                    {"metadata": {"name": "other"}, "spec": {"maxReplicas": 5,
                        "scaleTargetRef": {"kind": "Deployment", "name": "api"},
                        "metrics": [{"type": "Resource", "resource": {"name": "memory",
                            "target": {"type": "Utilization", "averageUtilization": 70}}}]}}
                ]),
            ),
            "/api/v1/nodes" => list(
                "NodeList",
                serde_json::json!([
                    {"metadata": {"name": "a"}, "status": {"allocatable": {"cpu": "4", "memory": "8Gi"}}},
                    {"metadata": {"name": "b"}, "status": {"allocatable": {"cpu": "8", "memory": "4Gi"}}}
                ]),
            ),
            _ => status(404, "NotFound", "no"),
        });
        let r = ReadClient::new(client, Duration::from_secs(5));
        let c = read_constraints(&r, "team-a", WorkloadKind::Deployment, "web", true).await;
        let lr = c.limit_range.unwrap();
        assert_eq!(lr.min.cpu, Some(0.05), "наибольший минимум");
        assert_eq!(lr.max.memory, Some(1024.0 * MI), "наименьший максимум");
        assert_eq!(lr.default_limit.cpu, Some(0.5), "первый заданный");
        assert_eq!(lr.max_ratio.cpu, Some(4.0));
        assert_eq!(
            c.quotas
                .iter()
                .map(|q| (q.resource.as_str(), q.hard, q.used))
                .collect::<Vec<_>>(),
            [
                ("requests.cpu", 4.0, 3.5),
                ("limits.memory", 8192.0 * MI, 0.0)
            ]
        );
        assert_eq!(
            c.largest_node,
            Some(Amounts {
                cpu: Some(8.0),
                memory: Some(8192.0 * MI)
            })
        );
        assert_eq!(c.hpa.len(), 1);
        assert_eq!((c.hpa[0].resource.as_str(), c.hpa[0].target), ("cpu", 80));
        let c = read_constraints(&r, "team-a", WorkloadKind::Deployment, "web", false).await;
        assert_eq!(c.largest_node, None, "без права на ноды — не читаются");
    }

    #[tokio::test]
    async fn forbidden_parts_give_no_constraint() {
        let (client, _) = mock_client(|_| status(403, "Forbidden", "no"));
        let r = ReadClient::new(client, Duration::from_secs(5));
        let c = read_constraints(&r, "team-a", WorkloadKind::Deployment, "web", true).await;
        assert_eq!(c, Constraints::default());
    }
}
