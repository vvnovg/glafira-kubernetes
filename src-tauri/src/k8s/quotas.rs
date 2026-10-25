//! Квоты проекта (спец. §6.3): ResourceQuota — использовано и лимит по каждому ресурсу,
//! LimitRange — границы и значения по умолчанию.

use std::collections::{BTreeMap, BTreeSet};

use k8s_openapi::api::core::v1::{LimitRange, ResourceQuota};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use kube::api::ListParams;
use serde::Serialize;

use crate::k8s::quantity::ratio;
use crate::k8s::read::ReadClient;
use crate::k8s::view::{Part, name};

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QuotaEntry {
    /// `requests.cpu`, `limits.memory`, `pods`, `count/deployments.apps`…
    pub resource: String,
    pub used: Option<String>,
    pub hard: String,
    /// Использовано / лимит (0,9 — 90 %), если оба разобраны.
    pub ratio: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QuotaView {
    pub name: String,
    pub scopes: Vec<String>,
    pub entries: Vec<QuotaEntry>,
}

/// Граница LimitRange для одного ресурса одного вида объектов.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LimitView {
    /// `Container`, `Pod`, `PersistentVolumeClaim`.
    #[serde(rename = "type")]
    pub kind: String,
    pub resource: String,
    pub min: Option<String>,
    pub max: Option<String>,
    pub default: Option<String>,
    pub default_request: Option<String>,
    pub max_limit_request_ratio: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LimitRangeView {
    pub name: String,
    pub limits: Vec<LimitView>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QuotasView {
    pub quotas: Part<Vec<QuotaView>>,
    pub limit_ranges: Part<Vec<LimitRangeView>>,
}

pub fn quota_view(q: &ResourceQuota) -> QuotaView {
    let hard = q
        .status
        .as_ref()
        .and_then(|s| s.hard.clone())
        .or_else(|| q.spec.as_ref().and_then(|s| s.hard.clone()))
        .unwrap_or_default();
    let used = q
        .status
        .as_ref()
        .and_then(|s| s.used.clone())
        .unwrap_or_default();
    QuotaView {
        name: name(&q.metadata),
        scopes: q
            .spec
            .as_ref()
            .and_then(|s| s.scopes.clone())
            .unwrap_or_default(),
        entries: hard
            .iter()
            .map(|(resource, h)| {
                let u = used.get(resource).map(|u| u.0.clone());
                QuotaEntry {
                    resource: resource.clone(),
                    ratio: u.as_deref().and_then(|u| ratio(u, &h.0)),
                    used: u,
                    hard: h.0.clone(),
                }
            })
            .collect(),
    }
}

pub fn limit_range_view(l: &LimitRange) -> LimitRangeView {
    let get = |m: &Option<BTreeMap<String, Quantity>>, r: &str| {
        m.as_ref().and_then(|m| m.get(r)).map(|q| q.0.clone())
    };
    let limits = l
        .spec
        .as_ref()
        .map(|s| {
            s.limits
                .iter()
                .flat_map(|item| {
                    let resources: BTreeSet<String> = [
                        &item.min,
                        &item.max,
                        &item.default,
                        &item.default_request,
                        &item.max_limit_request_ratio,
                    ]
                    .into_iter()
                    .flatten()
                    .flat_map(|m| m.keys().cloned())
                    .collect();
                    resources
                        .into_iter()
                        .map(|r| LimitView {
                            kind: item.type_.clone(),
                            min: get(&item.min, &r),
                            max: get(&item.max, &r),
                            default: get(&item.default, &r),
                            default_request: get(&item.default_request, &r),
                            max_limit_request_ratio: get(&item.max_limit_request_ratio, &r),
                            resource: r,
                        })
                        .collect::<Vec<_>>()
                })
                .collect()
        })
        .unwrap_or_default();
    LimitRangeView {
        name: name(&l.metadata),
        limits,
    }
}

pub async fn get_quotas(r: &ReadClient, namespace: &str) -> QuotasView {
    let ns = Some(namespace);
    let lp = ListParams::default();
    let (quotas, ranges) = tokio::join!(
        r.list::<ResourceQuota>(ns, &lp),
        r.list::<LimitRange>(ns, &lp)
    );
    QuotasView {
        quotas: quotas
            .map(|l| l.items.iter().map(quota_view).collect())
            .into(),
        limit_ranges: ranges
            .map(|l| l.items.iter().map(limit_range_view).collect())
            .into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::k8s::testing::mock_client;
    use std::time::Duration;

    #[tokio::test]
    async fn quotas_with_usage_and_limit_ranges_per_resource() {
        let (client, _server) = mock_client(|s| match s.path() {
            "/api/v1/namespaces/team-a/resourcequotas" => (
                200,
                serde_json::json!({"apiVersion": "v1", "kind": "ResourceQuotaList", "metadata": {}, "items": [
                    {"metadata": {"name": "compute"},
                        "spec": {"hard": {"requests.cpu": "4", "limits.memory": "8Gi", "pods": "20"},
                            "scopes": ["NotTerminating"]},
                        "status": {"hard": {"requests.cpu": "4", "limits.memory": "8Gi", "pods": "20"},
                            "used": {"requests.cpu": "3600m", "limits.memory": "2Gi"}}}]}),
            ),
            _ => (
                200,
                serde_json::json!({"apiVersion": "v1", "kind": "LimitRangeList", "metadata": {}, "items": [
                {"metadata": {"name": "defaults"}, "spec": {"limits": [
                    {"type": "Container", "default": {"cpu": "500m", "memory": "512Mi"},
                        "defaultRequest": {"cpu": "100m"}, "max": {"memory": "2Gi"},
                        "maxLimitRequestRatio": {"cpu": "10"}},
                    {"type": "PersistentVolumeClaim", "min": {"storage": "1Gi"}}
                ]}}]}),
            ),
        });
        let r = ReadClient::new(client, Duration::from_secs(5));
        let v = get_quotas(&r, "team-a").await;
        let q = &v.quotas.data.unwrap()[0];
        assert_eq!(q.scopes, ["NotTerminating"]);
        let cpu = q
            .entries
            .iter()
            .find(|e| e.resource == "requests.cpu")
            .unwrap();
        assert_eq!(cpu.used.as_deref(), Some("3600m"));
        assert!((cpu.ratio.unwrap() - 0.9).abs() < 1e-9);
        let pods = q.entries.iter().find(|e| e.resource == "pods").unwrap();
        assert_eq!((pods.used.as_deref(), pods.ratio), (None, None));
        let lr = &v.limit_ranges.data.unwrap()[0];
        let views: Vec<_> = lr
            .limits
            .iter()
            .map(|l| (l.kind.as_str(), l.resource.as_str()))
            .collect();
        assert_eq!(
            views,
            [
                ("Container", "cpu"),
                ("Container", "memory"),
                ("PersistentVolumeClaim", "storage")
            ]
        );
        let cpu = &lr.limits[0];
        assert_eq!(
            (
                cpu.default.as_deref(),
                cpu.default_request.as_deref(),
                cpu.max_limit_request_ratio.as_deref()
            ),
            (Some("500m"), Some("100m"), Some("10"))
        );
        assert_eq!(lr.limits[1].max.as_deref(), Some("2Gi"));
    }
}
