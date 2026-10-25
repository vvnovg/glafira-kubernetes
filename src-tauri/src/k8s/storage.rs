//! Хранилище проекта (спец. §6.3): PVC — статус, класс, ёмкость, режим доступа и кем смонтирован.

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::{PersistentVolumeClaim, Pod};
use kube::api::ListParams;
use serde::Serialize;

use crate::error::AppResult;
use crate::k8s::read::{Listed, ReadClient};
use crate::k8s::view::{created, name};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PvcRow {
    pub name: String,
    /// `Pending`, `Bound`, `Lost`.
    pub phase: Option<String>,
    pub class: Option<String>,
    /// Выделено (`status.capacity`) и запрошено (`spec.resources.requests`).
    pub capacity: Option<String>,
    pub requested: Option<String>,
    pub access_modes: Vec<String>,
    pub volume_mode: Option<String>,
    pub volume: Option<String>,
    /// Поды, которые монтируют PVC; `None` — поды не читаются.
    pub mounted_by: Option<Vec<String>>,
    pub created: Option<i64>,
}

pub fn pvc_row(p: &PersistentVolumeClaim, mounted_by: Option<Vec<String>>) -> PvcRow {
    let spec = p.spec.as_ref();
    let status = p.status.as_ref();
    let storage = |m: Option<
        &BTreeMap<String, k8s_openapi::apimachinery::pkg::api::resource::Quantity>,
    >| { m.and_then(|m| m.get("storage")).map(|q| q.0.clone()) };
    PvcRow {
        name: name(&p.metadata),
        phase: status.and_then(|s| s.phase.clone()),
        class: spec.and_then(|s| s.storage_class_name.clone()),
        capacity: storage(status.and_then(|s| s.capacity.as_ref())),
        requested: storage(
            spec.and_then(|s| s.resources.as_ref())
                .and_then(|r| r.requests.as_ref()),
        ),
        access_modes: spec
            .and_then(|s| s.access_modes.clone())
            .unwrap_or_default(),
        volume_mode: spec.and_then(|s| s.volume_mode.clone()),
        volume: spec.and_then(|s| s.volume_name.clone()),
        mounted_by,
        created: created(&p.metadata),
    }
}

/// Какие поды монтируют какие PVC.
pub fn mounts(pods: &[Pod]) -> BTreeMap<String, Vec<String>> {
    let mut out: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for p in pods {
        let claims = p
            .spec
            .as_ref()
            .and_then(|s| s.volumes.as_ref())
            .into_iter()
            .flatten()
            .filter_map(|v| v.persistent_volume_claim.as_ref())
            .map(|c| c.claim_name.clone());
        for claim in claims {
            out.entry(claim).or_default().push(name(&p.metadata));
        }
    }
    out
}

/// PVC проекта; кто их монтирует, видно, если читаются поды.
pub async fn list_pvcs(r: &ReadClient, namespace: &str) -> AppResult<Listed<PvcRow>> {
    let ns = Some(namespace);
    let lp = ListParams::default();
    let (pvcs, pods) = tokio::join!(
        r.list::<PersistentVolumeClaim>(ns, &lp),
        r.list::<Pod>(ns, &lp)
    );
    let mounts = pods.ok().map(|l| mounts(&l.items));
    Ok(pvcs?.map(|p| {
        let by = mounts
            .as_ref()
            .map(|m| m.get(&name(&p.metadata)).cloned().unwrap_or_default());
        pvc_row(&p, by)
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::k8s::testing::{mock_client, status};
    use std::time::Duration;

    fn pvcs() -> serde_json::Value {
        serde_json::json!({"apiVersion": "v1", "kind": "PersistentVolumeClaimList", "metadata": {}, "items": [
            {"metadata": {"name": "data"},
                "spec": {"storageClassName": "standard", "accessModes": ["ReadWriteOnce"], "volumeMode": "Filesystem",
                    "volumeName": "pvc-1", "resources": {"requests": {"storage": "10Gi"}}},
                "status": {"phase": "Bound", "capacity": {"storage": "10Gi"}}},
            {"metadata": {"name": "cache"},
                "spec": {"accessModes": ["ReadWriteMany"], "resources": {"requests": {"storage": "1Gi"}}},
                "status": {"phase": "Pending"}}
        ]})
    }

    #[tokio::test]
    async fn claims_with_the_pods_that_mount_them() {
        let (client, _server) = mock_client(|s| match s.path() {
            "/api/v1/namespaces/team-a/persistentvolumeclaims" => (200, pvcs()),
            _ => (
                200,
                serde_json::json!({"apiVersion": "v1", "kind": "PodList", "metadata": {}, "items": [
                    {"metadata": {"name": "db-0"}, "spec": {"containers": [],
                        "volumes": [{"name": "d", "persistentVolumeClaim": {"claimName": "data"}}]}},
                    {"metadata": {"name": "web"}, "spec": {"containers": [], "volumes": [{"name": "t", "emptyDir": {}}]}}
                ]}),
            ),
        });
        let r = ReadClient::new(client, Duration::from_secs(5));
        let l = list_pvcs(&r, "team-a").await.unwrap();
        let data = &l.items[0];
        assert_eq!(data.phase.as_deref(), Some("Bound"));
        assert_eq!(data.class.as_deref(), Some("standard"));
        assert_eq!(
            (data.capacity.as_deref(), data.requested.as_deref()),
            (Some("10Gi"), Some("10Gi"))
        );
        assert_eq!(data.access_modes, ["ReadWriteOnce"]);
        assert_eq!(data.mounted_by.as_deref(), Some(&["db-0".to_string()][..]));
        let cache = &l.items[1];
        assert_eq!(
            (cache.phase.as_deref(), cache.capacity.as_deref()),
            (Some("Pending"), None)
        );
        assert_eq!(cache.mounted_by.as_deref(), Some(&[][..]));
    }

    #[tokio::test]
    async fn without_pods_it_is_unknown_who_mounts() {
        let (client, _server) = mock_client(|s| match s.path() {
            "/api/v1/namespaces/team-a/persistentvolumeclaims" => (200, pvcs()),
            _ => status(403, "Forbidden", "pods is forbidden"),
        });
        let r = ReadClient::new(client, Duration::from_secs(5));
        let l = list_pvcs(&r, "team-a").await.unwrap();
        assert!(l.items.iter().all(|p| p.mounted_by.is_none()));
    }
}
