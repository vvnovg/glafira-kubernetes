//! `metrics.k8s.io` (спец. §7.1): текущее потребление подов (`PodMetrics`) — одна точка на
//! контейнер, без истории. Обычно доступно с ролью `view`.

use kube::api::{ApiResource, GroupVersionKind};
use serde::Serialize;

use super::prometheus::{Failure, Reason, SourceInfo, SourceKind, SourceOrigin};
use crate::k8s::quantity::parse_quantity;
use crate::k8s::read::{ReadClient, by_labels};

/// `pods.metrics.k8s.io/v1beta1`.
pub fn pod_metrics() -> ApiResource {
    ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk("metrics.k8s.io", "v1beta1", "PodMetrics"),
        "pods",
    )
}

/// Источник `metrics.k8s.io` для интерфейса и модели.
pub fn info() -> SourceInfo {
    SourceInfo {
        kind: SourceKind::MetricsServer,
        origin: SourceOrigin::Auto,
        location: "metrics.k8s.io".into(),
        history: false,
    }
}

/// Текущее потребление контейнера: ядра и байты.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerNow {
    pub name: String,
    pub cpu: Option<f64>,
    pub memory: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PodNow {
    pub pod: String,
    pub containers: Vec<ContainerNow>,
}

/// Текущее потребление подов проекта (с селектором меток — подов workload'а). Нет группы API —
/// `NotFound`, нет прав — `Forbidden`.
pub async fn pods_now(
    r: &ReadClient,
    namespace: &str,
    selector: Option<&str>,
) -> Result<Vec<PodNow>, Failure> {
    let l = r
        .list_dynamic(&pod_metrics(), Some(namespace), &by_labels(selector))
        .await
        .map_err(Failure::from_error)?;
    Ok(l.items
        .into_iter()
        .filter_map(|o| {
            let pod = o.metadata.name?;
            let containers = o.data["containers"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|c| {
                    let q = |k: &str| c["usage"][k].as_str().and_then(parse_quantity);
                    Some(ContainerNow {
                        name: c["name"].as_str()?.to_string(),
                        cpu: q("cpu"),
                        memory: q("memory"),
                    })
                })
                .collect();
            Some(PodNow { pod, containers })
        })
        .collect())
}

/// Есть ли группа `metrics.k8s.io` в кластере (права в проекте проверит само чтение).
pub async fn installed(r: &ReadClient) -> Result<(), Failure> {
    r.get_text("/apis/metrics.k8s.io/v1beta1")
        .await
        .map(|_| ())
        .map_err(|e| {
            let mut f = Failure::from_error(e);
            if f.reason == Reason::BadResponse {
                f.reason = Reason::NotFound;
            }
            f
        })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::k8s::testing::{mock_client, status};

    #[tokio::test]
    async fn pod_metrics_become_cores_and_bytes() {
        let (client, _) = mock_client(|s| {
            assert_eq!(
                s.path(),
                "/apis/metrics.k8s.io/v1beta1/namespaces/team-a/pods"
            );
            assert!(s.uri.contains("labelSelector=app%3Dweb"), "{}", s.uri);
            (
                200,
                serde_json::json!({"apiVersion": "metrics.k8s.io/v1beta1", "kind": "PodMetricsList",
                    "metadata": {}, "items": [{"metadata": {"name": "web-1", "namespace": "team-a"},
                    "timestamp": "2026-10-28T10:00:00Z", "window": "30s",
                    "containers": [{"name": "app", "usage": {"cpu": "250m", "memory": "128Mi"}},
                                   {"name": "sidecar", "usage": {"cpu": "12345n"}}]}]}),
            )
        });
        let r = ReadClient::new(client, Duration::from_secs(5));
        let pods = pods_now(&r, "team-a", Some("app=web")).await.unwrap();
        assert_eq!(pods[0].pod, "web-1");
        assert_eq!(
            pods[0].containers[0],
            ContainerNow {
                name: "app".into(),
                cpu: Some(0.25),
                memory: Some(134_217_728.0)
            }
        );
        assert_eq!(pods[0].containers[1].memory, None);
        assert!((pods[0].containers[1].cpu.unwrap() - 0.000_012_345).abs() < 1e-12);
    }

    #[tokio::test]
    async fn missing_group_and_rights_are_reasons() {
        for (code, reason) in [(404, Reason::NotFound), (403, Reason::Forbidden)] {
            let (client, _) = mock_client(move |_| status(code, "X", "no"));
            let r = ReadClient::new(client, Duration::from_secs(5));
            assert_eq!(
                pods_now(&r, "team-a", None).await.unwrap_err().reason,
                reason
            );
            assert_eq!(installed(&r).await.unwrap_err().reason, reason);
        }
    }
}
