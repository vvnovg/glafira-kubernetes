//! Обзор кластера (спец. §6.2): платформа и версия, состояние API-сервера по `/readyz?verbose`,
//! ноды, классы Ingress и Gateway, число проектов и часть платформы. Что не читается из-за прав
//! или ошибки, приходит причиной (`Part`), остальное показывается.

use k8s_openapi::api::networking::v1::IngressClass;
use kube::api::{ApiResource, GroupVersionKind, ListParams};
use serde::Serialize;

use crate::error::{AppError, AppResult};
use crate::k8s::cluster::ConnectionInfo;
use crate::k8s::errors::not_allowed;
use crate::k8s::nodes::{NodeSummary, list_nodes, summary};
use crate::k8s::read::ReadClient;
use crate::k8s::view::{Part, condition_flag, conditions};
use crate::platform::PlatformInfo;
use crate::platform::adapters::{PlatformOverview, adapter};

/// Больше стольких непройденных проверок `/readyz` не показывается.
const MAX_FAILED_CHECKS: usize = 20;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterOverview {
    pub platform: PlatformInfo,
    pub server_version: String,
    pub readyz: Readyz,
    pub nodes: Part<NodeSummary>,
    pub ingress_classes: Part<Vec<IngressClassRow>>,
    /// `None` — в кластере нет Gateway API.
    pub gateway_classes: Option<Part<Vec<GatewayClassRow>>>,
    pub projects: Part<ProjectCount>,
    /// Своя часть платформы (OpenShift: обновления, операторы, пулы машин).
    pub platform_details: Option<PlatformOverview>,
}

/// Состояние API-сервера: готов ли и какие проверки не прошли. `error` — ответа с проверками
/// нет вовсе (сеть, права).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Readyz {
    pub ok: bool,
    pub failed: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<AppError>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IngressClassRow {
    pub name: String,
    pub controller: Option<String>,
    /// Класс по умолчанию (аннотация `ingressclass.kubernetes.io/is-default-class`).
    pub default: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GatewayClassRow {
    pub name: String,
    pub controller: Option<String>,
    pub accepted: Option<bool>,
}

/// Сколько проектов видит пользователь: первые 500 и признак «есть ещё».
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectCount {
    pub count: usize,
    pub more: bool,
}

/// Имена непройденных проверок: строки `[-]etcd failed: reason withheld` → `etcd`.
pub fn failed_checks(text: &str) -> Vec<String> {
    text.lines()
        .filter_map(|l| l.trim().strip_prefix("[-]"))
        .map(|rest| rest.split_whitespace().next().unwrap_or(rest).to_string())
        .take(MAX_FAILED_CHECKS)
        .collect()
}

/// `/readyz?verbose` читает любой пользователь (`system:public-info-viewer`). Не готовый
/// API-сервер отвечает ошибкой 5xx со списком проверок в теле.
pub async fn readyz(r: &ReadClient) -> Readyz {
    match r.get_text("/readyz?verbose").await {
        Ok(text) => {
            let failed = failed_checks(&text);
            Readyz {
                ok: failed.is_empty(),
                failed,
                error: None,
            }
        }
        Err(e) => {
            let failed = e
                .params
                .get("message")
                .map(|m| failed_checks(m))
                .unwrap_or_default();
            Readyz {
                ok: false,
                error: failed.is_empty().then_some(e),
                failed,
            }
        }
    }
}

pub fn ingress_class_row(c: &IngressClass) -> IngressClassRow {
    IngressClassRow {
        name: c.metadata.name.clone().unwrap_or_default(),
        controller: c.spec.as_ref().and_then(|s| s.controller.clone()),
        default: c
            .metadata
            .annotations
            .as_ref()
            .and_then(|a| a.get("ingressclass.kubernetes.io/is-default-class"))
            .is_some_and(|v| v == "true"),
    }
}

pub fn gateway_classes() -> ApiResource {
    ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk("gateway.networking.k8s.io", "v1", "GatewayClass"),
        "gatewayclasses",
    )
}

pub async fn ingress_classes(r: &ReadClient) -> AppResult<Vec<IngressClassRow>> {
    let list = r
        .list_cluster::<IngressClass>(&ListParams::default())
        .await?;
    Ok(list.items.iter().map(ingress_class_row).collect())
}

async fn gateway_class_rows(r: &ReadClient) -> AppResult<Vec<GatewayClassRow>> {
    let list = r
        .list_dynamic(&gateway_classes(), None, &ListParams::default())
        .await?;
    Ok(list
        .items
        .iter()
        .map(|o| GatewayClassRow {
            name: o.metadata.name.clone().unwrap_or_default(),
            controller: o.data["spec"]["controllerName"].as_str().map(String::from),
            accepted: condition_flag(&conditions(&o.data["status"]["conditions"]), "Accepted"),
        })
        .collect())
}

/// Обзор кластера; запросы идут параллельно. Ноды и проекты без прав не читаются.
pub async fn cluster_overview(r: &ReadClient, info: &ConnectionInfo) -> ClusterOverview {
    let caps = &info.capabilities;
    let nodes = async {
        if !caps.list_nodes {
            return Part::err(not_allowed("list", "nodes", None));
        }
        list_nodes(r).await.map(|l| summary(&l.items)).into()
    };
    let projects = async {
        if !caps.list_projects {
            let resource = if info.api_groups.projects {
                "projects"
            } else {
                "namespaces"
            };
            return Part::err(not_allowed("list", resource, None));
        }
        r.project_names(info.api_groups.projects)
            .await
            .map(|(names, more)| ProjectCount {
                count: names.len(),
                more,
            })
            .into()
    };
    let gateways = async {
        match info.api_groups.gateway_api {
            true => Some(gateway_class_rows(r).await.into()),
            false => None,
        }
    };
    let (readyz, nodes, ingress_classes, gateway_classes, projects, platform_details) = tokio::join!(
        readyz(r),
        nodes,
        async { ingress_classes(r).await.into() },
        gateways,
        projects,
        adapter(info.platform.kind).overview(r, caps),
    );
    ClusterOverview {
        platform: info.platform.clone(),
        server_version: info.server_version.clone(),
        readyz,
        nodes,
        ingress_classes,
        gateway_classes,
        projects,
        platform_details,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::k8s::cluster::Capabilities;
    use crate::k8s::testing::{Seen, mock_raw};
    use crate::platform::{ApiGroups, Discovery, detect};
    use std::time::Duration;

    fn info(caps: Capabilities, groups: ApiGroups) -> ConnectionInfo {
        ConnectionInfo {
            user: "u".into(),
            groups: vec![],
            server_version: "v1.32.5".into(),
            platform: detect(&Discovery {
                git_version: "v1.32.5".into(),
                ..Default::default()
            }),
            api_groups: groups,
            capabilities: caps,
            token_expires_at: None,
        }
    }

    fn json(v: serde_json::Value) -> (u16, Vec<u8>) {
        (200, serde_json::to_vec(&v).unwrap())
    }

    fn cluster(s: &Seen) -> (u16, Vec<u8>) {
        match s.path() {
            "/readyz" => (
                500,
                b"[+]ping ok\n[-]etcd failed: reason withheld\n[-]poststarthook/x failed\nreadyz check failed\n"
                    .to_vec(),
            ),
            "/api/v1/nodes" => json(crate::k8s::nodes::fixtures::node_list()),
            "/apis/networking.k8s.io/v1/ingressclasses" => json(serde_json::json!({
                "apiVersion": "networking.k8s.io/v1", "kind": "IngressClassList", "metadata": {},
                "items": [
                    {"metadata": {"name": "nginx", "annotations": {"ingressclass.kubernetes.io/is-default-class": "true"}},
                        "spec": {"controller": "k8s.io/ingress-nginx"}},
                    {"metadata": {"name": "traefik"}, "spec": {"controller": "traefik.io/ingress-controller"}}
                ]})),
            "/apis/gateway.networking.k8s.io/v1/gatewayclasses" => json(serde_json::json!({
                "apiVersion": "gateway.networking.k8s.io/v1", "kind": "GatewayClassList", "metadata": {},
                "items": [{"metadata": {"name": "cilium"}, "spec": {"controllerName": "io.cilium/gateway-controller"},
                    "status": {"conditions": [{"type": "Accepted", "status": "True"}]}}]})),
            "/api/v1/namespaces" => json(serde_json::json!({"apiVersion": "v1", "kind": "NamespaceList",
                "metadata": {}, "items": [{"metadata": {"name": "a"}}, {"metadata": {"name": "b"}}]})),
            other => (404, format!("{other} not found").into_bytes()),
        }
    }

    #[test]
    fn failed_checks_of_readyz() {
        assert!(failed_checks("[+]ping ok\nreadyz check passed").is_empty());
        assert_eq!(
            failed_checks("[+]ping ok\n[-]etcd failed: reason withheld\n[-]informer-sync failed"),
            ["etcd", "informer-sync"]
        );
        let many: String = (0..30).map(|i| format!("[-]c{i} failed\n")).collect();
        assert_eq!(failed_checks(&many).len(), 20);
    }

    #[tokio::test]
    async fn overview_of_kubernetes_with_rights() {
        let (client, _server) = mock_raw(cluster);
        let r = ReadClient::new(client, Duration::from_secs(5));
        let caps = Capabilities {
            list_projects: true,
            list_nodes: true,
            ..Default::default()
        };
        let groups = ApiGroups {
            gateway_api: true,
            ..Default::default()
        };
        let o = cluster_overview(&r, &info(caps, groups)).await;
        assert_eq!(o.platform.label, "Kubernetes 1.32");
        assert!(!o.readyz.ok);
        assert_eq!(o.readyz.failed, ["etcd", "poststarthook/x"]);
        assert!(o.readyz.error.is_none());
        assert_eq!(
            o.nodes.data.as_ref().map(|n| (n.total, n.ready)),
            Some((4, 3))
        );
        let classes = o.ingress_classes.data.unwrap();
        assert!(classes[0].default && !classes[1].default);
        let gw = o.gateway_classes.unwrap().data.unwrap();
        assert_eq!(gw[0].accepted, Some(true));
        assert_eq!(
            o.projects.data,
            Some(ProjectCount {
                count: 2,
                more: false
            })
        );
        assert!(o.platform_details.is_none());
    }

    #[tokio::test]
    async fn overview_without_rights_reads_only_what_is_allowed() {
        let (client, server) = mock_raw(cluster);
        let r = ReadClient::new(client, Duration::from_secs(5));
        let o = cluster_overview(&r, &info(Capabilities::default(), ApiGroups::default())).await;
        assert_eq!(o.nodes.error.unwrap().params["resource"], "nodes");
        assert_eq!(o.projects.error.unwrap().params["resource"], "namespaces");
        assert!(o.gateway_classes.is_none(), "Gateway API нет");
        drop(r);
        let seen = server.await.unwrap();
        assert!(
            !seen
                .iter()
                .any(|s| s.path() == "/api/v1/nodes" || s.path() == "/api/v1/namespaces")
        );
    }

    #[tokio::test]
    async fn api_server_that_does_not_answer() {
        let (client, _server) = mock_raw(|_| (403, b"forbidden".to_vec()));
        let r = ReadClient::new(client, Duration::from_secs(5));
        let z = readyz(&r).await;
        assert!(!z.ok && z.failed.is_empty());
        assert_eq!(z.error.unwrap().code, crate::error::ErrorCode::Forbidden);
    }
}
