//! Что Glafira узнаёт при подключении: кто пользователь, платформа и группы API, версия и права
//! (спец. §4.3, §5.3–5.4). Только чтение; каждый запрос ограничен таймаутом профиля.

use std::future::Future;
use std::time::Duration;

use k8s_openapi::api::authentication::v1::SelfSubjectReview;
use k8s_openapi::api::authorization::v1::{
    ResourceAttributes, SelfSubjectAccessReview, SelfSubjectAccessReviewSpec,
};
use k8s_openapi::api::core::v1::{Namespace, Node};
use kube::api::{Api, ApiResource, DynamicObject, GroupVersionKind, ListParams, PostParams};
use serde::Serialize;

use crate::error::AppResult;
use crate::k8s::dropapp;
use crate::k8s::errors::{from_kube, timeout};
use crate::platform::{ApiGroups, Discovery, PlatformInfo, PlatformKind, ProjectSource, detect};

/// Что пользователь может читать на уровне кластера. Без права соответствующие узлы и вкладки
/// не показываются.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Capabilities {
    /// Список проектов (`project.openshift.io`, `projects.dropapp.ru`) или namespace.
    pub list_projects: bool,
    pub list_nodes: bool,
    pub list_all_pods: bool,
    pub read_cluster_operators: bool,
    pub read_cluster_version: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionInfo {
    pub user: String,
    pub groups: Vec<String>,
    /// `gitVersion` API-сервера, например `v1.32.5`.
    pub server_version: String,
    pub platform: PlatformInfo,
    pub api_groups: ApiGroups,
    pub capabilities: Capabilities,
    /// Когда истекает токен входа (секунды Unix), если платформа это сообщает (спец. §5.2).
    pub token_expires_at: Option<i64>,
}

/// Итог discovery: версия API-сервера, платформа и группы API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Discovered {
    pub server_version: String,
    pub platform: PlatformInfo,
    pub api_groups: ApiGroups,
}

/// Операция с таймаутом запроса; ошибки `kube` переводятся в `AppError`.
pub async fn timed<T>(limit: Duration, fut: impl Future<Output = kube::Result<T>>) -> AppResult<T> {
    match tokio::time::timeout(limit, fut).await {
        Ok(r) => r.map_err(|e| from_kube(&e)),
        Err(_) => Err(timeout()),
    }
}

/// Имя и группы текущего пользователя (`SelfSubjectReview`, Kubernetes 1.28+).
pub async fn whoami(client: &kube::Client, limit: Duration) -> AppResult<(String, Vec<String>)> {
    let api: Api<SelfSubjectReview> = Api::all(client.clone());
    let r = timed(
        limit,
        api.create(&PostParams::default(), &SelfSubjectReview::default()),
    )
    .await?;
    let info = r.status.and_then(|s| s.user_info).unwrap_or_default();
    Ok((
        info.username.unwrap_or_default(),
        info.groups.unwrap_or_default(),
    ))
}

/// Версия OpenShift из ClusterVersion `version`; нет прав или объекта — `None`.
async fn cluster_version(client: &kube::Client, limit: Duration) -> Option<String> {
    let ar = ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk("config.openshift.io", "v1", "ClusterVersion"),
        "clusterversions",
    );
    let api: Api<DynamicObject> = Api::all_with(client.clone(), &ar);
    match timed(limit, api.get("version")).await {
        Ok(cv) => cv.data["status"]["desired"]["version"]
            .as_str()
            .map(String::from),
        Err(e) => {
            tracing::debug!("cluster version is not readable: {}", e.message);
            None
        }
    }
}

/// Версия DropApp из первого ClusterMetadata; нет прав, объекта или поля — `None`.
async fn dropapp_version(client: &kube::Client, limit: Duration) -> Option<String> {
    let api: Api<DynamicObject> = Api::all_with(client.clone(), &dropapp::cluster_metadatas());
    match timed(limit, api.list(&ListParams::default().limit(1))).await {
        Ok(list) => list.items.first().and_then(dropapp::dapp_version),
        Err(e) => {
            tracing::debug!("cluster metadata is not readable: {}", e.message);
            None
        }
    }
}

/// Имена меток первой ноды (по ним узнаётся AKS); нет прав — пусто.
async fn first_node_labels(client: &kube::Client, limit: Duration) -> Vec<String> {
    let api: Api<Node> = Api::all(client.clone());
    match timed(limit, api.list(&ListParams::default().limit(1))).await {
        Ok(list) => list
            .items
            .into_iter()
            .next()
            .and_then(|n| n.metadata.labels)
            .map(|labels| labels.into_keys().collect())
            .unwrap_or_default(),
        Err(_) => Vec::new(),
    }
}

/// Версия API-сервера, группы API и платформа (спец. §4.3). ClusterVersion читается только
/// у OpenShift, ClusterMetadata — только у DropApp, ноды — только если дистрибутив Kubernetes
/// не узнан по версии и группам.
pub async fn discover(client: &kube::Client, limit: Duration) -> AppResult<Discovered> {
    let version = timed(limit, client.apiserver_version()).await?;
    let groups = timed(limit, client.list_api_groups()).await?;
    let mut d = Discovery {
        group_versions: groups
            .groups
            .iter()
            .flat_map(|g| g.versions.iter().map(|v| v.group_version.clone()))
            .collect(),
        git_version: version.git_version,
        ..Default::default()
    };
    if d.has_group("config.openshift.io") {
        d.cluster_version = cluster_version(client, limit).await;
    } else if d.has_group("config.dropapp.ru") {
        d.dropapp_version = dropapp_version(client, limit).await;
    }
    let mut platform = detect(&d);
    if platform.kind == PlatformKind::Kubernetes && platform.distribution.is_none() {
        d.node_labels = first_node_labels(client, limit).await;
        platform = detect(&d);
    }
    Ok(Discovered {
        api_groups: ApiGroups::of(&d),
        server_version: d.git_version,
        platform,
    })
}

/// Разрешено ли действие (`SelfSubjectAccessReview`). Ошибка самой проверки — «нет».
pub async fn can_i(
    client: &kube::Client,
    limit: Duration,
    verb: &str,
    group: &str,
    resource: &str,
    namespace: Option<&str>,
) -> bool {
    let api: Api<SelfSubjectAccessReview> = Api::all(client.clone());
    let review = SelfSubjectAccessReview {
        spec: SelfSubjectAccessReviewSpec {
            resource_attributes: Some(ResourceAttributes {
                verb: Some(verb.into()),
                group: Some(group.into()),
                resource: Some(resource.into()),
                namespace: namespace.map(String::from),
                ..Default::default()
            }),
            ..Default::default()
        },
        ..Default::default()
    };
    match timed(limit, api.create(&PostParams::default(), &review)).await {
        Ok(r) => r.status.is_some_and(|s| s.allowed),
        Err(e) => {
            tracing::warn!(verb, resource, "access review failed: {}", e.message);
            false
        }
    }
}

/// Права уровня кластера; проверки идут параллельно. ClusterOperators и ClusterVersion
/// спрашиваются только у OpenShift.
pub async fn capabilities(client: &kube::Client, limit: Duration, d: &Discovered) -> Capabilities {
    let open_shift = d.platform.kind == PlatformKind::Openshift;
    let source = d.api_groups.project_source();
    let projects = can_i(
        client,
        limit,
        "list",
        source.group(),
        source.resource(),
        None,
    );
    let operators = async {
        open_shift
            && can_i(
                client,
                limit,
                "list",
                "config.openshift.io",
                "clusteroperators",
                None,
            )
            .await
    };
    let version = async {
        open_shift
            && can_i(
                client,
                limit,
                "get",
                "config.openshift.io",
                "clusterversions",
                None,
            )
            .await
    };
    let (list_projects, list_nodes, list_all_pods, read_cluster_operators, read_cluster_version) = tokio::join!(
        projects,
        can_i(client, limit, "list", "", "nodes", None),
        can_i(client, limit, "list", "", "pods", None),
        operators,
        version,
    );
    Capabilities {
        list_projects,
        list_nodes,
        list_all_pods,
        read_cluster_operators,
        read_cluster_version,
    }
}

/// Имена проектов (`projects` группы `project.openshift.io`, иначе `projects.dropapp.ru`, иначе
/// `namespaces`): первые 500 и признак «есть ещё».
pub async fn list_project_names(
    client: &kube::Client,
    limit: Duration,
    source: ProjectSource,
) -> AppResult<(Vec<String>, bool)> {
    let params = ListParams::default().limit(500);
    let list = if let Some(ar) = crate::k8s::projects::project_resource(source) {
        let api: Api<DynamicObject> = Api::all_with(client.clone(), &ar);
        let l = timed(limit, api.list(&params)).await?;
        let more = l.metadata.continue_.is_some_and(|c| !c.is_empty());
        (
            l.items
                .into_iter()
                .filter_map(|p| p.metadata.name)
                .collect(),
            more,
        )
    } else {
        let api: Api<Namespace> = Api::all(client.clone());
        let l = timed(limit, api.list(&params)).await?;
        let more = l.metadata.continue_.is_some_and(|c| !c.is_empty());
        (
            l.items
                .into_iter()
                .filter_map(|n| n.metadata.name)
                .collect(),
            more,
        )
    };
    Ok(list)
}

/// Всё, что Glafira узнаёт при подключении. Первая ошибка («кто я», discovery) прерывает
/// подключение; ошибки проверок прав дают «нет права».
pub async fn probe(client: &kube::Client, limit: Duration) -> AppResult<ConnectionInfo> {
    let (user, groups) = whoami(client, limit).await?;
    let d = discover(client, limit).await?;
    let capabilities = capabilities(client, limit, &d).await;
    Ok(ConnectionInfo {
        user,
        groups,
        server_version: d.server_version,
        platform: d.platform,
        api_groups: d.api_groups,
        capabilities,
        token_expires_at: None,
    })
}

#[cfg(test)]
pub(crate) mod fixtures {
    use crate::k8s::testing::Seen;

    /// Ответы кластера OpenShift: пользователь `dev1`, разрешены только проекты и поды
    /// (ClusterVersion и ноды не читаются).
    pub fn openshift(s: &Seen) -> (u16, serde_json::Value) {
        match s.path() {
            "/apis/authentication.k8s.io/v1/selfsubjectreviews" => (
                201,
                serde_json::json!({"apiVersion": "authentication.k8s.io/v1", "kind": "SelfSubjectReview",
                    "metadata": {}, "status": {"userInfo": {"username": "dev1",
                    "groups": ["system:authenticated", "team-a"]}}}),
            ),
            "/version" => (
                200,
                serde_json::json!({"major": "1", "minor": "32", "gitVersion": "v1.32.5",
                    "gitCommit": "x", "gitTreeState": "clean", "buildDate": "2026-01-01T00:00:00Z",
                    "goVersion": "go1.23", "compiler": "gc", "platform": "linux/amd64"}),
            ),
            "/apis" => (
                200,
                serde_json::json!({"kind": "APIGroupList", "apiVersion": "v1", "groups": [
                    {"name": "apps", "versions": [{"groupVersion": "apps/v1", "version": "v1"}]},
                    {"name": "config.openshift.io", "versions": [{"groupVersion": "config.openshift.io/v1", "version": "v1"}]},
                    {"name": "route.openshift.io", "versions": [{"groupVersion": "route.openshift.io/v1", "version": "v1"}]},
                    {"name": "project.openshift.io", "versions": [{"groupVersion": "project.openshift.io/v1", "version": "v1"}]},
                    {"name": "apps.openshift.io", "versions": [{"groupVersion": "apps.openshift.io/v1", "version": "v1"}]}
                ]}),
            ),
            "/apis/config.openshift.io/v1/clusterversions/version" => crate::k8s::testing::status(
                403,
                "Forbidden",
                r#"clusterversions.config.openshift.io "version" is forbidden: User "dev1" cannot get resource "clusterversions" in API group "config.openshift.io" at the cluster scope"#,
            ),
            "/apis/project.openshift.io/v1/projects" => (
                200,
                serde_json::json!({"apiVersion": "project.openshift.io/v1", "kind": "ProjectList",
                "metadata": {}, "items": [
                    {"apiVersion": "project.openshift.io/v1", "kind": "Project", "metadata": {"name": "team-a"}},
                    {"apiVersion": "project.openshift.io/v1", "kind": "Project", "metadata": {"name": "team-b"}}
                ]}),
            ),
            "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews" => {
                let attrs = &s.body["spec"]["resourceAttributes"];
                let allowed = matches!(attrs["resource"].as_str(), Some("projects" | "pods"));
                (
                    201,
                    serde_json::json!({"apiVersion": "authorization.k8s.io/v1",
                        "kind": "SelfSubjectAccessReview", "metadata": {}, "spec": {},
                        "status": {"allowed": allowed}}),
                )
            }
            other => crate::k8s::testing::status(404, "NotFound", &format!("{other} not found")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;
    use crate::k8s::testing::{mock_client, status};

    const LIMIT: Duration = Duration::from_secs(5);

    #[tokio::test]
    async fn probe_collects_user_version_and_rights() {
        let (client, server) = mock_client(fixtures::openshift);
        let info = probe(&client, LIMIT).await.unwrap();
        assert_eq!(
            info,
            ConnectionInfo {
                user: "dev1".into(),
                groups: vec!["system:authenticated".into(), "team-a".into()],
                server_version: "v1.32.5".into(),
                platform: PlatformInfo {
                    kind: PlatformKind::Openshift,
                    distribution: None,
                    version: None,
                    label: "OpenShift".into(),
                },
                api_groups: ApiGroups {
                    routes: true,
                    projects: true,
                    dropapp_projects: false,
                    deployment_configs: true,
                    gateway_api: false,
                },
                capabilities: Capabilities {
                    list_projects: true,
                    list_nodes: false,
                    list_all_pods: true,
                    read_cluster_operators: false,
                    read_cluster_version: false,
                },
                token_expires_at: None,
            }
        );
        drop(client);
        let seen = server.await.unwrap();
        assert!(
            seen.iter()
                .filter(|s| s.path().ends_with("reviews"))
                .all(|s| s.method == http::Method::POST)
        );
        let reviews: Vec<_> = seen
            .iter()
            .filter(|s| s.path().ends_with("selfsubjectaccessreviews"))
            .map(|s| {
                s.body["spec"]["resourceAttributes"]["resource"]
                    .as_str()
                    .unwrap()
                    .to_string()
            })
            .collect();
        assert_eq!(reviews.len(), 5, "{reviews:?}");
        for r in [
            "projects",
            "nodes",
            "pods",
            "clusteroperators",
            "clusterversions",
        ] {
            assert!(reviews.iter().any(|x| x == r), "{r}");
        }
        assert!(
            !seen.iter().any(|s| s.path() == "/api/v1/nodes"),
            "у OpenShift ноды для определения дистрибутива не читаются"
        );
    }

    #[tokio::test]
    async fn openshift_version_comes_from_cluster_version() {
        let (client, _server) = mock_client(|s| match s.path() {
            "/apis/config.openshift.io/v1/clusterversions/version" => (
                200,
                serde_json::json!({"apiVersion": "config.openshift.io/v1", "kind": "ClusterVersion",
                    "metadata": {"name": "version"}, "status": {"desired": {"version": "4.18.3"}}}),
            ),
            _ => fixtures::openshift(s),
        });
        let d = discover(&client, LIMIT).await.unwrap();
        assert_eq!(d.platform.label, "OpenShift 4.18");
        assert_eq!(d.server_version, "v1.32.5");
    }

    /// Версия DropApp — `spec.dappVersion` первого ClusterMetadata; нет прав, объекта или поля —
    /// подпись по версии Kubernetes, подключение не падает.
    #[tokio::test]
    async fn dropapp_version_comes_from_cluster_metadata() {
        let metadata = |items: serde_json::Value| {
            (
                200,
                serde_json::json!({"apiVersion": "config.dropapp.ru/v1alpha1",
                    "kind": "ClusterMetadataList", "metadata": {}, "items": items}),
            )
        };
        let cases = [
            (
                metadata(
                    serde_json::json!([{"apiVersion": "config.dropapp.ru/v1alpha1",
                    "kind": "ClusterMetadata", "metadata": {"name": "cluster-metadata"},
                    "spec": {"clusterName": "prod", "dappVersion": "5.1.2"}}]),
                ),
                "DropApp 5.1.2",
                Some("5.1.2"),
            ),
            (
                metadata(serde_json::json!([])),
                "DropApp (Kubernetes 1.32)",
                Some("1.32"),
            ),
            (
                metadata(
                    serde_json::json!([{"apiVersion": "config.dropapp.ru/v1alpha1",
                    "kind": "ClusterMetadata", "metadata": {"name": "cluster-metadata"},
                    "spec": {"clusterName": "prod"}}]),
                ),
                "DropApp (Kubernetes 1.32)",
                Some("1.32"),
            ),
            (
                status(
                    403,
                    "Forbidden",
                    r#"clustermetadatas.config.dropapp.ru is forbidden: User "dev1" cannot list resource "clustermetadatas" in API group "config.dropapp.ru" at the cluster scope"#,
                ),
                "DropApp (Kubernetes 1.32)",
                Some("1.32"),
            ),
        ];
        for (reply, label, version) in cases {
            let (client, server) = mock_client(move |s| match s.path() {
                "/apis" => (
                    200,
                    serde_json::json!({"kind": "APIGroupList", "apiVersion": "v1", "groups": [
                        {"name": "apps", "versions": [{"groupVersion": "apps/v1", "version": "v1"}]},
                        {"name": "config.dropapp.ru", "versions": [{"groupVersion": "config.dropapp.ru/v1alpha1", "version": "v1alpha1"}]},
                        {"name": "nodes.dropapp.ru", "versions": [{"groupVersion": "nodes.dropapp.ru/v1alpha1", "version": "v1alpha1"}]}
                    ]}),
                ),
                "/apis/config.dropapp.ru/v1alpha1/clustermetadatas" => reply.clone(),
                _ => fixtures::openshift(s),
            });
            let d = discover(&client, LIMIT).await.unwrap();
            assert_eq!(d.platform.kind, PlatformKind::Dropapp, "{label}");
            assert_eq!(d.platform.label, label);
            assert_eq!(d.platform.version.as_deref(), version, "{label}");
            assert_eq!(d.platform.distribution, None, "{label}");
            drop(client);
            let seen = server.await.unwrap();
            assert!(
                seen.iter()
                    .any(|s| s.path() == "/apis/config.dropapp.ru/v1alpha1/clustermetadatas"),
                "{label}"
            );
            assert!(
                !seen.iter().any(|s| s.path() == "/api/v1/nodes"),
                "у DropApp ноды для определения дистрибутива не читаются"
            );
            assert!(
                !seen.iter().any(|s| s.path().contains("clusterversions")),
                "{label}"
            );
        }
    }

    #[tokio::test]
    async fn kubernetes_without_openshift_groups() {
        let (client, server) = mock_client(|s| match s.path() {
            "/apis" => (
                200,
                serde_json::json!({"kind": "APIGroupList", "apiVersion": "v1", "groups": [
                    {"name": "apps", "versions": [{"groupVersion": "apps/v1", "version": "v1"}]},
                    {"name": "route.openshift.io", "versions": [{"groupVersion": "route.openshift.io/v1", "version": "v1"}]},
                    {"name": "gateway.networking.k8s.io", "versions": [{"groupVersion": "gateway.networking.k8s.io/v1", "version": "v1"}]}
                ]}),
            ),
            "/api/v1/nodes" => (
                200,
                serde_json::json!({"apiVersion": "v1", "kind": "NodeList", "metadata": {},
                    "items": [{"metadata": {"name": "aks-1",
                        "labels": {"kubernetes.azure.com/cluster": "rg", "kubernetes.io/os": "linux"}}}]}),
            ),
            _ => fixtures::openshift(s),
        });
        let info = probe(&client, LIMIT).await.unwrap();
        assert_eq!(info.platform.kind, PlatformKind::Kubernetes);
        assert_eq!(info.platform.label, "Kubernetes 1.32 (AKS)");
        assert_eq!(
            info.api_groups,
            ApiGroups {
                routes: true,
                gateway_api: true,
                ..Default::default()
            },
            "CRD Route без config.openshift.io — это Kubernetes с Route"
        );
        drop(client);
        let seen = server.await.unwrap();
        assert!(
            seen.iter()
                .any(|s| s.uri.starts_with("/api/v1/nodes?") && s.uri.contains("limit=1"))
        );
        let resources: Vec<_> = seen
            .iter()
            .filter_map(|s| s.body["spec"]["resourceAttributes"]["resource"].as_str())
            .collect();
        assert!(resources.contains(&"namespaces"));
        assert!(
            !resources.contains(&"clusteroperators"),
            "без OpenShift не спрашиваем"
        );
    }

    #[tokio::test]
    async fn expired_token_stops_the_probe() {
        let (client, _server) = mock_client(|_| status(401, "Unauthorized", "Unauthorized"));
        let e = probe(&client, LIMIT).await.unwrap_err();
        assert_eq!(e.code, ErrorCode::SessionExpired);
    }

    #[tokio::test]
    async fn failed_access_review_means_no_right() {
        let (client, _server) = mock_client(|s| match s.path() {
            "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews" => {
                status(500, "InternalError", "etcd timeout")
            }
            _ => fixtures::openshift(s),
        });
        let info = probe(&client, LIMIT).await.unwrap();
        assert_eq!(info.capabilities, Capabilities::default());
    }

    #[tokio::test]
    async fn project_names_of_openshift_dropapp_and_kubernetes() {
        let (client, server) = mock_client(|s| match s.path() {
            "/apis/projects.dropapp.ru/v1alpha1/projects" => (
                200,
                serde_json::json!({"apiVersion": "projects.dropapp.ru/v1alpha1", "kind": "ProjectList",
                "metadata": {}, "items": [
                    {"apiVersion": "projects.dropapp.ru/v1alpha1", "kind": "Project", "metadata": {"name": "dev"}}
                ]}),
            ),
            "/api/v1/namespaces" => (
                200,
                serde_json::json!({"apiVersion": "v1", "kind": "NamespaceList",
                    "metadata": {"continue": "token"},
                    "items": [{"metadata": {"name": "default"}}]}),
            ),
            _ => fixtures::openshift(s),
        });
        let (names, more) = list_project_names(&client, LIMIT, ProjectSource::Openshift)
            .await
            .unwrap();
        assert_eq!(
            (names, more),
            (vec!["team-a".to_string(), "team-b".to_string()], false)
        );
        let (names, more) = list_project_names(&client, LIMIT, ProjectSource::Dropapp)
            .await
            .unwrap();
        assert_eq!((names, more), (vec!["dev".to_string()], false));
        let (names, more) = list_project_names(&client, LIMIT, ProjectSource::Namespaces)
            .await
            .unwrap();
        assert_eq!((names, more), (vec!["default".to_string()], true));
        drop(client);
        let seen = server.await.unwrap();
        assert!(seen.iter().all(|s| s.uri.contains("limit=500")));
        assert!(
            seen.iter()
                .any(|s| s.path() == "/apis/projects.dropapp.ru/v1alpha1/projects")
        );
    }

    /// Право на список проектов DropApp спрашивается в их группе; namespace — нет.
    #[tokio::test]
    async fn dropapp_projects_right_is_asked_in_their_group() {
        let (client, server) = mock_client(|s| match s.path() {
            "/apis" => (
                200,
                serde_json::json!({"kind": "APIGroupList", "apiVersion": "v1", "groups": [
                    {"name": "apps", "versions": [{"groupVersion": "apps/v1", "version": "v1"}]},
                    {"name": "config.dropapp.ru", "versions": [{"groupVersion": "config.dropapp.ru/v1alpha1", "version": "v1alpha1"}]},
                    {"name": "projects.dropapp.ru", "versions": [{"groupVersion": "projects.dropapp.ru/v1alpha1", "version": "v1alpha1"}]}
                ]}),
            ),
            _ => fixtures::openshift(s),
        });
        let info = probe(&client, LIMIT).await.unwrap();
        assert_eq!(info.api_groups.project_source(), ProjectSource::Dropapp);
        assert!(info.capabilities.list_projects);
        drop(client);
        let seen = server.await.unwrap();
        let reviews: Vec<_> = seen
            .iter()
            .filter(|s| s.path().ends_with("selfsubjectaccessreviews"))
            .map(|s| {
                let a = &s.body["spec"]["resourceAttributes"];
                (
                    a["group"].as_str().unwrap_or_default().to_string(),
                    a["resource"].as_str().unwrap_or_default().to_string(),
                )
            })
            .collect();
        assert!(
            reviews.contains(&("projects.dropapp.ru".into(), "projects".into())),
            "{reviews:?}"
        );
        assert!(
            !reviews.iter().any(|(_, r)| r == "namespaces"),
            "{reviews:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn slow_server_times_out() {
        let fut = async {
            tokio::time::sleep(Duration::from_secs(60)).await;
            Ok::<_, kube::Error>(())
        };
        let e = timed(Duration::from_secs(1), fut).await.unwrap_err();
        assert_eq!(e.code, ErrorCode::Timeout);
    }
}
