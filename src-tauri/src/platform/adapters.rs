//! Адаптеры платформы (спец. §4.3): то, что у вендоров устроено по-разному и из групп API не
//! выводится, — обзор уровня кластера и срок токена. Реализация выбирается по `PlatformKind`
//! при подключении; автопоиск метрик — этап O3.

use futures::future::BoxFuture;
use kube::api::ListParams;
use serde::Serialize;

use crate::k8s::cluster::Capabilities;
use crate::k8s::dropapp::{
    self, ComponentKind, ComponentRow, DropAppMetadata, DropAppNodeRow, IngressTlsRow,
    cluster_metadata, component_problems, dropapp_node_row, ingress_tls_problem,
};
use crate::k8s::errors::not_allowed;
use crate::k8s::openshift::{
    self, ClusterVersionInfo, MachineConfigPoolRow, OperatorRow, cluster_version_info,
    machine_config_pool_row, operator_row,
};
use crate::k8s::read::ReadClient;
use crate::k8s::view::Part;
use crate::platform::PlatformKind;

pub trait Platform: Send + Sync {
    /// Обзор уровня кластера, который есть только у этой платформы.
    fn overview<'a>(
        &'a self,
        r: &'a ReadClient,
        caps: &'a Capabilities,
    ) -> BoxFuture<'a, Option<PlatformOverview>>;

    /// Когда истекает токен входа (секунды Unix), если платформа это сообщает.
    fn token_expiry<'a>(&'a self, r: &'a ReadClient, token: &'a str) -> BoxFuture<'a, Option<i64>>;
}

/// Адаптер платформы.
pub fn adapter(kind: PlatformKind) -> &'static dyn Platform {
    match kind {
        PlatformKind::Openshift => &OpenShift,
        PlatformKind::Dropapp => &DropApp,
        PlatformKind::Kubernetes => &Kubernetes,
    }
}

/// Часть обзора, своя у каждой платформы. Собирается раз на запрос и сразу отдаётся, поэтому
/// разница размеров вариантов не важна.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
#[allow(clippy::large_enum_variant)]
pub enum PlatformOverview {
    Openshift(OpenShiftOverview),
    Dropapp(DropAppOverview),
}

/// Обзор кластера OpenShift (спец. §6.2): обновления, инфраструктура, консоль, операторы
/// и пулы машин.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenShiftOverview {
    pub cluster_version: Part<ClusterVersionInfo>,
    /// Тип инфраструктуры; `data: null` — объект есть, а типа в нём нет.
    pub infrastructure: Part<Option<String>>,
    /// Адрес веб-консоли; `data: null` — объект есть, а адреса в нём нет.
    pub console_url: Part<Option<String>>,
    pub operators: Part<OperatorSummary>,
    pub machine_config_pools: Part<Vec<MachineConfigPoolRow>>,
}

/// Операторы кластера: сколько всего и какие не в норме.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OperatorSummary {
    pub total: usize,
    pub problems: Vec<OperatorRow>,
}

/// Обзор кластера DropApp (спека O5 §7.1): версия и имя кластера, компоненты и инструменты,
/// узлы DropApp и сертификаты ingress.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DropAppOverview {
    pub metadata: Part<DropAppMetadata>,
    pub components: Part<ComponentSummary>,
    pub nodes: Part<DropAppNodeSummary>,
    pub ingress_tls: Part<IngressTlsSummary>,
}

/// `Component` и `Tool`: сколько всего и какие в сбое или обновляются.
#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ComponentSummary {
    pub total: usize,
    pub problems: Vec<ComponentRow>,
}

/// `DropAppNode`: сколько всего и какие не `ready`.
#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DropAppNodeSummary {
    pub total: usize,
    pub problems: Vec<DropAppNodeRow>,
}

/// `IngressTLS`: сколько всего и у каких `Ready` не `True`.
#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IngressTlsSummary {
    pub total: usize,
    pub problems: Vec<IngressTlsRow>,
}

/// Любой Kubernetes без адаптера: общий обзор без своей части, срока токена нет.
pub struct Kubernetes;

impl Platform for Kubernetes {
    fn overview<'a>(
        &'a self,
        _: &'a ReadClient,
        _: &'a Capabilities,
    ) -> BoxFuture<'a, Option<PlatformOverview>> {
        Box::pin(async { None })
    }

    fn token_expiry<'a>(&'a self, _: &'a ReadClient, _: &'a str) -> BoxFuture<'a, Option<i64>> {
        Box::pin(async { None })
    }
}

pub struct OpenShift;

/// Части обзора, которых у кластера может не быть: на ROSA HCP и других кластерах с внешней
/// плоскостью управления нет MachineConfigPool (группы `machineconfiguration.openshift.io`), а
/// объекта консоли нет, если она выключена. 404 — пустое значение («—»), а не ошибка.
fn absent_is_empty<T: Default>(r: crate::error::AppResult<T>) -> Part<T> {
    match r {
        Err(e) if e.code == crate::error::ErrorCode::NotFound => Part::ok(T::default()),
        r => r.into(),
    }
}

/// Все ClusterOperators.
pub async fn cluster_operators(r: &ReadClient) -> crate::error::AppResult<Vec<OperatorRow>> {
    let list = r
        .list_dynamic(
            &openshift::cluster_operators(),
            None,
            &ListParams::default(),
        )
        .await?;
    Ok(list.items.iter().map(operator_row).collect())
}

pub struct DropApp;

/// Объекты DropApp вида `ar` по всем namespace.
async fn dropapp_list(
    r: &ReadClient,
    ar: kube::api::ApiResource,
) -> crate::error::AppResult<Vec<kube::api::DynamicObject>> {
    Ok(r.list_dynamic(&ar, None, &ListParams::default())
        .await?
        .items)
}

/// `Component` и `Tool` с событиями проблемных. Нет одного из CRD (404) — он пустой; иная
/// ошибка любого из списков — ошибка части.
async fn dropapp_components(r: &ReadClient) -> crate::error::AppResult<ComponentSummary> {
    let (components, tools) = tokio::join!(
        dropapp_list(r, dropapp::components()),
        dropapp_list(r, dropapp::tools())
    );
    let absent = |r: crate::error::AppResult<Vec<_>>| match r {
        Err(e) if e.code == crate::error::ErrorCode::NotFound => Ok(Vec::new()),
        r => r,
    };
    let objects: Vec<_> = absent(components)?
        .into_iter()
        .map(|o| (ComponentKind::Component, o))
        .chain(absent(tools)?.into_iter().map(|o| (ComponentKind::Tool, o)))
        .collect();
    Ok(ComponentSummary {
        total: objects.len(),
        problems: component_problems(r, &objects).await,
    })
}

impl Platform for DropApp {
    /// Части без CRD (404) — пустые («—»), без прав — причина. Права на объекты DropApp
    /// заранее не проверяются: каждая часть отвечает своей причиной.
    fn overview<'a>(
        &'a self,
        r: &'a ReadClient,
        _: &'a Capabilities,
    ) -> BoxFuture<'a, Option<PlatformOverview>> {
        Box::pin(async move {
            let metadata = async {
                absent_is_empty(
                    dropapp_list(r, dropapp::cluster_metadatas())
                        .await
                        .map(|l| l.first().map(cluster_metadata).unwrap_or_default()),
                )
            };
            let components = async { dropapp_components(r).await.into() };
            let nodes = async {
                absent_is_empty(dropapp_list(r, dropapp::dropapp_nodes()).await.map(|l| {
                    DropAppNodeSummary {
                        total: l.len(),
                        problems: l
                            .iter()
                            .map(dropapp_node_row)
                            .filter(DropAppNodeRow::has_problem)
                            .collect(),
                    }
                }))
            };
            let ingress_tls = async {
                absent_is_empty(dropapp_list(r, dropapp::ingress_tlses()).await.map(|l| {
                    IngressTlsSummary {
                        total: l.len(),
                        problems: l.iter().filter_map(ingress_tls_problem).collect(),
                    }
                }))
            };
            let (metadata, components, nodes, ingress_tls) =
                tokio::join!(metadata, components, nodes, ingress_tls);
            Some(PlatformOverview::Dropapp(DropAppOverview {
                metadata,
                components,
                nodes,
                ingress_tls,
            }))
        })
    }

    /// Срок токена OIDC — задача входа через OIDC (спека O5 §5.3).
    fn token_expiry<'a>(&'a self, _: &'a ReadClient, _: &'a str) -> BoxFuture<'a, Option<i64>> {
        Box::pin(async { None })
    }
}

impl Platform for OpenShift {
    fn overview<'a>(
        &'a self,
        r: &'a ReadClient,
        caps: &'a Capabilities,
    ) -> BoxFuture<'a, Option<PlatformOverview>> {
        Box::pin(async move {
            let version = async {
                if !caps.read_cluster_version {
                    return Part::err(not_allowed("get", "clusterversions", None));
                }
                r.get_dynamic(&openshift::cluster_versions(), None, "version")
                    .await
                    .map(|o| cluster_version_info(&o))
                    .into()
            };
            let infrastructure = async {
                absent_is_empty(
                    r.get_dynamic(&openshift::infrastructures(), None, "cluster")
                        .await
                        .map(|o| openshift::infrastructure_type(&o)),
                )
            };
            let console = async {
                absent_is_empty(
                    r.get_dynamic(&openshift::consoles(), None, "cluster")
                        .await
                        .map(|o| openshift::console_url(&o)),
                )
            };
            let operators = async {
                if !caps.read_cluster_operators {
                    return Part::err(not_allowed("list", "clusteroperators", None));
                }
                cluster_operators(r)
                    .await
                    .map(|rows| OperatorSummary {
                        total: rows.len(),
                        problems: rows.into_iter().filter(OperatorRow::has_problem).collect(),
                    })
                    .into()
            };
            let pools = async {
                absent_is_empty(
                    r.list_dynamic(
                        &openshift::machine_config_pools(),
                        None,
                        &ListParams::default(),
                    )
                    .await
                    .map(|l| l.items.iter().map(machine_config_pool_row).collect()),
                )
            };
            let (cluster_version, infrastructure, console_url, operators, machine_config_pools) =
                tokio::join!(version, infrastructure, console, operators, pools);
            Some(PlatformOverview::Openshift(OpenShiftOverview {
                cluster_version,
                infrastructure,
                console_url,
                operators,
                machine_config_pools,
            }))
        })
    }

    /// Срок токена OAuth OpenShift — из его объекта `UserOAuthAccessToken` (спец. §5.2). Свои
    /// токены пользователь читать может; нет объекта или прав — срока нет.
    fn token_expiry<'a>(&'a self, r: &'a ReadClient, token: &'a str) -> BoxFuture<'a, Option<i64>> {
        Box::pin(async move {
            let name = openshift::oauth_token_object_name(token)?;
            match r
                .get_dynamic(&openshift::user_oauth_access_tokens(), None, &name)
                .await
            {
                Ok(o) => openshift::token_expires_at(&o),
                Err(e) => {
                    tracing::debug!("token expiry is not readable: {}", e.message);
                    None
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::k8s::dropapp::fixtures as dropapp_fixtures;
    use crate::k8s::openshift::fixtures;
    use crate::k8s::testing::{Seen, mock_client, status};
    use std::time::Duration;

    fn all_rights() -> Capabilities {
        Capabilities {
            list_projects: true,
            list_nodes: true,
            list_all_pods: true,
            read_cluster_operators: true,
            read_cluster_version: true,
        }
    }

    /// 403 так, как отвечает API-сервер обычному пользователю без прав на объекты `config.openshift.io`.
    fn forbidden(s: &Seen) -> (u16, serde_json::Value) {
        let mut path = s.path().trim_start_matches("/apis/").split('/');
        let group = path.next().unwrap_or_default();
        let resource = path.nth(1).unwrap_or_default();
        let verb = if path.next().is_some() { "get" } else { "list" };
        status(
            403,
            "Forbidden",
            &format!(
                r#"{resource}.{group} is forbidden: User "dev1" cannot {verb} resource "{resource}" in API group "{group}" at the cluster scope"#
            ),
        )
    }

    fn openshift_cluster(s: &Seen) -> (u16, serde_json::Value) {
        match s.path() {
            "/apis/config.openshift.io/v1/clusterversions/version" => {
                (200, fixtures::cluster_version())
            }
            "/apis/config.openshift.io/v1/clusteroperators" => (200, fixtures::operators()),
            "/apis/machineconfiguration.openshift.io/v1/machineconfigpools" => {
                (200, fixtures::pools())
            }
            "/apis/config.openshift.io/v1/infrastructures/cluster" => (
                200,
                serde_json::json!({"apiVersion": "config.openshift.io/v1", "kind": "Infrastructure",
                    "metadata": {"name": "cluster"}, "status": {"platformStatus": {"type": "VSphere"}}}),
            ),
            // Консоль обычному пользователю не читается — самый частый случай.
            _ => forbidden(s),
        }
    }

    fn reader(client: kube::Client) -> ReadClient {
        ReadClient::new(client, Duration::from_secs(5))
    }

    #[tokio::test]
    async fn openshift_overview_with_rights() {
        let (client, _server) = mock_client(openshift_cluster);
        let r = reader(client);
        let Some(PlatformOverview::Openshift(o)) = adapter(PlatformKind::Openshift)
            .overview(&r, &all_rights())
            .await
        else {
            panic!("у OpenShift есть своя часть обзора");
        };
        let cv = o.cluster_version.data.unwrap();
        assert_eq!(cv.available_updates, 2);
        assert_eq!(o.infrastructure.data, Some(Some("VSphere".into())));
        assert!(o.infrastructure.error.is_none());
        assert!(
            o.console_url.data.is_none(),
            "консоль не читается — адреса нет"
        );
        let denied = o.console_url.error.unwrap();
        assert_eq!(
            denied.key, "errors.k8s.forbiddenCluster",
            "причина, а не «—»"
        );
        assert_eq!(denied.params["resource"], "consoles");
        let ops = o.operators.data.unwrap();
        assert_eq!(ops.total, 3);
        let names: Vec<_> = ops.problems.iter().map(|p| p.name.as_str()).collect();
        assert_eq!(names, ["ingress", "monitoring"]);
        assert_eq!(o.machine_config_pools.data.unwrap().len(), 2);
        let json = serde_json::to_value(PlatformOverview::Openshift(OpenShiftOverview {
            cluster_version: Part::err(not_allowed("get", "clusterversions", None)),
            infrastructure: Part::ok(None),
            console_url: Part::err(not_allowed("get", "consoles", None)),
            operators: Part::ok(OperatorSummary {
                total: 0,
                problems: vec![],
            }),
            machine_config_pools: Part::ok(vec![]),
        }))
        .unwrap();
        assert_eq!(json["kind"], "openshift");
        assert_eq!(json["operators"]["data"]["total"], 0);
        // Как у остальных `Part`: либо `data` (у пустого значения — `null`), либо `error`.
        assert_eq!(json["infrastructure"], serde_json::json!({"data": null}));
        assert!(json["consoleUrl"].get("data").is_none());
        assert_eq!(
            json["consoleUrl"]["error"]["key"],
            "errors.k8s.forbiddenCluster"
        );
    }

    #[tokio::test]
    async fn infrastructure_and_console_are_parts_with_a_value_or_without_one() {
        let (client, _server) = mock_client(|s| match s.path() {
            // Объект есть, а типа в нём нет.
            "/apis/config.openshift.io/v1/infrastructures/cluster" => (
                200,
                serde_json::json!({"apiVersion": "config.openshift.io/v1", "kind": "Infrastructure",
                    "metadata": {"name": "cluster"}, "status": {}}),
            ),
            "/apis/config.openshift.io/v1/consoles/cluster" => (
                200,
                serde_json::json!({"apiVersion": "config.openshift.io/v1", "kind": "Console",
                    "metadata": {"name": "cluster"},
                    "status": {"consoleURL": "https://console-openshift-console.apps.c.example.com"}}),
            ),
            _ => forbidden(s),
        });
        let r = reader(client);
        let Some(overview) = adapter(PlatformKind::Openshift)
            .overview(&r, &all_rights())
            .await
        else {
            panic!("OpenShift");
        };
        let PlatformOverview::Openshift(o) = &overview else {
            panic!("OpenShift");
        };
        assert_eq!(
            o.infrastructure.data,
            Some(None),
            "значения нет, ошибки нет"
        );
        assert!(o.infrastructure.error.is_none());
        assert_eq!(
            o.console_url.data,
            Some(Some(
                "https://console-openshift-console.apps.c.example.com".into()
            ))
        );
        let json = serde_json::to_value(&overview).unwrap();
        assert_eq!(json["infrastructure"], serde_json::json!({"data": null}));
        assert_eq!(
            json["consoleUrl"],
            serde_json::json!({"data": "https://console-openshift-console.apps.c.example.com"})
        );
    }

    /// ROSA HCP: группы `machineconfiguration.openshift.io` нет, консоль выключена — на эти
    /// запросы сервер отвечает 404. Части пустые («—»), а не ошибки.
    #[tokio::test]
    async fn parts_that_the_cluster_does_not_have_are_empty_not_errors() {
        let (client, _server) = mock_client(|s| match s.path() {
            "/apis/machineconfiguration.openshift.io/v1/machineconfigpools" => status(
                404,
                "NotFound",
                "the server could not find the requested resource",
            ),
            "/apis/config.openshift.io/v1/consoles/cluster" => status(
                404,
                "NotFound",
                r#"consoles.config.openshift.io "cluster" not found"#,
            ),
            _ => openshift_cluster(s),
        });
        let r = reader(client);
        let Some(PlatformOverview::Openshift(o)) = adapter(PlatformKind::Openshift)
            .overview(&r, &all_rights())
            .await
        else {
            panic!("OpenShift");
        };
        assert_eq!(o.machine_config_pools.data, Some(vec![]));
        assert!(o.machine_config_pools.error.is_none());
        assert_eq!(o.console_url.data, Some(None));
        assert!(o.console_url.error.is_none());
        assert_eq!(o.infrastructure.data, Some(Some("VSphere".into())));
    }

    #[tokio::test]
    async fn openshift_overview_without_rights_does_not_ask() {
        // Пользователь без прав на объекты OpenShift: что бы ни спросили, ответ — 403.
        let (client, server) = mock_client(forbidden);
        let r = reader(client);
        let Some(PlatformOverview::Openshift(o)) = adapter(PlatformKind::Openshift)
            .overview(&r, &Capabilities::default())
            .await
        else {
            panic!("OpenShift");
        };
        assert_eq!(
            o.cluster_version.error.unwrap().key,
            "errors.k8s.forbiddenCluster"
        );
        assert_eq!(
            o.operators.error.unwrap().params["resource"],
            "clusteroperators"
        );
        // Остальные части спрашиваются без проверки прав, и каждая отвечает своей причиной.
        for (part, resource) in [
            (o.infrastructure.error, "infrastructures"),
            (o.console_url.error, "consoles"),
            (o.machine_config_pools.error, "machineconfigpools"),
        ] {
            let e = part.unwrap_or_else(|| panic!("{resource}: причины нет"));
            assert_eq!(e.key, "errors.k8s.forbiddenCluster");
            assert_eq!(e.params["resource"], resource);
        }
        assert!(o.machine_config_pools.data.is_none());
        drop(r);
        let seen = server.await.unwrap();
        assert!(
            !seen
                .iter()
                .any(|s| s.path().contains("clusteroperators")
                    || s.path().contains("clusterversions"))
        );
    }

    #[tokio::test]
    async fn kubernetes_has_no_platform_part_and_no_token_expiry() {
        let (client, server) = mock_client(openshift_cluster);
        let r = reader(client);
        let k = adapter(PlatformKind::Kubernetes);
        assert!(k.overview(&r, &all_rights()).await.is_none());
        assert_eq!(k.token_expiry(&r, "sha256~abc").await, None);
        drop(r);
        assert!(server.await.unwrap().is_empty(), "запросов нет");
    }

    fn dropapp_cluster(s: &Seen) -> (u16, serde_json::Value) {
        use dropapp_fixtures::*;
        match s.path() {
            "/apis/config.dropapp.ru/v1alpha1/clustermetadatas" => (200, cluster_metadata_list()),
            "/apis/config.dropapp.ru/v1alpha1/components" => (200, component_list()),
            "/apis/config.dropapp.ru/v1alpha1/tools" => (200, tool_list()),
            "/apis/nodes.dropapp.ru/v1alpha1/dropappnodes" => (200, node_list()),
            "/apis/tls.dropapp.ru/v1alpha1/ingresstlses" => (200, ingress_tls_list()),
            "/api/v1/namespaces/dapp-system/events" if s.uri.contains("uid-cert-manager") => (
                200,
                events(vec![warning(
                    "uid-cert-manager",
                    "InstallFailed",
                    "helm install failed: timed out",
                    "2026-10-31T11:00:00Z",
                )]),
            ),
            "/api/v1/events" if s.uri.contains("uid-dex") => (200, events(vec![])),
            // События `grafana` не читаются — у строки нет события, обзор цел.
            _ => status(500, "InternalError", "etcd is unavailable"),
        }
    }

    #[tokio::test]
    async fn dropapp_overview_with_problems() {
        let (client, server) = mock_client(dropapp_cluster);
        let r = reader(client);
        let Some(overview) = adapter(PlatformKind::Dropapp)
            .overview(&r, &Capabilities::default())
            .await
        else {
            panic!("у DropApp есть своя часть обзора");
        };
        let PlatformOverview::Dropapp(d) = &overview else {
            panic!("DropApp");
        };
        let m = d.metadata.data.as_ref().unwrap();
        assert_eq!(m.cluster_name.as_deref(), Some("prod-msk"));
        assert_eq!(m.version.as_deref(), Some("5.1.2"));
        let c = d.components.data.as_ref().unwrap();
        assert_eq!(c.total, 6);
        let names: Vec<_> = c.problems.iter().map(|p| p.name.as_str()).collect();
        assert_eq!(names, ["cert-manager", "grafana", "dex"]);
        assert_eq!(
            c.problems[0].event.as_deref(),
            Some("InstallFailed: helm install failed: timed out")
        );
        assert_eq!(c.problems[1].event, None);
        let n = d.nodes.data.as_ref().unwrap();
        assert_eq!(n.total, 3);
        let names: Vec<_> = n.problems.iter().map(|p| p.name.as_str()).collect();
        assert_eq!(names, ["worker-2"]);
        let tls = d.ingress_tls.data.as_ref().unwrap();
        assert_eq!(tls.total, 2);
        assert_eq!(tls.problems[0].name, "apps");
        let json = serde_json::to_value(&overview).unwrap();
        assert_eq!(json["kind"], "dropapp");
        assert_eq!(json["metadata"]["data"]["clusterName"], "prod-msk");
        assert_eq!(
            json["components"]["data"]["problems"][0]["kind"],
            "Component"
        );
        assert_eq!(
            json["ingressTls"]["data"]["problems"][0]["reason"],
            "CertificateExpired"
        );
        assert_eq!(
            adapter(PlatformKind::Dropapp)
                .token_expiry(&r, "eyJhbGciOi.x.y")
                .await,
            None
        );
        drop(r);
        let seen = server.await.unwrap();
        let events: Vec<_> = seen
            .iter()
            .filter(|s| s.path().ends_with("/events"))
            .collect();
        assert_eq!(events.len(), 3, "события — только у проблемных");
        assert_eq!(seen.len(), 5 + 3, "срок токена не спрашивается");
    }

    /// DropApp без части CRD (например, `tls.dropapp.ru` в старой версии): 404 — пустая часть.
    #[tokio::test]
    async fn dropapp_parts_without_crds_are_empty_not_errors() {
        let (client, server) = mock_client(|_| {
            status(
                404,
                "NotFound",
                "the server could not find the requested resource",
            )
        });
        let r = reader(client);
        let Some(PlatformOverview::Dropapp(d)) = adapter(PlatformKind::Dropapp)
            .overview(&r, &all_rights())
            .await
        else {
            panic!("DropApp");
        };
        let m = d.metadata.data.unwrap();
        assert_eq!((m.cluster_name, m.version), (None, None));
        let c = d.components.data.unwrap();
        assert_eq!((c.total, c.problems.len()), (0, 0));
        assert_eq!(d.nodes.data.unwrap().total, 0);
        assert_eq!(d.ingress_tls.data.unwrap().total, 0);
        assert!(d.metadata.error.is_none() && d.components.error.is_none());
        assert!(d.nodes.error.is_none() && d.ingress_tls.error.is_none());
        drop(r);
        assert_eq!(server.await.unwrap().len(), 5, "событий не спрашивали");
    }

    /// Нет `Tool`, а `Component` есть: часть компонентов — из одних `Component`.
    #[tokio::test]
    async fn dropapp_components_without_tools() {
        let (client, _server) = mock_client(|s| match s.path() {
            "/apis/config.dropapp.ru/v1alpha1/tools" => status(404, "NotFound", "not found"),
            _ => dropapp_cluster(s),
        });
        let r = reader(client);
        let Some(PlatformOverview::Dropapp(d)) = adapter(PlatformKind::Dropapp)
            .overview(&r, &all_rights())
            .await
        else {
            panic!("DropApp");
        };
        let c = d.components.data.unwrap();
        assert_eq!(c.total, 3);
        let names: Vec<_> = c.problems.iter().map(|p| p.name.as_str()).collect();
        assert_eq!(names, ["cert-manager"]);
    }

    #[tokio::test]
    async fn dropapp_overview_without_rights() {
        let (client, server) = mock_client(forbidden);
        let r = reader(client);
        let Some(PlatformOverview::Dropapp(d)) = adapter(PlatformKind::Dropapp)
            .overview(&r, &Capabilities::default())
            .await
        else {
            panic!("DropApp");
        };
        for (part, resource) in [
            (d.metadata.error, "clustermetadatas"),
            (d.components.error, "components"),
            (d.nodes.error, "dropappnodes"),
            (d.ingress_tls.error, "ingresstlses"),
        ] {
            let e = part.unwrap_or_else(|| panic!("{resource}: причины нет"));
            assert_eq!(e.key, "errors.k8s.forbiddenCluster");
            assert_eq!(e.params["resource"], resource);
        }
        assert!(d.components.data.is_none());
        drop(r);
        let seen = server.await.unwrap();
        assert!(!seen.iter().any(|s| s.path().ends_with("/events")));
    }

    #[tokio::test]
    async fn openshift_token_expiry_from_its_object() {
        let (client, server) = mock_client(|s| match s.path() {
            "/apis/oauth.openshift.io/v1/useroauthaccesstokens/sha256~ungWv48Bz-pBQUDeXa4iI7ADYaOWF3qctBD_YfIAFa0" => {
                (
                    200,
                    serde_json::json!({"apiVersion": "oauth.openshift.io/v1", "kind": "UserOAuthAccessToken",
                    "metadata": {"name": "sha256~x", "creationTimestamp": "2026-10-25T10:00:00Z"},
                    "expiresIn": 86400}),
                )
            }
            _ => status(404, "NotFound", "not found"),
        });
        let r = reader(client);
        let os = adapter(PlatformKind::Openshift);
        assert_eq!(
            os.token_expiry(&r, "sha256~abc").await,
            Some(1_792_922_400 + 86_400)
        );
        assert_eq!(
            os.token_expiry(&r, "sha256~other").await,
            None,
            "нет объекта"
        );
        assert_eq!(os.token_expiry(&r, "eyJhbGciOi.x.y").await, None);
        drop(r);
        assert_eq!(
            server.await.unwrap().len(),
            2,
            "токен не OAuth — не спрашиваем"
        );
    }
}
