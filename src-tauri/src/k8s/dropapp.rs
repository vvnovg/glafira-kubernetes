//! Объекты Platform V DropApp (спека O5 §1): их CRD (все `v1alpha1`) в `k8s-openapi` нет, поэтому
//! они читаются как `DynamicObject`, а нужные поля берутся из JSON. Здесь — их `ApiResource` и разбор.

use k8s_openapi::api::core::v1::Event;
use kube::api::{ApiResource, DynamicObject, GroupVersionKind, ListParams};
use serde::Serialize;

use crate::k8s::events::event_row;
use crate::k8s::read::{ReadClient, check_field_value};
use crate::k8s::view::{condition, conditions};

/// Имена ресурсов (plural) документация не даёт — по правилу CRD: вид в нижнем регистре и `s`
/// (`es` после `s`). Смоук на стенде поправит их здесь, если они другие.
pub const CLUSTER_METADATAS: &str = "clustermetadatas";
pub const COMPONENTS: &str = "components";
pub const TOOLS: &str = "tools";
pub const DROPAPP_NODES: &str = "dropappnodes";
pub const INGRESS_TLSES: &str = "ingresstlses";

/// Для скольких проблемных `Component`/`Tool` читается последнее событие `Warning`.
pub const MAX_EVENT_LOOKUPS: usize = 20;

fn resource(group: &str, kind: &str, plural: &str) -> ApiResource {
    ApiResource::from_gvk_with_plural(&GroupVersionKind::gvk(group, "v1alpha1", kind), plural)
}

/// `ClusterMetadata`: имя кластера и версия DropApp. Имя объекта и область CRD документация
/// не называет — объекты читаются списком по всем namespace.
pub fn cluster_metadatas() -> ApiResource {
    resource("config.dropapp.ru", "ClusterMetadata", CLUSTER_METADATAS)
}

/// `Project` (DropApp 5.x): проекция namespace агрегированным API `dapp-apiserver`, имя проекта —
/// имя namespace; в списке только проекты, доступные пользователю.
pub fn projects() -> ApiResource {
    resource("projects.dropapp.ru", "Project", "projects")
}

/// Компоненты платформы. Область CRD `Component`, `Tool`, `DropAppNode` и `IngressTLS`
/// документация не называет — они, как и `ClusterMetadata`, читаются списком по всем namespace.
pub fn components() -> ApiResource {
    resource("config.dropapp.ru", "Component", COMPONENTS)
}

/// Инструменты платформы (Dex, Grafana…).
pub fn tools() -> ApiResource {
    resource("config.dropapp.ru", "Tool", TOOLS)
}

pub fn dropapp_nodes() -> ApiResource {
    resource("nodes.dropapp.ru", "DropAppNode", DROPAPP_NODES)
}

/// Сертификаты ingress-контроллера.
pub fn ingress_tlses() -> ApiResource {
    resource("tls.dropapp.ru", "IngressTLS", INGRESS_TLSES)
}

/// Версия DropApp из `ClusterMetadata` (`spec.dappVersion`) как есть: `5.1.2`. Нет поля или
/// пустая строка — `None`.
pub fn dapp_version(o: &DynamicObject) -> Option<String> {
    o.data["spec"]["dappVersion"]
        .as_str()
        .filter(|v| !v.is_empty())
        .map(String::from)
}

/// Имя кластера и версия DropApp (`ClusterMetadata`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DropAppMetadata {
    pub cluster_name: Option<String>,
    pub version: Option<String>,
}

pub fn cluster_metadata(o: &DynamicObject) -> DropAppMetadata {
    DropAppMetadata {
        cluster_name: o.data["spec"]["clusterName"]
            .as_str()
            .filter(|n| !n.is_empty())
            .map(String::from),
        version: dapp_version(o),
    }
}

/// Значение колонки STATUS: `status.phase`, а если его нет — `status.status` (имя поля
/// документация не показывает, спека O5 §7.1). Пустая строка — статуса нет.
fn status_of(o: &DynamicObject) -> Option<String> {
    let status = &o.data["status"];
    [&status["phase"], &status["status"]]
        .into_iter()
        .find_map(|v| v.as_str().filter(|s| !s.is_empty()))
        .map(String::from)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum ComponentKind {
    Component,
    Tool,
}

impl ComponentKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ComponentKind::Component => "Component",
            ComponentKind::Tool => "Tool",
        }
    }
}

/// `Component` или `Tool`: статус как есть, число повторов (RETRY) и последнее событие `Warning`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ComponentRow {
    pub kind: ComponentKind,
    pub name: String,
    pub namespace: Option<String>,
    pub status: Option<String>,
    pub retry: Option<i64>,
    /// `причина: сообщение` последнего события `Warning` по объекту.
    pub event: Option<String>,
}

impl ComponentRow {
    /// Сбой или обновление (`failed`, `error`, `upgrading` без учёта регистра). Неизвестное
    /// значение проблемой не считается.
    pub fn has_problem(&self) -> bool {
        self.status.as_deref().is_some_and(|s| {
            ["failed", "error", "upgrading"]
                .iter()
                .any(|p| s.eq_ignore_ascii_case(p))
        })
    }

    /// Сбой (`failed`, `error` без учёта регистра) — правило `cluster.componentFailed`;
    /// обновление (`upgrading`) — проблема обзора, но не сбой.
    pub fn is_failed(&self) -> bool {
        self.status.as_deref().is_some_and(|s| {
            ["failed", "error"]
                .iter()
                .any(|p| s.eq_ignore_ascii_case(p))
        })
    }
}

/// Строка без события: его читает `component_problems`, только для проблемных.
pub fn component_row(kind: ComponentKind, o: &DynamicObject) -> ComponentRow {
    let status = &o.data["status"];
    ComponentRow {
        kind,
        name: o.metadata.name.clone().unwrap_or_default(),
        namespace: o.metadata.namespace.clone(),
        status: status_of(o),
        retry: status["retry"]
            .as_i64()
            .or_else(|| status["retryCount"].as_i64()),
        event: None,
    }
}

/// Проблемные из `objects` по порядку; последнее событие `Warning` читается для первых
/// `MAX_EVENT_LOOKUPS`, у остальных его нет.
pub async fn component_problems(
    r: &ReadClient,
    objects: &[(ComponentKind, DynamicObject)],
) -> Vec<ComponentRow> {
    let problems: Vec<_> = objects
        .iter()
        .map(|(kind, o)| (component_row(*kind, o), o.metadata.uid.as_deref()))
        .filter(|(row, _)| row.has_problem())
        .collect();
    let events = futures::future::join_all(problems.iter().take(MAX_EVENT_LOOKUPS).map(
        |(row, uid)| async move {
            match uid {
                Some(uid) => last_warning(r, row.namespace.as_deref(), uid).await,
                None => None,
            }
        },
    ))
    .await;
    let mut events = events.into_iter();
    problems
        .into_iter()
        .map(|(mut row, _)| {
            row.event = events.next().flatten();
            row
        })
        .collect()
}

/// Последнее событие `Warning` по объекту с uid `uid` — в его namespace или, у объекта уровня
/// кластера (`None`), во всех: `причина: сообщение`. Ошибка чтения событий — события нет.
pub async fn last_warning(r: &ReadClient, namespace: Option<&str>, uid: &str) -> Option<String> {
    check_field_value(uid).ok()?;
    let lp = ListParams::default().fields(&format!("involvedObject.uid={uid},type=Warning"));
    let events = match r.list::<Event>(namespace, &lp).await {
        Ok(l) => l.items,
        Err(e) => {
            tracing::debug!("warning events of {uid} are not readable: {}", e.message);
            return None;
        }
    };
    let last = events.iter().map(event_row).max_by_key(|e| e.last_seen)?;
    Some(match last.reason.is_empty() {
        true => last.message,
        false => format!("{}: {}", last.reason, last.message),
    })
}

/// `DropAppNode`: статус как есть.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DropAppNodeRow {
    pub name: String,
    pub status: Option<String>,
}

impl DropAppNodeRow {
    /// Не `ready` (без учёта регистра). Статуса нет — состояние неизвестно, не проблема.
    pub fn has_problem(&self) -> bool {
        self.status
            .as_deref()
            .is_some_and(|s| !s.eq_ignore_ascii_case("ready"))
    }
}

pub fn dropapp_node_row(o: &DynamicObject) -> DropAppNodeRow {
    DropAppNodeRow {
        name: o.metadata.name.clone().unwrap_or_default(),
        status: status_of(o),
    }
}

/// `IngressTLS`, у которого условие `Ready` не `True`: причина и сообщение условия.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IngressTlsRow {
    pub name: String,
    pub namespace: Option<String>,
    pub reason: Option<String>,
    pub message: Option<String>,
}

/// Строка `IngressTLS`, если `Ready` не `True`; условия `Ready` нет (объект ещё не обработан)
/// — состояние неизвестно, `None`.
pub fn ingress_tls_problem(o: &DynamicObject) -> Option<IngressTlsRow> {
    let list = conditions(&o.data["status"]["conditions"]);
    let ready = condition(&list, "Ready").filter(|c| !c.is_true())?;
    Some(IngressTlsRow {
        name: o.metadata.name.clone().unwrap_or_default(),
        namespace: o.metadata.namespace.clone(),
        reason: ready.reason.clone(),
        message: ready.message.clone(),
    })
}

#[cfg(test)]
pub(crate) mod fixtures {
    use kube::api::DynamicObject;
    use serde_json::{Value, json};

    pub fn object(v: Value) -> DynamicObject {
        serde_json::from_value(v).unwrap()
    }

    fn list(group: &str, kind: &str, items: Vec<Value>) -> Value {
        json!({"apiVersion": format!("{group}/v1alpha1"), "kind": format!("{kind}List"),
            "metadata": {}, "items": items})
    }

    /// Объект DropApp вида `kind`: `namespace` `None` — уровня кластера.
    pub fn item(kind: &str, name: &str, namespace: Option<&str>, status: Value) -> Value {
        let mut metadata = json!({"name": name, "uid": format!("uid-{name}")});
        if let Some(ns) = namespace {
            metadata["namespace"] = json!(ns);
        }
        json!({"kind": kind, "metadata": metadata, "status": status})
    }

    pub fn cluster_metadata_list() -> Value {
        list(
            "config.dropapp.ru",
            "ClusterMetadata",
            vec![
                json!({"kind": "ClusterMetadata", "metadata": {"name": "cluster-metadata"},
                "spec": {"clusterName": "prod-msk", "dappVersion": "5.1.2"}}),
            ],
        )
    }

    /// Поле статуса — `status.phase`; у `cert-manager` — сбой с повторами, `monitoring` — в
    /// статусе, которого документация не называет.
    pub fn component_list() -> Value {
        list(
            "config.dropapp.ru",
            "Component",
            vec![
                item(
                    "Component",
                    "ingress-nginx",
                    Some("dapp-system"),
                    json!({"phase": "Success"}),
                ),
                item(
                    "Component",
                    "cert-manager",
                    Some("dapp-system"),
                    json!({"phase": "Failed", "retry": 3}),
                ),
                item(
                    "Component",
                    "monitoring",
                    Some("dapp-monitoring"),
                    json!({"phase": "Installing"}),
                ),
            ],
        )
    }

    /// Поле статуса — `status.status`, объекты уровня кластера; у `loki` статуса ещё нет.
    pub fn tool_list() -> Value {
        list(
            "config.dropapp.ru",
            "Tool",
            vec![
                item(
                    "Tool",
                    "grafana",
                    None,
                    json!({"status": "failed", "retryCount": 2}),
                ),
                item("Tool", "dex", None, json!({"status": "upgrading"})),
                item("Tool", "loki", None, json!({})),
            ],
        )
    }

    pub fn node_list() -> Value {
        list(
            "nodes.dropapp.ru",
            "DropAppNode",
            vec![
                item("DropAppNode", "master-1", None, json!({"status": "ready"})),
                item(
                    "DropAppNode",
                    "worker-2",
                    None,
                    json!({"status": "NotReady"}),
                ),
                item("DropAppNode", "worker-3", None, json!({})),
            ],
        )
    }

    pub fn ingress_tls_list() -> Value {
        let tls = |name: &str, ready: &str, reason: &str, message: &str| {
            item(
                "IngressTLS",
                name,
                Some("dapp-system"),
                json!({"conditions": [{"type": "Ready", "status": ready, "reason": reason,
                    "message": message}]}),
            )
        };
        list(
            "tls.dropapp.ru",
            "IngressTLS",
            vec![
                tls("default", "True", "Issued", ""),
                tls(
                    "apps",
                    "False",
                    "CertificateExpired",
                    "certificate expired on 2026-10-30",
                ),
            ],
        )
    }

    /// Событие `Warning` по объекту с uid `uid`.
    pub fn warning(uid: &str, reason: &str, message: &str, last: &str) -> Value {
        json!({"metadata": {"name": format!("{uid}.{reason}"), "uid": format!("{uid}-{last}")},
            "involvedObject": {"uid": uid}, "type": "Warning", "reason": reason,
            "message": message, "lastTimestamp": last})
    }

    pub fn events(items: Vec<Value>) -> Value {
        json!({"apiVersion": "v1", "kind": "EventList", "metadata": {}, "items": items})
    }
}

#[cfg(test)]
mod tests {
    use super::fixtures::*;
    use super::*;
    use crate::k8s::testing::{mock_client, status};
    use std::time::Duration;

    fn items(list: serde_json::Value) -> Vec<DynamicObject> {
        list["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| object(v.clone()))
            .collect()
    }

    #[test]
    fn resources_of_the_overview() {
        let path = |ar: ApiResource| format!("{}/{}", ar.api_version, ar.plural);
        assert_eq!(
            path(cluster_metadatas()),
            "config.dropapp.ru/v1alpha1/clustermetadatas"
        );
        assert_eq!(path(components()), "config.dropapp.ru/v1alpha1/components");
        assert_eq!(path(tools()), "config.dropapp.ru/v1alpha1/tools");
        assert_eq!(
            path(dropapp_nodes()),
            "nodes.dropapp.ru/v1alpha1/dropappnodes"
        );
        assert_eq!(
            path(ingress_tlses()),
            "tls.dropapp.ru/v1alpha1/ingresstlses"
        );
    }

    #[test]
    fn cluster_name_and_version_from_cluster_metadata() {
        let m = cluster_metadata(&items(cluster_metadata_list())[0]);
        assert_eq!(m.cluster_name.as_deref(), Some("prod-msk"));
        assert_eq!(m.version.as_deref(), Some("5.1.2"));
        let empty = cluster_metadata(&object(serde_json::json!({"kind": "ClusterMetadata",
            "metadata": {"name": "m"}, "spec": {"clusterName": ""}})));
        assert_eq!(empty, DropAppMetadata::default());
    }

    #[test]
    fn component_status_from_phase_or_status_without_case() {
        let rows: Vec<_> = items(component_list())
            .iter()
            .map(|o| component_row(ComponentKind::Component, o))
            .chain(
                items(tool_list())
                    .iter()
                    .map(|o| component_row(ComponentKind::Tool, o)),
            )
            .collect();
        let view: Vec<_> = rows
            .iter()
            .map(|r| {
                (
                    r.name.as_str(),
                    r.namespace.as_deref(),
                    r.status.as_deref(),
                    r.retry,
                    r.has_problem(),
                )
            })
            .collect();
        assert_eq!(
            view,
            [
                (
                    "ingress-nginx",
                    Some("dapp-system"),
                    Some("Success"),
                    None,
                    false
                ),
                (
                    "cert-manager",
                    Some("dapp-system"),
                    Some("Failed"),
                    Some(3),
                    true
                ),
                // Значение, которого документация не называет, — как есть и не проблема.
                (
                    "monitoring",
                    Some("dapp-monitoring"),
                    Some("Installing"),
                    None,
                    false
                ),
                ("grafana", None, Some("failed"), Some(2), true),
                ("dex", None, Some("upgrading"), None, true),
                ("loki", None, None, None, false),
            ]
        );
        assert_eq!(rows[0].kind, ComponentKind::Component);
        assert_eq!(rows[3].kind, ComponentKind::Tool);
        assert!(rows.iter().all(|r| r.event.is_none()));
        let error = component_row(
            ComponentKind::Tool,
            &object(item(
                "Tool",
                "x",
                None,
                serde_json::json!({"phase": "ERROR"}),
            )),
        );
        assert!(error.has_problem());
        let json = serde_json::to_value(&rows[3]).unwrap();
        assert_eq!(
            json,
            serde_json::json!({"kind": "Tool", "name": "grafana", "namespace": null,
                "status": "failed", "retry": 2, "event": null})
        );
    }

    #[test]
    fn failed_is_failed_or_error_but_not_upgrading() {
        let row = |status: &str| {
            component_row(
                ComponentKind::Tool,
                &object(item(
                    "Tool",
                    "x",
                    None,
                    serde_json::json!({"phase": status}),
                )),
            )
        };
        let view: Vec<_> = ["Failed", "ERROR", "upgrading", "Success", ""]
            .into_iter()
            .map(|s| (s, row(s).is_failed()))
            .collect();
        assert_eq!(
            view,
            [
                ("Failed", true),
                ("ERROR", true),
                ("upgrading", false),
                ("Success", false),
                ("", false),
            ]
        );
    }

    #[test]
    fn dropapp_node_is_a_problem_unless_ready_but_no_status_is_unknown() {
        let rows: Vec<_> = items(node_list()).iter().map(dropapp_node_row).collect();
        let view: Vec<_> = rows
            .iter()
            .map(|r| (r.name.as_str(), r.status.as_deref(), r.has_problem()))
            .collect();
        assert_eq!(
            view,
            [
                ("master-1", Some("ready"), false),
                ("worker-2", Some("NotReady"), true),
                ("worker-3", None, false),
            ]
        );
        let ready = dropapp_node_row(&object(item(
            "DropAppNode",
            "n",
            None,
            serde_json::json!({"phase": "Ready"}),
        )));
        assert!(!ready.has_problem(), "без учёта регистра");
    }

    #[test]
    fn ingress_tls_is_a_problem_when_ready_is_not_true() {
        let problems: Vec<_> = items(ingress_tls_list())
            .iter()
            .filter_map(ingress_tls_problem)
            .collect();
        assert_eq!(
            problems,
            [IngressTlsRow {
                name: "apps".into(),
                namespace: Some("dapp-system".into()),
                reason: Some("CertificateExpired".into()),
                message: Some("certificate expired on 2026-10-30".into()),
            }]
        );
        // Условия `Ready` ещё нет — состояние неизвестно, не проблема.
        let fresh = object(item("IngressTLS", "new", None, serde_json::json!({})));
        assert_eq!(ingress_tls_problem(&fresh), None);
        let unknown = object(item(
            "IngressTLS",
            "u",
            None,
            serde_json::json!({"conditions": [{"type": "Ready", "status": "Unknown"}]}),
        ));
        assert_eq!(
            ingress_tls_problem(&unknown).map(|r| r.name),
            Some("u".into())
        );
    }

    fn reader(client: kube::Client) -> ReadClient {
        ReadClient::new(client, Duration::from_secs(5))
    }

    #[tokio::test]
    async fn last_warning_is_the_latest_by_its_uid() {
        let (client, server) = mock_client(|s| match s.path() {
            "/api/v1/namespaces/dapp-system/events" => (
                200,
                events(vec![
                    warning(
                        "uid-cert-manager",
                        "BackOff",
                        "retrying",
                        "2026-10-31T10:00:00Z",
                    ),
                    warning(
                        "uid-cert-manager",
                        "InstallFailed",
                        "helm install failed: timed out",
                        "2026-10-31T11:00:00Z",
                    ),
                ]),
            ),
            "/api/v1/events" => (200, events(vec![])),
            _ => status(403, "Forbidden", "events is forbidden"),
        });
        let r = reader(client);
        assert_eq!(
            last_warning(&r, Some("dapp-system"), "uid-cert-manager")
                .await
                .as_deref(),
            Some("InstallFailed: helm install failed: timed out")
        );
        assert_eq!(last_warning(&r, None, "uid-grafana").await, None);
        assert_eq!(
            last_warning(&r, Some("dapp-auth"), "uid-dex").await,
            None,
            "ошибка чтения событий — события нет"
        );
        drop(r);
        let seen = server.await.unwrap();
        assert!(
            seen[0]
                .uri
                .contains("fieldSelector=involvedObject.uid%3Duid-cert-manager%2Ctype%3DWarning"),
            "{}",
            seen[0].uri
        );
    }

    #[tokio::test]
    async fn events_are_asked_only_for_problems_and_at_most_twenty() {
        let (client, server) = mock_client(|s| {
            let uid = s.uri.split("involvedObject.uid%3D").nth(1).unwrap_or("");
            let uid = uid.split('%').next().unwrap_or("");
            (
                200,
                events(vec![warning(uid, "Failed", "boom", "2026-10-31T10:00:00Z")]),
            )
        });
        let r = reader(client);
        let mut objects: Vec<_> = (0..25)
            .map(|i| {
                (
                    ComponentKind::Tool,
                    object(item(
                        "Tool",
                        &format!("t{i:02}"),
                        None,
                        serde_json::json!({"phase": "failed"}),
                    )),
                )
            })
            .collect();
        objects.push((
            ComponentKind::Component,
            object(item(
                "Component",
                "fine",
                Some("dapp-system"),
                serde_json::json!({"phase": "success"}),
            )),
        ));
        let rows = component_problems(&r, &objects).await;
        assert_eq!(rows.len(), 25, "в списке все проблемные");
        assert!(rows.iter().all(|r| r.name != "fine"));
        assert_eq!(rows[0].event.as_deref(), Some("Failed: boom"));
        assert_eq!(rows.iter().filter(|r| r.event.is_some()).count(), 20);
        assert!(rows[20..].iter().all(|r| r.event.is_none()));
        drop(r);
        assert_eq!(server.await.unwrap().len(), MAX_EVENT_LOOKUPS);
    }
}
