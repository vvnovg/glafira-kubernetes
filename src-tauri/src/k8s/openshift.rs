//! Объекты OpenShift (спец. §2): своих типов в `k8s-openapi` у них нет, поэтому они читаются как
//! `DynamicObject`, а нужные поля берутся из JSON. Здесь — их `ApiResource` и разбор.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use kube::api::{ApiResource, DynamicObject, GroupVersionKind};
use serde::Serialize;
use sha2::Digest;

use crate::k8s::view::{ConditionView, conditions};

fn resource(group: &str, kind: &str, plural: &str) -> ApiResource {
    ApiResource::from_gvk_with_plural(&GroupVersionKind::gvk(group, "v1", kind), plural)
}

pub fn cluster_versions() -> ApiResource {
    resource("config.openshift.io", "ClusterVersion", "clusterversions")
}

pub fn cluster_operators() -> ApiResource {
    resource("config.openshift.io", "ClusterOperator", "clusteroperators")
}

pub fn infrastructures() -> ApiResource {
    resource("config.openshift.io", "Infrastructure", "infrastructures")
}

pub fn consoles() -> ApiResource {
    resource("config.openshift.io", "Console", "consoles")
}

pub fn machine_config_pools() -> ApiResource {
    resource(
        "machineconfiguration.openshift.io",
        "MachineConfigPool",
        "machineconfigpools",
    )
}

pub fn user_oauth_access_tokens() -> ApiResource {
    resource(
        "oauth.openshift.io",
        "UserOAuthAccessToken",
        "useroauthaccesstokens",
    )
}

/// Имя объекта `UserOAuthAccessToken` для токена `sha256~…`: `sha256~` и base64url без `=`
/// от SHA-256 части после префикса (так хранит токены OAuth-сервер OpenShift). Токены другого
/// вида (JWT внешнего OIDC, ServiceAccount) в этих объектах не хранятся — `None`.
pub fn oauth_token_object_name(token: &str) -> Option<String> {
    let raw = token.trim().strip_prefix("sha256~")?;
    let digest = sha2::Sha256::digest(raw.as_bytes());
    Some(format!("sha256~{}", URL_SAFE_NO_PAD.encode(digest)))
}

/// Когда истекает токен по его объекту: время создания + `expiresIn` (0 — бессрочный).
pub fn token_expires_at(o: &DynamicObject) -> Option<i64> {
    let created = o.metadata.creation_timestamp.as_ref()?;
    let expires_in = o.data["expiresIn"].as_i64().filter(|s| *s > 0)?;
    Some(crate::k8s::view::seconds(created) + expires_in)
}

/// Версия кластера (`ClusterVersion` `version`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterVersionInfo {
    pub version: Option<String>,
    pub channel: Option<String>,
    pub available_updates: usize,
    pub failing: Option<ConditionView>,
    pub progressing: Option<ConditionView>,
}

pub fn cluster_version_info(o: &DynamicObject) -> ClusterVersionInfo {
    let list = conditions(&o.data["status"]["conditions"]);
    let take = |kind: &str| list.iter().find(|c| c.kind == kind).cloned();
    ClusterVersionInfo {
        version: o.data["status"]["desired"]["version"]
            .as_str()
            .map(String::from),
        channel: o.data["spec"]["channel"]
            .as_str()
            .filter(|c| !c.is_empty())
            .map(String::from),
        available_updates: o.data["status"]["availableUpdates"]
            .as_array()
            .map_or(0, Vec::len),
        failing: take("Failing"),
        progressing: take("Progressing"),
    }
}

/// Инфраструктура кластера (`Infrastructure` `cluster`): `AWS`, `VSphere`, `BareMetal`, `None`…
pub fn infrastructure_type(o: &DynamicObject) -> Option<String> {
    let status = &o.data["status"];
    status["platformStatus"]["type"]
        .as_str()
        .or_else(|| status["platform"].as_str())
        .map(String::from)
}

/// Адрес веб-консоли (`Console` `cluster`).
pub fn console_url(o: &DynamicObject) -> Option<String> {
    o.data["status"]["consoleURL"]
        .as_str()
        .filter(|u| !u.is_empty())
        .map(String::from)
}

/// ClusterOperator: доступен ли, обновляется ли, деградировал ли и почему.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OperatorRow {
    pub name: String,
    pub version: Option<String>,
    pub available: Option<bool>,
    pub progressing: Option<bool>,
    pub degraded: Option<bool>,
    /// Сообщение условия, из-за которого оператор не в норме.
    pub message: Option<String>,
}

impl OperatorRow {
    /// Не в норме: недоступен, деградировал или обновляется.
    pub fn has_problem(&self) -> bool {
        self.available != Some(true)
            || self.degraded == Some(true)
            || self.progressing == Some(true)
    }
}

pub fn operator_row(o: &DynamicObject) -> OperatorRow {
    let list = conditions(&o.data["status"]["conditions"]);
    let flag = |kind: &str| crate::k8s::view::condition_flag(&list, kind);
    let message_of = |kind: &str, status: &str| {
        list.iter()
            .find(|c| c.kind == kind && c.status == status)
            .and_then(|c| c.message.clone())
    };
    let name = o.metadata.name.clone().unwrap_or_default();
    let version = o.data["status"]["versions"].as_array().and_then(|vs| {
        vs.iter()
            .find(|v| v["name"] == "operator")
            .and_then(|v| v["version"].as_str())
            .map(String::from)
    });
    OperatorRow {
        version,
        available: flag("Available"),
        progressing: flag("Progressing"),
        degraded: flag("Degraded"),
        message: message_of("Degraded", "True")
            .or_else(|| message_of("Available", "False"))
            .or_else(|| message_of("Progressing", "True")),
        name,
    }
}

/// MachineConfigPool: деградировал ли, обновляется ли, сколько машин в каком состоянии.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MachineConfigPoolRow {
    pub name: String,
    pub degraded: bool,
    pub updating: bool,
    pub machines: i64,
    pub ready: i64,
    pub updated: i64,
    pub degraded_machines: i64,
}

pub fn machine_config_pool_row(o: &DynamicObject) -> MachineConfigPoolRow {
    let status = &o.data["status"];
    let list = conditions(&status["conditions"]);
    let count = |k: &str| status[k].as_i64().unwrap_or(0);
    MachineConfigPoolRow {
        name: o.metadata.name.clone().unwrap_or_default(),
        degraded: crate::k8s::view::condition_flag(&list, "Degraded") == Some(true),
        updating: crate::k8s::view::condition_flag(&list, "Updating") == Some(true),
        machines: count("machineCount"),
        ready: count("readyMachineCount"),
        updated: count("updatedMachineCount"),
        degraded_machines: count("degradedMachineCount"),
    }
}

#[cfg(test)]
pub(crate) mod fixtures {
    use kube::api::DynamicObject;

    pub fn object(v: serde_json::Value) -> DynamicObject {
        serde_json::from_value(v).unwrap()
    }

    pub fn cluster_version() -> serde_json::Value {
        serde_json::json!({"apiVersion": "config.openshift.io/v1", "kind": "ClusterVersion",
        "metadata": {"name": "version"},
        "spec": {"channel": "stable-4.18"},
        "status": {"desired": {"version": "4.18.3"},
            "availableUpdates": [{"version": "4.18.4"}, {"version": "4.18.5"}],
            "conditions": [
                {"type": "Available", "status": "True"},
                {"type": "Failing", "status": "False"},
                {"type": "Progressing", "status": "True", "message": "Working towards 4.18.4"}
            ]}})
    }

    pub fn operators() -> serde_json::Value {
        let op = |name: &str, available: &str, degraded: &str, message: &str| {
            serde_json::json!({"metadata": {"name": name},
            "status": {"versions": [{"name": "operator", "version": "4.18.3"}],
                "conditions": [
                    {"type": "Available", "status": available},
                    {"type": "Progressing", "status": "False"},
                    {"type": "Degraded", "status": degraded, "message": message}
                ]}})
        };
        serde_json::json!({"apiVersion": "config.openshift.io/v1", "kind": "ClusterOperatorList",
        "metadata": {}, "items": [
            op("dns", "True", "False", ""),
            op("ingress", "True", "True", "Some ingresscontrollers are degraded"),
            op("monitoring", "False", "False", "")
        ]})
    }

    pub fn pools() -> serde_json::Value {
        serde_json::json!({"apiVersion": "machineconfiguration.openshift.io/v1",
        "kind": "MachineConfigPoolList", "metadata": {}, "items": [
            {"metadata": {"name": "master"}, "status": {"machineCount": 3,
                "readyMachineCount": 3, "updatedMachineCount": 3, "degradedMachineCount": 0,
                "conditions": [{"type": "Updating", "status": "False"},
                    {"type": "Degraded", "status": "False"}]}},
            {"metadata": {"name": "worker"}, "status": {"machineCount": 4,
                "readyMachineCount": 3, "updatedMachineCount": 2, "degradedMachineCount": 0,
                "conditions": [{"type": "Updating", "status": "True"},
                    {"type": "Degraded", "status": "False"}]}}
        ]})
    }
}

#[cfg(test)]
mod tests {
    use super::fixtures::*;
    use super::*;

    #[test]
    fn token_object_name_and_expiry() {
        assert_eq!(
            oauth_token_object_name("sha256~abc").as_deref(),
            Some("sha256~ungWv48Bz-pBQUDeXa4iI7ADYaOWF3qctBD_YfIAFa0")
        );
        assert_eq!(oauth_token_object_name("eyJhbGciOi.x.y"), None);
        let token = |expires_in: i64| {
            object(serde_json::json!({"apiVersion": "oauth.openshift.io/v1",
                "kind": "UserOAuthAccessToken",
                "metadata": {"name": "sha256~x", "creationTimestamp": "2026-10-25T10:00:00Z"},
                "expiresIn": expires_in}))
        };
        assert_eq!(
            token_expires_at(&token(86_400)),
            Some(1_792_922_400 + 86_400)
        );
        assert_eq!(token_expires_at(&token(0)), None, "бессрочный");
    }

    #[test]
    fn cluster_version_infrastructure_and_console() {
        let cv = cluster_version_info(&object(cluster_version()));
        assert_eq!(cv.version.as_deref(), Some("4.18.3"));
        assert_eq!(cv.channel.as_deref(), Some("stable-4.18"));
        assert_eq!(cv.available_updates, 2);
        assert_eq!(cv.failing.map(|c| c.status), Some("False".into()));
        assert_eq!(
            cv.progressing.and_then(|c| c.message),
            Some("Working towards 4.18.4".into())
        );
        let infra = object(serde_json::json!({"apiVersion": "config.openshift.io/v1",
            "kind": "Infrastructure", "metadata": {"name": "cluster"},
            "status": {"platform": "AWS", "platformStatus": {"type": "AWS"}}}));
        assert_eq!(infrastructure_type(&infra).as_deref(), Some("AWS"));
        let console = object(serde_json::json!({"apiVersion": "config.openshift.io/v1",
            "kind": "Console", "metadata": {"name": "cluster"},
            "status": {"consoleURL": "https://console-openshift-console.apps.c.example.com"}}));
        assert_eq!(
            console_url(&console).as_deref(),
            Some("https://console-openshift-console.apps.c.example.com")
        );
    }

    #[test]
    fn operators_and_pools() {
        let list: kube::api::ObjectList<DynamicObject> =
            serde_json::from_value(operators()).unwrap();
        let rows: Vec<_> = list.items.iter().map(operator_row).collect();
        assert_eq!(rows[0].version.as_deref(), Some("4.18.3"));
        assert!(!rows[0].has_problem());
        assert!(rows[1].has_problem());
        assert_eq!(
            rows[1].message.as_deref(),
            Some("Some ingresscontrollers are degraded")
        );
        assert_eq!(rows[2].available, Some(false));
        assert!(rows[2].has_problem());
        let pools: kube::api::ObjectList<DynamicObject> = serde_json::from_value(pools()).unwrap();
        let rows: Vec<_> = pools.items.iter().map(machine_config_pool_row).collect();
        assert!(!rows[0].updating && !rows[0].degraded);
        assert_eq!(
            (rows[1].updating, rows[1].machines, rows[1].updated),
            (true, 4, 2)
        );
    }
}
