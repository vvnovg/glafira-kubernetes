//! Конфигурация проекта (спец. §6.3): ConfigMaps — ключи, значения по запросу; Secrets — имя,
//! тип и ключи. Значения Secret из Rust не уходят никогда.

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::{ConfigMap, Secret};
use kube::api::ListParams;
use serde::Serialize;

use crate::error::AppResult;
use crate::k8s::read::ReadClient;
use crate::k8s::view::{Part, created, name};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigMapRow {
    pub name: String,
    pub keys: Vec<String>,
    /// Ключи `binaryData`.
    pub binary_keys: Vec<String>,
    pub created: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SecretRow {
    pub name: String,
    /// `Opaque`, `kubernetes.io/tls`, `kubernetes.io/dockerconfigjson`…
    #[serde(rename = "type")]
    pub kind: String,
    pub keys: Vec<String>,
    pub created: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigView {
    pub config_maps: Part<Vec<ConfigMapRow>>,
    pub secrets: Part<Vec<SecretRow>>,
}

pub fn config_map_row(c: &ConfigMap) -> ConfigMapRow {
    ConfigMapRow {
        name: name(&c.metadata),
        keys: c.data.iter().flatten().map(|(k, _)| k.clone()).collect(),
        binary_keys: c
            .binary_data
            .iter()
            .flatten()
            .map(|(k, _)| k.clone())
            .collect(),
        created: created(&c.metadata),
    }
}

/// Строка Secret: только имя, тип и ключи; значения отбрасываются здесь же.
pub fn secret_row(s: &Secret) -> SecretRow {
    SecretRow {
        name: name(&s.metadata),
        kind: s.type_.clone().unwrap_or_else(|| "Opaque".into()),
        keys: s.data.iter().flatten().map(|(k, _)| k.clone()).collect(),
        created: created(&s.metadata),
    }
}

pub async fn get_config(r: &ReadClient, namespace: &str) -> ConfigView {
    let ns = Some(namespace);
    let lp = ListParams::default();
    let (config_maps, secrets) =
        tokio::join!(r.list::<ConfigMap>(ns, &lp), r.list::<Secret>(ns, &lp));
    ConfigView {
        config_maps: config_maps
            .map(|l| l.items.iter().map(config_map_row).collect())
            .into(),
        secrets: secrets
            .map(|l| l.items.iter().map(secret_row).collect())
            .into(),
    }
}

/// Значение ключа ConfigMap: текст или размер двоичных данных.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigValue {
    pub text: Option<String>,
    pub binary_size: Option<usize>,
}

/// Значения ConfigMap — по щелчку в интерфейсе.
pub async fn config_map_values(
    r: &ReadClient,
    namespace: &str,
    config_map: &str,
) -> AppResult<BTreeMap<String, ConfigValue>> {
    let c: ConfigMap = r.get(namespace, config_map).await?;
    let mut out: BTreeMap<String, ConfigValue> = c
        .data
        .unwrap_or_default()
        .into_iter()
        .map(|(k, v)| {
            (
                k,
                ConfigValue {
                    text: Some(v),
                    binary_size: None,
                },
            )
        })
        .collect();
    for (k, v) in c.binary_data.unwrap_or_default() {
        out.insert(
            k,
            ConfigValue {
                text: None,
                binary_size: Some(v.0.len()),
            },
        );
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::k8s::testing::{mock_client, status};
    use std::time::Duration;

    #[tokio::test]
    async fn config_maps_with_keys_and_secrets_without_values() {
        let (client, _server) = mock_client(|s| match s.path() {
            "/api/v1/namespaces/team-a/configmaps" => (
                200,
                serde_json::json!({"apiVersion": "v1", "kind": "ConfigMapList", "metadata": {}, "items": [
                    {"metadata": {"name": "web"}, "data": {"LOG_LEVEL": "debug", "app.yaml": "a: 1"},
                        "binaryData": {"logo.png": "iVBORw0K"}}]}),
            ),
            "/api/v1/namespaces/team-a/secrets" => (
                200,
                serde_json::json!({"apiVersion": "v1", "kind": "SecretList", "metadata": {}, "items": [
                    {"metadata": {"name": "db"}, "type": "Opaque",
                        "data": {"password": "czNjcjN0", "user": "YWRtaW4="}},
                    {"metadata": {"name": "web-tls"}, "type": "kubernetes.io/tls",
                        "data": {"tls.crt": "eA==", "tls.key": "eQ=="}}]}),
            ),
            _ => status(404, "NotFound", "x"),
        });
        let r = ReadClient::new(client, Duration::from_secs(5));
        let v = get_config(&r, "team-a").await;
        let cms = v.config_maps.data.unwrap();
        assert_eq!(cms[0].keys, ["LOG_LEVEL", "app.yaml"]);
        assert_eq!(cms[0].binary_keys, ["logo.png"]);
        let secrets = v.secrets.data.unwrap();
        assert_eq!(secrets[0].keys, ["password", "user"]);
        assert_eq!(secrets[1].kind, "kubernetes.io/tls");
        let json = serde_json::to_string(&secrets).unwrap();
        assert!(
            !json.contains("czNjcjN0") && !json.contains("s3cr3t"),
            "{json}"
        );
    }

    #[tokio::test]
    async fn values_of_a_config_map_on_request() {
        let (client, server) = mock_client(|_| {
            (
                200,
                serde_json::json!({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "web"},
                    "data": {"LOG_LEVEL": "debug"}, "binaryData": {"logo.png": "AAECAw=="}}),
            )
        });
        let r = ReadClient::new(client, Duration::from_secs(5));
        let v = config_map_values(&r, "team-a", "web").await.unwrap();
        assert_eq!(v["LOG_LEVEL"].text.as_deref(), Some("debug"));
        assert_eq!(v["logo.png"].binary_size, Some(4));
        drop(r);
        assert_eq!(
            server.await.unwrap()[0].path(),
            "/api/v1/namespaces/team-a/configmaps/web"
        );
    }

    #[tokio::test]
    async fn forbidden_secrets_do_not_hide_config_maps() {
        let (client, _server) = mock_client(|s| match s.path() {
            "/api/v1/namespaces/team-a/secrets" => status(
                403,
                "Forbidden",
                r#"secrets is forbidden: User "u" cannot list resource "secrets" in API group "" in the namespace "team-a""#,
            ),
            _ => (
                200,
                serde_json::json!({"apiVersion": "v1", "kind": "ConfigMapList", "metadata": {}, "items": []}),
            ),
        });
        let r = ReadClient::new(client, Duration::from_secs(5));
        let v = get_config(&r, "team-a").await;
        assert!(v.config_maps.data.is_some());
        assert_eq!(v.secrets.error.unwrap().params["resource"], "secrets");
    }
}
