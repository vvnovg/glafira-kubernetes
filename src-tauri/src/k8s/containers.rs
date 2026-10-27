//! Описание контейнеров шаблона пода (спец. §6.4–6.5): образ, requests и limits, порты, пробы,
//! имена переменных окружения и их источники. Значения переменных не показываются.

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::{Container, PodSpec, Probe};
use serde::Serialize;

use crate::k8s::view::int_or_string;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerSpecView {
    pub name: String,
    pub image: String,
    pub init: bool,
    pub requests: BTreeMap<String, String>,
    pub limits: BTreeMap<String, String>,
    /// `8080/TCP (http)`.
    pub ports: Vec<String>,
    pub liveness: Option<String>,
    pub readiness: Option<String>,
    pub startup: Option<String>,
    pub env: Vec<EnvView>,
    /// `command` и `args` — только для MCP: крейт шлюза маскирует их (спец. §10.3). Интерфейс их
    /// не получает (обзор контейнера показывает env именами), их видно в YAML.
    #[serde(skip)]
    pub command: Vec<String>,
    #[serde(skip)]
    pub args: Vec<String>,
}

/// Переменная окружения: имя и откуда берётся значение (само значение не показывается).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EnvView {
    /// У `envFrom` — `*` (все ключи источника) или префикс с `*`.
    pub name: String,
    pub source: EnvSource,
    /// Объект и ключ источника: `app-config/LOG_LEVEL`, `metadata.name`, `limits.memory`.
    pub reference: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum EnvSource {
    Literal,
    ConfigMapKeyRef,
    SecretKeyRef,
    FieldRef,
    ResourceFieldRef,
    /// `envFrom.configMapRef`: все ключи ConfigMap.
    ConfigMapRef,
    /// `envFrom.secretRef`: все ключи Secret.
    SecretRef,
}

/// Проба как в `kubectl describe`: `http-get :8080/healthz delay=5s timeout=1s period=10s
/// #success=1 #failure=3`.
pub fn probe_text(p: &Probe) -> String {
    let action = if let Some(h) = &p.http_get {
        let scheme = h.scheme.as_deref().unwrap_or("HTTP").to_lowercase();
        format!(
            "http-get {scheme}://{}:{}{}",
            h.host.as_deref().unwrap_or(""),
            int_or_string(&h.port),
            h.path.as_deref().unwrap_or("/")
        )
    } else if let Some(t) = &p.tcp_socket {
        format!(
            "tcp-socket {}:{}",
            t.host.as_deref().unwrap_or(""),
            int_or_string(&t.port)
        )
    } else if let Some(g) = &p.grpc {
        format!("grpc :{}", g.port)
    } else if let Some(e) = &p.exec {
        format!("exec [{}]", e.command.clone().unwrap_or_default().join(" "))
    } else {
        "unknown".to_string()
    };
    format!(
        "{action} delay={}s timeout={}s period={}s #success={} #failure={}",
        p.initial_delay_seconds.unwrap_or(0),
        p.timeout_seconds.unwrap_or(1),
        p.period_seconds.unwrap_or(10),
        p.success_threshold.unwrap_or(1),
        p.failure_threshold.unwrap_or(3)
    )
}

fn env_views(c: &Container) -> Vec<EnvView> {
    let mut out: Vec<EnvView> = c
        .env
        .iter()
        .flatten()
        .map(|e| {
            let (source, reference) = match &e.value_from {
                None => (EnvSource::Literal, None),
                Some(f) => {
                    if let Some(r) = &f.config_map_key_ref {
                        (
                            EnvSource::ConfigMapKeyRef,
                            Some(format!("{}/{}", r.name, r.key)),
                        )
                    } else if let Some(r) = &f.secret_key_ref {
                        (
                            EnvSource::SecretKeyRef,
                            Some(format!("{}/{}", r.name, r.key)),
                        )
                    } else if let Some(r) = &f.field_ref {
                        (EnvSource::FieldRef, Some(r.field_path.clone()))
                    } else if let Some(r) = &f.resource_field_ref {
                        (EnvSource::ResourceFieldRef, Some(r.resource.clone()))
                    } else {
                        (EnvSource::Literal, None)
                    }
                }
            };
            EnvView {
                name: e.name.clone(),
                source,
                reference,
            }
        })
        .collect();
    for f in c.env_from.iter().flatten() {
        let name = format!("{}*", f.prefix.clone().unwrap_or_default());
        if let Some(r) = &f.config_map_ref {
            out.push(EnvView {
                name: name.clone(),
                source: EnvSource::ConfigMapRef,
                reference: Some(r.name.clone()),
            });
        }
        if let Some(r) = &f.secret_ref {
            out.push(EnvView {
                name,
                source: EnvSource::SecretRef,
                reference: Some(r.name.clone()),
            });
        }
    }
    out
}

pub fn container_view(c: &Container, init: bool) -> ContainerSpecView {
    let quantities =
        |m: Option<&BTreeMap<String, k8s_openapi::apimachinery::pkg::api::resource::Quantity>>| {
            m.map(|m| m.iter().map(|(k, q)| (k.clone(), q.0.clone())).collect())
                .unwrap_or_default()
        };
    let resources = c.resources.as_ref();
    ContainerSpecView {
        name: c.name.clone(),
        image: c.image.clone().unwrap_or_default(),
        init,
        requests: quantities(resources.and_then(|r| r.requests.as_ref())),
        limits: quantities(resources.and_then(|r| r.limits.as_ref())),
        ports: c
            .ports
            .iter()
            .flatten()
            .map(|p| {
                let proto = p.protocol.as_deref().unwrap_or("TCP");
                match &p.name {
                    Some(n) => format!("{}/{proto} ({n})", p.container_port),
                    None => format!("{}/{proto}", p.container_port),
                }
            })
            .collect(),
        liveness: c.liveness_probe.as_ref().map(probe_text),
        readiness: c.readiness_probe.as_ref().map(probe_text),
        startup: c.startup_probe.as_ref().map(probe_text),
        env: env_views(c),
        command: c.command.clone().unwrap_or_default(),
        args: c.args.clone().unwrap_or_default(),
    }
}

/// Init-контейнеры, затем обычные.
pub fn containers_of(spec: &PodSpec) -> Vec<ContainerSpecView> {
    spec.init_containers
        .iter()
        .flatten()
        .map(|c| container_view(c, true))
        .chain(spec.containers.iter().map(|c| container_view(c, false)))
        .collect()
}

/// Образы обычных контейнеров (без init).
pub fn images_of(spec: &PodSpec) -> Vec<String> {
    spec.containers
        .iter()
        .filter_map(|c| c.image.clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> PodSpec {
        serde_json::from_value(serde_json::json!({
            "initContainers": [{"name": "migrate", "image": "app:1"}],
            "containers": [{
                "name": "web", "image": "registry/app:1.2",
                "ports": [{"containerPort": 8080, "name": "http"}, {"containerPort": 9090, "protocol": "UDP"}],
                "resources": {"requests": {"cpu": "100m", "memory": "128Mi"}, "limits": {"memory": "256Mi"}},
                "readinessProbe": {"httpGet": {"path": "/ready", "port": 8080}, "periodSeconds": 5},
                "livenessProbe": {"tcpSocket": {"port": "http"}, "initialDelaySeconds": 10},
                "startupProbe": {"exec": {"command": ["cat", "/tmp/started"]}, "failureThreshold": 30},
                "env": [
                    {"name": "MODE", "value": "prod"},
                    {"name": "LOG_LEVEL", "valueFrom": {"configMapKeyRef": {"name": "cfg", "key": "level"}}},
                    {"name": "DB_PASSWORD", "valueFrom": {"secretKeyRef": {"name": "db", "key": "password"}}},
                    {"name": "POD", "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}},
                    {"name": "MEM", "valueFrom": {"resourceFieldRef": {"resource": "limits.memory"}}}
                ],
                "envFrom": [{"configMapRef": {"name": "common"}}, {"prefix": "S_", "secretRef": {"name": "extra"}}]
            }]
        }))
        .unwrap()
    }

    #[test]
    fn containers_with_resources_probes_and_env_sources() {
        let list = containers_of(&spec());
        assert_eq!(list.len(), 2);
        assert!(list[0].init && !list[1].init);
        let web = &list[1];
        assert_eq!(web.requests["cpu"], "100m");
        assert_eq!(web.limits.get("cpu"), None);
        assert_eq!(web.ports, ["8080/TCP (http)", "9090/UDP"]);
        assert_eq!(
            web.readiness.as_deref(),
            Some("http-get http://:8080/ready delay=0s timeout=1s period=5s #success=1 #failure=3")
        );
        assert_eq!(
            web.liveness.as_deref(),
            Some("tcp-socket :http delay=10s timeout=1s period=10s #success=1 #failure=3")
        );
        assert!(
            web.startup
                .as_deref()
                .unwrap()
                .starts_with("exec [cat /tmp/started]")
        );
        let env: Vec<_> = web
            .env
            .iter()
            .map(|e| (e.name.as_str(), e.source, e.reference.as_deref()))
            .collect();
        assert_eq!(
            env,
            [
                ("MODE", EnvSource::Literal, None),
                ("LOG_LEVEL", EnvSource::ConfigMapKeyRef, Some("cfg/level")),
                ("DB_PASSWORD", EnvSource::SecretKeyRef, Some("db/password")),
                ("POD", EnvSource::FieldRef, Some("metadata.name")),
                ("MEM", EnvSource::ResourceFieldRef, Some("limits.memory")),
                ("*", EnvSource::ConfigMapRef, Some("common")),
                ("S_*", EnvSource::SecretRef, Some("extra")),
            ]
        );
        let json = serde_json::to_string(&list).unwrap();
        assert!(
            !json.contains("prod"),
            "значения переменных не уходят: {json}"
        );
        assert_eq!(images_of(&spec()), ["registry/app:1.2"]);
    }
}
