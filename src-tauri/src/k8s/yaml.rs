//! YAML объекта только для чтения (спец. §6.4–6.5): объект из API без `metadata.managedFields`,
//! записанный в YAML. Свой вывод JSON → YAML вместо крейта: нужно только записывать, а
//! `serde_yaml` больше не поддерживается. Secret сюда не попадает — его вида нет в `YamlKind`.

use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, StatefulSet};
use k8s_openapi::api::batch::v1::{CronJob, Job};
use k8s_openapi::api::core::v1::{ConfigMap, PersistentVolumeClaim, Pod, Service};
use k8s_openapi::api::networking::v1::Ingress;
use kube::api::ApiResource;
use serde::Deserialize;

use crate::error::AppResult;
use crate::k8s::read::ReadClient;

/// Виды объектов, YAML которых показывает интерфейс.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub enum YamlKind {
    Pod,
    Deployment,
    StatefulSet,
    DaemonSet,
    DeploymentConfig,
    Job,
    CronJob,
    Service,
    Route,
    Ingress,
    ConfigMap,
    PersistentVolumeClaim,
    Gateway,
    #[serde(rename = "HTTPRoute")]
    HttpRoute,
    #[serde(rename = "GRPCRoute")]
    GrpcRoute,
}

impl YamlKind {
    fn resource(self) -> ApiResource {
        use crate::k8s::network;
        match self {
            YamlKind::Pod => ApiResource::erase::<Pod>(&()),
            YamlKind::Deployment => ApiResource::erase::<Deployment>(&()),
            YamlKind::StatefulSet => ApiResource::erase::<StatefulSet>(&()),
            YamlKind::DaemonSet => ApiResource::erase::<DaemonSet>(&()),
            YamlKind::DeploymentConfig => crate::k8s::workloads::deployment_configs(),
            YamlKind::Job => ApiResource::erase::<Job>(&()),
            YamlKind::CronJob => ApiResource::erase::<CronJob>(&()),
            YamlKind::Service => ApiResource::erase::<Service>(&()),
            YamlKind::Route => network::routes(),
            YamlKind::Ingress => ApiResource::erase::<Ingress>(&()),
            YamlKind::ConfigMap => ApiResource::erase::<ConfigMap>(&()),
            YamlKind::PersistentVolumeClaim => ApiResource::erase::<PersistentVolumeClaim>(&()),
            YamlKind::Gateway => network::gateways(),
            YamlKind::HttpRoute => network::http_routes(),
            YamlKind::GrpcRoute => network::grpc_routes(),
        }
    }
}

/// YAML объекта без `managedFields`.
pub async fn object_yaml(
    r: &ReadClient,
    namespace: &str,
    kind: YamlKind,
    object: &str,
) -> AppResult<String> {
    let o = r
        .get_dynamic(&kind.resource(), Some(namespace), object)
        .await?;
    let mut v = serde_json::to_value(&o)?;
    if let Some(meta) = v.get_mut("metadata").and_then(|m| m.as_object_mut()) {
        meta.remove("managedFields");
    }
    Ok(to_yaml(&v))
}

/// Строку можно записать без кавычек: она не похожа на число, `true`, `null` и не содержит
/// символов, которые YAML понял бы иначе.
fn plain(s: &str) -> bool {
    const RESERVED: &[&str] = &[
        "true", "false", "yes", "no", "on", "off", "y", "n", "null", "~",
    ];
    let Some(first) = s.chars().next() else {
        return false;
    };
    let number_like = s
        .trim_start_matches(['+', '-'])
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_digit() || c == '.');
    // `-`, `?` и `:` — знаки YAML, только если за ними пробел (`--port` — обычная строка).
    let indicator = match first {
        '-' | '?' | ':' => s.chars().nth(1).is_none_or(|c| c == ' '),
        c => ",[]{}#&*!|>'\"%@`".contains(c),
    };
    !RESERVED.contains(&s.to_lowercase().as_str())
        && !number_like
        && !indicator
        && !s.starts_with(' ')
        && !s.ends_with(' ')
        && !s.ends_with(':')
        && !s.contains(": ")
        && !s.contains(" #")
        && !s.chars().any(|c| c.is_control())
}

fn scalar(s: &str) -> String {
    if plain(s) {
        s.to_string()
    } else {
        // Строка JSON — допустимая строка YAML в двойных кавычках.
        serde_json::to_string(s).unwrap_or_default()
    }
}

/// Многострочный текст — блоком `|`: `|-` без перевода строки в конце, `|+`, если их несколько.
fn block(s: &str, indent: usize, out: &mut String) -> bool {
    // Первая строка задаёт отступ блока: она не может быть пустой или начинаться с пробела.
    if !s.contains('\n')
        || s.starts_with(['\n', ' ', '\t'])
        || s.chars().any(|c| c.is_control() && c != '\n')
    {
        return false;
    }
    let body = s.trim_end_matches('\n');
    let chomp = match s.len() - body.len() {
        0 => "-",
        1 => "",
        _ => "+",
    };
    out.push('|');
    out.push_str(chomp);
    out.push('\n');
    let pad = " ".repeat(indent);
    let lines: Vec<&str> = if chomp == "+" {
        s.split('\n').collect::<Vec<_>>()[..s.matches('\n').count()].to_vec()
    } else {
        body.split('\n').collect()
    };
    for line in lines {
        if !line.is_empty() {
            out.push_str(&pad);
            out.push_str(line);
        }
        out.push('\n');
    }
    true
}

fn value(v: &serde_json::Value, indent: usize, out: &mut String) {
    use serde_json::Value;
    let pad = " ".repeat(indent);
    match v {
        Value::Object(m) if !m.is_empty() => {
            out.push('\n');
            for (k, child) in m {
                out.push_str(&pad);
                out.push_str(&scalar(k));
                out.push(':');
                inline(child, indent + 2, out);
            }
        }
        Value::Array(a) if !a.is_empty() => {
            out.push('\n');
            for item in a {
                out.push_str(&pad);
                out.push('-');
                match item {
                    Value::Object(m) if !m.is_empty() => {
                        // Первый ключ — на строке с `-`, остальные — под ним.
                        let mut first = true;
                        for (k, child) in m {
                            if first {
                                out.push(' ');
                                first = false;
                            } else {
                                out.push_str(&pad);
                                out.push_str("  ");
                            }
                            out.push_str(&scalar(k));
                            out.push(':');
                            inline(child, indent + 4, out);
                        }
                    }
                    _ => inline(item, indent + 2, out),
                }
            }
        }
        // Скаляры пишет `inline`.
        _ => unreachable!("scalars are written by inline"),
    }
}

/// Значение после `ключ:` или `-`: скаляр на той же строке, коллекция — с новой.
fn inline(v: &serde_json::Value, indent: usize, out: &mut String) {
    use serde_json::Value;
    match v {
        Value::Object(m) if !m.is_empty() => value(v, indent, out),
        Value::Array(a) if !a.is_empty() => value(v, indent, out),
        Value::Object(_) => out.push_str(" {}\n"),
        Value::Array(_) => out.push_str(" []\n"),
        Value::Null => out.push_str(" null\n"),
        Value::Bool(b) => out.push_str(&format!(" {b}\n")),
        Value::Number(n) => out.push_str(&format!(" {n}\n")),
        Value::String(s) => {
            out.push(' ');
            if !block(s, indent, out) {
                out.push_str(&scalar(s));
                out.push('\n');
            }
        }
    }
}

/// YAML документа JSON.
pub fn to_yaml(v: &serde_json::Value) -> String {
    let mut out = String::new();
    match v {
        serde_json::Value::Object(m) if !m.is_empty() => value(v, 0, &mut out),
        serde_json::Value::Array(a) if !a.is_empty() => value(v, 0, &mut out),
        _ => {
            inline(v, 0, &mut out);
            return out.trim_start().to_string();
        }
    }
    out.trim_start_matches('\n').to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::k8s::testing::mock_client;
    use std::time::Duration;

    /// Ключи — по алфавиту, как в `kubectl get -o yaml`.
    #[test]
    fn nested_maps_lists_and_scalars() {
        let v = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "web", "labels": {"app.kubernetes.io/name": "web"}, "annotations": {}},
            "spec": {"containers": [
                {"name": "web", "args": ["--port", "8080"], "ports": [{"containerPort": 8080}], "env": []},
                {"name": "side", "tty": true, "command": null}
            ]}
        });
        assert_eq!(
            to_yaml(&v),
            "apiVersion: v1\nkind: Pod\nmetadata:\n  annotations: {}\n  labels:\n    app.kubernetes.io/name: web\n  name: web\nspec:\n  containers:\n    - args:\n        - --port\n        - \"8080\"\n      env: []\n      name: web\n      ports:\n        - containerPort: 8080\n    - command: null\n      name: side\n      tty: true\n"
        );
    }

    #[test]
    fn strings_that_need_quotes_or_blocks() {
        for (s, yaml) in [
            ("true", "\"true\""),
            ("1.0", "\"1.0\""),
            ("", "\"\""),
            ("a: b", "\"a: b\""),
            ("*", "\"*\""),
            ("- x", "\"- x\""),
            ("-", "\"-\""),
            ("--port", "--port"),
            ("\nx", "\"\\nx\""),
            (" lead", "\" lead\""),
            ("x #y", "\"x #y\""),
            ("tab\there", "\"tab\\there\""),
            ("registry/app:1.2", "registry/app:1.2"),
            ("Running", "Running"),
        ] {
            assert_eq!(scalar(s), yaml, "{s:?}");
        }
        let v = serde_json::json!({"a": "line 1\nline 2\n", "b": "x\n\ny", "c": "keep\n\n"});
        assert_eq!(
            to_yaml(&v),
            "a: |\n  line 1\n  line 2\nb: |-\n  x\n\n  y\nc: |+\n  keep\n\n"
        );
        assert_eq!(to_yaml(&serde_json::json!("x")), "x\n");
    }

    #[tokio::test]
    async fn yaml_of_a_workload_without_managed_fields() {
        let (client, server) = mock_client(|_| {
            (
                200,
                serde_json::json!({"apiVersion": "apps/v1", "kind": "Deployment",
                    "metadata": {"name": "web", "namespace": "team-a",
                        "managedFields": [{"manager": "kubectl", "operation": "Apply"}]},
                    "spec": {"replicas": 2}}),
            )
        });
        let r = ReadClient::new(client, Duration::from_secs(5));
        let y = object_yaml(&r, "team-a", YamlKind::Deployment, "web")
            .await
            .unwrap();
        assert!(y.contains("replicas: 2") && y.contains("name: web"), "{y}");
        assert!(!y.contains("managedFields"), "{y}");
        drop(r);
        assert_eq!(
            server.await.unwrap()[0].path(),
            "/apis/apps/v1/namespaces/team-a/deployments/web"
        );
    }

    #[test]
    fn secrets_have_no_yaml() {
        assert!(serde_json::from_value::<YamlKind>("Secret".into()).is_err());
        assert_eq!(
            serde_json::from_value::<YamlKind>("HTTPRoute".into()).unwrap(),
            YamlKind::HttpRoute
        );
    }
}
