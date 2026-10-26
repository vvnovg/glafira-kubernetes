//! YAML объекта только для чтения (спец. §6.4–6.5): объект из API без `metadata.managedFields`,
//! записанный в YAML. Свой вывод JSON → YAML вместо крейта: нужно только записывать, а
//! `serde_yaml` больше не поддерживается. Secret сюда не попадает — его вида нет в `YamlKind`;
//! ключ `spec.tls.key` Route и аннотации с копией объекта вырезаются до вывода, значения
//! переменных окружения с секретными именами скрыты (`redact`).

use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, StatefulSet};
use k8s_openapi::api::batch::v1::{CronJob, Job};
use k8s_openapi::api::core::v1::{ConfigMap, PersistentVolumeClaim, Pod, Service};
use k8s_openapi::api::networking::v1::Ingress;
use kube::api::ApiResource;
use serde::Deserialize;

use crate::error::AppResult;
use crate::k8s::read::ReadClient;
use crate::storage::profile::REDACTED;

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

/// Аннотации с копией всего объекта. Они повторяют YAML и унесли бы мимо `redact` то, что из него
/// вырезано (`spec.tls.key` Route, созданного `oc apply`): `kubectl apply` и `oc apply` (и всё, что
/// ими применяет, например Argo CD), kapp (Carvel), apply из wrangler (Rancher, Fleet, k3s; копия
/// сжата), ReplicationController'ы DeploymentConfig.
const OBJECT_COPIES: &[&str] = &[
    "kubectl.kubernetes.io/last-applied-configuration",
    "kapp.k14s.io/original",
    "objectset.rio.cattle.io/applied",
    "openshift.io/encoded-deployment-config",
];

/// Команда, записанная `--record`: `kubectl set env … DB_PASSWORD=x` оставил бы в ней значение,
/// которое скрыто в `env`.
const CHANGE_CAUSE: &str = "kubernetes.io/change-cause";

/// Объект для показа: без `managedFields` и аннотаций-копий, у Route — без закрытого ключа
/// `spec.tls.key` (спец. §10.2: «никогда, на любом уровне»), значения переменных окружения
/// с секретными именами скрыты (`mask_env`). Через неё проходит каждый YAML.
fn redact(kind: YamlKind, v: &mut serde_json::Value) {
    if let Some(meta) = v.get_mut("metadata").and_then(|m| m.as_object_mut()) {
        meta.remove("managedFields");
        if let Some(a) = meta.get_mut("annotations").and_then(|a| a.as_object_mut()) {
            a.retain(|k, _| !OBJECT_COPIES.contains(&k.as_str()));
            if let Some(cause) = a.get_mut(CHANGE_CAUSE)
                && cause.as_str().is_some_and(assigns_a_secret)
            {
                *cause = REDACTED.into();
            }
            if a.is_empty() {
                meta.remove("annotations");
            }
        }
    }
    if kind == YamlKind::Route
        && let Some(tls) = v.pointer_mut("/spec/tls").and_then(|t| t.as_object_mut())
    {
        tls.remove("key");
    }
    mask_env(v);
}

/// Значение переменной окружения с таким именем скрыто: слова `auth::exec::is_secret_name`
/// (`TOKEN`, `SECRET`, `PASS`, `KEY`, `BEARER`, `COOKIE`, `CREDENTIAL`) и имена полей спец. §10.3
/// по умолчанию, которых среди них нет (`private`, `authorization`); по вхождению, без учёта
/// регистра. Остальные имена §10.3 (`password`, `passwd`, `secret`, `token`, `apikey`, `api_key`,
/// `api-key`, `credentials`) содержат эти слова.
fn is_secret_env(name: &str) -> bool {
    let n = name.to_ascii_uppercase();
    crate::auth::exec::is_secret_name(name)
        || ["PRIVATE", "AUTHORIZATION"].iter().any(|w| n.contains(w))
}

/// Литеральные значения переменных с секретными именами — `***` (как `REDACTED` профиля и
/// маскировка спец. §10.3). Ищутся во всём объекте: списки `env` контейнеров, init- и
/// ephemeral-контейнеров пода и шаблонов workload'ов (с `jobTemplate` CronJob), хуков
/// `execNewPod` DeploymentConfig и `environment` его `customParams`. `valueFrom` — ссылка и
/// остаётся; пустое значение — тоже: секрета в нём нет.
fn mask_env(v: &mut serde_json::Value) {
    use serde_json::Value;
    match v {
        Value::Object(m) => {
            for (k, child) in m.iter_mut() {
                if matches!(k.as_str(), "env" | "environment")
                    && let Value::Array(vars) = child
                {
                    for var in vars.iter_mut().filter_map(Value::as_object_mut) {
                        let secret = var
                            .get("name")
                            .and_then(Value::as_str)
                            .is_some_and(is_secret_env);
                        let literal = var
                            .get("value")
                            .and_then(Value::as_str)
                            .is_some_and(|s| !s.is_empty());
                        if secret && literal {
                            var.insert("value".into(), REDACTED.into());
                        }
                    }
                }
                mask_env(child);
            }
        }
        Value::Array(items) => items.iter_mut().for_each(mask_env),
        _ => {}
    }
}

/// В команде есть присваивание секретному имени (`DB_PASSWORD=…`, `--env=API_TOKEN=…`,
/// `--password=…`) или секретный флаг со значением через пробел (`--token …`). Тогда скрыта вся
/// команда: где кончается значение в кавычках, по строке не понять.
fn assigns_a_secret(command: &str) -> bool {
    command.split_whitespace().any(|word| {
        let mut parts: Vec<&str> = word.split('=').collect();
        if parts.len() > 1 {
            parts.pop();
        } else if !word.starts_with('-') {
            return false;
        }
        parts
            .iter()
            .any(|p| is_secret_env(p.trim_start_matches('-')))
    })
}

/// YAML объекта без `managedFields`, аннотаций-копий и ключа TLS Route, со скрытыми значениями
/// переменных окружения с секретными именами.
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
    redact(kind, &mut v);
    Ok(to_yaml(&v))
}

/// Знак можно записать как есть: печатаемый по YAML 1.2 (`c-printable`), кроме таба и переводов
/// строки, и не перевод строки YAML 1.1 (U+0085, U+2028, U+2029); BOM — только в начале потока.
fn safe(c: char) -> bool {
    matches!(c, ' '..='~' | '\u{A0}'..='\u{D7FF}' | '\u{E000}'..='\u{FFFD}' | '\u{10000}'..)
        && !matches!(c, '\u{2028}' | '\u{2029}' | '\u{FEFF}')
}

/// Строку можно записать без кавычек: это простой скаляр YAML 1.2 в блоке, и ни YAML 1.2, ни
/// YAML 1.1 (его до сих пор читают PyYAML, SnakeYAML, go-yaml v2) не поймут её как другой тип.
fn plain(s: &str) -> bool {
    // Булевы и null YAML 1.1 и 1.2, ключ слияния `<<` и `=` (тип value YAML 1.1, PyYAML на нём
    // падает).
    const RESERVED: &[&str] = &[
        "true", "false", "yes", "no", "on", "off", "y", "n", "null", "~", "<<", "=",
    ];
    let Some(first) = s.chars().next() else {
        return false;
    };
    // Числа YAML 1.2 и 1.1 (`0x1f`, `0o17`, `017`, `1e3`, `1_000`, `1:30`, `.inf`) и даты YAML 1.1
    // начинаются с цифры или точки после знака; go-yaml v2 перед разбором убирает `_` (`-_1` —
    // число), а `inf` и `nan` без точки — числа для `strconv.ParseFloat` Go.
    let unsigned = s.trim_start_matches(['+', '-', '_']);
    let number_like = unsigned.starts_with(|c: char| c.is_ascii_digit() || c == '.')
        || ["inf", "infinity", "nan"].contains(&unsigned.to_ascii_lowercase().as_str());
    // `-` — знак YAML, только если за ним пробел (`--port` — обычная строка); `?` и `:` в начале
    // в кавычках всегда (`:x` — символ Ruby).
    let indicator = match first {
        '-' => s[1..].chars().next().is_none_or(|c| c == ' '),
        c => "?:,[]{}#&*!|>'\"%@`".contains(c),
    };
    !RESERVED.contains(&s.to_lowercase().as_str())
        && !number_like
        && !indicator
        // Начало и конец документа: в начале строки `---` и `...` — разметка, а не текст.
        && !s.starts_with("---")
        && !s.starts_with("...")
        && !s.starts_with(' ')
        && !s.ends_with(' ')
        && !s.ends_with(':')
        && !s.contains(": ")
        && !s.contains(" #")
        && s.chars().all(safe)
}

/// Строка в двойных кавычках: экранированы `"`, `\` и всё, что YAML не разрешает записать как есть.
fn quoted(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            c if safe(c) => out.push(c),
            // Всё остальное, что не `safe`, — не выше U+FFFF.
            c => out.push_str(&format!("\\u{:04x}", u32::from(c))),
        }
    }
    out.push('"');
    out
}

fn scalar(s: &str) -> String {
    if plain(s) { s.to_string() } else { quoted(s) }
}

/// Многострочный текст — блоком `|`: `|-` без перевода строки в конце, `|+`, если их несколько.
fn block(s: &str, indent: usize, out: &mut String) -> bool {
    // Первая строка задаёт отступ блока: она не может быть пустой или начинаться с пробела.
    if !s.contains('\n') || s.starts_with(['\n', ' ']) || !s.chars().all(|c| safe(c) || c == '\n') {
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
            // Блок `|` в корне — с отступом: PyYAML и libyaml не читают его текст с нулевой
            // колонки, а `---` там был бы началом документа.
            inline(v, 2, &mut out);
            // `inline` начинает с пробела, как после `ключ:`; `trim_start` срезал бы и U+00A0.
            return out.strip_prefix(' ').unwrap_or(&out).to_string();
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

    /// Простые скаляры по правилам YAML 1.2 и то, что YAML 1.1 прочитал бы не строкой; ключи — так же.
    #[test]
    fn plain_scalars_by_the_rules_of_yaml() {
        // Знаки YAML в начале, `: ` и ` #`, разметка документа.
        let indicators = [
            "-", "- x", "?", "? x", "?x", ":", ":x", ",x", "[x", "]x", "{x", "}x", "#x", "&a",
            "*a", "!tag", "|x", ">x", "'x", "\"x", "%x", "@x", "`x", "a: b", "a:", "x #y", "---",
            "--- x", "...", "...x",
        ];
        // Булевы и null YAML 1.1, ключ слияния `<<` и `=`.
        let yaml11 = [
            "yes", "No", "ON", "off", "y", "N", "True", "~", "null", "NULL", "<<", "=",
        ];
        // Числа YAML 1.2 и 1.1, даты YAML 1.1.
        let numbers = [
            "0x1F",
            "0o17",
            "017",
            "1e3",
            "-2E-3",
            "1_000",
            "-_1",
            "1:30",
            "190:20:30.15",
            ".inf",
            "-.inf",
            "+.INF",
            ".nan",
            "+1",
            "-1.5",
            "inf",
            "-Infinity",
            "NaN",
            "2026-10-26",
            "2026-10-26T10:00:00Z",
        ];
        // Пробелы по краям и пустая строка (U+00A0 для YAML — не пробел).
        let spaces = ["", " ", " lead", "trail ", "  both  "];
        assert_eq!(to_yaml(&serde_json::json!("\u{a0}a")), "\u{a0}a\n");
        for s in indicators
            .iter()
            .chain(&yaml11)
            .chain(&numbers)
            .chain(&spaces)
        {
            let want = format!("\"{}\"", s.replace('"', "\\\""));
            assert_eq!(scalar(s), want, "{s:?}");
            assert_eq!(
                to_yaml(&serde_json::json!({ *s: 1 })),
                format!("{want}: 1\n"),
                "ключ {s:?}"
            );
        }
        for s in [
            "--port",
            "-x",
            "a:b",
            "a#b",
            "http://x/y?z=1#f",
            "<<x",
            "a=b",
            "yesterday",
            "~x",
            "_x",
            "infra",
            "registry/app:1.2",
            "say \"hi\"",
            "C:\\dir",
            "x.y",
            "Привет",
        ] {
            assert_eq!(scalar(s), s, "{s:?}");
            assert_eq!(
                to_yaml(&serde_json::json!({ s: 1 })),
                format!("{s}: 1\n"),
                "ключ {s:?}"
            );
        }
    }

    /// В кавычках экранировано всё, что YAML не разрешает записать как есть (иначе PyYAML падает
    /// на U+007F, а YAML 1.1 считает U+0085, U+2028 и U+2029 переводом строки).
    #[test]
    fn quoted_strings_escape_what_yaml_cannot_hold() {
        for (s, yaml) in [
            ("\u{0}", "\"\\u0000\""),
            ("\u{1b}[0m", "\"\\u001b[0m\""),
            ("cr\rlf", "\"cr\\rlf\""),
            ("\u{7f}", "\"\\u007f\""),
            ("a\u{85}b", "\"a\\u0085b\""),
            ("a\u{2028}b", "\"a\\u2028b\""),
            ("\u{feff}x", "\"\\ufeffx\""),
            ("\u{ffff}", "\"\\uffff\""),
            ("\"q\" \\", "\"\\\"q\\\" \\\\\""),
            ("- é", "\"- é\""),
        ] {
            assert_eq!(scalar(s), yaml, "{s:?}");
        }
    }

    #[test]
    fn multi_line_strings_as_blocks() {
        let v = serde_json::json!({
            "blank": "a\n  \n",
            "indented": "a\n  b\n",
            "lead": " a\nb",
            "markers": "---\n...\n# text\n",
            "separator": "a\u{2028}\nb",
            "tab": "a\n\tb",
        });
        assert_eq!(
            to_yaml(&v),
            "blank: |\n  a\n    \nindented: |\n  a\n    b\nlead: \" a\\nb\"\nmarkers: |\n  ---\n  ...\n  # text\nseparator: \"a\\u2028\\nb\"\ntab: \"a\\n\\tb\"\n"
        );
        // Блок в корне — с отступом, иначе `---` в нём начал бы документ.
        assert_eq!(to_yaml(&serde_json::json!("---\nx\n")), "|\n  ---\n  x\n");
        assert_eq!(
            to_yaml(&serde_json::json!([{"k": "a\nb"}, "c\nd"])),
            "- k: |-\n    a\n    b\n- |-\n  c\n  d\n"
        );
    }

    /// Закрытый ключ Route не уходит ни полем `spec.tls.key`, ни копией объекта в
    /// `last-applied-configuration` (Route, созданный `oc apply`).
    #[tokio::test]
    async fn route_yaml_without_tls_key_and_applied_copy() {
        const KEY: &str =
            "-----BEGIN PRIVATE KEY-----\nMIIEvQk3yS3cr3t\n-----END PRIVATE KEY-----\n";
        let spec = serde_json::json!({"host": "web.apps.example.com",
            "to": {"kind": "Service", "name": "web"},
            "tls": {"termination": "edge", "key": KEY,
                "certificate": "-----BEGIN CERTIFICATE-----\nMIIBcert\n-----END CERTIFICATE-----\n"}});
        let applied = serde_json::json!({"apiVersion": "route.openshift.io/v1", "kind": "Route",
            "metadata": {"name": "web", "namespace": "team-a"}, "spec": spec});
        let route = serde_json::json!({"apiVersion": "route.openshift.io/v1", "kind": "Route",
            "metadata": {"name": "web", "namespace": "team-a", "annotations": {
                "kubectl.kubernetes.io/last-applied-configuration": applied.to_string()}},
            "spec": spec});
        let (client, server) = mock_client(move |_| (200, route.clone()));
        let r = ReadClient::new(client, Duration::from_secs(5));
        let y = object_yaml(&r, "team-a", YamlKind::Route, "web")
            .await
            .unwrap();
        assert!(
            !y.contains("MIIEvQk3yS3cr3t") && !y.contains("PRIVATE KEY"),
            "{y}"
        );
        assert!(
            !y.contains("last-applied-configuration") && !y.contains("annotations"),
            "{y}"
        );
        assert!(
            y.contains("  tls:\n    certificate: |\n      -----BEGIN CERTIFICATE-----\n")
                && y.contains("    termination: edge\n"),
            "{y}"
        );
        assert!(!y.contains("    key:"), "{y}");
        drop(r);
        assert_eq!(
            server.await.unwrap()[0].path(),
            "/apis/route.openshift.io/v1/namespaces/team-a/routes/web"
        );
    }

    /// Аннотации с копией объекта вырезаются у любого вида, остальные остаются.
    #[tokio::test]
    async fn yaml_without_annotations_that_copy_the_object() {
        let (client, _server) = mock_client(|_| {
            (
                200,
                serde_json::json!({"apiVersion": "apps/v1", "kind": "Deployment",
                    "metadata": {"name": "web", "namespace": "team-a", "annotations": {
                        "deployment.kubernetes.io/revision": "3",
                        "kubectl.kubernetes.io/last-applied-configuration":
                            "{\"apiVersion\":\"apps/v1\",\"kind\":\"Deployment\"}",
                        "kapp.k14s.io/original": "{\"kind\":\"Deployment\"}",
                        "objectset.rio.cattle.io/applied": "H4sIAAAAAAAA",
                        "openshift.io/encoded-deployment-config": "{\"kind\":\"DeploymentConfig\"}"}},
                    "spec": {"replicas": 2}}),
            )
        });
        let r = ReadClient::new(client, Duration::from_secs(5));
        let y = object_yaml(&r, "team-a", YamlKind::Deployment, "web")
            .await
            .unwrap();
        assert!(
            y.contains("metadata:\n  annotations:\n    deployment.kubernetes.io/revision: \"3\"\n  name: web\n"),
            "{y}"
        );
        for copy in OBJECT_COPIES {
            assert!(!y.contains(copy), "{copy}: {y}");
        }
        assert!(!y.contains("H4sI") && !y.contains("\\\"kind\\\""), "{y}");
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

    /// Переменные контейнеров и init-контейнеров.
    fn env() -> serde_json::Value {
        serde_json::json!([
            {"name": "DB_PASSWORD", "value": "x"},
            {"name": "API_TOKEN", "value": "y"},
            {"name": "LOG_LEVEL", "value": "debug"},
            {"name": "DB_SECRET", "valueFrom": {"secretKeyRef": {"name": "db", "key": "password"}}},
        ])
    }

    fn pod_spec() -> serde_json::Value {
        serde_json::json!({
            "initContainers": [{"name": "migrate", "image": "app", "env": env()}],
            "containers": [{"name": "web", "image": "app", "env": env()}]})
    }

    /// Значения переменных с секретными именами скрыты в YAML Deployment и пода, остальные
    /// значения и ссылки `valueFrom` — как есть.
    #[tokio::test]
    async fn secret_env_values_are_masked_in_workloads_and_pods() {
        let (client, _server) = mock_client(|s| match s.path() {
            "/apis/apps/v1/namespaces/team-a/deployments/web" => (
                200,
                serde_json::json!({"apiVersion": "apps/v1", "kind": "Deployment",
                    "metadata": {"name": "web", "namespace": "team-a"},
                    "spec": {"template": {"spec": pod_spec()}}}),
            ),
            _ => (
                200,
                serde_json::json!({"apiVersion": "v1", "kind": "Pod",
                    "metadata": {"name": "web-1", "namespace": "team-a"}, "spec": pod_spec()}),
            ),
        });
        let r = ReadClient::new(client, Duration::from_secs(5));
        for (kind, name, pad) in [
            (YamlKind::Deployment, "web", " ".repeat(12)),
            (YamlKind::Pod, "web-1", " ".repeat(8)),
        ] {
            let y = object_yaml(&r, "team-a", kind, name).await.unwrap();
            let var =
                |name: &str, value: &str| format!("{pad}- name: {name}\n{pad}  value: {value}\n");
            assert_eq!(y.matches(&var("DB_PASSWORD", "\"***\"")).count(), 2, "{y}");
            assert_eq!(y.matches(&var("API_TOKEN", "\"***\"")).count(), 2, "{y}");
            assert_eq!(y.matches(&var("LOG_LEVEL", "debug")).count(), 2, "{y}");
            assert!(
                !y.contains("value: x\n") && !y.contains("value: y\n"),
                "{y}"
            );
            let reference = format!(
                "{pad}- name: DB_SECRET\n{pad}  valueFrom:\n{pad}    secretKeyRef:\n{pad}      key: password\n"
            );
            assert_eq!(y.matches(&reference).count(), 2, "{y}");
        }
    }

    /// Списки `env` везде, где они бывают: `jobTemplate` CronJob, ephemeral-контейнеры пода, хуки
    /// и `customParams` DeploymentConfig; пустое значение остаётся; команда `--record` с секретом
    /// скрыта целиком.
    #[test]
    fn secret_env_values_are_masked_wherever_env_lives() {
        let var = || {
            serde_json::json!([{"name": "private_key", "value": "k"}, {"name": "MODE", "value": "m"},
            {"name": "AUTH_PASS", "value": ""}])
        };
        let mut cron = serde_json::json!({"spec": {"jobTemplate": {"spec": {"template": {"spec":
            {"containers": [{"name": "c", "env": var()}]}}}}}});
        redact(YamlKind::CronJob, &mut cron);
        let env = &cron["spec"]["jobTemplate"]["spec"]["template"]["spec"]["containers"][0]["env"];
        assert_eq!(
            env,
            &serde_json::json!([{"name": "private_key", "value": "***"}, {"name": "MODE", "value": "m"},
                {"name": "AUTH_PASS", "value": ""}])
        );
        let mut pod =
            serde_json::json!({"spec": {"ephemeralContainers": [{"name": "debug", "env": var()}]}});
        redact(YamlKind::Pod, &mut pod);
        assert_eq!(
            pod["spec"]["ephemeralContainers"][0]["env"][0]["value"],
            "***"
        );
        let mut dc = serde_json::json!({
            "metadata": {"annotations": {
                "kubernetes.io/change-cause": "oc set env dc/web DB_PASSWORD=\"s3 cr3t\" --record=true"}},
            "spec": {"strategy": {
                "recreateParams": {"pre": {"execNewPod": {"containerName": "web", "env": var()}}},
                "customParams": {"environment": var()}}}});
        redact(YamlKind::DeploymentConfig, &mut dc);
        let strategy = &dc["spec"]["strategy"];
        assert_eq!(
            strategy["recreateParams"]["pre"]["execNewPod"]["env"][0]["value"],
            "***"
        );
        assert_eq!(strategy["customParams"]["environment"][0]["value"], "***");
        assert_eq!(strategy["customParams"]["environment"][1]["value"], "m");
        assert_eq!(
            dc["metadata"]["annotations"]["kubernetes.io/change-cause"],
            "***"
        );
    }

    #[test]
    fn secret_names_of_env_and_of_commands() {
        // Имена §10.3 по умолчанию и слова `is_secret_name`.
        for s in [
            "DB_PASSWORD",
            "db_passwd",
            "CLIENT_SECRET",
            "API_TOKEN",
            "STRIPE_APIKEY",
            "api_key",
            "x-api-key",
            "GOOGLE_APPLICATION_CREDENTIALS",
            "PRIVATE_KEY_PEM",
            "AUTHORIZATION",
            "REDIS_PASS",
            "JWT_SIGNING_KEY",
            "SESSION_COOKIE",
            "AUTH_BEARER",
        ] {
            assert!(is_secret_env(s), "{s}");
        }
        for s in [
            "LOG_LEVEL",
            "JAVA_OPTS",
            "HOME",
            "PORT",
            "SPRING_PROFILES_ACTIVE",
        ] {
            assert!(!is_secret_env(s), "{s}");
        }
        for c in [
            "kubectl set env deployment/web DB_PASSWORD=x --record=true",
            "kubectl run web --env=API_TOKEN=y",
            "oc login --token sha256~abc",
            "kubectl create secret generic db --from-literal=password=x",
        ] {
            assert!(assigns_a_secret(c), "{c}");
        }
        for c in [
            "kubectl set image deployment/web web=registry/app:1.2 --record=true",
            "kubectl apply -f secret.yaml --record",
            "kubectl scale deployment/keycloak --replicas=2",
        ] {
            assert!(!assigns_a_secret(c), "{c}");
        }
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
