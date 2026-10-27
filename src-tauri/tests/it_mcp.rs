//! MCP-шлюз на настоящем кластере (kind, `dev/kind/setup.sh`): контроллер приложения поднимает
//! сервер, клиент rmcp подключается через `mcp.json` и рукопожатие и вызывает инструменты. LLM видит
//! только проект team-a (`llmNamespaces`), значения Secret в ответы не попадают.
//! Без `GO_IT_KUBECONFIG` тест пропускается.

mod common;

use std::time::Duration;

use base64::Engine;
use common::{import, kubeconfig_path};
use glafira_kubernetes_lib::mcp::McpController;
use glafira_kubernetes_lib::mcp::controller::ServerState;
use glafira_kubernetes_lib::mcp::files;
use glafira_kubernetes_lib::state::AppState;
use glafira_kubernetes_lib::storage::profile::LlmAccess;
use rmcp::model::{CallToolRequestParams, CallToolResult};
use rmcp::service::RunningService;
use rmcp::{RoleClient, ServiceExt};
use serde_json::{Value, json};

const NS: &str = "team-a";
/// Значение ключа `password` Secret `db` из `dev/kind/fixtures.yaml`.
const SECRET: &str = "s3cr3t-value";

async fn call(c: &RunningService<RoleClient, ()>, tool: &str, args: Value) -> CallToolResult {
    let mut p = CallToolRequestParams::new(tool.to_string());
    if let Value::Object(m) = args {
        p = p.with_arguments(m);
    }
    c.call_tool(p).await.unwrap()
}

fn text(r: &CallToolResult) -> String {
    r.content
        .iter()
        .filter_map(|c| c.as_text().map(|t| t.text.clone()))
        .collect()
}

/// Ответ без ошибки; значение Secret не попадает в него ни открытым текстом, ни в base64.
fn ok(r: CallToolResult) -> Value {
    assert_eq!(r.is_error, Some(false), "{}", text(&r));
    let t = text(&r);
    let base64 = base64::engine::general_purpose::STANDARD.encode(SECRET);
    assert!(
        !t.contains(SECRET) && !t.contains(&base64),
        "значение Secret в ответе: {t}"
    );
    r.structured_content.unwrap()
}

fn err(r: &CallToolResult) -> String {
    assert_eq!(r.is_error, Some(true), "{}", text(r));
    text(r)
}

#[tokio::test]
async fn the_gateway_reads_the_stand_within_the_llm_scope() {
    let Some(path) = kubeconfig_path() else {
        return;
    };
    let (mut profile, secrets) = import(&path, "admin");
    profile.llm_access = LlmAccess::Metadata;
    profile.llm_namespaces = Some(vec![NS.into()]);
    let id = profile.id.clone();
    let dir = tempfile::tempdir().unwrap();
    let st = AppState::new(dir.path(), secrets).unwrap();
    st.connections.upsert(profile).unwrap();
    // Клиент уже подтверждён: диалог подтверждения проверяют unit-тесты контроллера.
    st.settings.remember_client("rmcp", "it", 0).unwrap();
    st.settings.set_mcp_enabled(true).unwrap();
    let ctl = McpController::new(st.clone(), dir.path());
    ctl.apply().await;
    assert_eq!(
        ctl.status().state,
        ServerState::Running,
        "{:?}",
        ctl.status().error
    );

    let f = files::read(dir.path()).unwrap();
    let mut s = glafira_mcp_core::endpoint::connect(&f.endpoint)
        .await
        .unwrap();
    glafira_mcp_core::handshake::client(&mut s, &f.token)
        .await
        .unwrap();
    let c = ().serve(s).await.unwrap();

    let v = ok(call(&c, "list_connections", json!({})).await);
    assert_eq!(v["connections"][0]["id"], id.as_str());
    let v = ok(call(&c, "list_projects", json!({"connectionId": id})).await);
    assert_eq!(v["items"], json!([{"name": NS, "phase": "Active"}]), "{v}");
    let v = ok(call(&c, "list_connections", json!({})).await);
    assert_eq!(
        v["connections"][0]["platform"]["kind"], "kubernetes",
        "подключение в пуле"
    );

    let v = ok(call(&c, "get_cluster_overview", json!({"connectionId": id})).await);
    assert_eq!(v["apiServer"]["ready"], true);
    assert_eq!(v["capabilities"]["listNodes"], true);

    let msg = err(&call(
        &c,
        "list_pods",
        json!({"connectionId": id, "namespace": "default"}),
    )
    .await);
    assert!(msg.starts_with("not_allowed:"), "{msg}");

    let v = ok(call(
        &c,
        "describe_workload",
        json!({"connectionId": id, "namespace": NS, "kind": "Deployment", "name": "web"}),
    )
    .await);
    let env = &v["containers"][0]["env"];
    assert!(
        env.as_array()
            .unwrap()
            .iter()
            .any(|e| e["name"] == "DB_PASSWORD" && e["source"] == "secretKeyRef"),
        "{env}"
    );
    assert!(v["masked"].as_u64().is_some());

    let v = ok(call(
        &c,
        "get_network",
        json!({"connectionId": id, "namespace": NS}),
    )
    .await);
    assert_eq!(v["routes"][0]["name"], "web");
    assert_eq!(
        v["httpRoutes"][0]["parents"][0]["resolvedRefs"]["status"],
        "False"
    );

    let v = ok(call(
        &c,
        "list_events",
        json!({"connectionId": id, "namespace": NS, "type": "Warning", "limit": 5}),
    )
    .await);
    assert!(v["items"].as_array().is_some());

    // Проверка — только по проекту, открытому для LLM; находки вне него не приходят.
    let v = ok(call(
        &c,
        "health_check",
        json!({"connectionId": id, "limit": 200}),
    )
    .await);
    assert_eq!(v["scope"]["projects"], json!([NS]));
    let findings = v["findings"]["items"].as_array().unwrap();
    assert!(
        findings
            .iter()
            .all(|f| f["target"]["namespace"].as_str().is_none_or(|ns| ns == NS)),
        "{findings:?}"
    );
    assert!(
        findings
            .iter()
            .any(|f| f["ruleId"] == "pod.imagePull" && f["target"]["name"] == "pull"),
        "{findings:?}"
    );
    let msg = err(&call(
        &c,
        "health_check",
        json!({"connectionId": id, "namespaces": ["default"]}),
    )
    .await);
    assert!(msg.starts_with("not_allowed:"), "{msg}");

    let note = json!({"connectionId": id, "severity": "warning", "title": "Pulls from an invalid registry",
        "body": "registry.invalid is not resolvable", "relatedRuleId": "pod.imagePull",
        "target": {"kind": "workload", "namespace": NS, "name": "pull", "workloadKind": "Deployment"}});
    let v = ok(call(&c, "annotate", note.clone()).await);
    assert_eq!(v["created"], true);
    let mut ghost = note;
    ghost["target"]["name"] = "ghost".into();
    let msg = err(&call(&c, "annotate", ghost).await);
    assert!(msg.starts_with("not_found:"), "{msg}");
    let v = ok(call(&c, "list_annotations", json!({"connectionId": id})).await);
    assert_eq!(v["items"][0]["client"], "rmcp");
    let aid = v["items"][0]["id"].clone();
    let v = ok(call(
        &c,
        "remove_annotation",
        json!({"connectionId": id, "annotationId": aid}),
    )
    .await);
    assert_eq!(v["removed"], true);

    let v = ok(call(
        &c,
        "observe_workload",
        json!({"connectionId": id, "namespace": NS,
            "workload": {"kind": "Deployment", "name": "logger"}, "durationSeconds": 10}),
    )
    .await);
    assert_eq!(v["samples"], 6, "{v}");
    assert_eq!(v["initial"]["pods"].as_array().unwrap().len(), 1);

    let calls: Vec<Value> = ctl.audit_recent(50);
    assert!(
        calls
            .iter()
            .any(|a| a["tool"] == "health_check" && a["connection"]["id"] == id.as_str()),
        "{calls:?}"
    );
    let line = calls
        .iter()
        .find(|a| a["tool"] == "annotate")
        .unwrap()
        .to_string();
    assert!(
        !line.contains("invalid registry"),
        "текст замечания в журнале: {line}"
    );

    drop(c);
    tokio::time::timeout(Duration::from_secs(10), ctl.shutdown())
        .await
        .expect("сервер остановился");
    assert!(files::read(dir.path()).is_none(), "mcp.json удалён");
}
