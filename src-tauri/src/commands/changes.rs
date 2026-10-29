//! Изменения кластера из форм (спец. §9, §4.2): предпросмотр с dry-run и применение после
//! стражей. MCP-шлюз этих команд не вызывает (сторожок `mcp::guard`).

use tauri::State;

use crate::change::Change;
use crate::error::AppResult;
use crate::health::model::Target;
use crate::ops::Preview;
use crate::state::AppState;

/// «Было → станет», предупреждения и server-side dry-run. Профиль «только чтение» изменений
/// не готовит: формы на нём не открываются (`READ_ONLY`).
#[tauri::command]
pub async fn preview_change(
    state: State<'_, AppState>,
    connection_id: String,
    change: Change,
) -> AppResult<Preview> {
    preview_change_impl(state.inner(), &connection_id, change).await
}

pub async fn preview_change_impl(
    st: &AppState,
    connection_id: &str,
    change: Change,
) -> AppResult<Preview> {
    let profile = st.connections.get(connection_id)?;
    if profile.read_only {
        crate::guard::ensure_mutation_allowed(&profile, None)?;
    }
    let ns = change.namespace().to_string();
    // Future предпросмотра велика для стека потока в debug-сборке — в куче.
    Box::pin(st.read_in(connection_id, &ns, |c| async move {
        Box::pin(crate::ops::preview(
            &c.reader(),
            &c.writer(),
            change,
            &crate::ops::now_rfc3339(),
        ))
        .await
    }))
    .await
}

/// Применение: стражи профиля (только чтение, имя подключения для prod), у пода без
/// контроллера — его имя (`confirm_pod`), сверка с версией предпросмотра, журнал
/// приложения (подключение, вид, объект, исход — без значений), отметка предложения LLM.
#[tauri::command]
#[allow(clippy::too_many_arguments)]
pub async fn apply_change(
    state: State<'_, AppState>,
    connection_id: String,
    change: Change,
    expected_version: Option<String>,
    confirm: Option<String>,
    confirm_pod: Option<String>,
    proposal_id: Option<String>,
) -> AppResult<Change> {
    apply_change_impl(
        state.inner(),
        &connection_id,
        change,
        expected_version,
        confirm,
        confirm_pod,
        proposal_id,
    )
    .await
}

/// Вид объекта для журнала: у workload'а — его вид (`Deployment`), у остальных — вид цели
/// (`pod`, `hpa`).
fn object_kind(target: &Target) -> String {
    target
        .workload_kind
        .map_or_else(|| target.kind.as_str().to_string(), |k| format!("{k:?}"))
}

pub async fn apply_change_impl(
    st: &AppState,
    connection_id: &str,
    change: Change,
    expected_version: Option<String>,
    confirm: Option<String>,
    confirm_pod: Option<String>,
    proposal_id: Option<String>,
) -> AppResult<Change> {
    let profile = st.connections.get(connection_id)?;
    crate::guard::ensure_mutation_allowed(&profile, confirm.as_deref())?;
    let (kind, target) = (change.kind_name(), change.target());
    let ns = change.namespace().to_string();
    let result = Box::pin(st.read_in(connection_id, &ns, |c| async move {
        Box::pin(crate::ops::apply(
            &c.reader(),
            &c.writer(),
            change,
            expected_version.as_deref(),
            confirm_pod.as_deref(),
            &crate::ops::now_rfc3339(),
        ))
        .await
    }))
    .await;
    let object_kind = object_kind(&target);
    let object = format!(
        "{}/{}",
        target.namespace.as_deref().unwrap_or_default(),
        target.name.as_deref().unwrap_or_default()
    );
    match &result {
        Ok(_) => {
            tracing::info!(
                "change applied: connection={connection_id} kind={kind} object_kind={object_kind} object={object}"
            )
        }
        Err(e) => tracing::warn!(
            "change failed: connection={connection_id} kind={kind} object_kind={object_kind} object={object} code={:?}",
            e.code
        ),
    }
    if let Ok(applied) = &result {
        super::health::mark_applied(st, connection_id, proposal_id, applied.clone()).await;
    }
    result
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use serde_json::json;

    use super::*;
    use crate::error::ErrorCode;
    use crate::k8s::workloads::WorkloadKind;
    use crate::storage::profile::{Environment, sample_profile};

    /// Запросы, дошедшие до кластера: метод и путь со строкой запроса.
    type Requests = Arc<Mutex<Vec<(http::Method, String)>>>;

    /// Только запросы записи: чтения (GET) стражи не запрещают.
    fn writes(requests: &Requests) -> Vec<(http::Method, String)> {
        let mut writes = requests.lock().unwrap().clone();
        writes.retain(|(m, _)| m != http::Method::GET);
        writes
    }

    fn state(
        dir: &std::path::Path,
        env: Environment,
        read_only: bool,
        failing: Option<u16>,
    ) -> (AppState, Requests) {
        let st = AppState::new(dir, Arc::new(crate::secrets::MemoryStore::default())).unwrap();
        let mut p = sample_profile("c1");
        p.name = "prod-1".into();
        p.environment = env;
        p.read_only = read_only;
        st.connections.upsert(p).unwrap();
        let requests = Requests::default();
        let seen = requests.clone();
        let connected = crate::k8s::pool::test_connected(move |s| {
            seen.lock().unwrap().push((s.method.clone(), s.uri.clone()));
            match (s.path(), failing) {
                (_, Some(code)) if s.method != http::Method::GET => {
                    crate::k8s::testing::status(code, "Failure", "no")
                }
                ("/apis/autoscaling/v2/namespaces/team-a/horizontalpodautoscalers/web", _) => (
                    200,
                    json!({"apiVersion": "autoscaling/v2", "kind": "HorizontalPodAutoscaler",
                        "metadata": {"name": "web", "resourceVersion": "5", "generation": 4},
                        "spec": {"minReplicas": 2, "maxReplicas": 6,
                            "scaleTargetRef": {"kind": "Deployment", "name": "web"}}}),
                ),
                _ => crate::k8s::testing::status(404, "NotFound", "no"),
            }
        });
        assert!(
            st.pool
                .insert_if_current("c1", st.pool.generation("c1"), connected)
        );
        (st, requests)
    }

    fn hpa() -> Change {
        Change::SetHpaBounds {
            namespace: "team-a".into(),
            hpa: "web".into(),
            min_replicas: Some(1),
            max_replicas: 4,
        }
    }

    #[tokio::test]
    async fn guards_come_before_the_cluster() {
        let dir = tempfile::tempdir().unwrap();
        let (st, requests) = state(dir.path(), Environment::Prod, true, None);
        let e = apply_change_impl(
            &st,
            "c1",
            hpa(),
            Some("g4".into()),
            Some("prod-1".into()),
            None,
            None,
        )
        .await
        .unwrap_err();
        assert_eq!(
            e.code,
            ErrorCode::ReadOnly,
            "read-only — никакое подтверждение не помогает"
        );
        let e = preview_change_impl(&st, "c1", hpa()).await.unwrap_err();
        assert_eq!(e.code, ErrorCode::ReadOnly);
        assert!(
            requests.lock().unwrap().is_empty(),
            "стражи — до кластера: {:?}",
            requests.lock().unwrap()
        );
        let dir = tempfile::tempdir().unwrap();
        let (st, requests) = state(dir.path(), Environment::Prod, false, None);
        let e = apply_change_impl(
            &st,
            "c1",
            hpa(),
            Some("g4".into()),
            Some("prod".into()),
            None,
            None,
        )
        .await
        .unwrap_err();
        assert_eq!(e.code, ErrorCode::ConfirmationRequired);
        assert!(requests.lock().unwrap().is_empty());
        // Предпросмотр prod не требует имени: оно — для применения.
        let p = preview_change_impl(&st, "c1", hpa()).await.unwrap();
        assert_eq!(p.version.as_deref(), Some("g4"));
        assert_eq!(p.diff.len(), 2);
    }

    #[tokio::test]
    async fn apply_needs_the_version_of_the_preview_and_then_writes() {
        let dir = tempfile::tempdir().unwrap();
        let (st, requests) = state(dir.path(), Environment::Prod, false, None);
        let confirm = || Some("prod-1".to_string());
        // Без версии предпросмотра и с чужой — CONFLICT, в кластер ничего не записано.
        for version in [None, Some("g3".to_string())] {
            let e = apply_change_impl(&st, "c1", hpa(), version, confirm(), None, None)
                .await
                .unwrap_err();
            assert_eq!(e.code, ErrorCode::Conflict);
        }
        assert!(writes(&requests).is_empty(), "{:?}", writes(&requests));
        apply_change_impl(&st, "c1", hpa(), Some("g4".into()), confirm(), None, None)
            .await
            .unwrap();
        let sent = writes(&requests);
        assert_eq!(sent.len(), 1, "{sent:?}");
        assert_eq!(sent[0].0, http::Method::PATCH);
        assert!(
            sent[0].1.contains("fieldManager=glafira-kubernetes") && !sent[0].1.contains("dryRun"),
            "{}",
            sent[0].1
        );
    }

    #[tokio::test]
    async fn a_broken_client_in_the_dry_run_is_evicted() {
        // 401 на dry-run — не «API-сервер отклонил изменение»: предпросмотр падает ошибкой, и
        // `read_in` сбрасывает клиент, чтобы следующий вызов подключился заново.
        let dir = tempfile::tempdir().unwrap();
        let (st, requests) = state(dir.path(), Environment::Dev, false, Some(401));
        let e = preview_change_impl(&st, "c1", hpa()).await.unwrap_err();
        assert_eq!(e.code, ErrorCode::SessionExpired);
        assert_eq!(writes(&requests).len(), 1, "{:?}", writes(&requests));
        assert!(st.pool.get("c1").is_none(), "клиент сброшен");
    }

    #[test]
    fn the_log_names_the_kind_of_the_object() {
        let kind = |c: Change| object_kind(&c.target());
        assert_eq!(
            kind(Change::Scale {
                namespace: "team-a".into(),
                workload: crate::k8s::pods::WorkloadRef {
                    kind: WorkloadKind::StatefulSet,
                    name: "db".into()
                },
                replicas: 1,
            }),
            "StatefulSet"
        );
        assert_eq!(kind(hpa()), "hpa");
        assert_eq!(
            kind(Change::DeletePod {
                namespace: "team-a".into(),
                pod: "solo".into()
            }),
            "pod"
        );
    }
}
