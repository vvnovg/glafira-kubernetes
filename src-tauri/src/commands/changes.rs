//! Изменения кластера из форм (спец. §9, §4.2): предпросмотр с dry-run и применение после
//! стражей. MCP-шлюз этих команд не вызывает (сторожок `mcp::guard`).

use tauri::State;

use crate::change::Change;
use crate::error::AppResult;
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
    let object = format!(
        "{}/{}",
        target.namespace.as_deref().unwrap_or_default(),
        target.name.as_deref().unwrap_or_default()
    );
    match &result {
        Ok(_) => {
            tracing::info!("change applied: connection={connection_id} kind={kind} object={object}")
        }
        Err(e) => tracing::warn!(
            "change failed: connection={connection_id} kind={kind} object={object} code={:?}",
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
    use std::sync::Arc;

    use serde_json::json;

    use super::*;
    use crate::error::ErrorCode;
    use crate::storage::profile::{Environment, sample_profile};

    fn state(dir: &std::path::Path, env: Environment, read_only: bool) -> AppState {
        let st = AppState::new(dir, Arc::new(crate::secrets::MemoryStore::default())).unwrap();
        let mut p = sample_profile("c1");
        p.name = "prod-1".into();
        p.environment = env;
        p.read_only = read_only;
        st.connections.upsert(p).unwrap();
        let connected = crate::k8s::pool::test_connected(|s| match s.path() {
            "/apis/autoscaling/v2/namespaces/team-a/horizontalpodautoscalers/web" => (
                200,
                json!({"apiVersion": "autoscaling/v2", "kind": "HorizontalPodAutoscaler",
                    "metadata": {"name": "web", "resourceVersion": "5", "generation": 4},
                    "spec": {"minReplicas": 2, "maxReplicas": 6,
                        "scaleTargetRef": {"kind": "Deployment", "name": "web"}}}),
            ),
            _ => crate::k8s::testing::status(404, "NotFound", "no"),
        });
        assert!(
            st.pool
                .insert_if_current("c1", st.pool.generation("c1"), connected)
        );
        st
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
        let st = state(dir.path(), Environment::Prod, true);
        let e = apply_change_impl(&st, "c1", hpa(), None, Some("prod-1".into()), None, None)
            .await
            .unwrap_err();
        assert_eq!(
            e.code,
            ErrorCode::ReadOnly,
            "read-only — никакое подтверждение не помогает"
        );
        let e = preview_change_impl(&st, "c1", hpa()).await.unwrap_err();
        assert_eq!(e.code, ErrorCode::ReadOnly);
        let dir = tempfile::tempdir().unwrap();
        let st = state(dir.path(), Environment::Prod, false);
        let e = apply_change_impl(&st, "c1", hpa(), None, Some("prod".into()), None, None)
            .await
            .unwrap_err();
        assert_eq!(e.code, ErrorCode::ConfirmationRequired);
        // Предпросмотр prod не требует имени: оно — для применения.
        let p = preview_change_impl(&st, "c1", hpa()).await.unwrap();
        assert_eq!(p.version.as_deref(), Some("4"));
        assert_eq!(p.diff.len(), 2);
    }
}
