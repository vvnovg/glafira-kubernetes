use tauri::State;

use crate::error::AppResult;
use crate::k8s::access::NamespaceAccess;
use crate::k8s::projects::ProjectList;
use crate::state::AppState;

/// Проекты подключения для дерева (спец. §6.1).
#[tauri::command]
pub async fn list_projects(
    state: State<'_, AppState>,
    connection_id: String,
) -> AppResult<ProjectList> {
    let profile = state.connections.get(&connection_id)?;
    state
        .read(&connection_id, |c| async move {
            crate::k8s::projects::list_projects(&c.reader(), c.info.api_groups.projects, &profile)
                .await
        })
        .await
}

/// Права пользователя в проекте (спец. §5.4): из кэша, если им меньше 5 минут.
#[tauri::command]
pub async fn get_namespace_access(
    state: State<'_, AppState>,
    connection_id: String,
    namespace: String,
) -> AppResult<NamespaceAccess> {
    cached_namespace_access(state.inner(), &connection_id, &namespace).await
}

/// Права пользователя в проекте: из кэша или проверкой `SelfSubjectRulesReview`.
pub async fn cached_namespace_access(
    st: &AppState,
    connection_id: &str,
    namespace: &str,
) -> AppResult<NamespaceAccess> {
    if let Some(a) = st.access.get(connection_id, namespace) {
        return Ok(a);
    }
    // Сброс клиента во время проверки (вход, новый токен, 401) меняет эпоху: ответ прежнего
    // клиента вернётся вызывающему, но в кэш не попадёт.
    let epoch = st.access.epoch(connection_id);
    let ns = namespace.to_string();
    let access = st
        .read(connection_id, |c| async move {
            crate::k8s::access::namespace_access(&c.reader(), &ns).await
        })
        .await?;
    st.access
        .put(connection_id, namespace, epoch, access.clone());
    Ok(access)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::secrets::MemoryStore;

    /// Ответ `SelfSubjectRulesReview`: в проекте можно `list pods`.
    fn review() -> (u16, serde_json::Value) {
        (
            201,
            serde_json::json!({"apiVersion": "authorization.k8s.io/v1",
                "kind": "SelfSubjectRulesReview", "metadata": {}, "spec": {"namespace": "team-a"},
                "status": {"incomplete": false, "nonResourceRules": [],
                    "resourceRules": [{"verbs": ["list"], "apiGroups": [""], "resources": ["pods"]}]}}),
        )
    }

    /// Клиент сбросили (вход, новый токен), пока шла проверка прав: её ответ отдаётся, но в кэш
    /// не попадает — права прежнего клиента не живут ещё 5 минут.
    #[tokio::test]
    async fn rights_read_while_the_client_was_reset_are_not_cached() {
        let dir = tempfile::tempdir().unwrap();
        let st = AppState::new(dir.path(), Arc::new(MemoryStore::default())).unwrap();
        let during = st.clone();
        let c = crate::k8s::pool::test_connected(move |_| {
            during.evict_connection("a");
            review()
        });
        assert!(st.pool.insert_if_current("a", st.pool.generation("a"), c));
        let a = cached_namespace_access(&st, "a", "team-a").await.unwrap();
        assert!(a.resources["pods"].list);
        assert!(st.access.get("a", "team-a").is_none());

        // Без сброса права кэшируются.
        let c = crate::k8s::pool::test_connected(|_| review());
        assert!(st.pool.insert_if_current("a", st.pool.generation("a"), c));
        cached_namespace_access(&st, "a", "team-a").await.unwrap();
        assert_eq!(st.access.get("a", "team-a"), Some(a));
    }
}
