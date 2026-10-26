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

/// Права пользователя в проекте (спец. §5.4): из кэша, если им меньше 5 минут. `force` — ручное
/// обновление экрана: права проекта забываются и читаются заново (иначе скрытая по устаревшим
/// правам подвкладка не вернулась бы до конца срока кэша).
#[tauri::command]
pub async fn get_namespace_access(
    state: State<'_, AppState>,
    connection_id: String,
    namespace: String,
    force: bool,
) -> AppResult<NamespaceAccess> {
    cached_namespace_access(state.inner(), &connection_id, &namespace, force).await
}

/// Права пользователя в проекте: из кэша или проверкой `SelfSubjectRulesReview`; с `force` кэш
/// проекта сбрасывается до чтения.
pub async fn cached_namespace_access(
    st: &AppState,
    connection_id: &str,
    namespace: &str,
    force: bool,
) -> AppResult<NamespaceAccess> {
    if force {
        st.access.forget(connection_id, namespace);
    } else if let Some(a) = st.access.get(connection_id, namespace) {
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
        let a = cached_namespace_access(&st, "a", "team-a", false)
            .await
            .unwrap();
        assert!(a.resources["pods"].list);
        assert!(st.access.get("a", "team-a").is_none());

        // Без сброса права кэшируются.
        let c = crate::k8s::pool::test_connected(|_| review());
        assert!(st.pool.insert_if_current("a", st.pool.generation("a"), c));
        cached_namespace_access(&st, "a", "team-a", false)
            .await
            .unwrap();
        assert_eq!(st.access.get("a", "team-a"), Some(a));
    }

    /// Ответ `SelfSubjectRulesReview`: в проекте можно `list` перечисленных ресурсов.
    fn review_of(resources: &[&str]) -> (u16, serde_json::Value) {
        (
            201,
            serde_json::json!({"apiVersion": "authorization.k8s.io/v1",
                "kind": "SelfSubjectRulesReview", "metadata": {}, "spec": {"namespace": "team-a"},
                "status": {"incomplete": false, "nonResourceRules": [],
                    "resourceRules": [{"verbs": ["list"], "apiGroups": [""], "resources": resources}]}}),
        )
    }

    /// Обычный вызов берёт права из кэша, `force` — читает заново и заменяет ими кэш: подвкладка,
    /// скрытая по устаревшим правам, возвращается по ручному обновлению.
    #[tokio::test]
    async fn force_bypasses_the_cache_and_replaces_it() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let dir = tempfile::tempdir().unwrap();
        let st = AppState::new(dir.path(), Arc::new(MemoryStore::default())).unwrap();
        let reads = Arc::new(AtomicUsize::new(0));
        let counted = reads.clone();
        // Первое чтение: только `pods`; дальше права выданы и на `events`.
        let c = crate::k8s::pool::test_connected(move |_| {
            if counted.fetch_add(1, Ordering::SeqCst) == 0 {
                review_of(&["pods"])
            } else {
                review_of(&["pods", "events"])
            }
        });
        assert!(st.pool.insert_if_current("a", st.pool.generation("a"), c));

        let first = cached_namespace_access(&st, "a", "team-a", false)
            .await
            .unwrap();
        assert!(first.resources["pods"].list);
        assert!(!first.resources.get("events").is_some_and(|v| v.list));
        // Без `force` — из кэша, запроса к кластеру нет.
        let cached = cached_namespace_access(&st, "a", "team-a", false)
            .await
            .unwrap();
        assert_eq!(cached, first);
        assert_eq!(reads.load(Ordering::SeqCst), 1);

        // С `force` — новое чтение, и оно же в кэше.
        let fresh = cached_namespace_access(&st, "a", "team-a", true)
            .await
            .unwrap();
        assert_eq!(reads.load(Ordering::SeqCst), 2);
        assert!(fresh.resources["events"].list);
        assert_eq!(st.access.get("a", "team-a"), Some(fresh.clone()));
        let after = cached_namespace_access(&st, "a", "team-a", false)
            .await
            .unwrap();
        assert_eq!(after, fresh);
        assert_eq!(reads.load(Ordering::SeqCst), 2);
    }

    /// `force` забывает права только этого проекта.
    #[tokio::test]
    async fn force_keeps_the_rights_of_other_projects() {
        let dir = tempfile::tempdir().unwrap();
        let st = AppState::new(dir.path(), Arc::new(MemoryStore::default())).unwrap();
        let c = crate::k8s::pool::test_connected(|_| review());
        assert!(st.pool.insert_if_current("a", st.pool.generation("a"), c));
        let other = crate::k8s::access::NamespaceAccess {
            resources: Default::default(),
            incomplete: true,
        };
        st.access
            .put("a", "team-b", st.access.epoch("a"), other.clone());
        cached_namespace_access(&st, "a", "team-a", true)
            .await
            .unwrap();
        assert_eq!(st.access.get("a", "team-b"), Some(other));
    }
}
