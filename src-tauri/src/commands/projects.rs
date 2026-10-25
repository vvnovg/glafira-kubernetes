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
    if let Some(a) = state.access.get(&connection_id, &namespace) {
        return Ok(a);
    }
    let ns = namespace.clone();
    let access = state
        .read(&connection_id, |c| async move {
            crate::k8s::access::namespace_access(&c.reader(), &ns).await
        })
        .await?;
    state.access.put(&connection_id, &namespace, access.clone());
    Ok(access)
}
