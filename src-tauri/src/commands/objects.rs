//! Объекты проекта (спец. §6.3–6.5): чтение в namespace; ответ 403 сбрасывает кэш прав проекта.

use tauri::State;

use crate::error::AppResult;
use crate::k8s::workloads::{WorkloadDetails, WorkloadKind, WorkloadList};
use crate::state::AppState;

#[tauri::command]
pub async fn list_workloads(
    state: State<'_, AppState>,
    connection_id: String,
    namespace: String,
) -> AppResult<WorkloadList> {
    let ns = namespace.clone();
    state
        .read_in(&connection_id, &namespace, |c| async move {
            Ok(crate::k8s::workloads::list_workloads(&c.reader(), &ns, &c.info.api_groups).await)
        })
        .await
}

#[tauri::command]
pub async fn get_workload(
    state: State<'_, AppState>,
    connection_id: String,
    namespace: String,
    kind: WorkloadKind,
    name: String,
) -> AppResult<WorkloadDetails> {
    let ns = namespace.clone();
    state
        .read_in(&connection_id, &namespace, |c| async move {
            crate::k8s::workloads::get_workload(&c.reader(), &ns, kind, &name).await
        })
        .await
}
