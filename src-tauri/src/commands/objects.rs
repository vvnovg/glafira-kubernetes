//! Объекты проекта (спец. §6.3–6.5): чтение в namespace; ответ 403 сбрасывает кэш прав проекта.

use tauri::State;

use crate::error::AppResult;
use crate::k8s::events::{EventObject, EventRow};
use crate::k8s::network::NetworkView;
use crate::k8s::pods::{PodDetails, PodRow, WorkloadRef};
use crate::k8s::read::Listed;
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

/// Поды проекта или, если задан `workload`, его поды.
#[tauri::command]
pub async fn list_pods(
    state: State<'_, AppState>,
    connection_id: String,
    namespace: String,
    workload: Option<WorkloadRef>,
) -> AppResult<Listed<PodRow>> {
    let ns = namespace.clone();
    state
        .read_in(&connection_id, &namespace, |c| async move {
            crate::k8s::pods::list_pods(&c.reader(), &ns, workload.as_ref()).await
        })
        .await
}

#[tauri::command]
pub async fn get_pod(
    state: State<'_, AppState>,
    connection_id: String,
    namespace: String,
    name: String,
) -> AppResult<PodDetails> {
    let ns = namespace.clone();
    state
        .read_in(&connection_id, &namespace, |c| async move {
            crate::k8s::pods::get_pod(&c.reader(), &ns, &name).await
        })
        .await
}

/// События проекта или объекта (`object`): у workload'а — вместе с его подами.
#[tauri::command]
pub async fn list_events(
    state: State<'_, AppState>,
    connection_id: String,
    namespace: String,
    object: Option<EventObject>,
) -> AppResult<Listed<EventRow>> {
    let ns = namespace.clone();
    state
        .read_in(&connection_id, &namespace, |c| async move {
            crate::k8s::events::list_events(&c.reader(), &ns, object.as_ref()).await
        })
        .await
}

/// Сеть проекта: Services, Routes, Ingress, Gateway API — по группам API кластера.
#[tauri::command]
pub async fn get_network(
    state: State<'_, AppState>,
    connection_id: String,
    namespace: String,
) -> AppResult<NetworkView> {
    let ns = namespace.clone();
    state
        .read_in(&connection_id, &namespace, |c| async move {
            Ok(crate::k8s::network::get_network(&c.reader(), &ns, &c.info.api_groups).await)
        })
        .await
}
