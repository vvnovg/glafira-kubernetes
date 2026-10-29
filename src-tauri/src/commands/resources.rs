//! Ресурсы (спец. §4.2, §7): источник метрик подключения, статистика workload'а и текущее
//! потребление пода. Чтение в проекте идёт через `read_in`: 403 сбрасывает кэш прав проекта.

use tauri::State;

use crate::error::AppResult;
use crate::k8s::workloads::WorkloadKind;
use crate::metrics::select::SourceStatus;
use crate::resources::usage::{PodUsage, WorkloadUsage};
use crate::state::AppState;

pub(crate) fn now_s() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as i64)
}

/// Какой источник метрик выбран и почему отвергнуты остальные; `refresh` — выбрать заново.
#[tauri::command]
pub async fn get_metrics_source_status(
    state: State<'_, AppState>,
    connection_id: String,
    refresh: bool,
) -> AppResult<SourceStatus> {
    let st = state.inner().clone();
    let id = connection_id.clone();
    state
        .read(&connection_id, |c| async move {
            let s = crate::metrics::select::for_connection(&st, &id, &c, refresh).await?;
            Ok(s.status.clone())
        })
        .await
}

/// Статистика workload'а за окно `window_days` (1, 7, 14 или 30 дней).
#[tauri::command]
pub async fn get_workload_usage(
    state: State<'_, AppState>,
    connection_id: String,
    namespace: String,
    kind: WorkloadKind,
    name: String,
    window_days: u32,
) -> AppResult<WorkloadUsage> {
    let st = state.inner().clone();
    let (id, ns) = (connection_id.clone(), namespace.clone());
    state
        .read_in(&connection_id, &namespace, |c| async move {
            let sel = crate::metrics::select::for_connection(&st, &id, &c, false).await?;
            crate::resources::usage::workload_usage(
                &c.reader(),
                &sel,
                &ns,
                kind,
                &name,
                window_days,
                now_s(),
            )
            .await
        })
        .await
}

/// Текущее потребление контейнеров пода.
#[tauri::command]
pub async fn get_pod_usage(
    state: State<'_, AppState>,
    connection_id: String,
    namespace: String,
    pod: String,
) -> AppResult<PodUsage> {
    let st = state.inner().clone();
    let (id, ns) = (connection_id.clone(), namespace.clone());
    state
        .read_in(&connection_id, &namespace, |c| async move {
            let sel = crate::metrics::select::for_connection(&st, &id, &c, false).await?;
            crate::resources::usage::pod_usage(&c.reader(), &sel, &ns, &pod, now_s()).await
        })
        .await
}
