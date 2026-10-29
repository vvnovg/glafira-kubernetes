//! Ресурсы (спец. §4.2, §7): источник метрик подключения и его статус.

use tauri::State;

use crate::error::AppResult;
use crate::metrics::select::SourceStatus;
use crate::state::AppState;

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
