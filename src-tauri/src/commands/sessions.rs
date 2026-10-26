use tauri::State;
use tauri::ipc::Channel;

use crate::error::{AppResult, ErrorCode};
use crate::sessions::{SessionGuard, Sink};
use crate::state::AppState;
use crate::watch::{WatchEvent, WatchKind, WatchParams};

/// Останавливает сессию watch или логов. `false` — она уже завершилась.
#[tauri::command]
pub fn stop_session(state: State<'_, AppState>, session_id: String) -> AppResult<bool> {
    Ok(state.sessions.stop(&session_id))
}

/// Watch списка проекта (спец. §6.7); возвращает id сессии. Данные — в `on_event`.
#[tauri::command]
pub async fn start_watch(
    state: State<'_, AppState>,
    params: WatchParams,
    on_event: Channel<WatchEvent>,
) -> AppResult<String> {
    start_watch_impl(state.inner(), params, on_event).await
}

pub async fn start_watch_impl(
    st: &AppState,
    params: WatchParams,
    sink: impl Sink<WatchEvent>,
) -> AppResult<String> {
    if params.kind == WatchKind::Pods
        && params
            .workload
            .as_ref()
            .is_some_and(|w| w.kind == crate::k8s::workloads::WorkloadKind::CronJob)
    {
        return Err(app_err!(
            ErrorCode::InvalidInput,
            "errors.watch.cronJobPods",
            "Pods of a CronJob are read by the list, not by watch"
        ));
    }
    let ns = params.namespace.clone();
    let workload = params.workload.clone();
    let (reader, groups, selector) = st
        .read_in(&params.connection_id, &params.namespace, |c| async move {
            let r = c.reader();
            let selector = match &workload {
                Some(w) => crate::k8s::workloads::pod_selector(&r, &ns, w.kind, &w.name).await?,
                None => None,
            };
            Ok((r, c.info.api_groups, selector))
        })
        .await?;
    let sources = crate::watch::sources(
        &reader,
        &params.namespace,
        params.kind,
        selector.as_deref(),
        &groups,
    );
    let poll = crate::watch::poll(
        reader,
        params.namespace.clone(),
        params.kind,
        params.workload.clone(),
        groups,
    );
    let (id, cancel) = st.sessions.register(&params.connection_id);
    let guard = SessionGuard {
        sessions: st.sessions.clone(),
        id: id.clone(),
    };
    tokio::spawn(async move {
        let _guard = guard;
        crate::watch::run(sources, poll, sink, cancel).await;
    });
    Ok(id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::k8s::pods::WorkloadRef;
    use crate::k8s::workloads::WorkloadKind;
    use crate::sessions::Collect;
    use std::sync::Arc;

    fn params(kind: WatchKind, workload: Option<WorkloadRef>) -> WatchParams {
        WatchParams {
            connection_id: "a".into(),
            namespace: "team-a".into(),
            kind,
            workload,
        }
    }

    #[tokio::test]
    async fn a_session_is_registered_and_stopped() {
        let dir = tempfile::tempdir().unwrap();
        let st =
            AppState::new(dir.path(), Arc::new(crate::secrets::MemoryStore::default())).unwrap();
        let connected = crate::k8s::pool::test_connected(|s| {
            if s.uri.contains("watch=true") {
                // Watch, который не отвечает, пока сессию не остановят.
                crate::k8s::testing::status(504, "Timeout", "timeout")
            } else {
                (
                    200,
                    serde_json::json!({"apiVersion": "v1", "kind": "EventList",
                        "metadata": {"resourceVersion": "1"}, "items": []}),
                )
            }
        });
        assert!(
            st.pool
                .insert_if_current("a", st.pool.generation("a"), connected)
        );
        let sink: Collect<WatchEvent> = Collect::default();
        let id = start_watch_impl(&st, params(WatchKind::Events, None), sink.clone())
            .await
            .unwrap();
        assert_eq!(st.sessions.len(), 1);
        assert!(st.sessions.stop(&id));
        let ended = async {
            while !st.sessions.is_empty() {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        };
        tokio::time::timeout(std::time::Duration::from_secs(5), ended)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn pods_of_a_cron_job_are_not_watched() {
        let dir = tempfile::tempdir().unwrap();
        let st =
            AppState::new(dir.path(), Arc::new(crate::secrets::MemoryStore::default())).unwrap();
        let cron = WorkloadRef {
            kind: WorkloadKind::CronJob,
            name: "report".into(),
        };
        let e = start_watch_impl(&st, params(WatchKind::Pods, Some(cron)), Collect::default())
            .await
            .unwrap_err();
        assert_eq!(e.key, "errors.watch.cronJobPods");
    }
}
