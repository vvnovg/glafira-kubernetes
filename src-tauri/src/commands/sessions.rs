use tauri::State;
use tauri::ipc::Channel;

use crate::error::{AppResult, ErrorCode};
use crate::logs::{KubeLogs, LogEvent, LogParams, Matcher};
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
    // Сессия на учёте ещё до подключения: отключение, пока идёт вход (exec-плагин), её
    // остановит (`stop_for`). При ошибке её снимает с учёта `guard`.
    let (id, cancel) = st.sessions.register(&params.connection_id);
    let guard = SessionGuard {
        sessions: st.sessions.clone(),
        id: id.clone(),
    };
    let ns = params.namespace.clone();
    let workload = params.workload.clone();
    let (used, reader, selector) = st
        .read_in(&params.connection_id, &params.namespace, |c| async move {
            let r = c.reader();
            // У workload'а без селектора подов нет, как у `list_pods`: пустой селектор.
            let selector = match &workload {
                Some(w) => Some(
                    crate::k8s::workloads::pod_selector(&r, &ns, w.kind, &w.name)
                        .await?
                        .unwrap_or_default(),
                ),
                None => None,
            };
            Ok((c, r, selector))
        })
        .await?;
    if cancel.is_cancelled() {
        // Подключение отключили, пока сессия подключалась: её уже сняли с учёта.
        return Ok(id);
    }
    let groups = used.info.api_groups;
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
    // Как у `AppState::read_in`: 403 — права проекта перечитаются, ошибка учётных данных или
    // TLS сбрасывает клиент сессии, если он ещё в пуле.
    let report: crate::watch::Report = {
        let (st, cid, ns) = (
            st.clone(),
            params.connection_id.clone(),
            params.namespace.clone(),
        );
        Box::new(move |e| {
            if e.code == ErrorCode::Forbidden {
                st.access.forget(&cid, &ns);
            }
            st.evict_if_broken_client(&cid, &used, e);
        })
    };
    tokio::spawn(async move {
        let _guard = guard;
        crate::watch::run_reporting(sources, poll, sink, cancel, report).await;
    });
    Ok(id)
}

/// Логи контейнера (спец. §6.6); возвращает id сессии. Неверный regex — ошибка сразу.
#[tauri::command]
pub async fn start_logs(
    state: State<'_, AppState>,
    params: LogParams,
    on_event: Channel<LogEvent>,
) -> AppResult<String> {
    start_logs_impl(state.inner(), params, on_event).await
}

pub async fn start_logs_impl(
    st: &AppState,
    params: LogParams,
    sink: impl Sink<LogEvent>,
) -> AppResult<String> {
    let matcher = Matcher::new(params.filter.as_ref())?;
    let reader = st
        .read_in(&params.connection_id, &params.namespace, |c| async move {
            Ok(c.reader())
        })
        .await?;
    let follow = params.follow;
    let (id, cancel) = st.sessions.register(&params.connection_id);
    let guard = SessionGuard {
        sessions: st.sessions.clone(),
        id: id.clone(),
    };
    tokio::spawn(async move {
        let _guard = guard;
        crate::logs::run(KubeLogs { reader, params }, follow, matcher, sink, cancel).await;
    });
    Ok(id)
}

/// Сохраняет лог с теми же параметрами (без follow) в файл `path`; возвращает число строк.
#[tauri::command]
pub async fn save_logs(
    state: State<'_, AppState>,
    params: LogParams,
    path: String,
    timestamps: bool,
) -> AppResult<u64> {
    let matcher = Matcher::new(params.filter.as_ref())?;
    let mut params = params;
    params.follow = false;
    let (id, ns) = (params.connection_id.clone(), params.namespace.clone());
    state
        .read_in(&id, &ns, |c| async move {
            let logs = KubeLogs {
                reader: c.reader(),
                params,
            };
            crate::logs::save(&logs, &matcher, std::path::Path::new(&path), timestamps).await
        })
        .await
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
                // Watch сразу отвечает 504; watcher повторяет его, пока сессию не остановят.
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
    async fn invalid_regex_fails_before_the_session() {
        let dir = tempfile::tempdir().unwrap();
        let st =
            AppState::new(dir.path(), Arc::new(crate::secrets::MemoryStore::default())).unwrap();
        let params = LogParams {
            connection_id: "a".into(),
            namespace: "team-a".into(),
            pod: "web-1".into(),
            container: None,
            previous: false,
            tail_lines: None,
            since_seconds: None,
            since_time: None,
            follow: true,
            filter: Some(crate::logs::LogFilter {
                text: "[".into(),
                regex: true,
                case_sensitive: false,
            }),
        };
        let e = start_logs_impl(&st, params, Collect::default())
            .await
            .unwrap_err();
        assert_eq!(e.key, "errors.logs.badRegex");
        assert!(st.sessions.is_empty());
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
        assert!(st.sessions.is_empty());
    }

    /// Мок кластера: Deployment `web` — `deployment` (и `on_get` при его чтении), список подов
    /// пуст; запросы подов считаются в `pods`.
    fn cluster(
        st: &AppState,
        deployment: serde_json::Value,
        on_get: impl Fn() + Send + 'static,
    ) -> Arc<std::sync::atomic::AtomicUsize> {
        let pods = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = pods.clone();
        let connected = crate::k8s::pool::test_connected(move |s| {
            if s.path() == "/apis/apps/v1/namespaces/team-a/deployments/web" {
                on_get();
                (200, deployment.clone())
            } else {
                counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                (
                    200,
                    serde_json::json!({"apiVersion": "v1", "kind": "PodList",
                        "metadata": {"resourceVersion": "1"}, "items": []}),
                )
            }
        });
        assert!(
            st.pool
                .insert_if_current("a", st.pool.generation("a"), connected)
        );
        pods
    }

    fn web() -> Option<WorkloadRef> {
        Some(WorkloadRef {
            kind: WorkloadKind::Deployment,
            name: "web".into(),
        })
    }

    /// Отключение, пока сессия подключается (здесь — читает селектор workload'а): сессия
    /// останавливается и не начинается.
    #[tokio::test]
    async fn a_session_stopped_while_connecting_does_not_start() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let dir = tempfile::tempdir().unwrap();
        let st =
            AppState::new(dir.path(), Arc::new(crate::secrets::MemoryStore::default())).unwrap();
        let stopped = Arc::new(AtomicUsize::new(0));
        let pods = {
            let (sessions, stopped) = (st.sessions.clone(), stopped.clone());
            cluster(
                &st,
                crate::k8s::workloads::fixtures::deployment("web", 1, 1),
                move || stopped.store(sessions.stop_for("a"), Ordering::SeqCst),
            )
        };
        let sink: Collect<WatchEvent> = Collect::default();
        start_watch_impl(&st, params(WatchKind::Pods, web()), sink.clone())
            .await
            .unwrap();
        assert_eq!(stopped.load(Ordering::SeqCst), 1, "сессия уже на учёте");
        assert!(st.sessions.is_empty());
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert_eq!(pods.load(Ordering::SeqCst), 0, "watch не начат");
        assert!(sink.items().is_empty());
    }

    /// Workload без селектора: подов нет, как у `list_pods`, — пустой список без watch.
    #[tokio::test]
    async fn a_workload_without_a_selector_has_no_pods_to_watch() {
        let dir = tempfile::tempdir().unwrap();
        let st =
            AppState::new(dir.path(), Arc::new(crate::secrets::MemoryStore::default())).unwrap();
        let mut deployment = crate::k8s::workloads::fixtures::deployment("web", 1, 1);
        deployment["spec"]["selector"] = serde_json::json!({});
        let pods = cluster(&st, deployment, || {});
        let sink: Collect<WatchEvent> = Collect::default();
        start_watch_impl(&st, params(WatchKind::Pods, web()), sink.clone())
            .await
            .unwrap();
        let ended = async {
            while !st.sessions.is_empty() {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        };
        tokio::time::timeout(std::time::Duration::from_secs(5), ended)
            .await
            .unwrap();
        let events = sink.items();
        assert!(
            matches!(&events[..], [WatchEvent::Reset { items, errors, truncated: false }]
                if items.is_empty() && errors.is_empty()),
            "{events:?}"
        );
        assert_eq!(pods.load(std::sync::atomic::Ordering::SeqCst), 0);
    }
}
