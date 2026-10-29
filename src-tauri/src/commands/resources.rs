//! Ресурсы (спец. §4.2, §7): источник метрик подключения, статистика workload'а и текущее
//! потребление пода. Чтение в проекте идёт через `read_in`: 403 сбрасывает кэш прав проекта.

use serde::Serialize;
use tauri::State;
use tauri::ipc::Channel;

use crate::error::{AppError, AppResult};
use crate::k8s::workloads::WorkloadKind;
use crate::metrics::select::SourceStatus;
use crate::resources::Analysis;
use crate::resources::project::{PROJECT_DEADLINE, ProjectUsage};
use crate::resources::usage::{PodUsage, WorkloadUsage};
use crate::sessions::{SessionGuard, Sink};
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

/// Статистика workload'а за окно `window_days` (1, 7, 14 или 30 дней). `refresh` («Обновить»,
/// «Повторить») выбирает источник метрик заново, иначе берётся выбор из подключения (5 минут).
#[tauri::command]
pub async fn get_workload_usage(
    state: State<'_, AppState>,
    connection_id: String,
    namespace: String,
    kind: WorkloadKind,
    name: String,
    window_days: u32,
    refresh: Option<bool>,
) -> AppResult<WorkloadUsage> {
    let st = state.inner().clone();
    let (id, ns) = (connection_id.clone(), namespace.clone());
    let refresh = refresh.unwrap_or(false);
    state
        .read_in(&connection_id, &namespace, |c| async move {
            let sel = crate::metrics::select::for_connection(&st, &id, &c, refresh).await?;
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

/// Текущее потребление контейнеров пода; `refresh` — выбрать источник метрик заново.
#[tauri::command]
pub async fn get_pod_usage(
    state: State<'_, AppState>,
    connection_id: String,
    namespace: String,
    pod: String,
    refresh: Option<bool>,
) -> AppResult<PodUsage> {
    let st = state.inner().clone();
    let (id, ns) = (connection_id.clone(), namespace.clone());
    let refresh = refresh.unwrap_or(false);
    state
        .read_in(&connection_id, &namespace, |c| async move {
            let sel = crate::metrics::select::for_connection(&st, &id, &c, refresh).await?;
            crate::resources::usage::pod_usage(&c.reader(), &sel, &ns, &pod, now_s()).await
        })
        .await
}

/// Статистика workload'а и рекомендация requests и limits (вкладка «Ресурсы» workload'а);
/// `refresh` — выбрать источник метрик заново.
#[tauri::command]
pub async fn recommend_resources(
    state: State<'_, AppState>,
    connection_id: String,
    namespace: String,
    kind: WorkloadKind,
    name: String,
    window_days: u32,
    refresh: Option<bool>,
) -> AppResult<Analysis> {
    let st = state.inner().clone();
    let (id, ns) = (connection_id.clone(), namespace.clone());
    let refresh = refresh.unwrap_or(false);
    state
        .read_in(&connection_id, &namespace, |c| async move {
            let sel = crate::metrics::select::for_connection(&st, &id, &c, refresh).await?;
            let nodes = c.info.capabilities.list_nodes.then_some(&c.nodes);
            crate::resources::analyze(
                &c.reader(),
                &sel,
                &ns,
                kind,
                &name,
                window_days,
                now_s(),
                nodes,
            )
            .await
        })
        .await
}

/// Итог сессии сбора по проекту.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum ProjectUsageEvent {
    Done { usage: Box<ProjectUsage> },
    Failed { error: AppError },
}

/// Ресурсы проекта (вкладка «Ресурсы» проекта): фоновая сессия с дедлайном 45 с; итог — в
/// `on_event`, собранное живёт в кэше 5 минут (`refresh` — собрать заново). Возвращает id
/// сессии для `stop_session`.
#[tauri::command]
pub async fn get_project_usage(
    state: State<'_, AppState>,
    connection_id: String,
    namespace: String,
    window_days: u32,
    refresh: bool,
    on_event: Channel<ProjectUsageEvent>,
) -> AppResult<String> {
    get_project_usage_impl(
        state.inner(),
        connection_id,
        namespace,
        window_days,
        refresh,
        on_event,
    )
    .await
}

pub async fn get_project_usage_impl(
    st: &AppState,
    connection_id: String,
    namespace: String,
    days: u32,
    refresh: bool,
    sink: impl Sink<ProjectUsageEvent>,
) -> AppResult<String> {
    crate::k8s::read::check_namespace(&namespace)?;
    crate::resources::usage::check_window(days)?;
    let (id, cancel) = st.sessions.register(&connection_id);
    let guard = SessionGuard {
        sessions: st.sessions.clone(),
        id: id.clone(),
    };
    let st = st.clone();
    tokio::spawn(async move {
        let _guard = guard;
        let work = async {
            // Эпоха подключения — до кэша и до чтения: сброс клиента посреди сбора (новый токен,
            // 401, отключение) не даёт положить в кэш числа, собранные прежним клиентом.
            let epoch = st.resources.epoch(&connection_id);
            if !refresh && let Some(u) = st.resources.get(&connection_id, &namespace, days) {
                return Ok(u);
            }
            let (s, cid, ns) = (st.clone(), connection_id.clone(), namespace.clone());
            let read = st.read_in(&connection_id, &namespace, |c| async move {
                let sel = crate::metrics::select::for_connection(&s, &cid, &c, refresh).await?;
                let groups = c.info.api_groups;
                crate::resources::project::project_usage(
                    &c.reader(),
                    &sel,
                    &groups,
                    &ns,
                    days,
                    now_s(),
                )
                .await
            });
            let u = tokio::time::timeout(PROJECT_DEADLINE, Box::pin(read))
                .await
                .map_err(|_| crate::k8s::errors::timeout())??;
            // Результат с сетевой ошибкой (повторимой: таймаут, обрыв, 5xx) в кэш не идёт: такой
            // сбой разовый, и держать его пять минут незачем. Нет прав (403) или вида нет (404)
            // повторяется при каждом сборе, поэтому такой результат кэшируется, как и неполный.
            if !u.errors.iter().any(|e| e.error.retryable) {
                st.resources.put(&connection_id, epoch, &u);
            }
            Ok::<_, AppError>(u)
        };
        tokio::select! {
            _ = cancel.cancelled() => {}
            r = work => {
                sink.send(match r {
                    Ok(u) => ProjectUsageEvent::Done { usage: Box::new(u) },
                    Err(error) => ProjectUsageEvent::Failed { error },
                });
            }
        }
    });
    Ok(id)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::*;
    use crate::sessions::Collect;

    async fn done(sink: &Collect<ProjectUsageEvent>, n: usize) -> Vec<ProjectUsageEvent> {
        let wait = async {
            while sink.items().len() < n {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };
        tokio::time::timeout(Duration::from_secs(5), wait)
            .await
            .unwrap();
        sink.items()
    }

    #[tokio::test]
    async fn project_usage_is_collected_once_and_cached() {
        let dir = tempfile::tempdir().unwrap();
        let st =
            AppState::new(dir.path(), Arc::new(crate::secrets::MemoryStore::default())).unwrap();
        st.connections
            .upsert(crate::storage::profile::sample_profile("a"))
            .unwrap();
        let pods = Arc::new(AtomicUsize::new(0));
        let seen = pods.clone();
        let connected = crate::k8s::pool::test_connected(move |s| {
            let list = |kind: &str| {
                (
                    200,
                    serde_json::json!({"apiVersion": "v1", "kind": kind, "metadata": {}, "items": []}),
                )
            };
            match s.path() {
                "/apis/metrics.k8s.io/v1beta1" => (200, serde_json::json!({})),
                "/api/v1/namespaces/team-a/pods" => {
                    seen.fetch_add(1, Ordering::SeqCst);
                    list("PodList")
                }
                p if p.ends_with("/proxy/api/v1/query") => {
                    crate::k8s::testing::status(404, "NotFound", "no")
                }
                _ => list("List"),
            }
        });
        assert!(
            st.pool
                .insert_if_current("a", st.pool.generation("a"), connected)
        );
        let sink: Collect<ProjectUsageEvent> = Collect::default();
        get_project_usage_impl(&st, "a".into(), "team-a".into(), 7, false, sink.clone())
            .await
            .unwrap();
        let events = done(&sink, 1).await;
        let ProjectUsageEvent::Done { usage } = &events[0] else {
            panic!("{events:?}")
        };
        assert_eq!(usage.source.location, "metrics.k8s.io");
        get_project_usage_impl(&st, "a".into(), "team-a".into(), 7, false, sink.clone())
            .await
            .unwrap();
        done(&sink, 2).await;
        assert_eq!(pods.load(Ordering::SeqCst), 1, "второй раз — из кэша");
        get_project_usage_impl(&st, "a".into(), "team-a".into(), 7, true, sink.clone())
            .await
            .unwrap();
        done(&sink, 3).await;
        assert_eq!(pods.load(Ordering::SeqCst), 2, "«Обновить» собирает заново");
        let e = get_project_usage_impl(&st, "a".into(), "team-a".into(), 3, false, sink)
            .await
            .unwrap_err();
        assert_eq!(e.key, "errors.resources.badWindow");
    }

    /// Подключение `a` с кластером, где список Deployments отвечает `deployments`; поды
    /// читаются — `pods` раз; `on_pods` вызывается на каждом чтении подов (сброс подключения
    /// посреди сбора).
    fn project_state(
        deployments: (u16, &'static str),
        on_pods: impl Fn(&AppState) + Send + 'static,
    ) -> (tempfile::TempDir, AppState, Arc<AtomicUsize>) {
        let dir = tempfile::tempdir().unwrap();
        let st =
            AppState::new(dir.path(), Arc::new(crate::secrets::MemoryStore::default())).unwrap();
        st.connections
            .upsert(crate::storage::profile::sample_profile("a"))
            .unwrap();
        let pods = Arc::new(AtomicUsize::new(0));
        let (seen, during) = (pods.clone(), st.clone());
        let connected = crate::k8s::pool::test_connected(move |s| {
            let list = |kind: &str| {
                (
                    200,
                    serde_json::json!({"apiVersion": "v1", "kind": kind, "metadata": {}, "items": []}),
                )
            };
            match s.path() {
                "/apis/metrics.k8s.io/v1beta1" => (200, serde_json::json!({})),
                "/apis/apps/v1/namespaces/team-a/deployments" => {
                    crate::k8s::testing::status(deployments.0, deployments.1, "no")
                }
                "/api/v1/namespaces/team-a/pods" => {
                    seen.fetch_add(1, Ordering::SeqCst);
                    on_pods(&during);
                    list("PodList")
                }
                p if p.ends_with("/proxy/api/v1/query") => {
                    crate::k8s::testing::status(404, "NotFound", "no")
                }
                _ => list("List"),
            }
        });
        assert!(
            st.pool
                .insert_if_current("a", st.pool.generation("a"), connected)
        );
        (dir, st, pods)
    }

    /// Два сбора без `refresh`; в каждом ровно одна ошибка (вид Deployment не прочитался).
    async fn collect_twice(st: &AppState) {
        let sink: Collect<ProjectUsageEvent> = Collect::default();
        for n in 1..=2 {
            get_project_usage_impl(st, "a".into(), "team-a".into(), 7, false, sink.clone())
                .await
                .unwrap();
            let events = done(&sink, n).await;
            let ProjectUsageEvent::Done { usage } = &events[n - 1] else {
                panic!("{events:?}")
            };
            let kinds: Vec<_> = usage.errors.iter().map(|e| e.kind).collect();
            assert!(
                kinds.contains(&WorkloadKind::Deployment),
                "{:?}",
                usage.errors
            );
        }
    }

    /// Результат с повторимой ошибкой (500 на списке Deployments) в кэш не идёт: сбой мог быть
    /// разовым, следующий запрос читает кластер заново.
    #[tokio::test]
    async fn project_usage_with_a_retryable_error_is_not_cached() {
        let (_dir, st, pods) = project_state((500, "InternalError"), |_| {});
        collect_twice(&st).await;
        assert_eq!(pods.load(Ordering::SeqCst), 2, "с ошибкой — не из кэша");
        assert!(st.resources.get("a", "team-a", 7).is_none());
    }

    /// Нет прав на вид (403) повторяется при каждом сборе: такой результат кэшируется.
    #[tokio::test]
    async fn project_usage_with_a_forbidden_kind_is_cached() {
        let (_dir, st, pods) = project_state((403, "Forbidden"), |_| {});
        collect_twice(&st).await;
        assert_eq!(pods.load(Ordering::SeqCst), 1, "второй раз — из кэша");
        assert!(st.resources.get("a", "team-a", 7).is_some());
    }

    /// Подключение сбросили посреди сбора (новый токен, 401 в другой вкладке, «Подключиться
    /// заново»): числа, собранные прежним клиентом, в кэш не идут.
    #[tokio::test]
    async fn project_usage_collected_across_a_reset_is_not_cached() {
        let (_dir, st, pods) = project_state((403, "Forbidden"), |st| {
            st.resources.forget_connection("a");
        });
        collect_twice(&st).await;
        assert_eq!(pods.load(Ordering::SeqCst), 2, "сброс во время сбора");
        assert!(st.resources.get("a", "team-a", 7).is_none());
    }
}
