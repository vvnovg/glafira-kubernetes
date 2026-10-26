use std::sync::Arc;

use tauri::State;
use tauri::ipc::Channel;

use crate::error::{AppResult, ErrorCode};
use crate::logs::{KubeLogs, LogEvent, LogParams, Matcher, Reported};
use crate::sessions::{Connect, SessionGuard, Sink, StreamClient};
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
    let (used, selector) = st
        .read_in(&params.connection_id, &params.namespace, |c| async move {
            // У workload'а без селектора подов нет, как у `list_pods`: пустой селектор.
            let selector = match &workload {
                Some(w) => Some(
                    crate::k8s::workloads::pod_selector(&c.reader(), &ns, w.kind, &w.name)
                        .await?
                        .unwrap_or_default(),
                ),
                None => None,
            };
            Ok((c, selector))
        })
        .await?;
    if cancel.is_cancelled() {
        // Подключение отключили, пока сессия подключалась: её уже сняли с учёта.
        return Ok(id);
    }
    let groups = used.info.api_groups;
    let client = StreamClient::new(used.clone(), reconnect(st, &params.connection_id));
    let sources = crate::watch::sources(
        &client,
        &params.namespace,
        params.kind,
        selector.as_deref(),
        &groups,
    );
    let poll = crate::watch::poll(
        client.fork(),
        params.namespace.clone(),
        params.kind,
        params.workload.clone(),
        groups,
    );
    let report = session_report(st, &params.connection_id, &params.namespace, used);
    tokio::spawn(async move {
        let _guard = guard;
        crate::watch::run_reporting(sources, poll, sink, cancel, report).await;
    });
    Ok(id)
}

/// Клиент подключения для потоков сессии, которые открываются заново (`StreamClient`): клиент,
/// который кластер не принял, сбрасывается (`evict_if_broken_client`: только если в пуле всё
/// ещё он), и клиент берётся у `AppState::connected` — из пула (его мог уже заменить соседний
/// поток или чтение вкладки) или новым подключением. Истекающие учётные данные exec-плагина
/// `connected` выдаёт заново одной попыткой на всех ожидающих; политика проверяется так же, как
/// у любого чтения.
fn reconnect(st: &AppState, connection_id: &str) -> Connect {
    let (st, id) = (st.clone(), connection_id.to_string());
    Arc::new(move |broken| {
        let (st, id) = (st.clone(), id.clone());
        Box::pin(async move {
            if let Some((used, e)) = broken {
                st.evict_if_broken_client(&id, &used, &e);
            }
            st.connected(&id, false).await
        })
    })
}

/// Что делать с ошибками запросов сессии, которая начиналась клиентом `used`. Как у
/// `AppState::read_in`: 403 — права проекта перечитаются, ошибка учётных данных или TLS
/// сбрасывает этот клиент, если он ещё в пуле (клиент, взятый потоком потом, сбрасывает
/// `StreamClient::renew`).
fn session_report(
    st: &AppState,
    connection_id: &str,
    namespace: &str,
    used: std::sync::Arc<crate::k8s::pool::Connected>,
) -> crate::watch::Report {
    let (st, id, ns) = (st.clone(), connection_id.to_string(), namespace.to_string());
    Box::new(move |e| {
        if e.code == ErrorCode::Forbidden {
            st.access.forget(&id, &ns);
        }
        st.evict_if_broken_client(&id, &used, e);
    })
}

/// Логи контейнера (спец. §6.6); возвращает id сессии. Неверный regex или время начала — ошибка
/// сразу.
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
    crate::logs::check_since(&params)?;
    // Как у watch: сессия на учёте ещё до подключения, и отключение, пока идёт вход
    // (exec-плагин), её остановит (`stop_for`). При ошибке её снимает с учёта `guard`.
    let (id, cancel) = st.sessions.register(&params.connection_id);
    let guard = SessionGuard {
        sessions: st.sessions.clone(),
        id: id.clone(),
    };
    let used = st
        .read_in(&params.connection_id, &params.namespace, |c| async move {
            Ok(c)
        })
        .await?;
    if cancel.is_cancelled() {
        // Подключение отключили, пока сессия подключалась: её уже сняли с учёта.
        return Ok(id);
    }
    // Лог прежнего экземпляра контейнера не растёт: follow ему не нужен.
    let follow = params.follow && !params.previous;
    let client = StreamClient::new(used.clone(), reconnect(st, &params.connection_id));
    let report = session_report(st, &params.connection_id, &params.namespace, used);
    tokio::spawn(async move {
        let _guard = guard;
        let source = Reported {
            source: KubeLogs { client, params },
            report,
        };
        crate::logs::run(source, follow, matcher, sink, cancel).await;
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
    save_logs_impl(state.inner(), params, path, timestamps).await
}

/// Сохранение — тоже сессия подключения: отключение и удаление подключения (`stop_for`), как и
/// перезагрузка страницы (`stop_all`), его останавливают, и файл не появляется.
pub async fn save_logs_impl(
    st: &AppState,
    params: LogParams,
    path: String,
    timestamps: bool,
) -> AppResult<u64> {
    let matcher = Matcher::new(params.filter.as_ref())?;
    crate::logs::check_since(&params)?;
    let mut params = params;
    params.follow = false;
    let (id, cancel) = st.sessions.register(&params.connection_id);
    let _guard = SessionGuard {
        sessions: st.sessions.clone(),
        id,
    };
    let (cid, ns) = (params.connection_id.clone(), params.namespace.clone());
    let save = st.read_in(&cid, &ns, |c| async move {
        let logs = KubeLogs {
            client: StreamClient::fixed(c.reader()),
            params,
        };
        crate::logs::save(&logs, &matcher, std::path::Path::new(&path), timestamps).await
    });
    tokio::select! {
        _ = cancel.cancelled() => Err(crate::logs::save_stopped()),
        r = save => r,
    }
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

    fn log_params() -> LogParams {
        LogParams {
            connection_id: "a".into(),
            namespace: "team-a".into(),
            pod: "web-1".into(),
            container: None,
            previous: false,
            tail_lines: None,
            since_seconds: None,
            since_time: None,
            follow: true,
            filter: None,
        }
    }

    /// Отключение, пока сессия логов подключается (очередь подключения занята): сессия уже на
    /// учёте, останавливается и лог не открывает.
    #[tokio::test]
    async fn a_log_session_stopped_while_connecting_does_not_start() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let dir = tempfile::tempdir().unwrap();
        let st =
            AppState::new(dir.path(), Arc::new(crate::secrets::MemoryStore::default())).unwrap();
        let requests = Arc::new(AtomicUsize::new(0));
        let connected = {
            let requests = requests.clone();
            crate::k8s::pool::test_connected(move |_| {
                requests.fetch_add(1, Ordering::SeqCst);
                (200, serde_json::json!({}))
            })
        };
        assert!(
            st.pool
                .insert_if_current("a", st.pool.generation("a"), connected)
        );
        let turn = st.pool.connect_lock("a").await;
        let sink: Collect<LogEvent> = Collect::default();
        let task = {
            let (st, sink) = (st.clone(), sink.clone());
            tokio::spawn(async move { start_logs_impl(&st, log_params(), sink).await })
        };
        let registered = async {
            while st.sessions.is_empty() {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        };
        tokio::time::timeout(std::time::Duration::from_secs(5), registered)
            .await
            .expect("сессия на учёте до подключения");
        assert_eq!(st.sessions.stop_for("a"), 1);
        drop(turn);
        task.await.unwrap().unwrap();
        assert!(st.sessions.is_empty());
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert_eq!(requests.load(Ordering::SeqCst), 0, "лог не открыт");
        assert!(sink.items().is_empty());
    }

    /// Неверное время начала — ошибка до сессии, а не весь лог.
    #[tokio::test]
    async fn an_invalid_start_time_fails_before_the_session() {
        let dir = tempfile::tempdir().unwrap();
        let st =
            AppState::new(dir.path(), Arc::new(crate::secrets::MemoryStore::default())).unwrap();
        let params = || LogParams {
            since_time: Some("yesterday".into()),
            ..log_params()
        };
        let e = start_logs_impl(&st, params(), Collect::default())
            .await
            .unwrap_err();
        assert_eq!(e.key, "errors.logs.badSinceTime");
        let path = dir.path().join("web-1.log").display().to_string();
        let e = save_logs_impl(&st, params(), path, false)
            .await
            .unwrap_err();
        assert_eq!(e.key, "errors.logs.badSinceTime");
        assert!(st.sessions.is_empty());
    }

    /// Отключение останавливает и сохранение лога (здесь — пока оно ждёт подключения): файла
    /// нет, на учёте ничего не осталось.
    #[tokio::test]
    async fn a_disconnect_stops_saving_a_log() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let dir = tempfile::tempdir().unwrap();
        let st =
            AppState::new(dir.path(), Arc::new(crate::secrets::MemoryStore::default())).unwrap();
        let requests = Arc::new(AtomicUsize::new(0));
        let connected = {
            let requests = requests.clone();
            crate::k8s::pool::test_connected(move |_| {
                requests.fetch_add(1, Ordering::SeqCst);
                (200, serde_json::json!({}))
            })
        };
        assert!(
            st.pool
                .insert_if_current("a", st.pool.generation("a"), connected)
        );
        let out = tempfile::tempdir().unwrap();
        let path = out.path().join("web-1.log");
        let turn = st.pool.connect_lock("a").await;
        let task = {
            let (st, path) = (st.clone(), path.display().to_string());
            tokio::spawn(async move { save_logs_impl(&st, log_params(), path, false).await })
        };
        let registered = async {
            while st.sessions.is_empty() {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        };
        tokio::time::timeout(std::time::Duration::from_secs(5), registered)
            .await
            .expect("сохранение на учёте");
        assert_eq!(st.sessions.stop_for("a"), 1);
        let e = task.await.unwrap().unwrap_err();
        assert_eq!(e.key, "errors.logs.saveStopped");
        drop(turn);
        assert!(st.sessions.is_empty());
        assert_eq!(requests.load(Ordering::SeqCst), 0);
        assert_eq!(std::fs::read_dir(out.path()).unwrap().count(), 0);
    }

    /// Exec-плагин `kubectl oidc-login get-token`: каждый запуск — новый токен `tok-<номер>`
    /// (запуски — строки в файле `count`), срок — через несколько секунд, то есть учётные данные
    /// уже истекают (`Connected::expiring` берёт минуту запаса). Подключение `a` — к `port`.
    #[cfg(unix)]
    fn expiring_plugin(st: &AppState, dir: &std::path::Path, port: u16) -> std::path::PathBuf {
        use crate::storage::profile::{AuthConfig, ExecEnv, ProxyConfig, sample_exec};
        use std::os::unix::fs::PermissionsExt;
        let expires = (time::OffsetDateTime::now_utc() + time::Duration::seconds(5))
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap();
        let script = dir.join("kubectl");
        std::fs::write(
            &script,
            format!(
                "#!/bin/sh\necho run >> \"$COUNT_FILE\"\nn=$(wc -l < \"$COUNT_FILE\" | tr -d ' ')\nprintf '{{\"kind\":\"ExecCredential\",\"status\":{{\"token\":\"tok-%s\",\"expirationTimestamp\":\"{expires}\"}}}}' \"$n\"\n"
            ),
        )
        .unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        let count = dir.join("count");
        let mut exec = sample_exec();
        exec.command = script.display().to_string();
        exec.args = vec!["oidc-login".into(), "get-token".into()];
        exec.env = vec![ExecEnv {
            name: "COUNT_FILE".into(),
            value: count.display().to_string(),
        }];
        exec.approved = Some(crate::auth::exec::fingerprint(&exec));
        let mut p = crate::storage::connections::sample_profile("a");
        p.auth = AuthConfig::Exec(exec);
        p.proxy = ProxyConfig::None;
        st.connections.upsert(p).unwrap();
        // Простой HTTP, чтобы не поднимать TLS: профиль с http:// не сохранить, поэтому адрес
        // меняется в данных хранилища.
        st.connections
            .update(|d| {
                let p = d.connections.iter_mut().find(|c| c.id == "a").unwrap();
                p.api_url = format!("http://127.0.0.1:{port}");
                Ok(())
            })
            .unwrap();
        count
    }

    /// Ждёт, пока `done` не станет верным (до 10 с).
    async fn until(what: &str, done: impl Fn() -> bool) {
        let wait = async {
            while !done() {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        };
        tokio::time::timeout(std::time::Duration::from_secs(10), wait)
            .await
            .unwrap_or_else(|_| panic!("не дождались: {what}"));
    }

    /// Watch подов и follow лога открыты, а учётные данные exec-плагина истекли: кластер больше
    /// не принимает прежние токены и закрывает потоки, открытые ими. Обе сессии идут дальше —
    /// с токеном нового запуска плагина, без ошибки.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_watch_and_a_follow_log_outlive_the_exec_credential() {
        use crate::k8s::testing::{Reply, serve};
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio_util::sync::CancellationToken;
        let dir = tempfile::tempdir().unwrap();
        let st =
            AppState::new(dir.path(), Arc::new(crate::secrets::MemoryStore::default())).unwrap();
        // Токены с номером не больше — истекли; `rotate` закрывает потоки, открытые до этого.
        let expired = Arc::new(AtomicUsize::new(0));
        let rotate = CancellationToken::new();
        let port = {
            let (expired, rotate) = (expired.clone(), rotate.clone());
            let open = CancellationToken::new();
            serve(move |s, token| {
                let n: usize = token
                    .and_then(|t| t.strip_prefix("tok-"))
                    .and_then(|n| n.parse().ok())
                    .unwrap_or(0);
                let json = |(code, v): (u16, serde_json::Value)| {
                    Reply::Full(code, serde_json::to_vec(&v).unwrap())
                };
                if n == 0 || n <= expired.load(Ordering::SeqCst) {
                    return json(crate::k8s::testing::status(
                        401,
                        "Unauthorized",
                        "Unauthorized",
                    ));
                }
                let held = |body: Vec<u8>| match rotate.is_cancelled() {
                    false => Reply::Held(body, rotate.clone()),
                    true => Reply::Held(body, open.clone()),
                };
                match s.path() {
                    "/api/v1/namespaces/team-a/pods" if s.uri.contains("watch=true") => {
                        held(Vec::new())
                    }
                    "/api/v1/namespaces/team-a/pods" => json((
                        200,
                        serde_json::json!({"apiVersion": "v1", "kind": "PodList",
                            "metadata": {"resourceVersion": "1"},
                            "items": [crate::k8s::pods::fixtures::running(&format!("web-{n}"))]}),
                    )),
                    "/api/v1/namespaces/team-a/pods/web-1/log" => {
                        held(format!("2026-10-25T10:{n:02}:00Z line-{n}\n").into_bytes())
                    }
                    "/api/v1/namespaces/team-a/pods/web-1" => {
                        json((200, crate::k8s::pods::fixtures::running("web-1")))
                    }
                    _ => json(crate::k8s::cluster::fixtures::openshift(s)),
                }
            })
            .await
        };
        let count = expiring_plugin(&st, dir.path(), port);
        let runs = || {
            std::fs::read_to_string(&count)
                .map(|s| s.lines().count())
                .unwrap_or(0)
        };
        let pods: Collect<WatchEvent> = Collect::default();
        start_watch_impl(&st, params(WatchKind::Pods, None), pods.clone())
            .await
            .unwrap();
        let logs: Collect<LogEvent> = Collect::default();
        start_logs_impl(&st, log_params(), logs.clone())
            .await
            .unwrap();
        let lines = |logs: &Collect<LogEvent>| -> Vec<String> {
            logs.items()
                .iter()
                .flat_map(|e| match e {
                    LogEvent::Lines { lines, .. } => lines.iter().map(|l| l.text.clone()).collect(),
                    LogEvent::Reconnecting { .. } => vec!["<reconnecting>".into()],
                    LogEvent::End { .. } => vec!["<end>".into()],
                    LogEvent::Error { error } => vec![format!("<error {}>", error.key)],
                })
                .collect()
        };
        // Список подов: `web-<номер токена>` у `Reset`, ошибка — ключом.
        let watched = |pods: &Collect<WatchEvent>| -> Vec<String> {
            pods.items()
                .iter()
                .map(|e| match e {
                    WatchEvent::Reset { items, .. } => {
                        items.iter().map(|r| r.key()).collect::<Vec<_>>().join(",")
                    }
                    WatchEvent::Error { error, fatal } => format!("<error {} {fatal}>", error.key),
                    other => format!("{other:?}"),
                })
                .collect()
        };
        until(
            "первый список подов и первая строка лога",
            || watched(&pods).len() == 1 && lines(&logs).len() == 1,
        )
        .await;
        // Учётные данные истекли: прежние токены кластер больше не принимает.
        let before = runs();
        expired.store(before, Ordering::SeqCst);
        rotate.cancel();
        until(
            "второе событие списка и строка лога после переподключения",
            || watched(&pods).len() == 2 && lines(&logs).len() == 3,
        )
        .await;
        let issued_later = |text: &str, prefix: &str| {
            text.strip_prefix(prefix)
                .and_then(|n| n.parse::<usize>().ok())
                .is_some_and(|n| n > before)
        };
        let watched = watched(&pods);
        assert!(
            issued_later(&watched[1], "web-"),
            "список прочитан заново токеном нового запуска плагина: {watched:?}"
        );
        let lines = lines(&logs);
        assert!(
            lines[1] == "<reconnecting>" && issued_later(&lines[2], "line-"),
            "лог открыт снова токеном нового запуска плагина: {lines:?}"
        );
        assert!(runs() > before, "плагин запущен снова");
        assert_eq!(st.sessions.stop_for("a"), 2, "обе сессии идут");
    }

    /// Кластер отклонил токен при открытии лога: ошибка в сессии, клиент подключения сброшен.
    #[tokio::test]
    async fn a_rejected_log_open_evicts_the_client() {
        let dir = tempfile::tempdir().unwrap();
        let st =
            AppState::new(dir.path(), Arc::new(crate::secrets::MemoryStore::default())).unwrap();
        let connected = crate::k8s::pool::test_connected(|_| {
            crate::k8s::testing::status(401, "Unauthorized", "Unauthorized")
        });
        assert!(
            st.pool
                .insert_if_current("a", st.pool.generation("a"), connected)
        );
        let sink: Collect<LogEvent> = Collect::default();
        start_logs_impl(&st, log_params(), sink.clone())
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
            matches!(&events[..], [LogEvent::Error { error }] if error.key == "errors.k8s.unauthorized"),
            "{events:?}"
        );
        assert!(st.pool.get("a").is_none(), "клиент сброшен");
    }
}
