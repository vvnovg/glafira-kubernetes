//! Чтение кластера на настоящем API-сервере (kind, `dev/kind/setup.sh` с `fixtures.yaml`): проблемные
//! поды, workloads, события, сеть, конфигурация, PVC, квоты, YAML, логи, watch и права. Нужен
//! `GO_IT_KUBECONFIG`; без переменной тесты пропускаются.

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine;
use common::{import, kubeconfig_path, no_cancel};
use futures::FutureExt;
use glafira_kubernetes_lib::error::ErrorCode;
use glafira_kubernetes_lib::k8s::pods::{PodProblem, PodRow, WorkloadRef, list_pods};
use glafira_kubernetes_lib::k8s::pool::{Connected, open};
use glafira_kubernetes_lib::k8s::workloads::{WorkloadKind, WorkloadProblem};
use glafira_kubernetes_lib::k8s::{
    access, configs, events, network, overview, projects, quotas, storage, workloads, yaml,
};
use glafira_kubernetes_lib::logs::{self, KubeLogs, LogEvent, LogParams, Matcher};
use glafira_kubernetes_lib::sessions::{Sink, StreamClient};
use glafira_kubernetes_lib::watch::{self, Row, RowChange, WatchEvent, WatchKind};
use tokio_util::sync::CancellationToken;

const NS: &str = "team-a";
/// Значение ключа `password` Secret `db` из `dev/kind/fixtures.yaml`: в ответах Rust его быть не должно.
const SECRET: &str = "s3cr3t-value";

async fn connect(context: &str) -> Option<Connected> {
    let path = kubeconfig_path()?;
    let (profile, secrets) = import(&path, context);
    Some(open(profile, secrets, no_cancel()).await.unwrap())
}

/// Ждёт, пока `check` не вернёт `Ok`, не дольше `limit`. `Err` — «ещё нет», с описанием того, что
/// видно сейчас: если время вышло, последнее описание (или ошибка чтения) попадает в сообщение.
async fn until<T, F, Fut>(what: &str, limit: Duration, mut check: F) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, String>>,
{
    let mut last = String::from("проверка не успела выполниться");
    let wait = async {
        loop {
            match check().await {
                Ok(v) => return v,
                Err(seen) => last = seen,
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    };
    let outcome = tokio::time::timeout(limit, wait).await;
    match outcome {
        Ok(v) => v,
        Err(_) => panic!("не дождались: {what} (до {limit:?}); последнее: {last}"),
    }
}

/// Сессия должна завершиться вскоре после отмены.
async fn finished(task: tokio::task::JoinHandle<()>) {
    tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .expect("сессия не завершилась за 10 с после отмены")
        .unwrap();
}

/// Значение Secret не должно попасть в ответ ни открытым текстом, ни в base64 (как его хранит API).
fn assert_no_secret(what: &str, serialized: &str) {
    let base64 = base64::engine::general_purpose::STANDARD.encode(SECRET);
    assert!(
        !serialized.contains(SECRET),
        "{what}: значение Secret в открытом виде"
    );
    assert!(
        !serialized.contains(&base64),
        "{what}: значение Secret в base64"
    );
}

/// Виды событий watch — для сообщения о том, чего не дождались.
fn event_kinds(events: &[WatchEvent]) -> Vec<String> {
    events
        .iter()
        .map(|e| match e {
            WatchEvent::Reset { items, .. } => format!("Reset({})", items.len()),
            WatchEvent::Changes { changes } => format!("Changes({})", changes.len()),
            WatchEvent::Polling => "Polling".into(),
            WatchEvent::Error { error, fatal } => format!("Error({error:?}, fatal={fatal})"),
        })
        .collect()
}

fn by_app<'a>(pods: &'a [PodRow], app: &str) -> Vec<&'a PodRow> {
    pods.iter()
        .filter(|p| p.owner.as_ref().is_some_and(|o| o.name == app))
        .collect()
}

/// Приёмник событий сессии для тестов.
#[derive(Clone)]
struct Collected<T>(Arc<Mutex<Vec<T>>>);

impl<T> Default for Collected<T> {
    fn default() -> Self {
        Self(Arc::new(Mutex::new(Vec::new())))
    }
}

impl<T: Send + 'static> Sink<T> for Collected<T> {
    fn send(&self, item: T) -> bool {
        self.0.lock().unwrap().push(item);
        true
    }
}

#[tokio::test]
async fn problem_pods_are_recognized() {
    let Some(c) = connect("admin").await else {
        return;
    };
    let r = c.reader();
    // Kubelet 1.37 пишет CrashLoopBackOff не всегда сразу: бывает, что между первыми перезапусками
    // контейнер остаётся завершённым (статус Error), пока пауза перед перезапуском не вырастет до минут.
    let pods = until(
        "под crash в статусе CrashLoopBackOff",
        Duration::from_secs(600),
        || async {
            let pods = list_pods(&r, NS, None).await.unwrap().items;
            let crash: Vec<_> = by_app(&pods, "crash")
                .iter()
                .map(|p| (p.status.clone(), p.restarts))
                .collect();
            if crash.iter().any(|(status, _)| status == "CrashLoopBackOff") {
                Ok(pods)
            } else {
                Err(format!("поды crash (статус, перезапуски): {crash:?}"))
            }
        },
    )
    .await;
    let one = |app: &str| by_app(&pods, app)[0].clone();
    let crash = by_app(&pods, "crash")
        .into_iter()
        .find(|p| p.status == "CrashLoopBackOff")
        .unwrap()
        .clone();
    assert_eq!(crash.problem, Some(PodProblem::CrashLoop));
    assert!(
        crash.restarts >= 1 && crash.last_restart.is_some(),
        "{crash:?}"
    );
    let pull = one("pull");
    assert_eq!(pull.problem, Some(PodProblem::ImagePull), "{pull:?}");
    assert!(
        ["ErrImagePull", "ImagePullBackOff"].contains(&pull.status.as_str()),
        "{pull:?}"
    );
    let pending = one("pending");
    assert_eq!(
        (pending.phase.as_str(), pending.problem),
        ("Pending", Some(PodProblem::Pending))
    );
    let web = by_app(&pods, "web");
    assert_eq!(web.len(), 2);
    assert!(
        web.iter().all(|p| p.problem.is_none() && p.ready == 1),
        "{web:?}"
    );
    assert_eq!(web[0].owner.as_ref().unwrap().kind, "Deployment");
    let details = glafira_kubernetes_lib::k8s::pods::get_pod(&r, NS, &crash.name)
        .await
        .unwrap();
    let last = details.containers[0].last_state.as_ref().unwrap();
    assert_eq!(last.exit_code, Some(1));
}

#[tokio::test]
async fn workloads_with_details_and_their_pods() {
    let Some(c) = connect("admin").await else {
        return;
    };
    let r = c.reader();
    let list = workloads::list_workloads(&r, NS, &c.info.api_groups).await;
    assert!(list.errors.is_empty(), "{:?}", list.errors);
    let find = |kind: WorkloadKind, name: &str| {
        list.items
            .iter()
            .find(|w| w.kind == kind && w.name == name)
            .unwrap_or_else(|| panic!("{kind:?}/{name}"))
            .clone()
    };
    let web = find(WorkloadKind::Deployment, "web");
    assert_eq!((web.desired, web.available), (Some(2), Some(2)));
    assert_eq!(web.problem, None);
    assert_eq!(
        find(WorkloadKind::Deployment, "crash").problem,
        Some(WorkloadProblem::Unavailable)
    );
    assert_eq!(find(WorkloadKind::Job, "migrate").ready, Some(1));
    let report = find(WorkloadKind::CronJob, "report");
    assert_eq!(
        (report.schedule.as_deref(), report.suspended),
        (Some("0 3 * * *"), Some(true))
    );
    let d = workloads::get_workload(&r, NS, WorkloadKind::Deployment, "web")
        .await
        .unwrap();
    // У web переменная DB_PASSWORD берётся из Secret db: значения в описании нет.
    assert_no_secret("get_workload web", &serde_json::to_string(&d).unwrap());
    assert_eq!(d.selector.as_deref(), Some("app=web"));
    let history = d.history.unwrap().data.unwrap();
    assert!(history.iter().any(|h| h.current), "{history:?}");
    let env: Vec<_> = d.containers[0]
        .env
        .iter()
        .map(|e| e.name.as_str())
        .collect();
    assert_eq!(env, ["LOG_LEVEL", "DB_PASSWORD"]);
    assert!(d.hpa.data.unwrap().is_none(), "HPA у web не задан");
    assert!(d.pdb.data.unwrap().is_none(), "PDB у web не задан");
    let web_ref = WorkloadRef {
        kind: WorkloadKind::Deployment,
        name: "web".into(),
    };
    let pods = list_pods(&r, NS, Some(&web_ref)).await.unwrap();
    assert_eq!(pods.items.len(), 2);
    let y = yaml::object_yaml(&r, NS, yaml::YamlKind::Deployment, "web")
        .await
        .unwrap();
    assert!(
        y.contains("kind: Deployment") && !y.contains("managedFields"),
        "{y}"
    );
    assert_no_secret("object_yaml web", &y);
}

#[tokio::test]
async fn events_network_config_storage_and_quotas() {
    use k8s_openapi::api::core::v1::Secret;
    let Some(c) = connect("admin").await else {
        return;
    };
    let r = c.reader();
    let crash = events::EventObject {
        kind: "Deployment".into(),
        name: "crash".into(),
    };
    let evs = until(
        "событие BackOff (Warning) у Deployment crash",
        Duration::from_secs(300),
        || async {
            let l = events::list_events(&r, NS, Some(&crash)).await.unwrap();
            if l.items
                .iter()
                .any(|e| e.reason == "BackOff" && e.kind == "Warning")
            {
                Ok(l.items)
            } else {
                let seen: Vec<_> = l.items.iter().map(|e| (&e.kind, &e.reason)).collect();
                Err(format!("события (kind, reason): {seen:?}"))
            }
        },
    )
    .await;
    assert!(
        evs.iter().all(|e| e.object.name.starts_with("crash")),
        "{evs:?}"
    );

    let net = network::get_network(&r, NS, &c.info.api_groups).await;
    let services = net.services.data.unwrap();
    let web = services.iter().find(|s| s.name == "web").unwrap();
    assert_eq!(web.ports, ["80/TCP → 8080"]);
    assert_eq!(
        web.endpoints.map(|e| (e.ready, e.total)),
        Some((2, 2)),
        "готовые endpoints по EndpointSlices"
    );
    let ing = &net.ingresses.data.unwrap()[0];
    assert_eq!(ing.rules[0].host.as_deref(), Some("web.example.test"));
    assert_eq!(ing.rules[0].service.as_deref(), Some("web"));
    assert!(
        net.routes.is_none() && net.gateways.is_none(),
        "на kind нет Route и Gateway API"
    );

    let cfg = configs::get_config(&r, NS).await;
    // Весь ответ целиком, до того как из него заберут части.
    assert_no_secret("get_config", &serde_json::to_string(&cfg).unwrap());
    // Проверка утечки имеет смысл, только пока в кластере лежит именно это значение.
    let stored: kube::Api<Secret> = kube::Api::namespaced(c.client.clone(), NS);
    let stored = stored.get("db").await.unwrap().data.unwrap();
    assert_eq!(
        stored["password"].0,
        SECRET.as_bytes(),
        "Secret db в кластере не совпадает с fixtures.yaml"
    );
    let cm = cfg.config_maps.data.unwrap();
    assert!(
        cm.iter()
            .any(|c| c.name == "web-config" && c.keys == ["LOG_LEVEL"])
    );
    let secrets = cfg.secrets.data.unwrap();
    let db = secrets.iter().find(|s| s.name == "db").unwrap();
    assert_eq!(db.keys, ["password"]);
    let values = configs::config_map_values(&r, NS, "web-config")
        .await
        .unwrap();
    assert_eq!(values["LOG_LEVEL"].text.as_deref(), Some("debug"));

    let pvcs = storage::list_pvcs(&r, NS).await.unwrap();
    let data = pvcs.items.iter().find(|p| p.name == "data").unwrap();
    assert_eq!(
        data.phase.as_deref(),
        Some("Pending"),
        "WaitForFirstConsumer"
    );
    assert_eq!(data.requested.as_deref(), Some("1Gi"));
    assert_eq!(data.mounted_by.as_deref(), Some(&[][..]));

    let q = quotas::get_quotas(&r, NS).await;
    let compute = &q.quotas.data.unwrap()[0];
    let pods = compute
        .entries
        .iter()
        .find(|e| e.resource == "pods")
        .unwrap();
    assert_eq!(pods.hard, "20", "{pods:?}");
    assert!(pods.used.is_some() && pods.ratio.is_some(), "{pods:?}");
    let lr = &q.limit_ranges.data.unwrap()[0];
    assert!(
        lr.limits
            .iter()
            .any(|l| l.resource == "cpu" && l.default.as_deref() == Some("100m"))
    );
}

#[tokio::test]
async fn logs_of_a_crashed_and_of_a_running_container() {
    let Some(c) = connect("admin").await else {
        return;
    };
    let r = c.reader();
    // Под, у которого уже был предыдущий запуск, и работающий под logger.
    let (crash, logger) = until(
        "под crash с перезапуском и готовый под logger",
        Duration::from_secs(120),
        || async {
            let pods = list_pods(&r, NS, None).await.unwrap().items;
            let crash = by_app(&pods, "crash").into_iter().find(|p| p.restarts >= 1);
            let logger = by_app(&pods, "logger").into_iter().find(|p| p.ready == 1);
            match (crash, logger) {
                (Some(crash), Some(logger)) => Ok((crash.name.clone(), logger.name.clone())),
                (crash, logger) => Err(format!(
                    "crash с перезапуском: {}, logger готов: {}",
                    crash.is_some(),
                    logger.is_some()
                )),
            }
        },
    )
    .await;
    let params = |pod: &str, previous: bool, follow: bool, tail_lines: Option<i64>| LogParams {
        connection_id: "c".into(),
        namespace: NS.into(),
        pod: pod.into(),
        container: None,
        previous,
        tail_lines,
        since_seconds: None,
        since_time: None,
        follow,
        filter: None,
    };
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("crash.log");
    let source = KubeLogs {
        client: StreamClient::fixed(r.clone()),
        params: params(&crash, true, false, None),
    };
    // Между перезапусками (статус Error) «предыдущий» запуск kubelet уже мог убрать, и вместо лога
    // приходит одна строка «unable to retrieve container logs»: ждём, пока предыдущий запуск читается.
    // Ошибки чтения (например, «previous terminated container not found») временные: ждём дальше,
    // а по истечении времени последняя попадает в сообщение.
    until(
        "прошлый запуск crash читается (starting и boom)",
        Duration::from_secs(300),
        || async {
            let n = logs::save(&source, &Matcher::All, &path, false)
                .await
                .map_err(|e| format!("чтение лога: {e:?}"))?;
            let text = std::fs::read_to_string(&path).map_err(|e| format!("файл лога: {e}"))?;
            if n >= 2 && text.contains("starting") && text.contains("boom") {
                Ok(())
            } else {
                Err(format!("строк: {n}, текст: {text:?}"))
            }
        },
    )
    .await;

    let sink: Collected<LogEvent> = Collected::default();
    let cancel = CancellationToken::new();
    // Хвост — одна строка: остальные две из трёх приходят потоком, а не из накопленного.
    let follow = KubeLogs {
        client: StreamClient::fixed(r.clone()),
        params: params(&logger, false, true, Some(1)),
    };
    let task = tokio::spawn(logs::run(
        follow,
        true,
        Matcher::All,
        sink.clone(),
        cancel.clone(),
    ));
    let shown = until(
        "три строки follow (одна из хвоста, две потоком)",
        Duration::from_secs(20),
        || {
            let sink = sink.clone();
            async move {
                let lines: Vec<String> = sink
                    .0
                    .lock()
                    .unwrap()
                    .iter()
                    .flat_map(|e| match e {
                        LogEvent::Lines { lines, .. } => {
                            lines.iter().map(|l| l.text.clone()).collect()
                        }
                        _ => vec![],
                    })
                    .collect();
                if lines.len() >= 3 {
                    Ok(lines)
                } else {
                    Err(format!("строки: {lines:?}"))
                }
            }
        },
    )
    .await;
    assert!(!task.is_finished(), "follow завершился до отмены");
    cancel.cancel();
    finished(task).await;
    assert!(shown.iter().all(|l| l.starts_with("line ")), "{shown:?}");
    // Строка потока новее строки из хвоста.
    let number = |l: &String| -> u64 { l["line ".len()..].trim().parse().unwrap() };
    assert!(
        number(shown.last().unwrap()) > number(&shown[0]),
        "{shown:?}"
    );
}

#[tokio::test]
async fn watch_sees_a_new_pod() {
    use k8s_openapi::api::core::v1::Pod;
    let Some(c) = connect("admin").await else {
        return;
    };
    let r = c.reader();
    let sink: Collected<WatchEvent> = Collected::default();
    let cancel = CancellationToken::new();
    let task = tokio::spawn(watch::run(
        watch::sources(
            &StreamClient::fixed(r.clone()),
            NS,
            WatchKind::Pods,
            None,
            &c.info.api_groups,
        ),
        watch::poll(
            StreamClient::fixed(r.clone()),
            NS.into(),
            WatchKind::Pods,
            None,
            c.info.api_groups,
        ),
        sink.clone(),
        cancel.clone(),
    ));
    until(
        "Reset со списком подов",
        Duration::from_secs(20),
        || {
            let sink = sink.clone();
            async move {
                let events = sink.0.lock().unwrap();
                if events
                    .iter()
                    .any(|e| matches!(e, WatchEvent::Reset { items, .. } if !items.is_empty()))
                {
                    Ok(())
                } else {
                    Err(format!("события: {:?}", event_kinds(&events)))
                }
            }
        },
    )
    .await;
    let name = format!("watch-probe-{}", uuid::Uuid::new_v4().simple());
    let api: kube::Api<Pod> = kube::Api::namespaced(c.client.clone(), NS);
    let pod: Pod = serde_json::from_value(serde_json::json!({
        "metadata": {"name": name},
        "spec": {"nodeSelector": {"glafira/none": "true"}, "containers": [{"name": "x", "image": "registry.invalid/x:1"}]}
    }))
    .unwrap();
    api.create(&Default::default(), &pod).await.unwrap();
    // Ожидание может упасть: пробный под убирается в любом случае, и только потом паника идёт дальше.
    let seen = std::panic::AssertUnwindSafe(until(
        "изменение Upsert с новым подом",
        Duration::from_secs(20),
        || {
            let (sink, name) = (sink.clone(), name.clone());
            async move {
                let events = sink.0.lock().unwrap();
                let found = events.iter().any(|e| match e {
                    WatchEvent::Changes { changes } => changes.iter().any(
                        |c| matches!(c, RowChange::Upsert { row: Row::Pod(p) } if p.name == name),
                    ),
                    _ => false,
                });
                if found {
                    Ok(())
                } else {
                    Err(format!("события: {:?}", event_kinds(&events)))
                }
            }
        },
    ))
    .catch_unwind()
    .await;
    let deleted = api.delete(&name, &Default::default()).await;
    cancel.cancel();
    finished(task).await;
    if let Err(panic) = seen {
        std::panic::resume_unwind(panic);
    }
    deleted.unwrap();
    let events = sink.0.lock().unwrap();
    assert!(
        !events.iter().any(|e| matches!(e, WatchEvent::Polling)),
        "у admin есть право watch, опроса быть не должно: {:?}",
        event_kinds(&events)
    );
}

#[tokio::test]
async fn cluster_overview_and_the_rights_of_a_service_account() {
    let Some(admin) = connect("admin").await else {
        return;
    };
    let o = overview::cluster_overview(&admin.reader(), &admin.info).await;
    assert!(o.readyz.ok, "{:?}", o.readyz);
    let nodes = o.nodes.data.unwrap();
    assert_eq!((nodes.total, nodes.ready), (1, 1));
    assert_eq!(nodes.roles[0].role, "control-plane");
    assert!(o.projects.data.unwrap().count >= 5);
    assert!(o.platform_details.is_none() && o.gateway_classes.is_none());

    let dev = connect("dev-token").await.unwrap();
    let r = dev.reader();
    let o = overview::cluster_overview(&r, &dev.info).await;
    assert!(o.readyz.ok, "readyz читает любой пользователь");
    assert_eq!(o.nodes.error.unwrap().code, ErrorCode::Forbidden);
    let a = access::namespace_access(&r, NS).await.unwrap();
    assert!(a.resources["pods"].watch, "watch подов в team-a");
    assert!(a.resources["pods/log"].get, "логи подов в team-a");
    assert!(a.resources["deployments.apps"].list);
    let elsewhere = access::namespace_access(&r, "default").await.unwrap();
    assert!(
        !elsewhere.resources["pods"].list,
        "роль edit — только в team-a"
    );
    let mut profile = import(&kubeconfig_path().unwrap(), "dev-token").0;
    profile.manual_namespaces = vec![NS.into()];
    let p = projects::list_projects(&r, dev.info.api_groups.projects, &profile)
        .await
        .unwrap();
    assert_eq!(p.error.unwrap().code, ErrorCode::Forbidden);
    assert_eq!(p.items[0].name, NS);
    assert!(p.items[0].favorite && p.items[0].from_profile);
    let e = list_pods(&r, "default", None).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::Forbidden);
    assert_eq!(e.params["namespace"], "default");
}
