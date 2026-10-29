//! Источник метрик на настоящем API-сервере (kind, `dev/kind/setup.sh`, `dev/kind/metrics.yaml`):
//! автопоиск находит Service monitoring/prometheus-k8s и ходит в него через прокси API-сервера, а
//! отвечает поддельный Prometheus — HTTP-сервер этого теста на рабочей машине (порт 19090, на него
//! ведёт перенаправление стенда). По записанным ответам — статистика и рекомендация Deployment busy
//! и правила `resources.*` проверки; у dev-token прав на прокси нет — источник отвергнут.
//! Без `GO_IT_KUBECONFIG` тест пропускается.

mod common;

use std::sync::{Arc, Mutex};

use common::prometheus::{CPU_P95, MEMORY_MAX, THROTTLING, serve};
use common::{import, kubeconfig_path};
use glafira_kubernetes_lib::error::ErrorCode;
use glafira_kubernetes_lib::health::collect::{LIMITS, collect};
use glafira_kubernetes_lib::health::rules::evaluate;
use glafira_kubernetes_lib::k8s::workloads::WorkloadKind;
use glafira_kubernetes_lib::metrics::prometheus::Reason;
use glafira_kubernetes_lib::metrics::select::for_connection;
use glafira_kubernetes_lib::resources::analyze;
use glafira_kubernetes_lib::resources::recommend::{Confidence, Note};
use glafira_kubernetes_lib::resources::usage::workload_usage;
use glafira_kubernetes_lib::state::AppState;
use glafira_kubernetes_lib::storage::profile::Environment;
use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, ListParams};

const NS: &str = "team-a";

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

#[tokio::test]
async fn prometheus_behind_the_api_server_proxy() {
    let Some(path) = kubeconfig_path() else {
        return;
    };
    let pods = Arc::new(Mutex::new(Vec::new()));
    serve(&path, pods.clone()).await;

    // Администратор: автопоиск выбирает Prometheus за прокси API-сервера.
    let (profile, secrets) = import(&path, "admin");
    let id = profile.id.clone();
    let dir = tempfile::tempdir().unwrap();
    let st = AppState::new(dir.path(), secrets).unwrap();
    st.connections.upsert(profile.clone()).unwrap();
    let c = st.connected(&id, false).await.unwrap();
    let sel = for_connection(&st, &id, &c, true).await.unwrap();
    let info = sel.status.selected.clone().expect("источник выбран");
    assert_eq!(info.label(), "Prometheus monitoring/prometheus-k8s:9090");

    // Статистика и рекомендация Deployment busy за 7 дней.
    let a = analyze(
        &c.reader(),
        &sel,
        NS,
        WorkloadKind::Deployment,
        "busy",
        7,
        now(),
        Some(&c.nodes),
    )
    .await
    .unwrap();
    let app = &a.usage.containers[0];
    assert_eq!(
        (
            app.current.name.as_str(),
            app.stats.cpu.p95,
            app.stats.memory.max,
            app.stats.throttling.mean
        ),
        ("app", Some(CPU_P95), Some(MEMORY_MAX), Some(THROTTLING))
    );
    assert_eq!((a.usage.coverage, a.usage.owner_metrics), (Some(1.0), true));
    assert_eq!(a.usage.charts.as_ref().unwrap().cpu[0].points.len(), 2);
    // Запас к p95 CPU и максимуму памяти; троттлинг поднимает CPU limit.
    let r = &a.recommendation;
    let rec = r.containers[0].recommended.as_ref().unwrap();
    let mi = 1024.0 * 1024.0;
    assert_eq!(
        (
            rec.requests.cpu,
            rec.requests.memory,
            rec.limits.cpu,
            rec.limits.memory
        ),
        (Some(0.02), Some(32.0 * mi), Some(0.08), Some(48.0 * mi))
    );
    assert_eq!(r.confidence, Confidence::High);
    assert_eq!(r.containers[0].notes, [Note::CpuThrottled { p95: 0.6 }]);

    // Проверка проекта: троттлинг и память у лимита подов busy.
    let list = Api::<Pod>::namespaced(c.kube_client().clone(), NS)
        .list(&ListParams::default().labels("app=busy"))
        .await
        .unwrap();
    *pods.lock().unwrap() = list
        .items
        .into_iter()
        .filter_map(|p| p.metadata.name)
        .collect();
    let s = collect(
        &c.reader(),
        &c.info,
        &profile,
        Some(&[NS.to_string()]),
        LIMITS,
        now(),
        sel.history.as_deref(),
    )
    .await;
    let found: Vec<String> = evaluate(&s, Environment::Prod)
        .iter()
        .filter(|f| f.rule_id.starts_with("resources."))
        .map(|f| format!("{} {}", f.rule_id, f.target.name.as_deref().unwrap_or("-")))
        .collect();
    assert_eq!(
        found,
        [
            "resources.cpuThrottled busy",
            "resources.memoryNearLimit busy"
        ]
    );

    // dev-token: прокси к сервису в чужом проекте запрещён, metrics.k8s.io на kind нет.
    let (profile, secrets) = import(&path, "dev-token");
    let id = profile.id.clone();
    let dir = tempfile::tempdir().unwrap();
    let st = AppState::new(dir.path(), secrets).unwrap();
    st.connections.upsert(profile).unwrap();
    let c = st.connected(&id, false).await.unwrap();
    let sel = for_connection(&st, &id, &c, true).await.unwrap();
    assert!(sel.status.selected.is_none());
    let rejected = sel
        .status
        .rejected
        .iter()
        .find(|r| r.source.location == "monitoring/prometheus-k8s:9090")
        .expect("кандидат отвергнут");
    assert_eq!(rejected.reason, Reason::Forbidden);
    let e = workload_usage(
        &c.reader(),
        &sel,
        NS,
        WorkloadKind::Deployment,
        "busy",
        7,
        now(),
    )
    .await
    .unwrap_err();
    assert_eq!(e.code, ErrorCode::MetricsUnavailable);
}
