//! Имитация DropApp на kind (`dev/kind/dropapp/up.sh`): платформа `dropapp` с версией из
//! `ClusterMetadata`, обзор кластера с частью DropApp, правила `cluster.*` DropApp, проекты из
//! `projects.dropapp.ru` и источник метрик — vmselect VictoriaMetrics через прокси API-сервера.
//! Контекст `admin` (сертификат kind); вход через OIDC проверяет смоук — ему нужен браузер.
//! Нужен `GO_IT_DROPAPP_KUBECONFIG` (`dev/kind/dropapp/out/kubeconfig`); без него тесты
//! пропускаются. В CI не запускается: второй кластер с OIDC там не поднимается.

mod common;

use std::path::PathBuf;

use common::{import, no_cancel};
use glafira_kubernetes_lib::health::collect::{LIMITS, collect};
use glafira_kubernetes_lib::health::model::{Finding, TargetKind};
use glafira_kubernetes_lib::health::rules::evaluate;
use glafira_kubernetes_lib::k8s::dropapp::ComponentKind;
use glafira_kubernetes_lib::k8s::overview::cluster_overview;
use glafira_kubernetes_lib::k8s::pool::{Connected, open};
use glafira_kubernetes_lib::k8s::projects::list_projects;
use glafira_kubernetes_lib::metrics::prometheus::query;
use glafira_kubernetes_lib::metrics::select::for_connection;
use glafira_kubernetes_lib::platform::adapters::PlatformOverview;
use glafira_kubernetes_lib::platform::{PlatformKind, ProjectSource};
use glafira_kubernetes_lib::state::AppState;
use glafira_kubernetes_lib::storage::profile::{ConnectionProfile, Environment};

const NS: &str = "team-a";

fn kubeconfig_path() -> Option<PathBuf> {
    let p = std::env::var_os("GO_IT_DROPAPP_KUBECONFIG").map(PathBuf::from);
    if p.is_none() {
        eprintln!("GO_IT_DROPAPP_KUBECONFIG не задан — интеграционный тест DropApp пропущен");
    }
    p
}

async fn connect() -> Option<(Connected, ConnectionProfile)> {
    let path = kubeconfig_path()?;
    let (profile, secrets) = import(&path, "admin");
    let c = open(profile.clone(), secrets, no_cancel()).await.unwrap();
    Some((c, profile))
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

#[tokio::test]
async fn dropapp_platform_and_overview() {
    let Some((c, _)) = connect().await else {
        return;
    };
    assert_eq!(c.info.platform.kind, PlatformKind::Dropapp);
    assert_eq!(c.info.platform.version.as_deref(), Some("5.1.2"));
    assert_eq!(c.info.platform.label, "DropApp 5.1.2");

    let o = cluster_overview(&c.reader(), &c.info).await;
    let Some(PlatformOverview::Dropapp(d)) = o.platform_details else {
        panic!("нет части обзора DropApp: {:?}", o.platform_details);
    };
    let metadata = d.metadata.data.expect("ClusterMetadata прочитан");
    assert_eq!(
        (
            metadata.cluster_name.as_deref(),
            metadata.version.as_deref()
        ),
        (Some("glafira-dropapp"), Some("5.1.2"))
    );
    // Component: auth, cni, monitoring, ingress-controller; Tool: dex, grafana, console.
    let components = d.components.data.expect("Component и Tool прочитаны");
    assert_eq!(components.total, 7);
    let problems: Vec<_> = components
        .problems
        .iter()
        .map(|p| {
            (
                p.kind,
                p.name.as_str(),
                p.status.as_deref(),
                p.retry,
                p.event.as_deref(),
            )
        })
        .collect();
    assert_eq!(
        problems,
        [
            (
                ComponentKind::Component,
                "ingress-controller",
                Some("upgrading"),
                Some(0),
                None
            ),
            (
                ComponentKind::Tool,
                "console",
                Some("failed"),
                Some(3),
                Some("InstallFailed: helm upgrade failed: timed out waiting for the condition")
            ),
        ]
    );
    let nodes = d.nodes.data.expect("DropAppNode прочитаны");
    let node_problems: Vec<_> = nodes
        .problems
        .iter()
        .map(|n| (n.name.as_str(), n.status.as_deref()))
        .collect();
    assert_eq!(
        (nodes.total, node_problems),
        (2, vec![("worker-2", Some("provisioning"))])
    );
    let tls = d.ingress_tls.data.expect("IngressTLS прочитаны");
    let tls_problems: Vec<_> = tls
        .problems
        .iter()
        .map(|t| (t.namespace.as_deref(), t.name.as_str(), t.reason.as_deref()))
        .collect();
    assert_eq!(
        (tls.total, tls_problems),
        (
            2,
            vec![(Some("dapp-ingress"), "apps-cert", Some("IssuerNotReady"))]
        )
    );
}

/// `правило: names` у находок уровня кластера, кроме `check.partial`.
fn cluster_findings(v: &[Finding]) -> Vec<String> {
    v.iter()
        .filter(|f| f.target.kind == TargetKind::Cluster && f.rule_id != "check.partial")
        .map(|f| format!("{}: {}", f.rule_id, f.params["names"]))
        .collect()
}

#[tokio::test]
async fn dropapp_cluster_rules() {
    let Some((c, profile)) = connect().await else {
        return;
    };
    let requested = [NS.to_string()];
    let s = collect(
        &c.reader(),
        &c.info,
        &profile,
        Some(&requested),
        LIMITS,
        now(),
        None,
    )
    .await;
    let found = evaluate(&s, Environment::Prod);
    assert_eq!(
        cluster_findings(&found),
        [
            r#"cluster.componentFailed: ["Tool console"]"#,
            r#"cluster.dropappNodeNotReady: ["worker-2"]"#,
            r#"cluster.ingressTlsNotReady: ["dapp-ingress/apps-cert"]"#,
        ]
    );
    let failed = found
        .iter()
        .find(|f| f.rule_id == "cluster.componentFailed")
        .unwrap();
    assert_eq!(
        failed.params["components"][0]["event"],
        "InstallFailed: helm upgrade failed: timed out waiting for the condition"
    );
}

#[tokio::test]
async fn projects_from_projects_dropapp_ru() {
    let Some((c, profile)) = connect().await else {
        return;
    };
    let source = c.info.api_groups.project_source();
    assert_eq!(source, ProjectSource::Dropapp);
    let list = list_projects(&c.reader(), source, &profile).await.unwrap();
    let names: Vec<_> = list.items.iter().map(|p| p.name.as_str()).collect();
    assert_eq!(names, [NS]);
}

#[tokio::test]
async fn victoriametrics_vmselect_behind_the_api_server_proxy() {
    let Some(path) = kubeconfig_path() else {
        return;
    };
    let (profile, secrets) = import(&path, "admin");
    let id = profile.id.clone();
    let dir = tempfile::tempdir().unwrap();
    let st = AppState::new(dir.path(), secrets).unwrap();
    st.connections.upsert(profile).unwrap();
    let c = st.connected(&id, false).await.unwrap();
    let sel = for_connection(&st, &id, &c, true).await.unwrap();
    let info = sel.status.selected.clone().expect("источник выбран");
    assert_eq!(
        info.label(),
        "Prometheus dapp-monitoring/vmselect-victoria-metrics-k8s-stack:8481"
    );
    let source = sel.history.as_deref().expect("источник истории");
    let v = query(source, NS, "vector(1)", now()).await.unwrap();
    assert_eq!(v.iter().map(|s| s.value).collect::<Vec<_>>(), [1.0]);
}
