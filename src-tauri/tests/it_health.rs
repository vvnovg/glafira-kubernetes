//! Проверка кластера на настоящем API-сервере (kind, `dev/kind/setup.sh`): снимок проекта team-a
//! и правила §8.2 — проблемные поды, сеть, Gateway API, HPA, квота, частичная проверка без прав.
//! Без `GO_IT_KUBECONFIG` тесты пропускаются.

mod common;

use std::time::Duration;

use common::{import, kubeconfig_path, no_cancel};
use glafira_kubernetes_lib::health::collect::{LIMITS, collect};
use glafira_kubernetes_lib::health::model::Finding;
use glafira_kubernetes_lib::health::rules::evaluate;
use glafira_kubernetes_lib::health::snapshot::ClusterSnapshot;
use glafira_kubernetes_lib::k8s::pool::{Connected, open};
use glafira_kubernetes_lib::storage::profile::Environment;

const NS: &str = "team-a";
/// Значение ключа `password` Secret `db` из `dev/kind/fixtures.yaml`: в снимок оно не попадает.
const SECRET: &str = "s3cr3t-value";

async fn connect(
    context: &str,
) -> Option<(
    Connected,
    glafira_kubernetes_lib::storage::profile::ConnectionProfile,
)> {
    let path = kubeconfig_path()?;
    let (profile, secrets) = import(&path, context);
    let c = open(profile.clone(), secrets, no_cancel()).await.unwrap();
    Some((c, profile))
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

/// `правило объект`: `pod.crashLoop crash`, `service.noEndpoints orphan`.
fn ids(v: &[Finding]) -> Vec<String> {
    v.iter()
        .map(|f| format!("{} {}", f.rule_id, f.target.name.as_deref().unwrap_or("-")))
        .collect()
}

/// Снимок и находки на prod, пока среди них не появятся все `wanted` (не дольше `limit`):
/// поды стенда приходят к своему состоянию не сразу (kubelet, контроллер HPA, квоты).
async fn until_found(
    c: &Connected,
    profile: &glafira_kubernetes_lib::storage::profile::ConnectionProfile,
    wanted: &[&str],
    limit: Duration,
) -> (ClusterSnapshot, Vec<Finding>) {
    let requested = [NS.to_string()];
    let mut last = Vec::new();
    let wait = async {
        loop {
            let s = collect(
                &c.reader(),
                &c.info,
                profile,
                Some(&requested),
                LIMITS,
                now(),
            )
            .await;
            let found = evaluate(&s, Environment::Prod);
            let have = ids(&found);
            if wanted.iter().all(|w| have.iter().any(|h| h == w)) {
                return (s, found);
            }
            last = have;
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    };
    match tokio::time::timeout(limit, wait).await {
        Ok(v) => v,
        Err(_) => panic!("не дождались находок {wanted:?} за {limit:?}; последние: {last:?}"),
    }
}

#[tokio::test]
async fn rules_find_the_problems_of_the_stand() {
    let Some((c, profile)) = connect("admin").await else {
        return;
    };
    let wanted = [
        "pod.crashLoop crash",
        "pod.imagePull pull",
        "service.noEndpoints orphan",
        "ingress.classMissing broken",
        "ingress.classMissing web",
        "ingress.backendMissing broken",
        "route.notAdmitted web",
        "gatewayRoute.refsUnresolved api",
        "hpa.unableToScale logger",
        "quota.nearLimit objects",
        "workload.singleReplica logger",
        "container.noReadinessProbe web",
    ];
    // Цикл перезапусков — с первого рестарта (и между ними, пока kubelet пишет Error); контроллер HPA
    // отмечает отсутствие метрик за 15–30 с.
    let (s, found) = until_found(&c, &profile, &wanted, Duration::from_secs(600)).await;
    let have = ids(&found);
    // requests и limits по умолчанию LimitRange стенда даёт только подам, созданным после него:
    // создаёт ли контроллер поды раньше — гонка, так что `container.noRequests` здесь не проверяется.
    for absent in [
        "container.latestTag",
        "cluster.apiNotReady",
        "node.notReady",
        "check.partial",
    ] {
        assert!(
            !have.iter().any(|h| h.starts_with(absent)),
            "{absent}: {have:?}"
        );
    }
    let by = |rule: &str, name: &str| {
        found
            .iter()
            .find(|f| f.rule_id == rule && f.target.name.as_deref() == Some(name))
            .unwrap()
    };
    let crash = by("pod.crashLoop", "crash");
    assert_eq!(crash.target.namespace.as_deref(), Some(NS));
    assert_eq!(crash.params["containers"], serde_json::json!(["crash"]));
    assert_eq!(
        by("ingress.backendMissing", "broken").params["services"],
        serde_json::json!(["missing-svc"])
    );
    assert_eq!(
        by("service.noEndpoints", "orphan").params["referencedBy"],
        serde_json::json!(["Ingress/broken"])
    );
    assert_eq!(
        by("gatewayRoute.refsUnresolved", "api").params["reason"],
        "BackendNotFound"
    );
    assert_eq!(
        by("workload.singleReplica", "logger").params["hpa"],
        "logger"
    );
    assert_eq!(
        by("hpa.unableToScale", "logger").params["condition"],
        "ScalingActive",
        "{:?}",
        by("hpa.unableToScale", "logger").params
    );
    let project = &s.projects[0];
    let secrets = project.secrets.as_ref().unwrap();
    assert!(secrets.contains("db"), "{secrets:?}");
    assert!(
        !format!("{s:?}").contains(SECRET),
        "значение Secret попало в снимок"
    );
    assert!(s.nodes.as_ref().is_some_and(|n| !n.is_empty()));
    assert!(
        s.node_requests.is_some(),
        "администратор читает поды всех проектов"
    );
}

#[tokio::test]
async fn namespace_user_gets_a_partial_check() {
    let Some((c, profile)) = connect("dev-token").await else {
        return;
    };
    let (s, found) = until_found(
        &c,
        &profile,
        &["pod.imagePull pull", "check.partial -"],
        Duration::from_secs(300),
    )
    .await;
    let gaps: Vec<String> = s
        .coverage
        .gaps
        .iter()
        .map(|g| format!("{} {}", g.part, g.code))
        .collect();
    for g in [
        "nodes FORBIDDEN",
        "pods (all projects) FORBIDDEN",
        "ingressclasses FORBIDDEN",
    ] {
        assert!(gaps.iter().any(|x| x == g), "{g}: {gaps:?}");
    }
    let have = ids(&found);
    assert!(
        !have.iter().any(|h| h.starts_with("ingress.classMissing")),
        "классы не прочитаны — не судим: {have:?}"
    );
    assert!(s.nodes.is_none() && s.node_requests.is_none());
    assert!(
        s.readyz.as_ref().is_some_and(|r| r.ok),
        "/readyz читает любой пользователь"
    );
}
