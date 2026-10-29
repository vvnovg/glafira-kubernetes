//! Изменения §9 на настоящем API-сервере (kind, `dev/kind/setup.sh`, объекты `dev/kind/ops.yaml`):
//! предпросмотр с server-side dry-run, применение каждого вида изменения с версией предпросмотра
//! (идущая выкладка ей не мешает), `CONFLICT` при изменённом spec, стражи профиля (только чтение, имя prod-подключения,
//! имя пода без контроллера). Подключение — `dev-token`: роли `edit` в team-a хватает.
//! Без `GO_IT_KUBECONFIG` тест пропускается.

mod common;

use common::{import, kubeconfig_path};
use glafira_kubernetes_lib::change::Change;
use glafira_kubernetes_lib::commands::changes::{apply_change_impl, preview_change_impl};
use glafira_kubernetes_lib::error::ErrorCode;
use glafira_kubernetes_lib::ops::DryRun;
use glafira_kubernetes_lib::state::AppState;
use glafira_kubernetes_lib::storage::profile::Environment;
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::autoscaling::v2::HorizontalPodAutoscaler;
use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, Patch, PatchParams, PostParams};
use serde_json::json;

const NS: &str = "team-a";

fn change(v: serde_json::Value) -> Change {
    serde_json::from_value(v).unwrap()
}

#[tokio::test]
async fn every_change_kind_previews_applies_and_conflicts() {
    let Some(path) = kubeconfig_path() else {
        return;
    };
    let (profile, secrets) = import(&path, "dev-token");
    let id = profile.id.clone();
    let dir = tempfile::tempdir().unwrap();
    let st = AppState::new(dir.path(), secrets).unwrap();
    st.connections.upsert(profile.clone()).unwrap();
    let c = st.connected(&id, false).await.unwrap();
    let deployments: Api<Deployment> = Api::namespaced(c.kube_client().clone(), NS);
    let hpas: Api<HorizontalPodAutoscaler> = Api::namespaced(c.kube_client().clone(), NS);
    let workload = json!({"kind": "Deployment", "name": "tuned"});

    // Исходное состояние `ops.yaml`: тест можно повторять.
    let reset = json!({"spec": {"replicas": 1, "template": {"spec": {"containers": [{"name": "app",
        "resources": {"requests": {"cpu": "10m", "memory": "16Mi"}, "limits": {"memory": "64Mi"}}}]}}}});
    deployments
        .patch("tuned", &PatchParams::default(), &Patch::Strategic(reset))
        .await
        .unwrap();
    let reset = json!({"spec": {"minReplicas": 1, "maxReplicas": 2}});
    hpas.patch("tuned-hpa", &PatchParams::default(), &Patch::Merge(reset))
        .await
        .unwrap();

    // Ресурсы: предпросмотр с dry-run, применение, устаревшая версия — CONFLICT.
    let resources = change(
        json!({"kind": "setResources", "namespace": NS, "workload": workload,
        "containers": [{"name": "app", "requests": {"cpu": "20m"}, "limits": {"memory": "96Mi"}}]}),
    );
    let p = preview_change_impl(&st, &id, resources.clone())
        .await
        .unwrap();
    assert!(p.version.is_some(), "setResources: версия предпросмотра");
    assert!(matches!(p.dry_run, DryRun::Passed), "{:?}", p.dry_run);
    assert_eq!(p.diff.len(), 2, "{:?}", p.diff);
    let rv = p.version.clone();
    apply_change_impl(&st, &id, resources.clone(), rv.clone(), None, None, None)
        .await
        .unwrap();
    let d = deployments.get("tuned").await.unwrap();
    let r = d.spec.unwrap().template.spec.unwrap().containers[0]
        .resources
        .clone()
        .unwrap();
    assert_eq!(r.requests.unwrap()["cpu"].0, "20m");
    assert_eq!(r.limits.unwrap()["memory"].0, "96Mi");
    let managed = d.metadata.managed_fields.unwrap_or_default();
    assert!(
        managed
            .iter()
            .any(|m| m.manager.as_deref() == Some("glafira-kubernetes")),
        "fieldManager"
    );
    let e = apply_change_impl(&st, &id, resources, rv, None, None, None)
        .await
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::Conflict, "{e:?}");

    // Реплики через подресурс scale, перезапуск — аннотацией шаблона.
    let scale =
        change(json!({"kind": "scale", "namespace": NS, "workload": workload, "replicas": 2}));
    let p = preview_change_impl(&st, &id, scale.clone()).await.unwrap();
    assert!(p.version.is_some(), "scale: версия предпросмотра");
    apply_change_impl(&st, &id, scale, p.version, None, None, None)
        .await
        .unwrap();
    assert_eq!(
        deployments
            .get("tuned")
            .await
            .unwrap()
            .spec
            .unwrap()
            .replicas,
        Some(2)
    );
    let restart = change(json!({"kind": "rolloutRestart", "namespace": NS, "workload": workload}));
    let p = preview_change_impl(&st, &id, restart.clone())
        .await
        .unwrap();
    assert!(p.version.is_some(), "rolloutRestart: версия предпросмотра");
    apply_change_impl(&st, &id, restart, p.version, None, None, None)
        .await
        .unwrap();
    let annotations = deployments
        .get("tuned")
        .await
        .unwrap()
        .spec
        .unwrap()
        .template
        .metadata
        .unwrap()
        .annotations
        .unwrap();
    assert!(annotations.contains_key("kubectl.kubernetes.io/restartedAt"));
    let back =
        change(json!({"kind": "scale", "namespace": NS, "workload": workload, "replicas": 1}));
    let p = preview_change_impl(&st, &id, back.clone()).await.unwrap();
    assert!(p.version.is_some(), "scale (назад): версия предпросмотра");
    apply_change_impl(&st, &id, back, p.version, None, None, None)
        .await
        .unwrap();

    // Границы HPA.
    let hpa = change(
        json!({"kind": "setHpaBounds", "namespace": NS, "hpa": "tuned-hpa",
        "minReplicas": 1, "maxReplicas": 3}),
    );
    let p = preview_change_impl(&st, &id, hpa.clone()).await.unwrap();
    assert!(p.version.is_some(), "setHpaBounds: версия предпросмотра");
    assert!(matches!(p.dry_run, DryRun::Passed), "{:?}", p.dry_run);
    apply_change_impl(&st, &id, hpa, p.version, None, None, None)
        .await
        .unwrap();
    assert_eq!(
        hpas.get("tuned-hpa")
            .await
            .unwrap()
            .spec
            .unwrap()
            .max_replicas,
        3
    );

    // Под без контроллера удаляется только с его именем; имя у каждого прогона своё.
    let pods: Api<Pod> = Api::namespaced(c.kube_client().clone(), NS);
    let image = deployments
        .get("tuned")
        .await
        .unwrap()
        .spec
        .unwrap()
        .template
        .spec
        .unwrap()
        .containers[0]
        .image
        .clone();
    let solo: Pod = serde_json::from_value(json!({"metadata": {"generateName": "solo-"},
        "spec": {"terminationGracePeriodSeconds": 1, "containers": [{"name": "app", "image": image,
            "imagePullPolicy": "IfNotPresent", "command": ["bash", "-c", "exec 3<> <(:); read -u 3"]}]}}))
    .unwrap();
    let solo = pods.create(&PostParams::default(), &solo).await.unwrap();
    let name = solo.metadata.name.unwrap();
    let delete = change(json!({"kind": "deletePod", "namespace": NS, "pod": name}));
    let p = preview_change_impl(&st, &id, delete.clone()).await.unwrap();
    assert!(p.version.is_some(), "deletePod: версия предпросмотра");
    assert!(p.confirm_pod_name);
    assert!(matches!(p.dry_run, DryRun::Passed), "{:?}", p.dry_run);
    let after = pods.get(&name).await.unwrap();
    assert!(
        after.metadata.deletion_timestamp.is_none(),
        "dry-run не удаляет: {:?}",
        after.metadata
    );
    let e = apply_change_impl(
        &st,
        &id,
        delete.clone(),
        p.version.clone(),
        None,
        None,
        None,
    )
    .await
    .unwrap_err();
    assert_eq!(e.code, ErrorCode::ConfirmationRequired, "{e:?}");
    apply_change_impl(&st, &id, delete, p.version, None, Some(name.clone()), None)
        .await
        .unwrap();
    let pod = pods.get_opt(&name).await.unwrap();
    assert!(
        pod.is_none_or(|p| p.metadata.deletion_timestamp.is_some()),
        "под удаляется"
    );

    // Стражи профиля — до кластера.
    let mut prod = profile.clone();
    prod.environment = Environment::Prod;
    st.connections.upsert(prod.clone()).unwrap();
    let scale =
        change(json!({"kind": "scale", "namespace": NS, "workload": workload, "replicas": 1}));
    let e = apply_change_impl(&st, &id, scale.clone(), None, None, None, None)
        .await
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::ConfirmationRequired);
    let p = preview_change_impl(&st, &id, scale.clone()).await.unwrap();
    assert!(p.version.is_some(), "scale (prod): версия предпросмотра");
    apply_change_impl(
        &st,
        &id,
        scale.clone(),
        p.version,
        Some(prod.name.clone()),
        None,
        None,
    )
    .await
    .unwrap();
    prod.read_only = true;
    st.connections.upsert(prod.clone()).unwrap();
    let e = apply_change_impl(&st, &id, scale, None, Some(prod.name.clone()), None, None)
        .await
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::ReadOnly);
}
