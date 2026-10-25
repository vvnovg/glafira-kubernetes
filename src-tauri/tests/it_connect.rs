//! Интеграция с настоящим API-сервером (kind, `dev/kind/setup.sh`). Нужен `GO_IT_KUBECONFIG` —
//! kubeconfig с контекстами `admin` и `dev-token`; без переменной тесты пропускаются.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use glafira_kubernetes_lib::error::ErrorCode;
use glafira_kubernetes_lib::k8s::cluster::can_i;
use glafira_kubernetes_lib::k8s::{connectivity, kubeconfig, pool};
use glafira_kubernetes_lib::platform::PlatformKind;
use glafira_kubernetes_lib::policy::PolicyStatus;
use glafira_kubernetes_lib::secrets::{CLIENT_KEY, MemoryStore, SecretStore, TOKEN, secret_key};
use glafira_kubernetes_lib::storage::profile::ConnectionProfile;
use tokio::sync::oneshot;

/// Сигнал отмены, который не срабатывает (отправитель живёт до конца теста).
fn no_cancel() -> oneshot::Receiver<()> {
    let (tx, rx) = oneshot::channel();
    std::mem::forget(tx);
    rx
}

async fn test(p: &ConnectionProfile, secrets: Arc<MemoryStore>) -> connectivity::TestReport {
    connectivity::test_connection(p, secrets, &PolicyStatus::default(), no_cancel()).await
}

fn kubeconfig_path() -> Option<PathBuf> {
    let p = std::env::var_os("GO_IT_KUBECONFIG").map(PathBuf::from);
    if p.is_none() {
        eprintln!("GO_IT_KUBECONFIG не задан — интеграционный тест пропущен");
    }
    p
}

/// Подключение из контекста kubeconfig — так же, как импорт в приложении.
fn import(path: &Path, context: &str) -> (ConnectionProfile, Arc<MemoryStore>) {
    let kc = kubeconfig::read(path).unwrap();
    let preview = kubeconfig::preview(&kc, path, &[], &PolicyStatus::default(), &|_, _| None);
    let ctx = preview
        .contexts
        .iter()
        .find(|c| c.name == context)
        .unwrap_or_else(|| panic!("нет контекста {context}"));
    let imported = kubeconfig::import_context(&kc, context, ctx).unwrap();
    let secrets = Arc::new(MemoryStore::default());
    if let Some(t) = &imported.token {
        secrets
            .set(&secret_key(&imported.profile.id, TOKEN), t)
            .unwrap();
    }
    if let Some(k) = &imported.client_key {
        secrets
            .set(&secret_key(&imported.profile.id, CLIENT_KEY), k)
            .unwrap();
    }
    (imported.profile, secrets)
}

fn steps(r: &connectivity::TestReport) -> Vec<(&str, bool)> {
    r.steps.iter().map(|s| (s.name, s.ok)).collect()
}

#[tokio::test]
async fn admin_certificate_passes_every_step() {
    let Some(path) = kubeconfig_path() else {
        return;
    };
    let (profile, secrets) = import(&path, "admin");
    let r = test(&profile, secrets.clone()).await;
    assert!(r.ok, "{:#?}", r.steps);
    assert_eq!(
        steps(&r),
        [
            ("config", true),
            ("tcp", true),
            ("tls", true),
            ("auth", true),
            ("discovery", true),
            ("projects", true)
        ]
    );
    assert_eq!(
        r.steps[3].message.key, "messages.connectionTest.authOkGroups",
        "у администратора kind есть группы"
    );
    assert!(r.steps[3].message.params["groups"].contains("system:authenticated"));
    assert_eq!(r.steps[4].message.key, "messages.connectionTest.discovery");
    assert!(r.steps[4].message.params["platform"].starts_with("Kubernetes 1."));
    let c = pool::open(profile, secrets, no_cancel()).await.unwrap();
    assert_eq!(c.info.platform.kind, PlatformKind::Kubernetes);
    assert_eq!(
        c.info.platform.distribution, None,
        "kind — без дистрибутива"
    );
    assert!(!c.info.api_groups.routes && !c.info.api_groups.projects);
    assert!(
        c.info.capabilities.list_nodes,
        "у администратора kind есть права на ноды"
    );
    assert!(c.info.server_version.starts_with('v'));
}

#[tokio::test]
async fn service_account_sees_only_its_project() {
    let Some(path) = kubeconfig_path() else {
        return;
    };
    let (mut profile, secrets) = import(&path, "dev-token");
    assert_eq!(profile.favorite_namespaces, vec!["team-a"]);
    let r = test(&profile, secrets.clone()).await;
    assert_eq!(r.steps.last().unwrap().name, "projects");
    assert!(
        !r.ok,
        "список namespace учётной записи недоступен: {:#?}",
        r.steps
    );
    assert_eq!(
        r.steps.last().unwrap().error.as_ref().unwrap().code,
        ErrorCode::Forbidden
    );
    profile.manual_namespaces = vec!["team-a".into()];
    let r = test(&profile, secrets.clone()).await;
    assert!(r.ok, "{:#?}", r.steps);
    let c = pool::open(profile, secrets, no_cancel()).await.unwrap();
    assert_eq!(c.info.user, "system:serviceaccount:team-a:glafira-dev");
    assert!(!c.info.capabilities.list_nodes);
    assert!(!c.info.capabilities.list_projects);
    // Роль edit есть только в team-a: права проверяются и в обе стороны.
    let limit = std::time::Duration::from_secs(10);
    let create =
        |ns: &'static str| can_i(&c.client, limit, "create", "apps", "deployments", Some(ns));
    assert!(create("team-a").await, "роль edit в team-a");
    assert!(!create("default").await, "в default прав нет");
}

#[tokio::test]
async fn wrong_token_fails_the_sign_in_step() {
    let Some(path) = kubeconfig_path() else {
        return;
    };
    let (profile, secrets) = import(&path, "dev-token");
    secrets
        .set(&secret_key(&profile.id, TOKEN), "not-a-token")
        .unwrap();
    let r = test(&profile, secrets.clone()).await;
    assert_eq!(steps(&r).last().copied(), Some(("auth", false)));
    assert_eq!(
        r.steps.last().unwrap().error.as_ref().unwrap().code,
        ErrorCode::SessionExpired
    );
    let e = pool::open(profile, secrets, no_cancel())
        .await
        .err()
        .unwrap();
    assert_eq!(e.code, ErrorCode::SessionExpired);
}

/// Exec-плагин против настоящего API-сервера: скрипт с именем `kubectl` (`oidc-login get-token`)
/// выдаёт токен сервисной учётной записи.
#[cfg(unix)]
#[tokio::test]
async fn exec_plugin_token_signs_in() {
    use glafira_kubernetes_lib::auth::exec::fingerprint;
    use glafira_kubernetes_lib::storage::profile::{AuthConfig, ExecAuth, ExecEnv};
    use std::os::unix::fs::PermissionsExt;
    let Some(path) = kubeconfig_path() else {
        return;
    };
    let (mut profile, secrets) = import(&path, "dev-token");
    let token = secrets
        .get(&secret_key(&profile.id, TOKEN))
        .unwrap()
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let plugin = dir.path().join("kubectl");
    std::fs::write(
        &plugin,
        "#!/bin/sh\nprintf '{\"kind\":\"ExecCredential\",\"status\":{\"token\":\"%s\"}}' \"$SA_TOKEN\"\n",
    )
    .unwrap();
    std::fs::set_permissions(&plugin, std::fs::Permissions::from_mode(0o755)).unwrap();
    let mut exec = ExecAuth {
        command: plugin.display().to_string(),
        args: vec!["oidc-login".into(), "get-token".into()],
        env: vec![ExecEnv {
            name: "SA_TOKEN".into(),
            value: token,
        }],
        api_version: "client.authentication.k8s.io/v1".into(),
        provide_cluster_info: false,
        approved: None,
    };
    exec.approved = Some(fingerprint(&exec));
    profile.auth = AuthConfig::Exec(exec);
    profile.manual_namespaces = vec!["team-a".into()];
    let secrets = Arc::new(MemoryStore::default());
    let r = test(&profile, secrets.clone()).await;
    assert!(r.ok, "{:#?}", r.steps);
    assert_eq!(steps(&r)[2], ("exec", true));
    let c = pool::open(profile, secrets, no_cancel()).await.unwrap();
    assert_eq!(c.info.user, "system:serviceaccount:team-a:glafira-dev");
}
