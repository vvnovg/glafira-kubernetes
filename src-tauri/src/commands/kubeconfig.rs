use std::path::{Path, PathBuf};

use kube::config::Kubeconfig;
use tauri::State;

use super::blocking;
use super::connections::{ConnectionInput, SecretsInput, save_connection_blocking};
use crate::auth::exec::{
    fingerprint, is_executable_file, is_secret_name, mask_secret_args, resolve_command,
};
use crate::error::{AppError, AppResult, ErrorCode};
use crate::k8s::config::process_env;
use crate::k8s::kubeconfig::{self, KubeContext, KubeconfigPreview};
use crate::state::AppState;
use crate::storage::profile::{AuthConfig, ConnectionProfile, REDACTED};

fn resolve_path(path: Option<String>) -> AppResult<PathBuf> {
    match path.filter(|p| !p.trim().is_empty()) {
        Some(p) => Ok(PathBuf::from(p)),
        None => kubeconfig::default_path(&process_env, dirs::home_dir()).ok_or_else(|| {
            app_err!(
                ErrorCode::InvalidInput,
                "errors.kubeconfig.noDefault",
                "Cannot find the default kubeconfig — choose a file"
            )
        }),
    }
}

/// Контексты kubeconfig (`path` или файл по умолчанию) для выбора в диалоге импорта.
#[tauri::command]
pub async fn preview_kubeconfig_import(
    state: State<'_, AppState>,
    path: Option<String>,
) -> AppResult<KubeconfigPreview> {
    let st = state.inner().clone();
    let dirs = crate::auth::exec::command_dirs().await;
    blocking(move || preview_for_ui(&st, &resolve_path(path)?, &dirs)).await
}

/// Предпросмотр для webview: секретные значения exec-плагина (переменные окружения и аргументы
/// вроде `--oidc-client-secret`) заменены на `***`, как в `Debug` профиля. Применение импорта
/// берёт настоящие значения из файла заново и предпросмотра не использует. Отпечаток строки
/// запуска (`launch_fingerprint`) посчитан по настоящим значениям до замены: человек
/// подтверждает показанную строку, а применение сверяет с ним строку из файла.
fn preview_for_ui(st: &AppState, path: &Path, dirs: &[PathBuf]) -> AppResult<KubeconfigPreview> {
    let mut preview = preview_blocking(st, path, dirs)?;
    for exec in preview.contexts.iter_mut().filter_map(|c| c.exec.as_mut()) {
        for env in exec.env.iter_mut().filter(|e| is_secret_name(&e.name)) {
            env.value = REDACTED.into();
        }
        exec.args = mask_secret_args(&exec.args, REDACTED);
    }
    Ok(preview)
}

/// Читает файл и строит предпросмотр.
fn preview_blocking(st: &AppState, path: &Path, dirs: &[PathBuf]) -> AppResult<KubeconfigPreview> {
    Ok(preview_from(&kubeconfig::read(path)?, path, st, dirs))
}

/// Предпросмотр уже прочитанного файла: им пользуются и диалог, и применение импорта, поэтому
/// они не расходятся. Команды exec-плагинов ищутся в `dirs` (PATH оболочки входа и стандартные
/// каталоги), годятся только исполняемые файлы.
fn preview_from(
    kc: &Kubeconfig,
    path: &Path,
    st: &AppState,
    dirs: &[PathBuf],
) -> KubeconfigPreview {
    let resolve =
        |cmd: &str, base: Option<&Path>| resolve_command(cmd, base, dirs, &is_executable_file);
    kubeconfig::preview(
        kc,
        path,
        &st.connections.list(),
        &st.policy.status,
        &resolve,
    )
}

/// Импортирует выбранные контексты: новые — в папку `folder_id`, совпавшие обновляются.
/// `approve_exec` — отпечатки строк запуска, которые человек подтвердил в диалоге
/// (`launchFingerprint` предпросмотра). Строка запуска сохраняется подтверждённой, только если
/// отпечаток именно той строки, что читается из файла сейчас, есть в `approve_exec`: имя
/// контекста ничего не подтверждает, а изменившийся после предпросмотра файл даёт
/// неподтверждённую строку, которую форма подключения потом спросит заново.
#[tauri::command]
pub async fn apply_kubeconfig_import(
    state: State<'_, AppState>,
    path: String,
    contexts: Vec<String>,
    folder_id: Option<String>,
    approve_exec: Vec<String>,
) -> AppResult<Vec<ConnectionProfile>> {
    let st = state.inner().clone();
    let dirs = crate::auth::exec::command_dirs().await;
    blocking(move || {
        let request = ApplyRequest {
            path: Path::new(&path),
            names: &contexts,
            folder_id,
            approve_exec: &approve_exec,
            dirs: &dirs,
        };
        apply_blocking(&st, request)
    })
    .await
}

/// Что импортировать: файл, контексты, папка для новых, отпечатки подтверждённых строк запуска.
struct ApplyRequest<'a> {
    path: &'a Path,
    names: &'a [String],
    folder_id: Option<String>,
    approve_exec: &'a [String],
    dirs: &'a [PathBuf],
}

/// Обновлённое подключение сохраняет своё имя, окружение, цвет, флаг «только чтение» и место
/// в дереве; из kubeconfig берутся адрес, вход, CA, прокси и проект контекста.
fn merge(existing: ConnectionProfile, imported: ConnectionProfile) -> ConnectionProfile {
    let mut favorites = existing.favorite_namespaces.clone();
    favorites.extend(imported.favorite_namespaces);
    ConnectionProfile {
        api_url: imported.api_url,
        auth: imported.auth,
        ca: imported.ca,
        tls_server_name: imported.tls_server_name,
        proxy: imported.proxy,
        favorite_namespaces: favorites,
        ..existing
    }
}

/// Имя контекста — в подробностях ошибки, чтобы было видно, какой из выбранных не импортировался.
fn in_context(e: AppError, name: &str) -> AppError {
    let details = match &e.details {
        Some(d) => format!("context {name}: {d}"),
        None => format!("context {name}"),
    };
    e.with_details(details)
}

/// Контекст в виде, готовом к сохранению; ничего не записывает.
fn prepare(
    st: &AppState,
    kc: &Kubeconfig,
    ctx: &KubeContext,
    r: &ApplyRequest,
) -> AppResult<ConnectionInput> {
    let imported = kubeconfig::import_context(kc, &ctx.name, ctx)?;
    let confirm_exec = match &imported.profile.auth {
        AuthConfig::Exec(e) => r.approve_exec.contains(&fingerprint(e)),
        _ => false,
    };
    let profile = match st.connections.get(&imported.profile.id) {
        Ok(existing) => merge(existing, imported.profile),
        Err(_) => ConnectionProfile {
            folder_id: r.folder_id.clone(),
            ..imported.profile
        },
    };
    // Пустая строка удаляет секрет, который этот способ входа не использует. Токен входа через
    // браузер в kubeconfig не приходит, а сохранённый получен ранее: его оставляем.
    let token = match profile.auth {
        AuthConfig::Browser => None,
        _ => Some(imported.token.unwrap_or_default()),
    };
    Ok(ConnectionInput {
        profile,
        secrets: SecretsInput {
            token,
            client_key: Some(imported.client_key.unwrap_or_default()),
        },
        confirm_exec,
    })
}

/// Сначала все выбранные контексты разбираются без записи: если хоть один не импортируется,
/// ошибка называет его, и не сохранено ничего. Потом они сохраняются.
fn apply_blocking(st: &AppState, r: ApplyRequest) -> AppResult<Vec<ConnectionProfile>> {
    let kc = kubeconfig::read(r.path)?;
    let preview = preview_from(&kc, r.path, st, r.dirs);
    let prepared = r
        .names
        .iter()
        .map(|name| {
            preview
                .contexts
                .iter()
                .find(|c| &c.name == name)
                .ok_or_else(|| {
                    app_err!(
                        ErrorCode::NotFound,
                        "errors.kubeconfig.noContext",
                        "Context “{{name}}” not found in kubeconfig",
                        name = name
                    )
                })
                .and_then(|ctx| prepare(st, &kc, ctx, &r))
                .map(|input| (name, input))
                .map_err(|e| in_context(e, name))
        })
        .collect::<AppResult<Vec<_>>>()?;
    prepared
        .into_iter()
        .map(|(name, input)| save_connection_blocking(st, input).map_err(|e| in_context(e, name)))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::auth::exec::write_test_file;
    use crate::secrets::{MemoryStore, TOKEN, secret_key};
    use crate::storage::profile::{
        AuthConfig, Environment, ExecAuth, TEST_CERT_PEM, sample_profile,
    };
    use crate::storage::tree::{TreeItemKind, TreeItemRef};

    fn new_state(dir: &Path) -> AppState {
        AppState::new(&dir.join("data"), Arc::new(MemoryStore::default())).unwrap()
    }

    fn token_of(st: &AppState, id: &str) -> Option<String> {
        st.secrets.get(&secret_key(id, TOKEN)).unwrap()
    }

    fn apply(
        st: &AppState,
        path: &Path,
        names: &[&str],
        folder_id: Option<String>,
        approve_exec: &[&str],
        dirs: &[PathBuf],
    ) -> AppResult<Vec<ConnectionProfile>> {
        let names: Vec<String> = names.iter().map(|s| s.to_string()).collect();
        let approve: Vec<String> = approve_exec.iter().map(|s| s.to_string()).collect();
        let request = ApplyRequest {
            path,
            names: &names,
            folder_id,
            approve_exec: &approve,
            dirs,
        };
        apply_blocking(st, request)
    }

    /// Каталог с исполняемым `kubelogin`: команда exec-плагина «установлена на этой машине».
    fn kubelogin_dirs(dir: &Path) -> Vec<PathBuf> {
        let bin = dir.join("bin");
        std::fs::create_dir(&bin).unwrap();
        write_test_file(&bin.join("kubelogin"), 0o755);
        vec![bin]
    }

    /// Строка запуска сохранённого подключения подтверждена человеком.
    fn approved(p: &ConnectionProfile) -> bool {
        match &p.auth {
            AuthConfig::Exec(e) => crate::auth::exec::is_approved(e),
            _ => false,
        }
    }

    /// Отпечаток строки запуска контекста, как его показал предпросмотр для webview.
    fn shown_fingerprint(st: &AppState, path: &Path, dirs: &[PathBuf], name: &str) -> String {
        let preview = preview_for_ui(st, path, dirs).unwrap();
        let ctx = preview.contexts.iter().find(|c| c.name == name).unwrap();
        ctx.launch_fingerprint.clone().unwrap()
    }

    fn write_kubeconfig(dir: &Path) -> PathBuf {
        use base64::Engine;
        let ca = base64::engine::general_purpose::STANDARD.encode(TEST_CERT_PEM);
        let path = dir.join("config");
        std::fs::write(
            &path,
            format!(
                r#"
clusters:
- name: dev
  cluster: {{server: "https://api.dev:6443", certificate-authority-data: "{ca}"}}
users:
- name: alice
  user: {{token: sha256~alice}}
- name: oidc
  user:
    exec:
      apiVersion: client.authentication.k8s.io/v1
      command: kubelogin
      args: [get-token, --oidc-issuer-url=https://sso.corp]
contexts:
- name: dev-alice
  context: {{cluster: dev, user: alice, namespace: team-a}}
- name: dev-browser
  context: {{cluster: dev}}
- name: dev-oidc
  context: {{cluster: dev, user: oidc}}
"#
            ),
        )
        .unwrap();
        path
    }

    #[test]
    fn import_creates_profiles_in_folder_and_stores_tokens() {
        let dir = tempfile::tempdir().unwrap();
        let st = AppState::new(&dir.path().join("data"), Arc::new(MemoryStore::default())).unwrap();
        let folder = st.connections.create_folder("Kubeconfig", None).unwrap();
        let path = write_kubeconfig(dir.path());
        let p = preview_blocking(&st, &path, &[]).unwrap();
        assert_eq!(p.contexts.len(), 3);
        let saved = apply(
            &st,
            &path,
            &["dev-alice", "dev-browser"],
            Some(folder.id.clone()),
            &[],
            &[],
        )
        .unwrap();
        assert_eq!(saved.len(), 2);
        let alice = &saved[0];
        assert_eq!(alice.folder_id.as_deref(), Some(folder.id.as_str()));
        assert_eq!(
            st.secrets
                .get(&secret_key(&alice.id, TOKEN))
                .unwrap()
                .as_deref(),
            Some("sha256~alice")
        );
        assert_eq!(saved[1].auth, AuthConfig::Browser);
        assert_eq!(
            st.secrets.get(&secret_key(&saved[1].id, TOKEN)).unwrap(),
            None
        );
    }

    #[test]
    fn reimport_updates_existing_profile_keeping_local_choices() {
        let dir = tempfile::tempdir().unwrap();
        let st = new_state(dir.path());
        let path = write_kubeconfig(dir.path());
        let first = apply(&st, &path, &["dev-alice"], None, &[], &[]).unwrap();
        assert_eq!(first[0].favorite_namespaces, vec!["team-a"]);
        // Своё место в дереве: папка и второе по счёту место рядом с соседом.
        let kept = st.connections.create_folder("Kept", None).unwrap();
        let target = st.connections.create_folder("Target", None).unwrap();
        st.connections.upsert(sample_profile("neighbour")).unwrap();
        let conn = |id: &str| TreeItemRef {
            kind: TreeItemKind::Connection,
            id: id.into(),
        };
        st.connections
            .move_items(
                &[conn("neighbour"), conn(&first[0].id)],
                Some(&kept.id),
                None,
            )
            .unwrap();
        let mut local = st.connections.get(&first[0].id).unwrap();
        local.environment = Environment::Prod;
        local.read_only = true;
        local.color = "#ff0000".into();
        // Проект контекста уже в избранном: после импорта он не должен повториться.
        local.favorite_namespaces = vec!["ops".into(), "team-a".into()];
        st.connections.upsert(local).unwrap();
        let before = st.connections.get(&first[0].id).unwrap();
        assert_eq!(before.order, 1, "подготовка: профиль второй в папке");

        let again = apply(&st, &path, &["dev-alice"], Some(target.id), &[], &[]).unwrap();
        let again = &again[0];
        assert_eq!(again.id, first[0].id);
        assert_eq!(again.environment, Environment::Prod);
        assert!(again.read_only);
        assert_eq!(again.color, "#ff0000");
        // Совпадение ищется по имени и адресу (спец. §5.5), поэтому имя остаётся именем контекста.
        assert_eq!(again.name, "dev-alice");
        assert_eq!(
            again.folder_id.as_deref(),
            Some(kept.id.as_str()),
            "папка из диалога — только для новых подключений"
        );
        assert_eq!(again.order, 1);
        assert_eq!(again.favorite_namespaces, vec!["ops", "team-a"]);
        assert_eq!(st.connections.list().len(), 2);
    }

    #[test]
    fn reimport_of_browser_context_keeps_the_saved_token() {
        let dir = tempfile::tempdir().unwrap();
        let st = new_state(dir.path());
        let path = write_kubeconfig(dir.path());
        let first = apply(&st, &path, &["dev-browser", "dev-alice"], None, &[], &[]).unwrap();
        let (browser, alice) = (&first[0], &first[1]);
        // Токен браузерного входа получен позже; токен из файла в keychain устарел.
        st.secrets
            .set(&secret_key(&browser.id, TOKEN), "sha256~oauth")
            .unwrap();
        st.secrets
            .set(&secret_key(&alice.id, TOKEN), "sha256~stale")
            .unwrap();
        let again = apply(&st, &path, &["dev-browser", "dev-alice"], None, &[], &[]).unwrap();
        assert_eq!(again[0].id, browser.id);
        assert_eq!(token_of(&st, &browser.id).as_deref(), Some("sha256~oauth"));
        assert_eq!(
            token_of(&st, &alice.id).as_deref(),
            Some("sha256~alice"),
            "токен из файла перезаписывается"
        );
    }

    #[test]
    fn one_bad_context_saves_nothing_and_is_named() {
        let dir = tempfile::tempdir().unwrap();
        let st = new_state(dir.path());
        let path = write_kubeconfig(dir.path());
        // Хорошие контексты идут до плохого: до исправления они уже были бы сохранены.
        // `dev-oidc` не импортируется — команды `kubelogin` на этой машине нет.
        let names = ["dev-alice", "dev-browser", "dev-oidc"];
        let e = apply(&st, &path, &names, None, &[], &[]).unwrap_err();
        assert_eq!(e.key, "errors.kubeconfig.execNotFound");
        assert!(e.details.as_deref().unwrap_or("").contains("dev-oidc"));
        assert!(st.connections.list().is_empty(), "ничего не сохранено");
        let e = apply(&st, &path, &["dev-alice", "nope"], None, &[], &[]).unwrap_err();
        assert_eq!(e.key, "errors.kubeconfig.noContext");
        assert!(e.details.as_deref().unwrap_or("").contains("nope"));
        assert!(st.connections.list().is_empty());
    }

    #[test]
    fn preview_for_the_webview_masks_exec_secrets_but_import_keeps_them() {
        let dir = tempfile::tempdir().unwrap();
        let st = new_state(dir.path());
        let path = dir.path().join("config");
        std::fs::write(
            &path,
            r#"
clusters:
- name: dev
  cluster: {server: "https://api.dev:6443", insecure-skip-tls-verify: true}
users:
- name: oidc
  user:
    exec:
      apiVersion: client.authentication.k8s.io/v1
      command: kubelogin
      args: [get-token, --oidc-client-secret=arg-s3cr3t, --oidc-issuer-url=https://sso.corp]
      env:
      - {name: OIDC_CLIENT_SECRET, value: env-s3cr3t}
      - {name: TEAM, value: ops}
contexts:
- name: dev-oidc
  context: {cluster: dev, user: oidc}
"#,
        )
        .unwrap();
        let dirs = kubelogin_dirs(dir.path());

        let preview = preview_for_ui(&st, &path, &dirs).unwrap();
        let exec = preview.contexts[0].exec.as_ref().unwrap();
        assert_eq!(
            exec.args,
            [
                "get-token",
                "--oidc-client-secret=***",
                "--oidc-issuer-url=https://sso.corp"
            ]
        );
        let env: Vec<_> = exec.env.iter().map(|e| (&*e.name, &*e.value)).collect();
        assert_eq!(env, [("OIDC_CLIENT_SECRET", "***"), ("TEAM", "ops")]);
        let json = serde_json::to_string(&preview).unwrap();
        assert!(!json.contains("s3cr3t"), "{json}");
        assert!(json.contains("launchFingerprint"), "{json}");

        // Отпечаток посчитан по настоящей строке, а не по скрытой в предпросмотре.
        let shown = preview.contexts[0].launch_fingerprint.clone().unwrap();
        assert_ne!(shown, fingerprint(exec), "по скрытым значениям");

        // Применение берёт настоящие значения из файла, а не из предпросмотра, и принимает
        // отпечаток из предпросмотра.
        let saved = apply(&st, &path, &["dev-oidc"], None, &[shown.as_str()], &dirs).unwrap();
        let AuthConfig::Exec(e) = &saved[0].auth else {
            panic!("exec-плагин")
        };
        assert_eq!(e.args[1], "--oidc-client-secret=arg-s3cr3t");
        assert_eq!(e.env[0].value, "env-s3cr3t");
        assert_eq!(fingerprint(e), shown);
        assert!(
            approved(&saved[0]),
            "отпечаток предпросмотра подтверждает строку"
        );
    }

    #[cfg(unix)]
    #[test]
    fn non_executable_command_is_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let st = new_state(dir.path());
        let path = write_kubeconfig(dir.path());
        let bin = dir.path().join("bin");
        std::fs::create_dir(&bin).unwrap();
        write_test_file(&bin.join("kubelogin"), 0o644);
        let dirs = [bin];
        let p = preview_blocking(&st, &path, &dirs).unwrap();
        let oidc = p.contexts.iter().find(|c| c.name == "dev-oidc").unwrap();
        assert!(oidc.exec.is_none());
        assert_eq!(
            oidc.unsupported.as_ref().unwrap().key,
            "errors.kubeconfig.execNotFound"
        );
    }

    #[test]
    fn unknown_context_is_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let st = AppState::new(&dir.path().join("data"), Arc::new(MemoryStore::default())).unwrap();
        let path = write_kubeconfig(dir.path());
        let e = apply(&st, &path, &["nope"], None, &[], &[]).unwrap_err();
        assert_eq!(e.key, "errors.kubeconfig.noContext");
    }

    #[test]
    fn exec_context_is_approved_only_when_confirmed() {
        let dir = tempfile::tempdir().unwrap();
        let st = AppState::new(&dir.path().join("data"), Arc::new(MemoryStore::default())).unwrap();
        let path = write_kubeconfig(dir.path());
        let dirs = kubelogin_dirs(dir.path());
        let p = preview_for_ui(&st, &path, &dirs).unwrap();
        let oidc = p.contexts.iter().find(|c| c.name == "dev-oidc").unwrap();
        assert_eq!(
            oidc.exec.as_ref().unwrap().command,
            dirs[0].join("kubelogin").display().to_string()
        );
        let fp = oidc.launch_fingerprint.clone().unwrap();
        let alice = p.contexts.iter().find(|c| c.name == "dev-alice").unwrap();
        assert_eq!(
            alice.launch_fingerprint, None,
            "без exec-плагина отпечатка нет"
        );
        assert!(
            !serde_json::to_string(alice)
                .unwrap()
                .contains("launchFingerprint")
        );

        let saved = apply(&st, &path, &["dev-oidc"], None, &[], &dirs).unwrap();
        assert!(!approved(&saved[0]), "без согласия в диалоге");
        let saved = apply(&st, &path, &["dev-oidc"], None, &[fp.as_str()], &dirs).unwrap();
        assert!(approved(&saved[0]));
        // Без команды на машине контекст не импортируется.
        let e = apply(&st, &path, &["dev-oidc"], None, &[fp.as_str()], &[]).unwrap_err();
        assert_eq!(e.key, "errors.kubeconfig.execNotFound");
    }

    #[test]
    fn context_name_approves_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let st = new_state(dir.path());
        let path = write_kubeconfig(dir.path());
        let dirs = kubelogin_dirs(dir.path());
        let saved = apply(&st, &path, &["dev-oidc"], None, &["dev-oidc"], &dirs).unwrap();
        assert!(!approved(&saved[0]), "имя контекста — не подтверждение");
        // Отпечаток чужой строки запуска тоже ничего не подтверждает.
        let other = fingerprint(&ExecAuth {
            command: "/bin/kubelogin".into(),
            args: Vec::new(),
            env: Vec::new(),
            api_version: crate::storage::profile::EXEC_API_V1.into(),
            provide_cluster_info: false,
            approved: None,
        });
        let saved = apply(&st, &path, &["dev-oidc"], None, &[other.as_str()], &dirs).unwrap();
        assert!(!approved(&saved[0]));
    }

    #[test]
    fn file_changed_after_preview_saves_exec_unconfirmed() {
        let dir = tempfile::tempdir().unwrap();
        let st = new_state(dir.path());
        let path = write_kubeconfig(dir.path());
        let dirs = kubelogin_dirs(dir.path());
        let shown = shown_fingerprint(&st, &path, &dirs, "dev-oidc");

        // Между предпросмотром и применением файл изменился: другие аргументы.
        let text = std::fs::read_to_string(&path).unwrap();
        let changed = text.replace("sso.corp", "evil.example");
        assert_ne!(text, changed);
        std::fs::write(&path, changed).unwrap();

        let saved = apply(&st, &path, &["dev-oidc"], None, &[shown.as_str()], &dirs).unwrap();
        let AuthConfig::Exec(e) = &saved[0].auth else {
            panic!("exec-плагин")
        };
        assert!(e.args[1].contains("evil.example"), "строка взята из файла");
        assert!(
            !approved(&saved[0]),
            "показанную строку подтвердили, а не эту"
        );
        // Новая строка подтверждается её собственным отпечатком.
        let now = shown_fingerprint(&st, &path, &dirs, "dev-oidc");
        assert_ne!(now, shown);
        let saved = apply(&st, &path, &["dev-oidc"], None, &[now.as_str()], &dirs).unwrap();
        assert!(approved(&saved[0]));
    }

    #[test]
    fn reimport_over_a_confirmed_connection_drops_the_stale_approval() {
        let dir = tempfile::tempdir().unwrap();
        let st = new_state(dir.path());
        let path = write_kubeconfig(dir.path());
        let dirs = kubelogin_dirs(dir.path());
        let shown = shown_fingerprint(&st, &path, &dirs, "dev-oidc");
        let saved = apply(&st, &path, &["dev-oidc"], None, &[shown.as_str()], &dirs).unwrap();
        let AuthConfig::Exec(e) = &saved[0].auth else {
            panic!("exec-плагин")
        };
        assert_eq!(e.approved.as_deref(), Some(shown.as_str()));
        // Файл не менялся, согласия в диалоге нет: подтверждение прежней строки остаётся.
        let saved = apply(&st, &path, &["dev-oidc"], None, &[], &dirs).unwrap();
        assert!(approved(&saved[0]));
        // Между предпросмотром и применением файл изменился: прежнее подтверждение не
        // относится к новой строке и в профиле не остаётся.
        let text = std::fs::read_to_string(&path).unwrap();
        std::fs::write(&path, text.replace("sso.corp", "evil.example")).unwrap();
        let saved = apply(&st, &path, &["dev-oidc"], None, &[shown.as_str()], &dirs).unwrap();
        let AuthConfig::Exec(e) = &saved[0].auth else {
            panic!("exec-плагин")
        };
        assert!(e.args[1].contains("evil.example"));
        assert_eq!(e.approved, None);
        let AuthConfig::Exec(stored) = &st.connections.get(&saved[0].id).unwrap().auth else {
            panic!("exec-плагин")
        };
        assert_eq!(stored.approved, None);
    }
}
