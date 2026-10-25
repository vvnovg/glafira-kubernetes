use std::path::{Path, PathBuf};

use kube::config::Kubeconfig;
use tauri::State;

use super::blocking;
use super::connections::{ConnectionInput, SecretsInput, save_connection_blocking};
use crate::auth::exec::{is_executable_file, is_secret_name, mask_secret_args, resolve_command};
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
/// берёт настоящие значения из файла заново и предпросмотра не использует.
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
/// Строка запуска exec-плагина подтверждена, только если контекст есть в `approve_exec`.
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

/// Что импортировать: файл, контексты, папка для новых, подтверждённые exec-плагины.
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
    let confirm_exec =
        matches!(imported.profile.auth, AuthConfig::Exec(_)) && r.approve_exec.contains(&ctx.name);
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
    use crate::storage::profile::{AuthConfig, Environment, TEST_CERT_PEM, sample_profile};
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
        let e = apply(&st, &path, &names, None, &["dev-oidc"], &[]).unwrap_err();
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
        let bin = dir.path().join("bin");
        std::fs::create_dir(&bin).unwrap();
        write_test_file(&bin.join("kubelogin"), 0o755);
        let dirs = [bin];

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

        // Применение берёт настоящие значения из файла, а не из предпросмотра.
        let saved = apply(&st, &path, &["dev-oidc"], None, &["dev-oidc"], &dirs).unwrap();
        let AuthConfig::Exec(e) = &saved[0].auth else {
            panic!("exec-плагин")
        };
        assert_eq!(e.args[1], "--oidc-client-secret=arg-s3cr3t");
        assert_eq!(e.env[0].value, "env-s3cr3t");
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
        let bin = dir.path().join("bin");
        std::fs::create_dir(&bin).unwrap();
        write_test_file(&bin.join("kubelogin"), 0o755);
        let dirs = [bin.clone()];
        let p = preview_blocking(&st, &path, &dirs).unwrap();
        let oidc = p.contexts.iter().find(|c| c.name == "dev-oidc").unwrap();
        assert_eq!(
            oidc.exec.as_ref().unwrap().command,
            bin.join("kubelogin").display().to_string()
        );
        let approved = |p: &ConnectionProfile| match &p.auth {
            AuthConfig::Exec(e) => crate::auth::exec::is_approved(e),
            _ => false,
        };
        let saved = apply(&st, &path, &["dev-oidc"], None, &[], &dirs).unwrap();
        assert!(!approved(&saved[0]), "без согласия в диалоге");
        let saved = apply(&st, &path, &["dev-oidc"], None, &["dev-oidc"], &dirs).unwrap();
        assert!(approved(&saved[0]));
        // Без команды на машине контекст не импортируется.
        let e = apply(&st, &path, &["dev-oidc"], None, &["dev-oidc"], &[]).unwrap_err();
        assert_eq!(e.key, "errors.kubeconfig.execNotFound");
    }
}
