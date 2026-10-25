use std::path::{Path, PathBuf};

use tauri::State;

use super::blocking;
use super::connections::{ConnectionInput, SecretsInput, save_connection_blocking};
use crate::error::{AppResult, ErrorCode};
use crate::k8s::config::process_env;
use crate::k8s::kubeconfig::{self, KubeconfigPreview};
use crate::state::AppState;
use crate::storage::profile::{AuthConfig, ConnectionProfile};

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
    blocking(move || preview_blocking(&st, &resolve_path(path)?, &dirs)).await
}

/// Команды exec-плагинов ищутся в `dirs` (PATH оболочки входа и стандартные каталоги).
fn preview_blocking(st: &AppState, path: &Path, dirs: &[PathBuf]) -> AppResult<KubeconfigPreview> {
    let kc = kubeconfig::read(path)?;
    let resolve = |cmd: &str, base: Option<&Path>| {
        crate::auth::exec::resolve_command(cmd, base, dirs, &|p| p.is_file())
    };
    Ok(kubeconfig::preview(
        &kc,
        path,
        &st.connections.list(),
        &st.policy.status,
        &resolve,
    ))
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

fn apply_blocking(st: &AppState, r: ApplyRequest) -> AppResult<Vec<ConnectionProfile>> {
    let kc = kubeconfig::read(r.path)?;
    let preview = preview_blocking(st, r.path, r.dirs)?;
    let (names, folder_id) = (r.names, r.folder_id);
    let mut saved = Vec::new();
    for name in names {
        let Some(ctx) = preview.contexts.iter().find(|c| &c.name == name) else {
            return Err(app_err!(
                ErrorCode::NotFound,
                "errors.kubeconfig.noContext",
                "Context “{{name}}” not found in kubeconfig",
                name = name
            ));
        };
        let imported = kubeconfig::import_context(&kc, name, ctx)?;
        let confirm_exec =
            matches!(imported.profile.auth, AuthConfig::Exec(_)) && r.approve_exec.contains(name);
        let profile = match st.connections.get(&imported.profile.id) {
            Ok(existing) => merge(existing, imported.profile),
            Err(_) => ConnectionProfile {
                folder_id: folder_id.clone(),
                ..imported.profile
            },
        };
        // Пустая строка удаляет секрет, который этот способ входа не использует.
        let secrets = SecretsInput {
            token: Some(imported.token.unwrap_or_default()),
            client_key: Some(imported.client_key.unwrap_or_default()),
        };
        saved.push(save_connection_blocking(
            st,
            ConnectionInput {
                profile,
                secrets,
                confirm_exec,
            },
        )?);
    }
    Ok(saved)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::secrets::{MemoryStore, TOKEN, secret_key};
    use crate::storage::profile::{AuthConfig, Environment, TEST_CERT_PEM};

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
        let st = AppState::new(&dir.path().join("data"), Arc::new(MemoryStore::default())).unwrap();
        let path = write_kubeconfig(dir.path());
        let first = apply(&st, &path, &["dev-alice"], None, &[], &[]).unwrap();
        let mut local = first[0].clone();
        local.environment = Environment::Prod;
        local.read_only = true;
        local.favorite_namespaces = vec!["ops".into()];
        st.connections.upsert(local).unwrap();

        let again = apply(&st, &path, &["dev-alice"], None, &[], &[]).unwrap();
        assert_eq!(again[0].id, first[0].id);
        assert_eq!(again[0].environment, Environment::Prod);
        assert!(again[0].read_only);
        assert_eq!(again[0].favorite_namespaces, vec!["ops", "team-a"]);
        assert_eq!(st.connections.list().len(), 1);
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
        std::fs::write(bin.join("kubelogin"), "").unwrap();
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
