use std::path::PathBuf;

use tauri::State;
use tauri_plugin_opener::OpenerExt;

use super::blocking;
use super::connections::token_not_used;
use crate::auth::oauth::{self, LOGIN_TIMEOUT};
use crate::error::{AppResult, ErrorCode};
use crate::k8s::cluster::ConnectionInfo;
use crate::k8s::connectivity::ensure_allowed;
use crate::secrets::{TOKEN, secret_key};
use crate::state::AppState;
use crate::storage::profile::ConnectionProfile;

/// Подключение, с которым можно входить через браузер: политика его разрешает (иначе вход
/// и запрос метаданных OAuth шли бы мимо запрета небезопасного TLS) и способ входа хранит токен.
fn login_profile(st: &AppState, id: &str) -> AppResult<ConnectionProfile> {
    let profile = st.connections.get(id)?;
    ensure_allowed(&profile, &st.policy.status)?;
    if !profile.auth.uses_token() {
        return Err(token_not_used());
    }
    Ok(profile)
}

/// Вход через браузер: открывает страницу входа OAuth OpenShift, ждёт redirect, сохраняет токен
/// в keychain и подключается заново.
#[tauri::command]
pub async fn start_oauth_login(
    app: tauri::AppHandle,
    state: State<'_, AppState>,
    connection_id: String,
) -> AppResult<ConnectionInfo> {
    let st = state.inner().clone();
    let profile = login_profile(&st, &connection_id)?;
    let (n, cancel) = st.logins.begin(&connection_id);
    let open = |url: &str| {
        app.opener().open_url(url, None::<&str>).map_err(|e| {
            app_err!(
                ErrorCode::Internal,
                "errors.oauth.browser",
                "Cannot open the browser: {{error}}",
                error = e
            )
        })
    };
    let result = oauth::login(&profile, open, cancel, LOGIN_TIMEOUT).await;
    st.logins.finish(&connection_id, n);
    let token = result?;
    let (st2, id) = (st.clone(), connection_id.clone());
    blocking(move || st2.secrets.set(&secret_key(&id, TOKEN), &token)).await?;
    let c = st.connected(&connection_id, true).await?;
    Ok(c.info.clone())
}

/// Отменяет вход через браузер или ожидание exec-плагина (подключение и проверка).
#[tauri::command]
pub fn cancel_oauth_login(state: State<'_, AppState>, connection_id: String) -> AppResult<bool> {
    Ok(state.logins.cancel(&connection_id))
}

/// Абсолютный путь к команде exec-плагина — кнопка «Найти» в форме: PATH оболочки входа
/// и стандартные каталоги (спец. §5.2).
#[tauri::command]
pub async fn resolve_exec_command(command: String) -> AppResult<String> {
    let dirs = crate::auth::exec::command_dirs().await;
    blocking(move || resolve_exec_blocking(&command, &dirs)).await
}

/// Поиск проверяет файлы (`stat`), поэтому идёт на blocking-потоке.
fn resolve_exec_blocking(command: &str, dirs: &[PathBuf]) -> AppResult<String> {
    crate::auth::exec::resolve_command(command, None, dirs, &crate::auth::exec::is_executable_file)
        .map(|p| p.display().to_string())
        .ok_or_else(|| {
            app_err!(
                ErrorCode::InvalidInput,
                "errors.exec.commandNotFound",
                "Command “{{command}}” is not found in PATH or the standard folders — specify the full path",
                command = command.trim()
            )
        })
}

/// Адрес страницы «Display Token» кластера — запасной путь, если вход через браузер не работает.
#[tauri::command]
pub async fn oauth_token_page(
    state: State<'_, AppState>,
    connection_id: String,
) -> AppResult<String> {
    token_page(state.inner(), &connection_id).await
}

async fn token_page(st: &AppState, id: &str) -> AppResult<String> {
    let profile = login_profile(st, id)?;
    let http = oauth::http_client(&profile)?;
    let meta = oauth::fetch_metadata(&http, &profile.api_url).await?;
    Ok(oauth::token_request_page(&meta))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    #[cfg(unix)]
    use crate::auth::exec::write_test_file;
    use crate::policy::Policy;
    use crate::secrets::MemoryStore;
    use crate::storage::profile::{
        AuthConfig, CaConfig, TEST_CERT_PEM, sample_exec, sample_profile,
    };

    /// Адрес, на котором никто не слушает: запрос к нему — ошибка соединения, а не отказ политики.
    const CLOSED: &str = "https://127.0.0.1:1";

    fn state(dir: &std::path::Path, policy: Policy) -> AppState {
        AppState::new(dir, Arc::new(MemoryStore::default()))
            .unwrap()
            .with_policy(policy)
    }

    fn no_insecure_tls() -> Policy {
        let mut policy = Policy::default();
        policy.status.allow_insecure_tls = false;
        policy
    }

    fn profile(id: &str, change: impl FnOnce(&mut ConnectionProfile)) -> ConnectionProfile {
        let mut p = sample_profile(id);
        p.api_url = CLOSED.into();
        change(&mut p);
        p
    }

    #[test]
    fn login_needs_a_token_method_and_an_allowing_policy() {
        let dir = tempfile::tempdir().unwrap();
        let st = state(dir.path(), no_insecure_tls());
        let put = |p| st.connections.upsert(p).unwrap();
        put(profile("token", |_| {}));
        put(profile("browser", |p| p.auth = AuthConfig::Browser));
        put(profile("insecure", |p| p.ca = CaConfig::Insecure));
        put(profile("cert", |p| {
            p.auth = AuthConfig::ClientCert {
                certificate_pem: TEST_CERT_PEM.into(),
            }
        }));
        put(profile("exec", |p| {
            p.auth = AuthConfig::Exec(sample_exec())
        }));
        assert!(login_profile(&st, "token").is_ok());
        assert!(login_profile(&st, "browser").is_ok());
        for (id, key) in [
            ("insecure", "errors.connections.insecureForbidden"),
            ("cert", "errors.connections.tokenNotUsed"),
            ("exec", "errors.connections.tokenNotUsed"),
            ("nope", "errors.connections.notFound"),
        ] {
            assert_eq!(login_profile(&st, id).unwrap_err().key, key, "{id}");
        }
    }

    #[tokio::test]
    async fn token_page_obeys_policy_and_method_before_any_request() {
        let dir = tempfile::tempdir().unwrap();
        let st = state(dir.path(), no_insecure_tls());
        st.connections
            .upsert(profile("insecure", |p| p.ca = CaConfig::Insecure))
            .unwrap();
        st.connections
            .upsert(profile("cert", |p| {
                p.auth = AuthConfig::ClientCert {
                    certificate_pem: TEST_CERT_PEM.into(),
                }
            }))
            .unwrap();
        for (id, key) in [
            ("insecure", "errors.connections.insecureForbidden"),
            ("cert", "errors.connections.tokenNotUsed"),
        ] {
            assert_eq!(token_page(&st, id).await.unwrap_err().key, key, "{id}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn non_executable_file_does_not_shadow_a_later_command() {
        let dir = tempfile::tempdir().unwrap();
        let (early, late) = (dir.path().join("early"), dir.path().join("late"));
        std::fs::create_dir(&early).unwrap();
        std::fs::create_dir(&late).unwrap();
        write_test_file(&early.join("kubelogin"), 0o644);
        let dirs = [early.clone(), late.clone()];
        let e = resolve_exec_blocking("kubelogin", &dirs).unwrap_err();
        assert_eq!(e.key, "errors.exec.commandNotFound", "файл без права x");
        write_test_file(&late.join("kubelogin"), 0o755);
        assert_eq!(
            resolve_exec_blocking(" kubelogin ", &dirs).unwrap(),
            late.join("kubelogin").display().to_string()
        );
    }
}
