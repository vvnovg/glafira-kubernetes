use tauri::State;
use tauri_plugin_opener::OpenerExt;

use super::blocking;
use crate::auth::oauth::{self, LOGIN_TIMEOUT};
use crate::error::{AppResult, ErrorCode};
use crate::k8s::cluster::ConnectionInfo;
use crate::secrets::{TOKEN, secret_key};
use crate::state::AppState;

/// Вход через браузер: открывает страницу входа OAuth OpenShift, ждёт redirect, сохраняет токен
/// в keychain и подключается заново.
#[tauri::command]
pub async fn start_oauth_login(
    app: tauri::AppHandle,
    state: State<'_, AppState>,
    connection_id: String,
) -> AppResult<ConnectionInfo> {
    let st = state.inner().clone();
    let profile = st.connections.get(&connection_id)?;
    crate::k8s::connectivity::ensure_allowed(&profile, &st.policy.status)?;
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
    crate::auth::exec::resolve_command(&command, None, &dirs, &|p| p.is_file())
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
    let profile = state.connections.get(&connection_id)?;
    let http = oauth::http_client(&profile)?;
    let meta = oauth::fetch_metadata(&http, &profile.api_url).await?;
    Ok(oauth::token_request_page(&meta))
}
