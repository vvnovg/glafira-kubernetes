//! Команды раздела «LLM-ассистенты (MCP)» в «Настройках» (спец. §10.1, §4.2).

use std::sync::Arc;

use tauri::State;

use super::blocking;
use crate::error::{AppResult, ErrorCode};
use crate::mcp::{ClientConfigs, McpController, McpStatus, client_configs, launch_path};

type Ctl<'a> = State<'a, Arc<McpController>>;

#[tauri::command]
pub fn get_mcp_status(ctl: Ctl<'_>) -> McpStatus {
    ctl.status()
}

#[tauri::command]
pub async fn set_mcp_enabled(ctl: Ctl<'_>, enabled: bool) -> AppResult<McpStatus> {
    ctl.inner().clone().set_enabled(enabled).await
}

#[tauri::command]
pub async fn answer_mcp_approval(
    ctl: Ctl<'_>,
    session_id: String,
    approve: bool,
) -> AppResult<McpStatus> {
    let c = ctl.inner().clone();
    blocking(move || c.answer(&session_id, approve)).await
}

#[tauri::command]
pub async fn revoke_mcp_client(ctl: Ctl<'_>, name: String) -> AppResult<McpStatus> {
    let c = ctl.inner().clone();
    blocking(move || c.revoke(&name)).await
}

#[tauri::command]
pub fn disconnect_mcp_sessions(ctl: Ctl<'_>, session_id: Option<String>) -> McpStatus {
    ctl.disconnect(session_id.as_deref())
}

#[tauri::command]
pub async fn get_mcp_audit(ctl: Ctl<'_>, limit: usize) -> AppResult<Vec<serde_json::Value>> {
    let c = ctl.inner().clone();
    blocking(move || Ok(c.audit_recent(limit.min(1000)))).await
}

#[tauri::command]
pub fn get_mcp_client_configs() -> AppResult<ClientConfigs> {
    let exe = std::env::current_exe().map_err(|e| {
        app_err!(
            ErrorCode::Internal,
            "errors.mcp.noExe",
            "Cannot find the Glafira executable: {{error}}",
            error = e
        )
    })?;
    Ok(client_configs(&launch_path(
        &exe,
        std::env::var_os("APPIMAGE"),
    )))
}

#[tauri::command]
pub fn reveal_mcp_audit(ctl: Ctl<'_>) -> AppResult<()> {
    tauri_plugin_opener::reveal_item_in_dir(ctl.audit_path()).map_err(|e| {
        app_err!(
            ErrorCode::NotFound,
            "errors.mcp.auditMissing",
            "The MCP log is not available yet: {{error}}",
            error = e
        )
    })
}
