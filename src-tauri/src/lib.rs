#[macro_use]
pub mod i18n;
pub mod auth;
pub mod commands;
mod dock_name;
pub mod error;
pub mod k8s;
pub mod logging;
pub mod platform;
pub mod policy;
pub mod product;
pub mod secrets;
pub mod sessions;
pub mod settings;
pub mod state;
pub mod storage;
pub mod sync;
pub mod tls;

use std::sync::Arc;

use tauri::Manager;

use crate::secrets::KeyringStore;
use crate::state::AppState;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    crate::tls::ensure_crypto_provider();
    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_dialog::init())
        .setup(|app| {
            crate::logging::init(&app.path().app_log_dir()?);
            tracing::info!(version = env!("CARGO_PKG_VERSION"), "starting");
            if !crate::dock_name::set(crate::product::PRODUCT_NAME) {
                tracing::warn!("dock name was not set: LaunchServices refused");
            }
            let dir = app.path().app_data_dir()?;
            let policy = crate::policy::sources::load();
            tracing::info!(managed = policy.status.managed, "policy");
            let st = AppState::new(&dir, Arc::new(KeyringStore::default()))?.with_policy(policy);
            app.manage(st);
            Ok(())
        })
        // Окно одно, и его каналы сессий живут, пока жива страница. Перезагрузка (Cmd+R, полная
        // перезагрузка HMR) рвёт их все, а `Channel::send` этого не замечает (`sessions::Sink`).
        .on_page_load(|webview, payload| {
            if payload.event() == tauri::webview::PageLoadEvent::Started
                && let Some(st) = webview.try_state::<AppState>()
            {
                st.sessions.stop_all();
            }
        })
        .invoke_handler(tauri::generate_handler![
            commands::app::get_app_info,
            commands::connections::list_connections,
            commands::connections::save_connection,
            commands::connections::delete_connection,
            commands::connections::test_connection,
            commands::connections::connect,
            commands::connections::disconnect,
            commands::connections::set_connection_token,
            commands::auth::start_oauth_login,
            commands::auth::cancel_oauth_login,
            commands::auth::cancel_connection_test,
            commands::auth::oauth_token_page,
            commands::auth::resolve_exec_command,
            commands::kubeconfig::preview_kubeconfig_import,
            commands::kubeconfig::apply_kubeconfig_import,
            commands::folders::list_folders,
            commands::folders::create_folder,
            commands::folders::rename_folder,
            commands::folders::delete_folder,
            commands::folders::move_items,
            commands::folders::export_connections,
            commands::folders::preview_import,
            commands::folders::apply_import,
            commands::settings::get_language_settings,
            commands::settings::set_language,
            commands::settings::get_policy_status,
            commands::sessions::stop_session,
            commands::cluster::get_cluster_overview,
            commands::cluster::list_nodes,
            commands::cluster::get_node,
            commands::cluster::list_cluster_operators,
            commands::projects::list_projects,
            commands::projects::get_namespace_access,
            commands::objects::list_workloads,
            commands::objects::get_workload,
            commands::objects::list_pods,
            commands::objects::get_pod,
            commands::objects::list_events,
        ])
        .run(tauri::generate_context!())
        .expect("error while building tauri application");
}
