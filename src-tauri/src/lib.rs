#[macro_use]
pub mod i18n;
mod dock_name;
pub mod error;
pub mod logging;
pub mod product;
pub mod secrets;
pub mod storage;
pub mod sync;

use tauri::Manager;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_dialog::init())
        .setup(|app| {
            crate::logging::init(&app.path().app_log_dir()?);
            tracing::info!(version = env!("CARGO_PKG_VERSION"), "starting");
            if !crate::dock_name::set(crate::product::PRODUCT_NAME) {
                tracing::warn!("dock name was not set: LaunchServices refused");
            }
            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("error while building tauri application");
}
