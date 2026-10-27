// Prevents additional console window on Windows in release, DO NOT REMOVE!!
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

fn main() {
    // Режим шима MCP (спец. §10.1): без окна и Tauri, stdio ↔ сокет запущенной Glafira.
    if std::env::args_os().nth(1).is_some_and(|a| a == "mcp") {
        std::process::exit(glafira_kubernetes_lib::mcp::shim::run());
    }
    glafira_kubernetes_lib::run()
}
