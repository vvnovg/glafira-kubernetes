//! `mcp.json` (спец. §10.1): адрес и токен работающего MCP-сервера. Файл существует, только
//! пока приложение обслуживает MCP; его читает шим.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::AppResult;
use crate::storage::fs::{read_json, write_json};

pub const FILE_NAME: &str = "mcp.json";
pub const VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct McpFile {
    pub version: u32,
    pub endpoint: String,
    pub token: String,
    pub pid: u32,
}

/// Атомарная запись с правами 0600 (`write_json`).
pub fn write(dir: &Path, f: &McpFile) -> AppResult<()> {
    write_json(&dir.join(FILE_NAME), f)
}

/// Нечитаемый файл или файл другой версии — как будто его нет.
pub fn read(dir: &Path) -> Option<McpFile> {
    read_json::<McpFile>(&dir.join(FILE_NAME))
        .ok()
        .flatten()
        .filter(|f| f.version == VERSION)
}

/// Удаляет `mcp.json`, только если в нём `pid` и `token` из `f`: файл, который успел записать
/// другой экземпляр Glafira, остаётся. Ошибки — в журнал.
pub fn remove_if(dir: &Path, f: &McpFile) {
    if !read(dir).is_some_and(|cur| cur.pid == f.pid && cur.token == f.token) {
        return;
    }
    if let Err(e) = std::fs::remove_file(dir.join(FILE_NAME))
        && e.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!("mcp.json not removed: {e}");
    }
}

/// Каталог данных без Tauri — так его находит шим. Совпадает с `app_data_dir()` Tauri
/// (`<data_dir ОС>/<identifier>`); при запуске приложение сверяет оба пути.
pub fn data_dir() -> Option<PathBuf> {
    dirs::data_dir().map(|d| d.join(crate::product::IDENTIFIER))
}

/// 64 hex-символа из генератора ОС (uuid v4 берёт случайность у `getrandom`).
pub fn new_token() -> String {
    format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    )
}

/// Случайная часть имени pipe или запасного сокета.
pub fn new_suffix() -> String {
    uuid::Uuid::new_v4().simple().to_string()[..16].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn written_file_is_read_back_private_and_removed() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(read(dir.path()), None);
        let f = McpFile {
            version: VERSION,
            endpoint: "/tmp/x.sock".into(),
            token: new_token(),
            pid: 42,
        };
        write(dir.path(), &f).unwrap();
        assert_eq!(read(dir.path()), Some(f.clone()));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(dir.path().join(FILE_NAME))
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o600);
        }
        let foreign = McpFile {
            token: new_token(),
            ..f.clone()
        };
        remove_if(dir.path(), &foreign);
        assert_eq!(read(dir.path()), Some(f.clone()), "чужой файл не удаляется");
        remove_if(dir.path(), &f);
        assert_eq!(read(dir.path()), None);
        remove_if(dir.path(), &f);
    }

    #[test]
    fn other_version_or_garbage_is_ignored() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(FILE_NAME),
            br#"{"version":2,"endpoint":"e","token":"t","pid":1}"#,
        )
        .unwrap();
        assert_eq!(read(dir.path()), None);
        std::fs::write(dir.path().join(FILE_NAME), b"{").unwrap();
        assert_eq!(read(dir.path()), None);
    }

    #[test]
    fn tokens_are_long_hex_and_differ() {
        let (a, b) = (new_token(), new_token());
        assert_eq!(a.len(), 64);
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(a, b);
        assert_eq!(new_suffix().len(), 16);
    }

    #[test]
    fn shim_data_dir_ends_with_identifier() {
        let d = data_dir().unwrap();
        assert!(d.ends_with(crate::product::IDENTIFIER));
    }
}
