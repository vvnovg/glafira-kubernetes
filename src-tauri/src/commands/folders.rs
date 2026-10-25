use std::path::Path;

use tauri::State;
use time::format_description::well_known::Rfc3339;

use super::blocking;
use crate::error::{AppError, AppResult};
use crate::state::AppState;
use crate::storage::connections::ConnectionFolder;
use crate::storage::exchange::{
    ExportSummary, ImportOptions, ImportPreview, build_export, parse_exchange, plan_import,
};
use crate::storage::fs::write_json;
use crate::storage::tree::TreeItemRef;

#[tauri::command]
pub fn list_folders(state: State<'_, AppState>) -> AppResult<Vec<ConnectionFolder>> {
    Ok(state.connections.list_folders())
}

#[tauri::command]
pub async fn create_folder(
    state: State<'_, AppState>,
    name: String,
    parent_id: Option<String>,
) -> AppResult<ConnectionFolder> {
    let st = state.inner().clone();
    blocking(move || st.connections.create_folder(&name, parent_id.as_deref())).await
}

#[tauri::command]
pub async fn rename_folder(
    state: State<'_, AppState>,
    id: String,
    name: String,
) -> AppResult<ConnectionFolder> {
    let st = state.inner().clone();
    blocking(move || st.connections.rename_folder(&id, &name)).await
}

#[tauri::command]
pub async fn delete_folder(state: State<'_, AppState>, id: String) -> AppResult<()> {
    let st = state.inner().clone();
    blocking(move || st.connections.delete_folder(&id)).await
}

#[tauri::command]
pub async fn move_items(
    state: State<'_, AppState>,
    items: Vec<TreeItemRef>,
    parent_id: Option<String>,
    before: Option<TreeItemRef>,
) -> AppResult<()> {
    let st = state.inner().clone();
    blocking(move || {
        st.connections
            .move_items(&items, parent_id.as_deref(), before.as_ref())
    })
    .await
}

/// Пишет файл `glafira-kubernetes-connections` (всё дерево или папку `folder_id`) без секретов.
#[tauri::command]
pub async fn export_connections(
    state: State<'_, AppState>,
    path: String,
    folder_id: Option<String>,
) -> AppResult<ExportSummary> {
    let st = state.inner().clone();
    blocking(move || {
        let now = time::OffsetDateTime::now_utc()
            .format(&Rfc3339)
            .map_err(|e| AppError::internal_with(format!("exportedAt: {e}")))?;
        export_to_file(&st, Path::new(&path), folder_id.as_deref(), &now)
    })
    .await
}

fn export_to_file(
    st: &AppState,
    path: &Path,
    folder_id: Option<&str>,
    now: &str,
) -> AppResult<ExportSummary> {
    let out = build_export(
        &st.connections,
        st.secrets.as_ref(),
        folder_id,
        env!("CARGO_PKG_VERSION"),
        now,
    )?;
    write_json(path, &out)?;
    Ok(ExportSummary {
        connections: out.connections.len(),
        folders: out.folders.len(),
    })
}

/// Предпросмотр импорта файла `path` под папку `target_folder_id` (или в корень); ничего не меняет.
#[tauri::command]
pub async fn preview_import(
    state: State<'_, AppState>,
    path: String,
    target_folder_id: Option<String>,
    update_existing: bool,
) -> AppResult<ImportPreview> {
    let st = state.inner().clone();
    blocking(move || {
        import_from_file(
            &st,
            Path::new(&path),
            target_folder_id.as_deref(),
            update_existing,
            false,
        )
    })
    .await
}

/// Применяет импорт: план пересчитывается на свежих данных и пишется одной записью.
#[tauri::command]
pub async fn apply_import(
    state: State<'_, AppState>,
    path: String,
    target_folder_id: Option<String>,
    update_existing: bool,
) -> AppResult<ImportPreview> {
    let st = state.inner().clone();
    blocking(move || {
        import_from_file(
            &st,
            Path::new(&path),
            target_folder_id.as_deref(),
            update_existing,
            true,
        )
    })
    .await
}

fn import_from_file(
    st: &AppState,
    path: &Path,
    target: Option<&str>,
    update_existing: bool,
    apply: bool,
) -> AppResult<ImportPreview> {
    let parsed = parse_exchange(&std::fs::read(path)?)?;
    let opts = ImportOptions {
        target,
        update_existing,
        allow_insecure_tls: st.policy.status.allow_insecure_tls,
    };
    if !apply {
        return Ok(plan_import(&st.connections.snapshot(), &parsed, &opts)?.preview);
    }
    st.connections.update(|data| {
        let plan = plan_import(data, &parsed, &opts)?;
        *data = plan.next;
        Ok(plan.preview)
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::secrets::{MemoryStore, SecretStore, secret_key};
    use crate::storage::connections::sample_profile;

    #[test]
    fn export_writes_file_without_secrets() {
        let dir = tempfile::tempdir().unwrap();
        let secrets = Arc::new(MemoryStore::default());
        secrets
            .set(&secret_key("a", crate::secrets::TOKEN), "s3cr3t")
            .unwrap();
        let st = AppState::new(dir.path(), secrets).unwrap();
        let f = st.connections.create_folder("ИФТ", None).unwrap();
        let mut p = sample_profile("a");
        p.folder_id = Some(f.id.clone());
        st.connections.upsert(p).unwrap();
        st.connections.upsert(sample_profile("b")).unwrap();

        let path = dir.path().join("out.glafira-kubernetes-connections.json");
        let sum = export_to_file(&st, &path, Some(&f.id), "2026-10-17T12:00:00Z").unwrap();
        assert_eq!(
            sum,
            ExportSummary {
                connections: 1,
                folders: 1
            }
        );
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(!text.contains("s3cr3t"));
        let v: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(v["format"], "glafira-kubernetes-connections");
        assert_eq!(v["appVersion"], env!("CARGO_PKG_VERSION"));
        assert_eq!(v["connections"][0]["id"], "a");

        let err = export_to_file(&st, &path, Some("nope"), "now").unwrap_err();
        assert_eq!(err.key, "errors.folders.notFound");
    }

    fn write_file(dir: &Path, api_url: &str) -> std::path::PathBuf {
        let mut p = serde_json::to_value(sample_profile("a")).unwrap();
        p["apiUrl"] = api_url.into();
        let mut n = serde_json::to_value(sample_profile("n")).unwrap();
        n["environment"] = "prod".into();
        let path = dir.join("in.glafira-kubernetes-connections.json");
        std::fs::write(
            &path,
            serde_json::to_vec(&serde_json::json!({
                "format": "glafira-kubernetes-connections", "version": 1,
                "folders": [], "connections": [p, n],
            }))
            .unwrap(),
        )
        .unwrap();
        path
    }

    #[test]
    fn preview_does_not_write_and_apply_writes_once() {
        let dir = tempfile::tempdir().unwrap();
        let st = AppState::new(dir.path(), Arc::new(MemoryStore::default())).unwrap();
        st.connections.upsert(sample_profile("a")).unwrap();
        let input = tempfile::tempdir().unwrap();
        let path = write_file(input.path(), "https://api.changed:6443");
        let conns = dir.path().join("connections.json");
        let before = std::fs::read(&conns).unwrap();

        let preview = import_from_file(&st, &path, None, true, false).unwrap();
        assert_eq!(preview.updated.len(), 1);
        assert_eq!(preview.added.len(), 1);
        // prod-подключение импортируется как в файле: без принудительного read-only.
        assert!(preview.warnings.is_empty());
        assert_eq!(std::fs::read(&conns).unwrap(), before);

        let applied = import_from_file(&st, &path, None, true, true).unwrap();
        assert_eq!(applied, preview);
        assert_eq!(
            st.connections.get("a").unwrap().api_url,
            "https://api.changed:6443"
        );
        assert!(!st.connections.get("n").unwrap().read_only);
        let reopened = crate::storage::connections::ConnectionStore::open(dir.path()).unwrap();
        assert_eq!(reopened.list().len(), 2);
    }

    #[cfg(unix)]
    #[test]
    fn failed_apply_changes_nothing() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        let st = AppState::new(&data, Arc::new(MemoryStore::default())).unwrap();
        st.connections.upsert(sample_profile("a")).unwrap();
        let path = write_file(dir.path(), "https://api.changed:6443");
        let conns = data.join("connections.json");
        let before = std::fs::read(&conns).unwrap();

        // Каталог данных только для чтения: временный файл не создать, запись падает.
        std::fs::set_permissions(&data, std::fs::Permissions::from_mode(0o555)).unwrap();
        let result = import_from_file(&st, &path, None, true, true);
        std::fs::set_permissions(&data, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(result.is_err());
        assert_eq!(std::fs::read(&conns).unwrap(), before);
        assert_eq!(st.connections.list().len(), 1);
        assert_eq!(
            st.connections.get("a").unwrap().api_url,
            "https://api.dev.example.com:6443"
        );
    }

    #[test]
    fn policy_forbidding_insecure_tls_skips_such_connections() {
        let dir = tempfile::tempdir().unwrap();
        let mut policy = crate::policy::Policy::default();
        policy.status.allow_insecure_tls = false;
        let st = AppState::new(dir.path(), Arc::new(MemoryStore::default()))
            .unwrap()
            .with_policy(policy);
        let mut insecure = serde_json::to_value(sample_profile("i")).unwrap();
        insecure["ca"] = serde_json::json!({"mode": "insecure"});
        let path = dir.path().join("in.glafira-kubernetes-connections.json");
        std::fs::write(
            &path,
            serde_json::to_vec(&serde_json::json!({
                "format": "glafira-kubernetes-connections", "version": 1,
                "folders": [], "connections": [insecure],
            }))
            .unwrap(),
        )
        .unwrap();
        let applied = import_from_file(&st, &path, None, true, true).unwrap();
        assert!(applied.added.is_empty());
        assert_eq!(
            applied.skipped[0].reason.key,
            "errors.import.insecureForbidden"
        );
        assert!(st.connections.list().is_empty());
    }

    #[test]
    fn foreign_file_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let st = AppState::new(dir.path(), Arc::new(MemoryStore::default())).unwrap();
        let path = dir.path().join("x.json");
        std::fs::write(&path, br#"{"format":"other","version":1}"#).unwrap();
        let err = import_from_file(&st, &path, None, true, false).unwrap_err();
        assert_eq!(err.key, "errors.import.notGlafiraFile");
    }
}
