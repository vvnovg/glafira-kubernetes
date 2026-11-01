use std::path::Path;

use tauri::State;
use time::format_description::well_known::Rfc3339;

use super::blocking;
use crate::error::{AppError, AppResult};
use crate::state::AppState;
use crate::storage::connections::ConnectionFolder;
use crate::storage::exchange::{
    ExportSummary, ImportOptions, ImportPreview, StoredSecrets, build_export, parse_exchange,
    plan_import,
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
        allow_exec_auth: st.policy.status.allow_exec_auth,
    };
    let snapshot = st.connections.snapshot();
    // Keychain читается до `update`: запрос доступа (на macOS — окно с паролем) не держит
    // блокировку записи хранилища, под которой ждали бы `list_connections` и `connections.get`.
    // Без «Обновлять существующие» сохранённые подключения не меняются — предупреждать не о чем,
    // и keychain не читается вовсе.
    let stored = if update_existing {
        StoredSecrets::read(st.secrets.as_ref(), &snapshot, &parsed)
    } else {
        StoredSecrets::default()
    };
    let planned = plan_import(&snapshot, &parsed, &opts, &stored)?;
    if !apply {
        return Ok(planned.preview);
    }
    // Токены входа и секрет клиента подключений, у которых сменились издатель OIDC, client id или
    // CA издателя либо способ входа с OIDC или на OIDC (`ImportPlan::oidc_reset`), удаляются до
    // записи профиля и вне блокировки хранилища: сбой keychain останавливает импорт, пока ничего
    // не изменено, а секреты прежнего издателя не достаются новому. Подключения, которые план под блокировкой добавит к этому списку (хранилище
    // изменилось между снимком и записью), чистятся после записи.
    reset_oidc_secrets(st, &planned.oidc_reset)?;
    let (preview, reset) = st.connections.update(|data| {
        let plan = plan_import(data, &parsed, &opts, &stored)?;
        *data = plan.next;
        Ok((plan.preview, plan.oidc_reset))
    })?;
    let late: Vec<String> = reset
        .into_iter()
        .filter(|id| !planned.oidc_reset.contains(id))
        .collect();
    if let Err(e) = reset_oidc_secrets(st, &late) {
        tracing::error!("failed to delete OIDC secrets after import: {e}");
    }
    // Обновлённые подключения переподключатся с новым профилем, их сессии на прежнем клиенте
    // останавливаются — как при сохранении формы.
    for item in &preview.updated {
        st.sessions.stop_for(&item.id);
        st.evict_connection(&item.id);
    }
    Ok(preview)
}

/// Удаляет токены входа и секрет клиента OIDC подключений `ids`.
fn reset_oidc_secrets(st: &AppState, ids: &[String]) -> AppResult<()> {
    use crate::secrets::{OIDC_CLIENT_SECRET, OIDC_ID_TOKEN, OIDC_REFRESH_TOKEN, secret_key};
    for id in ids {
        for field in [OIDC_ID_TOKEN, OIDC_REFRESH_TOKEN, OIDC_CLIENT_SECRET] {
            st.secrets.delete(&secret_key(id, field))?;
        }
    }
    Ok(())
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

        let (_, watch) = st.sessions.register("a");
        let preview = import_from_file(&st, &path, None, true, false).unwrap();
        assert_eq!(preview.updated.len(), 1);
        assert_eq!(preview.added.len(), 1);
        // prod-подключение импортируется как в файле: без принудительного read-only.
        assert!(preview.warnings.is_empty());
        assert_eq!(std::fs::read(&conns).unwrap(), before);
        assert!(!watch.is_cancelled(), "предпросмотр ничего не меняет");

        let applied = import_from_file(&st, &path, None, true, true).unwrap();
        assert_eq!(applied, preview);
        assert!(
            watch.is_cancelled(),
            "сессии обновлённого подключения остановлены"
        );
        assert_eq!(
            st.connections.get("a").unwrap().api_url,
            "https://api.changed:6443"
        );
        assert!(!st.connections.get("n").unwrap().read_only);
        let reopened = crate::storage::connections::ConnectionStore::open(dir.path()).unwrap();
        assert_eq!(reopened.list().len(), 2);
    }

    #[test]
    fn changed_address_of_a_connection_with_a_token_is_warned_about() {
        let dir = tempfile::tempdir().unwrap();
        let secrets = Arc::new(MemoryStore::default());
        secrets
            .set(&secret_key("a", crate::secrets::TOKEN), "sha256~t")
            .unwrap();
        let st = AppState::new(dir.path(), secrets.clone()).unwrap();
        st.connections.upsert(sample_profile("a")).unwrap();
        let path = write_file(dir.path(), "https://api.changed:6443");

        let preview = import_from_file(&st, &path, None, true, false).unwrap();
        assert_eq!(preview.warnings.len(), 1);
        assert_eq!(preview.warnings[0].key, "messages.import.addressChanged");
        assert_eq!(preview.warnings[0].params["name"], "conn-a");
        // Результат применения содержит то же предупреждение, токен остаётся на месте.
        let applied = import_from_file(&st, &path, None, true, true).unwrap();
        assert_eq!(applied, preview);
        assert_eq!(
            secrets
                .get(&secret_key("a", crate::secrets::TOKEN))
                .unwrap()
                .as_deref(),
            Some("sha256~t")
        );
    }

    /// Сохраняет OIDC-подключение `a` с токенами и секретом клиента, применяет файл, где его вход
    /// изменён `edit`, и возвращает предпросмотр и оставшиеся секреты.
    fn oidc_secrets_after_import(
        edit: impl FnOnce(&mut crate::storage::profile::OidcAuth),
    ) -> (ImportPreview, [Option<String>; 3]) {
        let local =
            crate::storage::profile::AuthConfig::Oidc(crate::storage::profile::sample_oidc());
        secrets_after_import(local, |p| {
            if let crate::storage::profile::AuthConfig::Oidc(o) = &mut p.auth {
                edit(o);
            }
        })
    }

    /// Подключение `a` со способом входа `local_auth` и секретами OIDC в keychain (у не-OIDC —
    /// остатки), применение файла, где профиль изменён `edit`; предпросмотр и оставшиеся секреты.
    fn secrets_after_import(
        local_auth: crate::storage::profile::AuthConfig,
        edit: impl FnOnce(&mut crate::storage::connections::ConnectionProfile),
    ) -> (ImportPreview, [Option<String>; 3]) {
        use crate::secrets::{OIDC_CLIENT_SECRET, OIDC_ID_TOKEN, OIDC_REFRESH_TOKEN};
        let dir = tempfile::tempdir().unwrap();
        let secrets = Arc::new(MemoryStore::default());
        let fields = [OIDC_ID_TOKEN, OIDC_REFRESH_TOKEN, OIDC_CLIENT_SECRET];
        for f in fields {
            secrets.set(&secret_key("a", f), "s3cr3t").unwrap();
        }
        let st = AppState::new(dir.path(), secrets.clone()).unwrap();
        let mut local = sample_profile("a");
        local.auth = local_auth;
        st.connections.upsert(local.clone()).unwrap();
        let mut changed = local;
        changed.environment = crate::storage::profile::Environment::Prod;
        edit(&mut changed);
        let path = dir.path().join("in.glafira-kubernetes-connections.json");
        std::fs::write(
            &path,
            serde_json::to_vec(&serde_json::json!({
                "format": "glafira-kubernetes-connections", "version": 1,
                "folders": [], "connections": [changed],
            }))
            .unwrap(),
        )
        .unwrap();
        let preview = import_from_file(&st, &path, None, true, true).unwrap();
        assert_eq!(preview.updated.len(), 1);
        let left = fields.map(|f| secrets.get(&secret_key("a", f)).unwrap());
        (preview, left)
    }

    /// Импорт, после которого подключение не входит через OIDC, удаляет токены и секрет клиента
    /// (как форма — `secret_ops`) и предупреждает: иначе следующий импорт с другим издателем
    /// отправил бы их ему.
    #[test]
    fn import_leaving_oidc_drops_oidc_secrets() {
        let oidc =
            crate::storage::profile::AuthConfig::Oidc(crate::storage::profile::sample_oidc());
        let token = sample_profile("a").auth;
        let (preview, left) = secrets_after_import(oidc, |p| p.auth = token);
        assert_eq!(preview.warnings.len(), 1);
        assert_eq!(preview.warnings[0].key, "messages.import.oidcDropped");
        assert_eq!(preview.warnings[0].params["name"], "conn-a");
        assert_eq!(left, [None, None, None]);
    }

    /// Переход на OIDC импортом удаляет оставшиеся от прежнего входа токены и секрет клиента.
    #[test]
    fn import_switching_to_oidc_drops_leftover_oidc_secrets() {
        let (_, left) = secrets_after_import(sample_profile("a").auth, |p| {
            p.auth =
                crate::storage::profile::AuthConfig::Oidc(crate::storage::profile::sample_oidc())
        });
        assert_eq!(left, [None, None, None]);
    }

    /// Импорт с другим издателем, client id или CA издателя предупреждает и удаляет токены и
    /// секрет клиента прежнего издателя, как сохранение формы (`oidc_identity_changed`).
    #[test]
    fn import_changing_the_oidc_identity_drops_oidc_secrets() {
        let edits: [fn(&mut crate::storage::profile::OidcAuth); 3] = [
            |o| o.issuer_url = "https://other.example.com".into(),
            |o| o.client_id = "other-client".into(),
            |o| o.issuer_ca_pem = Some(crate::storage::profile::TEST_CERT_PEM.into()),
        ];
        for edit in edits {
            let (preview, left) = oidc_secrets_after_import(edit);
            assert_eq!(preview.warnings.len(), 1);
            assert_eq!(preview.warnings[0].key, "messages.import.oidcChanged");
            assert_eq!(left, [None, None, None]);
        }
        // Вход тот же (завершающий «/» адреса — та же нормализация): секреты остаются.
        let (preview, left) = oidc_secrets_after_import(|o| o.issuer_url.push('/'));
        assert!(preview.warnings.is_empty());
        assert_eq!(left, [(); 3].map(|_| Some("s3cr3t".to_string())));
    }

    /// Keychain, который запоминает для каждого чтения, держал ли кто-то в этот момент
    /// блокировку записи хранилища подключений.
    #[derive(Default)]
    struct LockProbe {
        inner: MemoryStore,
        store: std::sync::OnceLock<Arc<crate::storage::connections::ConnectionStore>>,
        reads: std::sync::Mutex<Vec<bool>>,
    }

    impl SecretStore for LockProbe {
        fn get(&self, key: &str) -> AppResult<Option<String>> {
            let locked = self.store.get().is_some_and(|s| s.is_write_locked());
            self.reads.lock().unwrap().push(locked);
            self.inner.get(key)
        }
        fn set(&self, key: &str, value: &str) -> AppResult<()> {
            self.inner.set(key, value)
        }
        fn delete(&self, key: &str) -> AppResult<()> {
            self.inner.delete(key)
        }
    }

    /// Применение импорта читает keychain до блокировки записи хранилища: запрос доступа к
    /// keychain не задерживает `list_connections` и `connections.get`.
    #[test]
    fn import_reads_the_keychain_outside_the_store_lock() {
        let dir = tempfile::tempdir().unwrap();
        let probe = Arc::new(LockProbe::default());
        probe
            .inner
            .set(&secret_key("a", crate::secrets::TOKEN), "sha256~t")
            .unwrap();
        let st = AppState::new(dir.path(), probe.clone()).unwrap();
        assert!(probe.store.set(st.connections.clone()).is_ok());
        st.connections.upsert(sample_profile("a")).unwrap();
        probe.reads.lock().unwrap().clear();
        let path = write_file(dir.path(), "https://api.changed:6443");

        let applied = import_from_file(&st, &path, None, true, true).unwrap();
        assert_eq!(applied.warnings[0].key, "messages.import.addressChanged");
        let reads = probe.reads.lock().unwrap().clone();
        assert!(!reads.is_empty(), "токен прочитан");
        assert!(reads.iter().all(|locked| !locked), "{reads:?}");
    }

    /// Без «Обновлять существующие» предупреждать не о чем: ни предпросмотр, ни применение
    /// keychain не читают (на macOS — без лишнего запроса доступа).
    #[test]
    fn import_without_updating_existing_does_not_touch_the_keychain() {
        let dir = tempfile::tempdir().unwrap();
        let probe = Arc::new(LockProbe::default());
        let st = AppState::new(dir.path(), probe.clone()).unwrap();
        st.connections.upsert(sample_profile("a")).unwrap();
        probe.reads.lock().unwrap().clear();
        let path = write_file(dir.path(), "https://api.changed:6443");

        let preview = import_from_file(&st, &path, None, false, false).unwrap();
        assert!(preview.updated.is_empty() && preview.warnings.is_empty());
        let applied = import_from_file(&st, &path, None, false, true).unwrap();
        assert_eq!(applied.added.len(), 1);
        assert!(probe.reads.lock().unwrap().is_empty());
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
