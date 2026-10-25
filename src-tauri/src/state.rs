use std::path::Path;
use std::sync::Arc;

use crate::error::{AppError, AppResult, ErrorCode};
use crate::k8s::pool::{ClientPool, Connected, open};
use crate::secrets::SecretStore;
use crate::storage::connections::ConnectionStore;

/// Общее состояние приложения: хранилища, политика и подключённые кластеры.
/// Клонируется дёшево (всё за `Arc`).
#[derive(Clone)]
pub struct AppState {
    pub connections: Arc<ConnectionStore>,
    pub secrets: Arc<dyn SecretStore>,
    pub policy: Arc<crate::policy::Policy>,
    pub settings: Arc<crate::settings::SettingsStore>,
    pub pool: Arc<ClientPool>,
}

impl AppState {
    pub fn new(data_dir: &Path, secrets: Arc<dyn SecretStore>) -> AppResult<Self> {
        Ok(Self {
            connections: Arc::new(ConnectionStore::open(data_dir)?),
            secrets,
            policy: Arc::new(crate::policy::Policy::default()),
            settings: Arc::new(crate::settings::SettingsStore::open(data_dir)),
            pool: Arc::new(ClientPool::default()),
        })
    }

    /// Политика администратора (читается при запуске в lib.rs).
    pub fn with_policy(mut self, policy: crate::policy::Policy) -> Self {
        self.policy = Arc::new(policy);
        self
    }

    /// Клиент подключения: из пула или новое подключение (`fresh` — всегда новое).
    pub async fn connected(&self, id: &str, fresh: bool) -> AppResult<Arc<Connected>> {
        if fresh {
            self.pool.evict(id);
        } else if let Some(c) = self.pool.get(id) {
            return Ok(c);
        }
        let generation = self.pool.generation(id);
        let profile = self.connections.get(id)?;
        let c = Arc::new(open(profile, self.secrets.clone()).await?);
        self.pool.insert_if_current(id, generation, c.clone());
        Ok(c)
    }

    /// Сбрасывает клиент, если ошибка значит, что с этими учётными данными или TLS он работать
    /// не будет: следующий вызов подключится заново и подхватит новый токен.
    pub fn evict_if_broken(&self, id: &str, e: &AppError) -> bool {
        let broken = matches!(
            e.code,
            ErrorCode::SessionExpired | ErrorCode::AuthFailed | ErrorCode::TlsError
        );
        if broken {
            self.pool.evict(id);
        }
        broken
    }

    pub fn evict_connection(&self, id: &str) {
        self.pool.evict(id);
    }

    /// Удаляет подключение целиком: профиль, секреты и клиент. Блокирующий вызов (keychain).
    pub fn delete_connection_full(&self, id: &str) -> AppResult<()> {
        self.connections.delete(id)?;
        for field in crate::secrets::CONNECTION_SECRET_FIELDS {
            self.secrets
                .delete(&crate::secrets::secret_key(id, field))?;
        }
        self.evict_connection(id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::i18n::LocalizedText;
    use crate::secrets::MemoryStore;
    use crate::storage::connections::sample_profile;

    fn state(dir: &Path) -> AppState {
        AppState::new(dir, Arc::new(MemoryStore::default())).unwrap()
    }

    #[tokio::test]
    async fn unknown_connection_is_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let st = state(dir.path());
        let e = st.connected("nope", false).await.err().unwrap();
        assert_eq!(e.code, ErrorCode::NotFound);
    }

    #[test]
    fn delete_connection_removes_all_secrets() {
        let dir = tempfile::tempdir().unwrap();
        let st = state(dir.path());
        st.connections.upsert(sample_profile("a")).unwrap();
        for f in crate::secrets::CONNECTION_SECRET_FIELDS {
            st.secrets
                .set(&crate::secrets::secret_key("a", f), "x")
                .unwrap();
        }
        st.delete_connection_full("a").unwrap();
        assert!(st.connections.get("a").is_err());
        for f in crate::secrets::CONNECTION_SECRET_FIELDS {
            assert_eq!(
                st.secrets.get(&crate::secrets::secret_key("a", f)).unwrap(),
                None,
                "{f}"
            );
        }
    }

    #[test]
    fn only_credential_errors_evict() {
        let dir = tempfile::tempdir().unwrap();
        let st = state(dir.path());
        for (code, evicts) in [
            (ErrorCode::SessionExpired, true),
            (ErrorCode::AuthFailed, true),
            (ErrorCode::TlsError, true),
            (ErrorCode::Timeout, false),
            (ErrorCode::Forbidden, false),
        ] {
            let before = st.pool.generation("a");
            let e = AppError::localized(code, LocalizedText::raw("x"));
            assert_eq!(st.evict_if_broken("a", &e), evicts, "{code:?}");
            assert_eq!(st.pool.generation("a") > before, evicts, "{code:?}");
        }
    }
}
