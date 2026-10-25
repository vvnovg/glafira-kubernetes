use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use std::time::SystemTime;

use crate::error::{AppError, AppResult, ErrorCode};
use crate::k8s::connectivity::ensure_allowed;
use crate::k8s::pool::{ClientPool, Connected, open};
use crate::secrets::SecretStore;
use crate::storage::connections::ConnectionStore;
use crate::storage::profile::AuthConfig;

/// Общее состояние приложения: хранилища, политика и подключённые кластеры.
/// Клонируется дёшево (всё за `Arc`).
#[derive(Clone)]
pub struct AppState {
    pub connections: Arc<ConnectionStore>,
    pub secrets: Arc<dyn SecretStore>,
    pub policy: Arc<crate::policy::Policy>,
    pub settings: Arc<crate::settings::SettingsStore>,
    pub pool: Arc<ClientPool>,
    /// Идущие входы: через браузер и запуском exec-плагина.
    pub logins: Arc<crate::auth::Logins>,
    /// Сессии watch и логов.
    pub sessions: Arc<crate::sessions::Sessions>,
}

impl AppState {
    pub fn new(data_dir: &Path, secrets: Arc<dyn SecretStore>) -> AppResult<Self> {
        Ok(Self {
            connections: Arc::new(ConnectionStore::open(data_dir)?),
            secrets,
            policy: Arc::new(crate::policy::Policy::default()),
            settings: Arc::new(crate::settings::SettingsStore::open(data_dir)),
            pool: Arc::new(ClientPool::default()),
            logins: Arc::new(crate::auth::Logins::default()),
            sessions: Arc::new(crate::sessions::Sessions::default()),
        })
    }

    /// Политика администратора (читается при запуске в lib.rs).
    pub fn with_policy(mut self, policy: crate::policy::Policy) -> Self {
        self.policy = Arc::new(policy);
        self
    }

    /// Клиент подключения: из пула или новое подключение (`fresh` — всегда новое; истекающие
    /// учётные данные exec-плагина — тоже). Подключения одного профиля идут по очереди, ожидание
    /// exec-плагина отменяется так же, как вход через браузер (`logins`). Кто ждал очереди, пока
    /// чужая попытка не удалась (или её отменили), получает ту же ошибку: чтения одной вкладки
    /// не запускают плагин по разу каждое. Подключение, которое запрещено политикой (CA
    /// `insecure`, exec-плагин), не открывается.
    pub async fn connected(&self, id: &str, fresh: bool) -> AppResult<Arc<Connected>> {
        let seen = self.pool.finished_attempts(id);
        let _turn = self.pool.connect_lock(id).await;
        if fresh {
            self.pool.evict(id);
        } else if let Some(c) = self.pool.get(id)
            && !c.expiring(SystemTime::now())
        {
            return Ok(c);
        } else if let Some(e) = self.pool.failure_since(id, seen) {
            return Err(e);
        }
        let generation = self.pool.generation(id);
        let profile = self.connections.get(id)?;
        // Политика проверяется здесь, а не только в командах: ни одна из них её не обойдёт.
        ensure_allowed(&profile, &self.policy.status)?;
        let login = matches!(profile.auth, AuthConfig::Exec(_)).then(|| self.logins.begin(id));
        let (n, cancel) = match login {
            Some((n, rx)) => (Some(n), rx),
            // Не exec-плагин: `open` сигнал отмены не ждёт.
            None => (None, tokio::sync::oneshot::channel().1),
        };
        let result = open(profile, self.secrets.clone(), cancel).await;
        if let Some(n) = n {
            self.logins.finish(id, n);
        }
        self.pool
            .finish_attempt(id, generation, result.as_ref().err());
        let c = Arc::new(result?);
        self.pool.insert_if_current(id, generation, c.clone());
        Ok(c)
    }

    /// Чтение кластера для команды: клиент из пула (или новое подключение) и `f` с ним. Ошибка
    /// учётных данных или TLS сбрасывает клиент (`evict_if_broken`): следующее чтение подключится
    /// заново.
    pub async fn read<T, F, Fut>(&self, id: &str, f: F) -> AppResult<T>
    where
        F: FnOnce(Arc<Connected>) -> Fut,
        Fut: Future<Output = AppResult<T>>,
    {
        let result = match self.connected(id, false).await {
            Ok(c) => f(c).await,
            Err(e) => Err(e),
        };
        if let Err(e) = &result {
            self.evict_if_broken(id, e);
        }
        result
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

    /// Удаляет подключение целиком: профиль, секреты, клиент и сессии. Блокирующий вызов
    /// (keychain).
    pub fn delete_connection_full(&self, id: &str) -> AppResult<()> {
        self.connections.delete(id)?;
        self.sessions.stop_for(id);
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

    #[tokio::test]
    async fn connect_obeys_the_policy_whatever_the_caller() {
        use crate::storage::profile::{AuthConfig, CaConfig, sample_exec};
        let dir = tempfile::tempdir().unwrap();
        let mut policy = crate::policy::Policy::default();
        policy.status.allow_insecure_tls = false;
        policy.status.allow_exec_auth = false;
        let st = state(dir.path()).with_policy(policy);
        // Закрытый порт: без проверки политики ошибка была бы другой (соединение или вход).
        let mut insecure = sample_profile("insecure");
        insecure.api_url = "https://127.0.0.1:1".into();
        insecure.ca = CaConfig::Insecure;
        let mut exec = sample_profile("exec");
        exec.api_url = "https://127.0.0.1:1".into();
        exec.auth = AuthConfig::Exec(sample_exec());
        st.connections.upsert(insecure).unwrap();
        st.connections.upsert(exec).unwrap();
        for (id, key) in [
            ("insecure", "errors.connections.insecureForbidden"),
            ("exec", "errors.connections.execForbidden"),
        ] {
            for fresh in [false, true] {
                let e = st.connected(id, fresh).await.err().unwrap();
                assert_eq!(e.key, key, "{id}, fresh = {fresh}");
            }
        }
    }

    /// Exec-плагин, который считает свои запуски (строка в `COUNT_FILE`) и через 0,3 с падает
    /// (`fail`) или ждёт отмены (`wait`).
    #[cfg(unix)]
    fn counting_plugin(st: &AppState, dir: &Path, mode: &str) -> std::path::PathBuf {
        use crate::storage::profile::{AuthConfig, ExecEnv, sample_exec};
        use std::os::unix::fs::PermissionsExt;
        let script = dir.join("kubectl");
        std::fs::write(
            &script,
            "#!/bin/sh\necho run >> \"$COUNT_FILE\"\ncase \"$3\" in\n  fail) sleep 0.3; exit 1 ;;\n  wait) sleep 30 ;;\nesac\n",
        )
        .unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        let count = dir.join("count");
        let mut exec = sample_exec();
        exec.command = script.display().to_string();
        exec.args = vec!["oidc-login".into(), "get-token".into(), mode.into()];
        exec.env = vec![ExecEnv {
            name: "COUNT_FILE".into(),
            value: count.display().to_string(),
        }];
        exec.approved = Some(crate::auth::exec::fingerprint(&exec));
        let mut p = sample_profile("e");
        p.auth = AuthConfig::Exec(exec);
        st.connections.upsert(p).unwrap();
        count
    }

    /// Три чтения подключаются одновременно (`connect` — одно подключение профиля `e`): плагин
    /// запускается один раз, все получают его ошибку.
    #[cfg(unix)]
    async fn concurrent_connects<T, F>(
        st: &AppState,
        connect: impl Fn(AppState) -> F,
    ) -> Vec<AppError>
    where
        T: Send + 'static,
        F: std::future::Future<Output = AppResult<T>> + Send + 'static,
    {
        let first = tokio::spawn(connect(st.clone()));
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let rest: Vec<_> = (0..2).map(|_| tokio::spawn(connect(st.clone()))).collect();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let mut errors = Vec::new();
        for h in std::iter::once(first).chain(rest) {
            errors.push(h.await.unwrap().err().expect("подключение не удалось"));
        }
        errors
    }

    /// Плагин падает: ожидавшие получают его ошибку, следующее подключение — новая попытка.
    #[cfg(unix)]
    async fn waiters_share_the_failure<T, F>(connect: impl Fn(AppState) -> F + Copy)
    where
        T: Send + 'static,
        F: std::future::Future<Output = AppResult<T>> + Send + 'static,
    {
        let dir = tempfile::tempdir().unwrap();
        let st = state(dir.path());
        let count = counting_plugin(&st, dir.path(), "fail");
        let errors = concurrent_connects(&st, connect).await;
        assert!(
            errors.iter().all(|e| e.key == "errors.exec.failed"),
            "{errors:?}"
        );
        assert_eq!(std::fs::read_to_string(&count).unwrap().lines().count(), 1);
        // Следующее подключение после неудачи — новая попытка.
        let e = connect(st.clone()).await.err().unwrap();
        assert_eq!(e.key, "errors.exec.failed");
        assert_eq!(std::fs::read_to_string(&count).unwrap().lines().count(), 2);
    }

    /// Ожидание плагина отменили: отмена — итог всех ожидавших, плагин больше не запускается.
    #[cfg(unix)]
    async fn cancel_reaches_every_waiter<T, F>(connect: impl Fn(AppState) -> F + Send + 'static)
    where
        T: Send + 'static,
        F: std::future::Future<Output = AppResult<T>> + Send + 'static,
    {
        let dir = tempfile::tempdir().unwrap();
        let st = state(dir.path());
        let count = counting_plugin(&st, dir.path(), "wait");
        let waiting = tokio::spawn({
            let st = st.clone();
            async move { concurrent_connects(&st, connect).await }
        });
        // Плагин запущен (записал строку) — отменяем его ожидание.
        let started = async {
            while !count.exists() {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        };
        tokio::time::timeout(std::time::Duration::from_secs(10), started)
            .await
            .expect("плагин запущен");
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        assert!(st.logins.cancel("e"), "ожидание плагина отменяется");
        let errors = waiting.await.unwrap();
        assert!(
            errors.iter().all(|e| e.key == "errors.oauth.cancelled"),
            "{errors:?}"
        );
        assert_eq!(std::fs::read_to_string(&count).unwrap().lines().count(), 1);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn waiters_share_the_failure_instead_of_rerunning_the_plugin() {
        waiters_share_the_failure(|st| async move { st.connected("e", false).await }).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cancelling_the_plugin_cancels_every_waiter() {
        cancel_reaches_every_waiter(|st| async move { st.connected("e", false).await }).await;
    }

    /// То же через команду `connect`: её ошибка подключения не сбрасывает клиент, иначе новое
    /// поколение лишило бы ожидавших общего итога и каждый запустил бы плагин заново.
    #[cfg(unix)]
    #[tokio::test]
    async fn connect_command_waiters_share_the_failure_and_the_cancel() {
        let connect = |st: AppState| async move {
            crate::commands::connections::connection_info(&st, "e", false).await
        };
        waiters_share_the_failure(connect).await;
        cancel_reaches_every_waiter(connect).await;
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

    #[tokio::test]
    async fn failed_read_of_an_unknown_connection_reports_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let st = state(dir.path());
        let e = st.read("nope", |_| async { Ok(()) }).await.unwrap_err();
        assert_eq!(e.code, ErrorCode::NotFound);
    }

    #[test]
    fn deleting_a_connection_stops_its_sessions() {
        let dir = tempfile::tempdir().unwrap();
        let st = state(dir.path());
        st.connections.upsert(sample_profile("a")).unwrap();
        let (_, cancel) = st.sessions.register("a");
        st.delete_connection_full("a").unwrap();
        assert!(cancel.is_cancelled());
        assert!(st.sessions.is_empty());
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
