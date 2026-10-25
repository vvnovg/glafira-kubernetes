use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use std::time::SystemTime;

use crate::error::{AppError, AppResult, ErrorCode};
use crate::k8s::connectivity::ensure_allowed;
use crate::k8s::errors::breaks_client;
use crate::k8s::pool::{ClientPool, Connected, open};
use crate::k8s::read::{Observed, observe};
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
    /// Права пользователя в проектах (кэш на 5 минут).
    pub access: Arc<crate::k8s::access::AccessCache>,
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
            access: Arc::new(crate::k8s::access::AccessCache::default()),
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
            // Вход, новый токен, «Подключиться заново»: с новым токеном и права могут быть
            // другими — кэш прав забывается вместе с клиентом.
            self.evict_connection(id);
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
        // Чтения при подключении (срок токена: у обычного пользователя 403 — ожидаемый ответ) —
        // не ошибки чтения, внутри которого подключаются.
        let (result, _) = observe(open(profile, self.secrets.clone(), cancel)).await;
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
    /// учётных данных или TLS сбрасывает этот клиент (`evict_if_broken_client`), если он всё
    /// ещё в пуле: следующее чтение подключится заново. Считается и ошибка, которую `f` вернул,
    /// и ошибка любого запроса `ReadClient` внутри `f` (`k8s::read::observe`), даже если `f`
    /// уложил её в часть экрана (`Part`) или в ошибку вида списка и вернул `Ok`. Ошибка самого
    /// подключения клиент не сбрасывает: неудачная попытка в пул не попадает, а сброс сменил бы
    /// поколение, и ожидавшие очереди не получили бы её итог — каждое чтение снова запустило бы
    /// exec-плагин или вход.
    pub async fn read<T, F, Fut>(&self, id: &str, f: F) -> AppResult<T>
    where
        F: FnOnce(Arc<Connected>) -> Fut,
        Fut: Future<Output = AppResult<T>>,
    {
        self.read_observed(id, f).await.0
    }

    /// Чтение в проекте: как `read`, а ответ 403 ещё и сбрасывает кэш прав этого проекта —
    /// права могли измениться (спец. §5.4). Как и в `read`, считается и 403 части экрана или
    /// вида списка, при котором `f` вернул `Ok`.
    pub async fn read_in<T, F, Fut>(&self, id: &str, namespace: &str, f: F) -> AppResult<T>
    where
        F: FnOnce(Arc<Connected>) -> Fut,
        Fut: Future<Output = AppResult<T>>,
    {
        let (result, seen) = self.read_observed(id, f).await;
        if seen.forbidden
            || result
                .as_ref()
                .is_err_and(|e| e.code == ErrorCode::Forbidden)
        {
            self.access.forget(id, namespace);
        }
        result
    }

    /// `read` и ошибки запросов `ReadClient`, которые были внутри `f`.
    async fn read_observed<T, F, Fut>(&self, id: &str, f: F) -> (AppResult<T>, Observed)
    where
        F: FnOnce(Arc<Connected>) -> Fut,
        Fut: Future<Output = AppResult<T>>,
    {
        let c = match self.connected(id, false).await {
            Ok(c) => c,
            Err(e) => return (Err(e), Observed::default()),
        };
        let (result, seen) = observe(f(c.clone())).await;
        // Ошибка команды, а если она не о клиенте — ошибка части экрана или вида списка.
        let broken = match &result {
            Err(e) if breaks_client(e.code) => Some(e),
            _ => seen.broken.as_ref(),
        };
        if let Some(e) = broken {
            self.evict_if_broken_client(id, &c, e);
        }
        (result, seen)
    }

    /// Сбрасывает клиент `used` после ошибки запроса им, если с этими учётными данными или TLS
    /// он работать не будет: следующий вызов подключится заново и подхватит новый токен.
    /// Сбрасывает, только пока в пуле он сам (`ClientPool::evict_if_same`): ответ на запрос,
    /// ушедший прежним клиентом, не выбивает клиент, к которому уже подключились заново. Вместе
    /// с клиентом забывается кэш прав подключения — с новым токеном права могут быть другими.
    /// `true` — клиент сброшен.
    pub fn evict_if_broken_client(&self, id: &str, used: &Arc<Connected>, e: &AppError) -> bool {
        let evicted = breaks_client(e.code) && self.pool.evict_if_same(id, used);
        if evicted {
            self.access.forget_connection(id);
        }
        evicted
    }

    /// Сбрасывает клиент подключения и кэш его прав в проектах: со следующим подключением
    /// (новый токен, другой профиль) права могут быть другими.
    pub fn evict_connection(&self, id: &str) {
        self.pool.evict(id);
        self.access.forget_connection(id);
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

    /// Права проекта в кэше подключения (пустые).
    fn cache_rights(st: &AppState, connection_id: &str, namespace: &str) {
        let epoch = st.access.epoch(connection_id);
        let rights = crate::k8s::access::NamespaceAccess::default();
        assert!(st.access.put(connection_id, namespace, epoch, rights));
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

    /// То же через `read`: одновременные чтения вкладки подключаются один раз и получают один
    /// итог — и сбой плагина, и отмену.
    #[cfg(unix)]
    #[tokio::test]
    async fn concurrent_reads_share_the_failure_and_the_cancel() {
        let read = |st: AppState| async move { st.read("e", |_| async { Ok(()) }).await };
        waiters_share_the_failure(read).await;
        cancel_reaches_every_waiter(read).await;
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

    /// Клиент для пула: мок API-сервера OpenShift; сведения о подключении — из `probe`, без
    /// литерала структуры. Каждый вызов — новый клиент.
    async fn pooled_client() -> Arc<Connected> {
        let (client, _) =
            crate::k8s::testing::mock_client(crate::k8s::cluster::fixtures::openshift);
        let timeout = std::time::Duration::from_secs(1);
        let info = crate::k8s::cluster::probe(&client, timeout).await.unwrap();
        Arc::new(Connected {
            client,
            info,
            timeout,
            expires_at: None,
        })
    }

    /// Ошибка учётных данных или TLS от клиента из пула сбрасывает его вместе с кэшем прав
    /// подключения, другие ошибки — нет.
    #[tokio::test]
    async fn credential_error_of_the_pooled_client_evicts_it() {
        let dir = tempfile::tempdir().unwrap();
        let st = state(dir.path());
        let pooled = pooled_client().await;
        for (code, evicts) in [
            (ErrorCode::Forbidden, false),
            (ErrorCode::Timeout, false),
            (ErrorCode::SessionExpired, true),
            (ErrorCode::AuthFailed, true),
            (ErrorCode::TlsError, true),
        ] {
            assert!(
                st.pool
                    .insert_if_current("a", st.pool.generation("a"), pooled.clone())
            );
            cache_rights(&st, "a", "team-a");
            let expected = pooled.clone();
            let e = st
                .read("a", move |c| async move {
                    assert!(Arc::ptr_eq(&c, &expected), "чтение получило клиент из пула");
                    Err::<(), _>(AppError::localized(code, LocalizedText::raw("x")))
                })
                .await
                .unwrap_err();
            assert_eq!(e.code, code);
            assert_eq!(st.pool.get("a").is_none(), evicts, "{code:?}");
            assert_eq!(
                st.access.get("a", "team-a").is_none(),
                evicts,
                "{code:?}: кэш прав"
            );
        }
    }

    /// Ошибка запроса прежним клиентом не выбивает новый: пока запрос шёл, клиент сбросили и
    /// подключились заново.
    #[tokio::test]
    async fn stale_error_of_a_replaced_client_keeps_the_new_one() {
        let dir = tempfile::tempdir().unwrap();
        let st = state(dir.path());
        let (old, new) = (pooled_client().await, pooled_client().await);
        assert!(st.pool.insert_if_current("a", st.pool.generation("a"), old));
        cache_rights(&st, "a", "team-a");
        let pool = st.pool.clone();
        let replacement = new.clone();
        let e = st
            .read("a", move |_| async move {
                pool.evict("a");
                assert!(pool.insert_if_current("a", pool.generation("a"), replacement));
                Err::<(), _>(AppError::localized(
                    ErrorCode::SessionExpired,
                    LocalizedText::raw("x"),
                ))
            })
            .await
            .unwrap_err();
        assert_eq!(e.code, ErrorCode::SessionExpired);
        let kept = st.pool.get("a").expect("новый клиент остался в пуле");
        assert!(Arc::ptr_eq(&kept, &new));
        assert!(
            st.access.get("a", "team-a").is_some(),
            "кэш прав нового клиента не тронут"
        );
    }

    #[tokio::test]
    async fn failed_read_of_an_unknown_connection_reports_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let st = state(dir.path());
        let e = st.read("nope", |_| async { Ok(()) }).await.unwrap_err();
        assert_eq!(e.code, ErrorCode::NotFound);
    }

    #[tokio::test]
    async fn forbidden_read_in_a_project_forgets_its_rights() {
        let dir = tempfile::tempdir().unwrap();
        let st = state(dir.path());
        let connected = crate::k8s::pool::test_connected(|_| (200, serde_json::json!({})));
        assert!(
            st.pool
                .insert_if_current("a", st.pool.generation("a"), connected)
        );
        cache_rights(&st, "a", "team-a");
        cache_rights(&st, "a", "team-b");
        let ok = st.read_in("a", "team-a", |_| async { Ok(1) }).await;
        assert_eq!(ok.unwrap(), 1);
        assert!(st.access.get("a", "team-a").is_some());
        let e = st
            .read_in("a", "team-a", |_| async {
                Err::<(), _>(crate::k8s::errors::not_allowed(
                    "list",
                    "pods",
                    Some("team-a"),
                ))
            })
            .await
            .unwrap_err();
        assert_eq!(e.code, ErrorCode::Forbidden);
        assert!(
            st.access.get("a", "team-a").is_none(),
            "права проекта перечитаются"
        );
        assert!(st.access.get("a", "team-b").is_some());
        assert!(st.pool.get("a").is_some(), "403 клиент не сбрасывает");
    }

    /// Подключение `a` в пуле: мок API-сервера с ответами `handler`.
    fn pool_mock(
        st: &AppState,
        handler: impl Fn(&crate::k8s::testing::Seen) -> (u16, serde_json::Value) + Send + 'static,
    ) {
        let connected = crate::k8s::pool::test_connected(handler);
        assert!(
            st.pool
                .insert_if_current("a", st.pool.generation("a"), connected)
        );
    }

    fn empty_list() -> (u16, serde_json::Value) {
        (200, crate::k8s::workloads::fixtures::list("X", vec![]))
    }

    /// 403 одного вида списка: `list_workloads` вернул `Ok`, ошибка — в `errors`, а права
    /// проекта всё равно перечитаются; права другого проекта и клиент не тронуты.
    #[tokio::test]
    async fn forbidden_kind_of_a_list_forgets_the_rights_of_its_project() {
        let dir = tempfile::tempdir().unwrap();
        let st = state(dir.path());
        pool_mock(&st, |s| match s.path() {
            "/apis/batch/v1/namespaces/team-a/cronjobs" => crate::k8s::testing::status(
                403,
                "Forbidden",
                r#"cronjobs.batch is forbidden: User "u" cannot list resource "cronjobs" in API group "batch" in the namespace "team-a""#,
            ),
            _ => empty_list(),
        });
        cache_rights(&st, "a", "team-a");
        cache_rights(&st, "a", "team-b");
        let l = st
            .read_in("a", "team-a", |c| async move {
                Ok(
                    crate::k8s::workloads::list_workloads(
                        &c.reader(),
                        "team-a",
                        &c.info.api_groups,
                    )
                    .await,
                )
            })
            .await
            .unwrap();
        assert_eq!(l.errors.len(), 1);
        assert_eq!(l.errors[0].error.code, ErrorCode::Forbidden);
        assert!(
            st.access.get("a", "team-a").is_none(),
            "права проекта перечитаются"
        );
        assert!(st.access.get("a", "team-b").is_some());
        assert!(st.pool.get("a").is_some(), "403 клиент не сбрасывает");
    }

    /// 401 части экрана (HPA): подробности пришли (`Ok`), а клиент, которым читали, сброшен
    /// вместе с кэшем прав подключения.
    #[tokio::test]
    async fn expired_session_in_a_part_evicts_the_client() {
        let dir = tempfile::tempdir().unwrap();
        let st = state(dir.path());
        pool_mock(&st, |s| match s.path() {
            "/apis/apps/v1/namespaces/team-a/deployments/web" => (
                200,
                crate::k8s::workloads::fixtures::deployment("web", 1, 1),
            ),
            "/apis/autoscaling/v2/namespaces/team-a/horizontalpodautoscalers" => {
                crate::k8s::testing::status(401, "Unauthorized", "Unauthorized")
            }
            _ => empty_list(),
        });
        cache_rights(&st, "a", "team-a");
        cache_rights(&st, "a", "team-b");
        let d = st
            .read_in("a", "team-a", |c| async move {
                crate::k8s::workloads::get_workload(
                    &c.reader(),
                    "team-a",
                    crate::k8s::workloads::WorkloadKind::Deployment,
                    "web",
                )
                .await
            })
            .await
            .unwrap();
        assert_eq!(d.hpa.error.unwrap().code, ErrorCode::SessionExpired);
        assert!(d.pdb.error.is_none());
        assert!(
            st.pool.get("a").is_none(),
            "следующее чтение подключится заново"
        );
        assert!(
            st.access.get("a", "team-b").is_none(),
            "кэш прав подключения забыт"
        );
    }

    /// Чтение без проекта (`read`, обзор кластера): 403 части экрана не трогает ни права
    /// проектов, ни клиент, а 401 части сбрасывает клиент.
    #[tokio::test]
    async fn parts_of_a_cluster_read_count_only_for_the_client() {
        let dir = tempfile::tempdir().unwrap();
        let st = state(dir.path());
        let overview = |st: AppState| async move {
            st.read("a", |c| async move {
                Ok(crate::k8s::overview::cluster_overview(&c.reader(), &c.info).await)
            })
            .await
            .unwrap()
        };
        pool_mock(&st, |s| match s.path() {
            "/apis/networking.k8s.io/v1/ingressclasses" => crate::k8s::testing::status(
                403,
                "Forbidden",
                r#"ingressclasses.networking.k8s.io is forbidden: User "u" cannot list resource "ingressclasses" in API group "networking.k8s.io" at the cluster scope"#,
            ),
            _ => empty_list(),
        });
        cache_rights(&st, "a", "team-a");
        let o = overview(st.clone()).await;
        assert_eq!(o.ingress_classes.error.unwrap().code, ErrorCode::Forbidden);
        assert!(st.access.get("a", "team-a").is_some());
        assert!(st.pool.get("a").is_some());

        pool_mock(&st, |s| match s.path() {
            "/apis/networking.k8s.io/v1/ingressclasses" => {
                crate::k8s::testing::status(401, "Unauthorized", "Unauthorized")
            }
            _ => empty_list(),
        });
        let o = overview(st.clone()).await;
        assert_eq!(
            o.ingress_classes.error.unwrap().code,
            ErrorCode::SessionExpired
        );
        assert!(st.pool.get("a").is_none());
    }

    #[test]
    fn evicting_a_client_forgets_the_rights_of_the_connection() {
        let dir = tempfile::tempdir().unwrap();
        let st = state(dir.path());
        cache_rights(&st, "a", "team-a");
        st.evict_connection("a");
        assert!(st.access.get("a", "team-a").is_none());
    }

    /// Новое подключение («Подключиться заново», вход через браузер, вставленный токен)
    /// забывает и кэш прав: с новым токеном права могут быть другими.
    #[tokio::test]
    async fn fresh_connect_forgets_the_rights_of_the_connection() {
        let dir = tempfile::tempdir().unwrap();
        let st = state(dir.path());
        cache_rights(&st, "a", "team-a");
        // Профиля нет: подключение не удастся, но клиент сбрасывается до чтения профиля.
        let e = st.connected("a", true).await.err().unwrap();
        assert_eq!(e.code, ErrorCode::NotFound);
        assert!(st.access.get("a", "team-a").is_none());
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
}
