//! Подключённые кластеры: один `kube::Client` и сведения о подключении на профиль (спец. §4.1).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use tokio::sync::oneshot;

use crate::error::{AppError, AppResult};
use crate::k8s::cluster::{ConnectionInfo, probe};
use crate::k8s::config::{apply_exec_credential, build_kube_config, process_env, request_timeout};
use crate::k8s::errors::from_kube;
use crate::k8s::read::ReadClient;
use crate::secrets::SecretStore;
use crate::storage::profile::{AuthConfig, ConnectionProfile};
use crate::sync::MutexExt;

pub struct Connected {
    pub client: kube::Client,
    pub info: ConnectionInfo,
    /// Таймаут одного запроса (из профиля).
    pub timeout: Duration,
    /// Когда истекают учётные данные exec-плагина (`expirationTimestamp`).
    pub expires_at: Option<SystemTime>,
}

impl Connected {
    /// Чтение кластера этим подключением.
    pub fn reader(&self) -> ReadClient {
        ReadClient::new(self.client.clone(), self.timeout)
    }

    /// Учётные данные истекают в ближайшую минуту: пора запустить плагин снова.
    pub fn expiring(&self, now: SystemTime) -> bool {
        self.expires_at
            .is_some_and(|t| now + Duration::from_secs(60) >= t)
    }
}

/// Клиенты подключений. Поколение растёт при каждом сбросе: клиент, который подключался
/// дольше, чем жил профиль (его сохранили или удалили), в пул не попадает.
#[derive(Default)]
pub struct ClientPool {
    inner: Mutex<PoolInner>,
    /// По замку на подключение: одновременные подключения ждут первое, а не запускают
    /// exec-плагин ещё раз.
    connecting: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

#[derive(Default)]
struct PoolInner {
    clients: HashMap<String, Arc<Connected>>,
    generations: HashMap<String, u64>,
    attempts: HashMap<String, Attempts>,
}

/// Попытки подключения профиля: сколько завершилось и ошибка последней — вместе с поколением,
/// при котором она началась.
#[derive(Default)]
struct Attempts {
    finished: u64,
    failure: Option<(u64, AppError)>,
}

impl ClientPool {
    pub fn get(&self, id: &str) -> Option<Arc<Connected>> {
        self.inner.lock_safe().clients.get(id).cloned()
    }

    pub fn generation(&self, id: &str) -> u64 {
        self.inner
            .lock_safe()
            .generations
            .get(id)
            .copied()
            .unwrap_or(0)
    }

    /// Кладёт клиент, только если подключение не сбрасывали с момента `generation`.
    pub fn insert_if_current(&self, id: &str, generation: u64, c: Arc<Connected>) -> bool {
        let mut inner = self.inner.lock_safe();
        if inner.generations.get(id).copied().unwrap_or(0) != generation {
            return false;
        }
        inner.clients.insert(id.to_string(), c);
        true
    }

    /// Сколько попыток подключения `id` завершилось. Ожидающий очереди запоминает число до
    /// `connect_lock`, чтобы потом узнать, чем кончилась попытка, которую он ждал.
    pub fn finished_attempts(&self, id: &str) -> u64 {
        self.inner
            .lock_safe()
            .attempts
            .get(id)
            .map_or(0, |a| a.finished)
    }

    /// Записывает итог попытки, начатой при поколении `generation`; `None` — удалась.
    pub fn finish_attempt(&self, id: &str, generation: u64, failure: Option<&AppError>) {
        let mut inner = self.inner.lock_safe();
        let a = inner.attempts.entry(id.to_string()).or_default();
        a.finished += 1;
        a.failure = failure.map(|e| (generation, e.clone()));
    }

    /// Ошибка попытки, завершившейся после `seen`, если профиль с её начала не сбрасывали:
    /// ожидавшие очереди получают её, а не запускают exec-плагин или вход ещё раз.
    pub fn failure_since(&self, id: &str, seen: u64) -> Option<AppError> {
        let inner = self.inner.lock_safe();
        let a = inner.attempts.get(id)?;
        let current = inner.generations.get(id).copied().unwrap_or(0);
        match &a.failure {
            Some((g, e)) if a.finished > seen && *g == current => Some(e.clone()),
            _ => None,
        }
    }

    /// Замок подключения `id` на время подключения.
    pub async fn connect_lock(&self, id: &str) -> tokio::sync::OwnedMutexGuard<()> {
        let lock = self
            .connecting
            .lock_safe()
            .entry(id.to_string())
            .or_default()
            .clone();
        lock.lock_owned().await
    }

    /// Сбрасывает клиент подключения. `true` — клиент был.
    pub fn evict(&self, id: &str) -> bool {
        let mut inner = self.inner.lock_safe();
        *inner.generations.entry(id.to_string()).or_default() += 1;
        inner.clients.remove(id).is_some()
    }

    /// Убирает из пула `used`, если там всё ещё он: ошибка запроса, ушедшего прежним клиентом,
    /// не выбивает клиент, к которому уже подключились заново. Поколение не растёт — профиль
    /// не сбрасывали, и подключение, начатое раньше (истекающие учётные данные exec-плагина),
    /// попадёт в пул, а его ошибку получат те, кто его ждал. `true` — клиент убран.
    pub fn evict_if_same(&self, id: &str, used: &Arc<Connected>) -> bool {
        let mut inner = self.inner.lock_safe();
        if !inner.clients.get(id).is_some_and(|c| Arc::ptr_eq(c, used)) {
            return false;
        }
        inner.clients.remove(id);
        true
    }
}

/// Новое подключение: клиент из профиля (keychain — на blocking-потоке), учётные данные
/// exec-плагина (`cancel` прерывает его ожидание) и `probe`.
pub async fn open(
    profile: ConnectionProfile,
    secrets: Arc<dyn SecretStore>,
    cancel: oneshot::Receiver<()>,
) -> AppResult<Connected> {
    let timeout = request_timeout(&profile);
    let p = profile.clone();
    let mut config =
        tokio::task::spawn_blocking(move || build_kube_config(&p, secrets.as_ref(), &process_env))
            .await
            .map_err(|e| AppError::internal_with(e.to_string()))??;
    let mut expires_at = None;
    if matches!(profile.auth, AuthConfig::Exec(_)) {
        let cred =
            crate::auth::exec::credential_for(&profile, cancel, crate::auth::oauth::LOGIN_TIMEOUT)
                .await?;
        expires_at = cred.expires_at;
        apply_exec_credential(&mut config, &cred);
    }
    let client = kube::Client::try_from(config).map_err(|e| from_kube(&e))?;
    let info = probe(&client, timeout).await?;
    Ok(Connected {
        client,
        info,
        timeout,
        expires_at,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::k8s::cluster::Capabilities;

    fn connected() -> Arc<Connected> {
        let (client, _) = crate::k8s::testing::mock_client(|_| (200, serde_json::json!({})));
        Arc::new(Connected {
            client,
            info: ConnectionInfo {
                user: "u".into(),
                groups: vec![],
                server_version: "v1.32.0".into(),
                platform: crate::platform::detect(&Default::default()),
                api_groups: Default::default(),
                capabilities: Capabilities::default(),
            },
            timeout: Duration::from_secs(1),
            expires_at: None,
        })
    }

    #[tokio::test]
    async fn eviction_bumps_generation_and_blocks_stale_insert() {
        let pool = ClientPool::default();
        let g = pool.generation("a");
        assert!(pool.insert_if_current("a", g, connected()));
        assert!(pool.get("a").is_some());
        let stale = pool.generation("a");
        assert!(pool.evict("a"));
        assert!(pool.get("a").is_none());
        assert!(
            !pool.insert_if_current("a", stale, connected()),
            "профиль сменился во время подключения"
        );
        assert!(!pool.evict("a"), "клиента уже нет");
        assert!(pool.insert_if_current("a", pool.generation("a"), connected()));
    }

    #[tokio::test]
    async fn evict_if_same_drops_only_the_client_that_failed() {
        let pool = ClientPool::default();
        let (old, new) = (connected(), connected());
        assert!(!pool.evict_if_same("a", &old), "клиента нет");
        assert!(pool.insert_if_current("a", pool.generation("a"), new.clone()));
        let g = pool.generation("a");
        assert!(!pool.evict_if_same("a", &old), "в пуле уже другой клиент");
        assert!(Arc::ptr_eq(&pool.get("a").unwrap(), &new));
        assert!(pool.evict_if_same("a", &new));
        assert!(pool.get("a").is_none());
        assert_eq!(pool.generation("a"), g, "профиль не сбрасывали");
    }

    #[test]
    fn failure_is_shared_only_with_those_who_waited_for_it() {
        let pool = ClientPool::default();
        let e = AppError::internal_with("boom");
        let seen = pool.finished_attempts("a");
        assert_eq!(pool.failure_since("a", seen).map(|e| e.key), None);
        pool.finish_attempt("a", pool.generation("a"), Some(&e));
        assert_eq!(
            pool.failure_since("a", seen).and_then(|e| e.details),
            Some("boom".into()),
            "ждал эту попытку"
        );
        let later = pool.finished_attempts("a");
        assert!(
            pool.failure_since("a", later).is_none(),
            "пришёл после неё — подключается сам"
        );
        pool.evict("a");
        assert!(
            pool.failure_since("a", seen).is_none(),
            "профиль сбросили — прежняя ошибка не годится"
        );
        pool.finish_attempt("a", pool.generation("a"), None);
        assert!(pool.failure_since("a", seen).is_none(), "удачная попытка");
    }

    #[tokio::test]
    async fn open_reports_missing_token_without_network() {
        let p = crate::storage::profile::sample_profile("a");
        let (_keep, cancel) = oneshot::channel();
        let e = open(p, Arc::new(crate::secrets::MemoryStore::default()), cancel)
            .await
            .err()
            .unwrap();
        assert_eq!(e.key, "errors.connections.tokenMissing");
    }

    #[tokio::test]
    async fn unapproved_exec_plugin_is_not_started() {
        let mut p = crate::storage::profile::sample_profile("a");
        p.auth = AuthConfig::Exec(crate::storage::profile::sample_exec());
        let (_keep, cancel) = oneshot::channel();
        let e = open(p, Arc::new(crate::secrets::MemoryStore::default()), cancel)
            .await
            .err()
            .unwrap();
        assert_eq!(e.key, "errors.exec.notApproved");
    }

    #[tokio::test]
    async fn exec_credentials_expire_a_minute_early() {
        let (client, _) = crate::k8s::testing::mock_client(|_| (200, serde_json::json!({})));
        let now = SystemTime::now();
        let mut c = Connected {
            client,
            info: connected().info.clone(),
            timeout: Duration::from_secs(1),
            expires_at: None,
        };
        assert!(!c.expiring(now), "токен без срока");
        c.expires_at = Some(now + Duration::from_secs(30));
        assert!(c.expiring(now));
        c.expires_at = Some(now + Duration::from_secs(600));
        assert!(!c.expiring(now));
    }

    #[tokio::test]
    async fn connect_lock_serializes_connections_of_one_profile() {
        let pool = ClientPool::default();
        let first = pool.connect_lock("a").await;
        let other = tokio::time::timeout(Duration::from_millis(50), pool.connect_lock("b")).await;
        assert!(other.is_ok(), "другое подключение не ждёт");
        let same = tokio::time::timeout(Duration::from_millis(50), pool.connect_lock("a")).await;
        assert!(same.is_err(), "второе подключение того же профиля ждёт");
        drop(first);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), pool.connect_lock("a"))
                .await
                .is_ok()
        );
    }
}
