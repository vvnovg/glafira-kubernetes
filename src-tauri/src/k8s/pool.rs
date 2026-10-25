//! Подключённые кластеры: один `kube::Client` и сведения о подключении на профиль (спец. §4.1).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::error::{AppError, AppResult};
use crate::k8s::cluster::{ConnectionInfo, probe};
use crate::k8s::config::{build_kube_config, process_env, request_timeout};
use crate::k8s::errors::from_kube;
use crate::secrets::SecretStore;
use crate::storage::profile::ConnectionProfile;
use crate::sync::MutexExt;

pub struct Connected {
    pub client: kube::Client,
    pub info: ConnectionInfo,
    /// Таймаут одного запроса (из профиля).
    pub timeout: Duration,
}

/// Клиенты подключений. Поколение растёт при каждом сбросе: клиент, который подключался
/// дольше, чем жил профиль (его сохранили или удалили), в пул не попадает.
#[derive(Default)]
pub struct ClientPool {
    inner: Mutex<PoolInner>,
}

#[derive(Default)]
struct PoolInner {
    clients: HashMap<String, Arc<Connected>>,
    generations: HashMap<String, u64>,
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

    /// Сбрасывает клиент подключения. `true` — клиент был.
    pub fn evict(&self, id: &str) -> bool {
        let mut inner = self.inner.lock_safe();
        *inner.generations.entry(id.to_string()).or_default() += 1;
        inner.clients.remove(id).is_some()
    }
}

/// Новое подключение: клиент из профиля (keychain — на blocking-потоке) и `probe`.
pub async fn open(
    profile: ConnectionProfile,
    secrets: Arc<dyn SecretStore>,
) -> AppResult<Connected> {
    let timeout = request_timeout(&profile);
    let config = tokio::task::spawn_blocking(move || {
        build_kube_config(&profile, secrets.as_ref(), &process_env)
    })
    .await
    .map_err(|e| AppError::internal_with(e.to_string()))??;
    let client = kube::Client::try_from(config).map_err(|e| from_kube(&e))?;
    let info = probe(&client, timeout).await?;
    Ok(Connected {
        client,
        info,
        timeout,
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
    async fn open_reports_missing_token_without_network() {
        let p = crate::storage::profile::sample_profile("a");
        let e = open(p, Arc::new(crate::secrets::MemoryStore::default()))
            .await
            .err()
            .unwrap();
        assert_eq!(e.key, "errors.connections.tokenMissing");
    }
}
