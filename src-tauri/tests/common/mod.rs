//! Общее для интеграционных тестов на kind: kubeconfig стенда и подключение из его контекста.
#![allow(dead_code)]

pub mod prometheus;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use glafira_kubernetes_lib::k8s::kubeconfig;
use glafira_kubernetes_lib::policy::PolicyStatus;
use glafira_kubernetes_lib::secrets::{CLIENT_KEY, MemoryStore, SecretStore, TOKEN, secret_key};
use glafira_kubernetes_lib::storage::profile::ConnectionProfile;
use tokio::sync::oneshot;

/// Сигнал отмены, который не срабатывает (отправитель живёт до конца теста).
pub fn no_cancel() -> oneshot::Receiver<()> {
    let (tx, rx) = oneshot::channel();
    std::mem::forget(tx);
    rx
}

pub fn kubeconfig_path() -> Option<PathBuf> {
    let p = std::env::var_os("GO_IT_KUBECONFIG").map(PathBuf::from);
    if p.is_none() {
        eprintln!("GO_IT_KUBECONFIG не задан — интеграционный тест пропущен");
    }
    p
}

/// Подключение из контекста kubeconfig — так же, как импорт в приложении.
pub fn import(path: &Path, context: &str) -> (ConnectionProfile, Arc<MemoryStore>) {
    let kc = kubeconfig::read(path).unwrap();
    let preview = kubeconfig::preview(&kc, path, &[], &PolicyStatus::default(), &|_, _| None);
    let ctx = preview
        .contexts
        .iter()
        .find(|c| c.name == context)
        .unwrap_or_else(|| panic!("нет контекста {context}"));
    let imported = kubeconfig::import_context(&kc, context, ctx).unwrap();
    let secrets = Arc::new(MemoryStore::default());
    if let Some(t) = &imported.token {
        secrets
            .set(&secret_key(&imported.profile.id, TOKEN), t)
            .unwrap();
    }
    if let Some(k) = &imported.client_key {
        secrets
            .set(&secret_key(&imported.profile.id, CLIENT_KEY), k)
            .unwrap();
    }
    (imported.profile, secrets)
}
