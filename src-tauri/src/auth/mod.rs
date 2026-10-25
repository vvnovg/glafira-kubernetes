//! Вход через браузер в OAuth OpenShift (спец. §5.2): Authorization Code + PKCE, как `oc login --web`;
//! exec-плагины kubeconfig.

pub mod callback;
pub mod exec;
pub mod oauth;
pub mod pkce;

use std::collections::HashMap;
use std::sync::Mutex;

use tokio::sync::oneshot;

use crate::sync::MutexExt;

/// Идущие входы — через браузер и запуском exec-плагина: на подключение — один, новый
/// отменяет прежний.
#[derive(Default)]
pub struct Logins {
    inner: Mutex<LoginsInner>,
}

/// Счётчик входов и идущий вход каждого подключения: номер и сигнал отмены.
#[derive(Default)]
struct LoginsInner {
    last: u64,
    active: HashMap<String, (u64, oneshot::Sender<()>)>,
}

impl Logins {
    /// Регистрирует вход и возвращает его номер и сигнал отмены.
    pub fn begin(&self, id: &str) -> (u64, oneshot::Receiver<()>) {
        let (tx, rx) = oneshot::channel();
        let mut g = self.inner.lock_safe();
        g.last += 1;
        let n = g.last;
        if let Some((_, old)) = g.active.insert(id.to_string(), (n, tx)) {
            let _ = old.send(());
        }
        (n, rx)
    }

    /// Отменяет идущий вход подключения. `false` — входа не было.
    pub fn cancel(&self, id: &str) -> bool {
        match self.inner.lock_safe().active.remove(id) {
            Some((_, tx)) => {
                let _ = tx.send(());
                true
            }
            None => false,
        }
    }

    /// Снимает вход с учёта, если его не заменил более новый.
    pub fn finish(&self, id: &str, n: u64) {
        let mut g = self.inner.lock_safe();
        if g.active.get(id).is_some_and(|(m, _)| *m == n) {
            g.active.remove(id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_login_cancels_previous_and_finish_keeps_newer() {
        let l = Logins::default();
        let (n1, mut rx1) = l.begin("a");
        let (n2, mut rx2) = l.begin("a");
        assert!(rx1.try_recv().is_ok(), "прежний вход отменён");
        l.finish("a", n1);
        assert!(l.cancel("a"), "новый вход остался");
        assert!(rx2.try_recv().is_ok());
        assert!(!l.cancel("a"));
        l.finish("a", n2);
    }
}
