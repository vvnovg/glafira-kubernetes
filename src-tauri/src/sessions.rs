//! Длительные операции — watch открытых списков и логи (спец. §4.1): фоновые задачи tokio с id
//! сессии. Данные уходят во фронтенд каналом пакетами (не больше `MAX_BATCH` элементов или раз
//! в `BATCH_WINDOW`), отмена — `stop_session` через `CancellationToken`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::sync::MutexExt;

/// Идущие сессии по id.
#[derive(Default)]
pub struct Sessions {
    inner: Mutex<HashMap<String, Entry>>,
}

struct Entry {
    connection_id: String,
    cancel: CancellationToken,
}

impl Sessions {
    /// Новая сессия подключения: id и сигнал отмены.
    pub fn register(&self, connection_id: &str) -> (String, CancellationToken) {
        let id = uuid::Uuid::new_v4().to_string();
        let cancel = CancellationToken::new();
        self.inner.lock_safe().insert(
            id.clone(),
            Entry {
                connection_id: connection_id.to_string(),
                cancel: cancel.clone(),
            },
        );
        (id, cancel)
    }

    /// Останавливает сессию. `false` — её уже нет.
    pub fn stop(&self, id: &str) -> bool {
        match self.inner.lock_safe().remove(id) {
            Some(e) => {
                e.cancel.cancel();
                true
            }
            None => false,
        }
    }

    /// Останавливает все сессии подключения (отключение, удаление); возвращает их число.
    pub fn stop_for(&self, connection_id: &str) -> usize {
        let mut inner = self.inner.lock_safe();
        let ids: Vec<String> = inner
            .iter()
            .filter(|(_, e)| e.connection_id == connection_id)
            .map(|(id, _)| id.clone())
            .collect();
        for id in &ids {
            if let Some(e) = inner.remove(id) {
                e.cancel.cancel();
            }
        }
        ids.len()
    }

    /// Останавливает все сессии: страница окна загружается заново (перезагрузка, полная
    /// перезагрузка HMR), и каналы прежней страницы никто не читает. Возвращает их число.
    pub fn stop_all(&self) -> usize {
        let mut inner = self.inner.lock_safe();
        let n = inner.len();
        for (_, e) in inner.drain() {
            e.cancel.cancel();
        }
        n
    }

    /// Снимает с учёта сессию, которая завершилась сама.
    pub fn remove(&self, id: &str) {
        self.inner.lock_safe().remove(id);
    }

    pub fn len(&self) -> usize {
        self.inner.lock_safe().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Снимает сессию с учёта, когда её задача завершилась любым путём.
pub struct SessionGuard {
    pub sessions: Arc<Sessions>,
    pub id: String,
}

impl Drop for SessionGuard {
    fn drop(&mut self) {
        self.sessions.remove(&self.id);
    }
}

/// Куда сессия отправляет данные: канал Tauri или, в тестах, вектор. `false` — данные не
/// отправлены (окно закрыто), сессия завершается. Канал Tauri замечает не всё: после
/// перезагрузки страницы отправка проходит (скрипт выполняется, хотя обработчика канала уже
/// нет), а после закрытия окна проходит отправка JSON от 8 КБ. Такие сессии останавливает
/// `Sessions::stop_all` в начале загрузки страницы (`lib.rs`), а отключение и удаление
/// подключения — `stop_for`.
pub trait Sink<T>: Send + Sync + 'static {
    fn send(&self, item: T) -> bool;
}

impl<T: tauri::ipc::IpcResponse + Send + Sync + 'static> Sink<T> for tauri::ipc::Channel<T> {
    fn send(&self, item: T) -> bool {
        tauri::ipc::Channel::send(self, item).is_ok()
    }
}

/// Приёмник для тестов: копит всё, что отправила сессия.
#[cfg(test)]
#[derive(Clone)]
pub struct Collect<T>(pub Arc<Mutex<Vec<T>>>);

#[cfg(test)]
impl<T> Default for Collect<T> {
    fn default() -> Self {
        Self(Arc::new(Mutex::new(Vec::new())))
    }
}

#[cfg(test)]
impl<T: Clone> Collect<T> {
    pub fn items(&self) -> Vec<T> {
        self.0.lock_safe().clone()
    }
}

#[cfg(test)]
impl<T: Send + 'static> Sink<T> for Collect<T> {
    fn send(&self, item: T) -> bool {
        self.0.lock_safe().push(item);
        true
    }
}

/// Не больше стольких элементов в одном пакете.
pub const MAX_BATCH: usize = 200;
/// Не реже, чем раз в столько, уходит неполный пакет.
pub const BATCH_WINDOW: Duration = Duration::from_millis(100);

/// Накопитель пакета: полный пакет отдаётся сразу, неполный — когда прошло `BATCH_WINDOW` с его
/// первого элемента (`is_due`).
pub struct Batch<T> {
    items: Vec<T>,
    since: Option<Instant>,
}

impl<T> Default for Batch<T> {
    fn default() -> Self {
        Self {
            items: Vec::new(),
            since: None,
        }
    }
}

impl<T> Batch<T> {
    /// Добавляет элемент; если пакет заполнен — возвращает его.
    pub fn push(&mut self, item: T) -> Option<Vec<T>> {
        self.since.get_or_insert_with(Instant::now);
        self.items.push(item);
        (self.items.len() >= MAX_BATCH).then(|| self.take())
    }

    /// Пора отправить неполный пакет.
    pub fn is_due(&self) -> bool {
        self.since.is_some_and(|t| t.elapsed() >= BATCH_WINDOW)
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn take(&mut self) -> Vec<T> {
        self.since = None;
        std::mem::take(&mut self.items)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_stop_and_remove() {
        let s = Sessions::default();
        let (id, cancel) = s.register("c");
        assert_eq!(s.len(), 1);
        assert!(s.stop(&id));
        assert!(cancel.is_cancelled());
        assert!(!s.stop(&id), "уже остановлена");
        let (id, cancel) = s.register("c");
        s.remove(&id);
        assert!(s.is_empty());
        assert!(!cancel.is_cancelled(), "завершилась сама — не отменяем");
    }

    #[test]
    fn stopping_a_connection_stops_only_its_sessions() {
        let s = Sessions::default();
        let (_, a1) = s.register("a");
        let (_, a2) = s.register("a");
        let (_, b) = s.register("b");
        assert_eq!(s.stop_for("a"), 2);
        assert!(a1.is_cancelled() && a2.is_cancelled());
        assert!(!b.is_cancelled());
        assert_eq!(s.len(), 1);
        assert_eq!(s.stop_for("missing"), 0);
    }

    #[test]
    fn page_reload_stops_every_session() {
        let s = Sessions::default();
        let (_, a) = s.register("a");
        let (_, b) = s.register("b");
        assert_eq!(s.stop_all(), 2);
        assert!(a.is_cancelled() && b.is_cancelled());
        assert!(s.is_empty());
        assert_eq!(s.stop_all(), 0);
    }

    #[test]
    fn guard_removes_the_finished_session() {
        let s = Arc::new(Sessions::default());
        let (id, _) = s.register("a");
        drop(SessionGuard {
            sessions: s.clone(),
            id,
        });
        assert!(s.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn batches_are_full_or_due() {
        let mut b = Batch::default();
        assert!(!b.is_due() && b.is_empty());
        for i in 0..MAX_BATCH - 1 {
            assert!(b.push(i).is_none());
        }
        assert_eq!(b.push(0).map(|v| v.len()), Some(MAX_BATCH), "полный пакет");
        assert!(b.is_empty() && !b.is_due());
        b.push(1);
        tokio::time::advance(Duration::from_millis(50)).await;
        assert!(!b.is_due());
        tokio::time::advance(Duration::from_millis(50)).await;
        assert!(b.is_due());
        assert_eq!(b.take(), vec![1]);
        assert!(!b.is_due());
    }
}
