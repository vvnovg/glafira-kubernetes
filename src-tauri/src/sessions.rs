//! Длительные операции — watch открытых списков и логи (спец. §4.1): фоновые задачи tokio с id
//! сессии. Данные уходят во фронтенд каналом пакетами (не больше `MAX_BATCH` элементов или раз
//! в `BATCH_WINDOW`), отмена — `stop_session` через `CancellationToken`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::future::BoxFuture;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::error::{AppError, AppResult};
use crate::k8s::errors::breaks_client;
use crate::k8s::pool::Connected;
use crate::k8s::read::ReadClient;
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

/// Чаще одного раза за столько поток не берёт новый клиент (`StreamClient::renew`): если кластер
/// не принял и новый клиент (токен отозван, учётные данные не те), замена не поможет, и сессия
/// кончается ошибкой.
pub const RENEW_AGAIN: Duration = Duration::from_secs(60);

/// Клиент подключения для потока сессии, который открывается заново (`commands::sessions`:
/// `AppState::connected` без `fresh`). `broken` — клиент, которым поток читал, и ошибка, с
/// которой он дальше не работает (`breaks_client`): до подключения его надо сбросить.
pub type Connect = Arc<
    dyn Fn(Option<(Arc<Connected>, AppError)>) -> BoxFuture<'static, AppResult<Arc<Connected>>>
        + Send
        + Sync,
>;

/// Клиент, которым читает один поток сессии watch или логов (спец. §4.1). Сессия живёт дольше
/// учётных данных exec-плагина (EKS — около 15 минут, kubelogin — час), а `kube::Client` их не
/// обновляет: токен в нём постоянный. Поэтому поток, который открывается снова, берёт клиент у
/// `connect`: перед открытием — `refresh` (тот же клиент, пока он годен, а истекающие учётные
/// данные плагин выдаёт заново — одной попыткой на всех ожидающих), после ошибки, с которой клиент
/// дальше не работает, — `renew` (этот клиент сбрасывается, и подключение — заново). У каждого
/// потока свой `StreamClient` (`fork`): сбросить надо клиент, которым читал этот поток, а не тот,
/// к которому уже подключился соседний. Без `connect` (`fixed`: тесты, сохранение лога) клиент
/// один на всю сессию.
pub struct StreamClient {
    current: Mutex<Current>,
    connect: Option<Connect>,
}

struct Current {
    reader: ReadClient,
    /// Подключение, которому принадлежит `reader`; у `fixed` его нет.
    used: Option<Arc<Connected>>,
    /// Когда поток в последний раз взял новый клиент (`renew`).
    renewed: Option<Instant>,
}

impl StreamClient {
    /// Один клиент на всю сессию.
    pub fn fixed(reader: ReadClient) -> Self {
        Self {
            current: Mutex::new(Current {
                reader,
                used: None,
                renewed: None,
            }),
            connect: None,
        }
    }

    /// Сначала клиент `used`, при открытии заново — от `connect`.
    pub fn new(used: Arc<Connected>, connect: Connect) -> Self {
        Self {
            current: Mutex::new(Current {
                reader: used.reader(),
                used: Some(used),
                renewed: None,
            }),
            connect: Some(connect),
        }
    }

    /// Клиент ещё одного потока той же сессии: сейчас — тот же, дальше — свой.
    pub fn fork(&self) -> Self {
        let c = self.current.lock_safe();
        Self {
            current: Mutex::new(Current {
                reader: c.reader.clone(),
                used: c.used.clone(),
                renewed: None,
            }),
            connect: self.connect.clone(),
        }
    }

    pub fn reader(&self) -> ReadClient {
        self.current.lock_safe().reader.clone()
    }

    fn set(&self, c: Arc<Connected>) {
        let mut current = self.current.lock_safe();
        current.reader = c.reader();
        current.used = Some(c);
    }

    /// Клиент заново перед тем, как открыть поток снова: сессия могла прожить дольше учётных
    /// данных. Ошибка — подключиться не удалось.
    pub async fn refresh(&self) -> AppResult<()> {
        if let Some(connect) = &self.connect {
            self.set(connect(None).await?);
        }
        Ok(())
    }

    /// Новый клиент после ошибки `error`, с которой этот дальше не работает. `false` — заменить
    /// нечем: ошибка не о клиенте, клиент один на сессию или его меняли меньше `RENEW_AGAIN`
    /// назад; тогда поток кончается ошибкой `error`.
    pub async fn renew(&self, error: &AppError) -> AppResult<bool> {
        let Some(connect) = &self.connect else {
            return Ok(false);
        };
        let broken = {
            let mut current = self.current.lock_safe();
            if !breaks_client(error.code)
                || current.renewed.is_some_and(|t| t.elapsed() < RENEW_AGAIN)
            {
                return Ok(false);
            }
            current.renewed = Some(Instant::now());
            current.used.clone().map(|used| (used, error.clone()))
        };
        self.set(connect(broken).await?);
        Ok(true)
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
