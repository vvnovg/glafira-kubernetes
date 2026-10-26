//! Сессия MCP глазами приложения: клиент, право на вызовы, ошибка для LLM и связь с приложением
//! ([`Hooks`]). Доменных типов здесь нет: их задаёт крейт продукта.

use std::future::Future;
use std::pin::Pin;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientIdent {
    pub name: String,
    pub version: String,
}

/// Сессия глазами приложения: клиент становится известен из `initialize` или из `_meta`
/// первого запроса (протокол 2026-07-28 без `initialize`) и дальше не меняется.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionView {
    pub id: String,
    pub client: Option<ClientIdent>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionAccess {
    Approved,
    Pending,
    Rejected,
    /// MCP запрещён (политика) или выключается.
    Disabled,
}

/// Ошибка для LLM: `code` — стабильный код (`not_allowed`, `FORBIDDEN`…), `message` — по-английски.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendError {
    pub code: String,
    pub message: String,
}

impl BackendError {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }

    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self::new("invalid_argument", message)
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new("not_found", message)
    }

    pub fn unexpected() -> Self {
        Self::new("internal", "Unexpected backend response")
    }
}

/// Связь сервера с приложением: право сессии, имя подключения для журнала, события сессий.
pub trait Hooks: Send + Sync + 'static {
    /// Право сессии на вызовы. Вызывается на каждый запрос — должно быть быстрым.
    fn access(&self, session: &SessionView) -> SessionAccess;
    /// Имя подключения для журнала; `None`, если подключения нет.
    fn connection_name(&self, id: &str) -> Option<String>;
    /// Сессия закрыта (снять ожидающее подтверждение и т. п.).
    fn session_closed(&self, session_id: &str);
    /// Сессии изменились: открыта, закрыта, опознан клиент, начат или закончен вызов.
    fn sessions_changed(&self);
}
