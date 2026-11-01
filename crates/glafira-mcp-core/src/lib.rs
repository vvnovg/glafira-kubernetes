//! Общая часть MCP-шлюзов Glafira (спец. §3, §10.1, §10.7): транспорт шима (Unix socket или
//! named pipe), рукопожатие с токеном, сессии и право на вызовы, лимиты, журнал и предел размера
//! ответа. Доменных типов здесь нет: инструменты продукта (Kubernetes) строит свой крейт поверх
//! [`server::start`] и вызывает их через [`server::Session::call`].

pub mod audit;
pub mod endpoint;
pub mod fit;
pub mod handshake;
pub mod limits;
pub mod schema;
pub mod server;
pub mod session;

pub use audit::{AuditEvent, AuditLog};
pub use endpoint::{BoxStream, Endpoint};
pub use schema::{inline_refs, normalize_args, shape};
pub use server::{Exclusive, McpServerHandle, Session, SessionSnapshot, Slots, start, to_value};
pub use session::{BackendError, BoxFuture, ClientIdent, Hooks, SessionAccess, SessionView};
