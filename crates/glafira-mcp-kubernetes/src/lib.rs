//! MCP-шлюз Glafira for Kubernetes (спец. §10): инструменты, типы ответов для LLM, маскировка и
//! трейт [`Backend`]. Транспорт, сессии, лимиты и журнал — в `glafira-mcp-core`.
//!
//! Крейт не зависит от `kube`, `k8s-openapi`, `reqwest` и крейта приложения: данные он получает
//! через [`Backend`], изменяющих операций в [`ReadOp`] нет. Замечания ([`health`]) и предложения
//! ([`proposals`]) LLM пишутся в Glafira, а не в кластер: изменение применяет человек.

pub mod health;
pub mod logs;
pub mod mask;
pub mod model;
pub mod observe;
pub mod proposals;
pub mod resources;
pub mod server;

pub use model::{Backend, ReadOp, ReadOutput, Scope};
pub use server::{COMPACT_HIDDEN_TOOLS, GatewayOptions, INSTRUCTIONS, instructions, start};
