//! MCP-шлюз Glafira for Kubernetes (спец. §10): инструменты только для чтения, типы ответов для
//! LLM, маскировка и трейт [`Backend`]. Транспорт, сессии, лимиты и журнал — в `glafira-mcp-core`.
//!
//! Крейт не зависит от `kube`, `k8s-openapi`, `reqwest` и крейта приложения: данные он получает
//! через [`Backend`], изменяющих операций в [`ReadOp`] нет. Замечания LLM ([`health`]) пишутся
//! в Glafira, а не в кластер.

pub mod health;
pub mod mask;
pub mod model;
pub mod observe;

pub use model::{Backend, ReadOp, ReadOutput, Scope};
