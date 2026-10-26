//! MCP-шлюз для LLM (спец. §10): протокол, сессии, инструменты, маскировка и лимиты — в крейтах
//! `glafira-mcp-core` и `glafira-mcp-kubernetes`; здесь — связь с приложением: `mcp.json`,
//! подтверждение клиентов и реализация `Backend` поверх чтений кластера и проверки.

pub mod approvals;
pub mod backend;
pub mod files;
pub mod map;
