//! Проверка кластера и проектов, замечания LLM (спец. §8): снимок, правила, находки.
//! Кластер здесь только читается — изменяющих запросов нет и быть не должно.

/// Событие интерфейсу: изменилось состояние проверки или замечаний подключения.
pub const CHANGED_EVENT: &str = "health://changed";

pub mod collect;
pub mod model;
pub mod rules;
pub mod service;
pub mod snapshot;
pub mod store;
pub mod texts;
