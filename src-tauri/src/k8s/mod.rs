//! Работа с API-сервером: сборка клиента из профиля, ошибки, подключение и проверка.

pub mod cluster;
pub mod config;
pub mod connectivity;
pub mod errors;
pub mod kubeconfig;
pub mod pool;
#[cfg(test)]
pub(crate) mod testing;
