//! Работа с API-сервером: сборка клиента из профиля, ошибки, подключение и проверка.

pub mod cluster;
pub mod config;
pub mod connectivity;
pub mod errors;
pub mod kubeconfig;
pub mod nodes;
pub mod openshift;
pub mod overview;
pub mod pool;
pub mod read;
#[cfg(test)]
pub(crate) mod testing;
pub mod view;
