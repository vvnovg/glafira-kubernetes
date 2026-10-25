//! Работа с API-сервером: сборка клиента из профиля, ошибки, подключение и проверка.

pub mod access;
pub mod cluster;
pub mod config;
pub mod connectivity;
pub mod containers;
pub mod errors;
pub mod events;
pub mod kubeconfig;
pub mod network;
pub mod nodes;
pub mod openshift;
pub mod overview;
pub mod pods;
pub mod pool;
pub mod projects;
pub mod read;
pub mod selectors;
#[cfg(test)]
pub(crate) mod testing;
pub mod view;
pub mod workloads;
