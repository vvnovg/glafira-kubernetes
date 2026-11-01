//! Работа с API-сервером: сборка клиента из профиля, ошибки, подключение и проверка.

pub mod access;
pub mod cluster;
pub mod config;
pub mod configs;
pub mod connectivity;
pub mod containers;
pub mod dropapp;
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
pub mod quantity;
pub mod quotas;
pub mod read;
pub mod scheduling;
pub mod selectors;
pub mod storage;
#[cfg(test)]
pub(crate) mod testing;
pub mod view;
pub mod workloads;
pub mod yaml;
