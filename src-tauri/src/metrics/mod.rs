//! Метрики потребления ресурсов (спец. §7.1, §7.2): источники с историей (Thanos Querier,
//! Prometheus через прокси API-сервера, свой URL) за трейтом `MetricsSource`, текущие значения
//! `metrics.k8s.io`, выбор источника и PromQL-шаблоны — чистые функции.

pub mod discovery;
pub mod http;
pub mod pods;
pub mod prometheus;
pub mod promql;
pub mod select;
