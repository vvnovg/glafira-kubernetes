//! Анализ ресурсов (спец. §7): текущие requests и limits, статистика потребления workload'а и
//! пода по выбранному источнику метрик, движок рекомендаций и ограничения кластера.

pub mod constraints;
pub mod current;
pub mod project;
pub mod recommend;
pub mod usage;

use serde::Serialize;

use crate::error::AppResult;
use crate::k8s::read::ReadClient;
use crate::k8s::workloads::WorkloadKind;
use crate::metrics::select::Selected;

/// Статистика workload'а и рекомендация по ней (вкладка «Ресурсы» workload'а).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Analysis {
    pub usage: usage::WorkloadUsage,
    pub recommendation: recommend::Recommendation,
}

/// Статистика за окно и рекомендация с ограничениями кластера; `nodes` — ноды читаются.
#[allow(clippy::too_many_arguments)]
pub async fn analyze(
    r: &ReadClient,
    sel: &Selected,
    namespace: &str,
    kind: WorkloadKind,
    name: &str,
    days: u32,
    now: i64,
    nodes: bool,
) -> AppResult<Analysis> {
    let usage = usage::workload_usage(r, sel, namespace, kind, name, days, now).await?;
    let c = constraints::read_constraints(r, namespace, kind, name, nodes).await;
    Ok(Analysis {
        recommendation: recommend::recommend(&usage, &c),
        usage,
    })
}
