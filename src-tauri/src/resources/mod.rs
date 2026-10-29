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

/// Статистика за окно и рекомендация с ограничениями кластера; `nodes` — кэш нод подключения
/// (`Connected::nodes`), если ноды читаются: список нод читается раз в 5 минут, а не при каждом
/// анализе.
#[allow(clippy::too_many_arguments)]
pub async fn analyze(
    r: &ReadClient,
    sel: &Selected,
    namespace: &str,
    kind: WorkloadKind,
    name: &str,
    days: u32,
    now: i64,
    nodes: Option<&constraints::NodeCache>,
) -> AppResult<Analysis> {
    // Future сбора велики для стека потока в debug-сборке — в куче.
    let usage = Box::pin(usage::workload_usage(
        r, sel, namespace, kind, name, days, now,
    ))
    .await?;
    let c = Box::pin(constraints::read_constraints(
        r, namespace, kind, name, nodes,
    ))
    .await;
    Ok(Analysis {
        recommendation: recommend::recommend(&usage, &c),
        usage,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::*;
    use crate::k8s::testing::mock_client;
    use crate::resources::usage::tests::{NOW, cluster, source_status};

    /// Два анализа подряд (вкладка «Ресурсы» открыта дважды, `recommend_resources`) читают
    /// список нод один раз: allocatable самой крупной ноды живёт в подключении.
    #[tokio::test]
    async fn two_analyses_read_the_nodes_once() {
        let reads = Arc::new(AtomicUsize::new(0));
        let seen = reads.clone();
        let (client, _) = mock_client(move |s| {
            if s.path() != "/api/v1/nodes" {
                return cluster(s);
            }
            seen.fetch_add(1, Ordering::SeqCst);
            (
                200,
                serde_json::json!({"apiVersion": "v1", "kind": "NodeList", "metadata": {},
                    "items": [{"metadata": {"name": "a"},
                        "status": {"allocatable": {"cpu": "4", "memory": "8Gi"}}}]}),
            )
        });
        let r = ReadClient::new(client, Duration::from_secs(5));
        let sel = Selected {
            history: None,
            status: source_status(true),
        };
        let nodes = constraints::NodeCache::default();
        for _ in 0..2 {
            analyze(
                &r,
                &sel,
                "team-a",
                WorkloadKind::Deployment,
                "web",
                7,
                NOW,
                Some(&nodes),
            )
            .await
            .unwrap();
        }
        assert_eq!(reads.load(Ordering::SeqCst), 1, "второй анализ — из кэша");
        // Без права на ноды они не читаются вовсе.
        analyze(
            &r,
            &sel,
            "team-a",
            WorkloadKind::Deployment,
            "web",
            7,
            NOW,
            None,
        )
        .await
        .unwrap();
        assert_eq!(reads.load(Ordering::SeqCst), 1);
    }
}
