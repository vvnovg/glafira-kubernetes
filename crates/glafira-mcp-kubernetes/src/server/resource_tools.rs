//! Инструменты анализа ресурсов (спец. §7, §10.4): статистика потребления workload'а и
//! рекомендация `requests` / `limits`. Кластер только читается: метрики — у источника, который
//! выбрала Glafira; без источника истории — текущие значения `metrics.k8s.io` с
//! `confidence: "low"`, без них — `metrics_unavailable` с причинами.

use glafira_mcp_core::fit::MAX_RESPONSE_BYTES;
use glafira_mcp_core::{BackendError, to_value};
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::CallToolResult;
use rmcp::service::RequestContext;
use rmcp::{ErrorData, RoleServer, tool, tool_router};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::KubeServer;
use crate::model::{ReadOp, ReadOutput, WorkloadRef};
use crate::resources::{WINDOW_DEFAULT, WINDOWS};

/// Ряды не поместились в предел ответа.
const SERIES_OMITTED_HINT: &str =
    "The series were omitted to fit the 100 KB response limit; ask for a shorter window";

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ResourceParams {
    /// Connection id from list_connections.
    pub connection_id: String,
    /// Project (namespace) from list_projects.
    pub namespace: String,
    pub workload: WorkloadRef,
    /// Analysis window in days: 1, 7, 14 or 30. Default: 7.
    #[serde(default)]
    pub window_days: Option<u32>,
}

fn window(p: &ResourceParams) -> Result<u32, BackendError> {
    let days = p.window_days.unwrap_or(WINDOW_DEFAULT);
    if WINDOWS.contains(&days) {
        return Ok(days);
    }
    Err(BackendError::invalid_argument(format!(
        "windowDays must be 1, 7, 14 or 30, got {days}"
    )))
}

impl KubeServer {
    async fn usage(&self, p: &ResourceParams) -> Result<Value, BackendError> {
        let op = ReadOp::ResourceUsage {
            connection_id: p.connection_id.clone(),
            namespace: p.namespace.clone(),
            workload: p.workload.clone(),
            window_days: window(p)?,
        };
        let ReadOutput::ResourceUsage(mut u) = self.read(op).await? else {
            return Err(BackendError::unexpected());
        };
        // Ряды — самая большая часть ответа: не поместились — без них.
        let size = serde_json::to_vec(&u).map_or(0, |v| v.len());
        if size > MAX_RESPONSE_BYTES {
            u.series = None;
            let mut v = to_value(&u)?;
            v["hint"] = json!(SERIES_OMITTED_HINT);
            return Ok(v);
        }
        to_value(&u)
    }

    async fn recommendation(&self, p: &ResourceParams) -> Result<Value, BackendError> {
        let op = ReadOp::RecommendResources {
            connection_id: p.connection_id.clone(),
            namespace: p.namespace.clone(),
            workload: p.workload.clone(),
            window_days: window(p)?,
        };
        match self.read(op).await? {
            ReadOutput::Recommendation(r) => to_value(&r),
            _ => Err(BackendError::unexpected()),
        }
    }
}

#[tool_router(router = resource_router, vis = "pub(crate)")]
impl KubeServer {
    #[tool(
        description = "Resource usage of a workload over a window (1, 7, 14 or 30 days) for each container: current requests and limits (with values set by LimitRange defaults), CPU p50/p95/p99/max and memory p95/p99/max of the worst pod as quantities, CPU throttling, OOM kills and restarts (when kube-state-metrics is available), series for charts (cpuCores, memoryMib as [secondsAgo, value]), the metrics source, the coverage of the window and the sources that were rejected. Without a history source only the current values of metrics.k8s.io are returned.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            open_world_hint = false
        )
    )]
    async fn get_resource_usage(
        &self,
        Parameters(p): Parameters<ResourceParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let args = serde_json::to_value(&p).unwrap_or(Value::Null);
        Ok(self
            .session
            .call(
                &ctx,
                "get_resource_usage",
                args,
                Some(&p.connection_id),
                self.usage(&p),
            )
            .await)
    }

    #[tool(
        description = "Recommended requests and limits for each container of a workload from its actual usage over a window (1, 7, 14 or 30 days), with notes (CPU throttled by the limit, OOM kills, no CPU limit, LimitRange bounds, JVM or Go memory settings), project and cluster constraints (quota room with replicas and rollout surge, the largest node), the HPA target shift and the change of total requests and limits. confidence is high, medium or low: say so to the user when it is low. Start a resources review with this tool; propose the values with propose_change (kind setResources).",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            open_world_hint = false
        )
    )]
    async fn recommend_resources(
        &self,
        Parameters(p): Parameters<ResourceParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let args = serde_json::to_value(&p).unwrap_or(Value::Null);
        Ok(self
            .session
            .call(
                &ctx,
                "recommend_resources",
                args,
                Some(&p.connection_id),
                self.recommendation(&p),
            )
            .await)
    }
}
