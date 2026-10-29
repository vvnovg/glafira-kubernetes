//! Анализ ресурсов для LLM (спец. §7, §10.4): статистика потребления workload'а за окно и
//! рекомендация `requests` / `limits`. Значения CPU и памяти — строками quantity (`250m`,
//! `512Mi`), их можно сразу предложить в `propose_change`. Адрес своего источника метрик из
//! профиля модель не получает.

use serde::Serialize;

use crate::model::Resources;

/// Окна анализа, дни (спец. §7.2).
pub const WINDOWS: [u32; 4] = [1, 7, 14, 30];
pub const WINDOW_DEFAULT: u32 = 7;

/// Источник метрик.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Source {
    /// thanos, prometheus или metricsServer.
    pub kind: String,
    /// auto (найден автопоиском) или profile (свой из настроек подключения).
    pub origin: String,
    /// Сервис или маршрут кластера; у своего источника — нет.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
    /// Есть история; у metricsServer — только текущее значение.
    pub history: bool,
}

/// Источник, который не подошёл, и почему (forbidden, notFound, unavailable, timeout,
/// badResponse, tokenMissing).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RejectedSource {
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
    pub reason: String,
}

/// Статистика контейнера за окно: «худший под».
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageStats {
    pub cpu_p50: Option<String>,
    pub cpu_p95: Option<String>,
    pub cpu_p99: Option<String>,
    pub cpu_max: Option<String>,
    pub memory_p95: Option<String>,
    pub memory_p99: Option<String>,
    pub memory_max: Option<String>,
    pub throttling_mean_percent: Option<f64>,
    pub throttling_p95_percent: Option<f64>,
    /// `None` — нет kube-state-metrics.
    pub oom_count: Option<u32>,
    pub last_oom_seconds_ago: Option<i64>,
    pub restarts: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerUsage {
    pub name: String,
    pub init: bool,
    pub requests: Resources,
    pub limits: Resources,
    /// Значения работающего пода от `LimitRange` по умолчанию (в шаблоне их нет):
    /// cpuRequest, cpuLimit, memoryRequest, memoryLimit.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub limit_range_defaults: Vec<String>,
    pub stats: UsageStats,
}

/// Ряд по контейнеру: `[секунд назад, значение]`.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerSeries {
    pub container: String,
    pub points: Vec<(i64, f64)>,
}

/// Рестарты или OOM контейнера за шаг.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Marker {
    pub container: String,
    pub seconds_ago: i64,
    pub count: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageSeries {
    pub step_seconds: u64,
    pub cpu_cores: Vec<ContainerSeries>,
    pub memory_mib: Vec<ContainerSeries>,
    pub restarts: Vec<Marker>,
    pub ooms: Vec<Marker>,
}

/// Ответ `get_resource_usage`.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceUsage {
    pub window_days: u32,
    pub source: Source,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub rejected: Vec<RejectedSource>,
    pub coverage_percent: Option<f64>,
    pub data_days: Option<f64>,
    pub kube_state_metrics: bool,
    pub replicas: Option<i32>,
    pub containers: Vec<ContainerUsage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub series: Option<UsageSeries>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerRecommendation {
    pub name: String,
    pub current_requests: Resources,
    pub current_limits: Resources,
    /// `None` — данных о контейнере за окно нет.
    pub recommended_requests: Option<Resources>,
    pub recommended_limits: Option<Resources>,
    /// Пометки словами: троттлинг, OOM, CPU limit не задан, границы LimitRange, JVM и Go.
    pub notes: Vec<String>,
}

/// Разница суммарных requests и limits с учётом реплик (рекомендовано минус сейчас).
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Totals {
    pub requests_cpu_cores: Option<f64>,
    pub requests_memory_mib: Option<f64>,
    pub limits_cpu_cores: Option<f64>,
    pub limits_memory_mib: Option<f64>,
}

/// Ответ `recommend_resources`.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Recommendation {
    pub window_days: u32,
    pub source: Source,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub rejected: Vec<RejectedSource>,
    /// high, medium или low.
    pub confidence: String,
    pub coverage_percent: Option<f64>,
    pub data_days: Option<f64>,
    pub replicas: Option<i32>,
    pub containers: Vec<ContainerRecommendation>,
    /// Ограничения проекта и кластера словами: запас квоты, allocatable ноды.
    pub violations: Vec<String>,
    /// HPA по utilization: смена request сдвигает порог масштабирования.
    pub hpa: Vec<String>,
    pub totals: Totals,
}
