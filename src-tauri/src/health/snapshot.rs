//! Снимок кластера, по которому считаются правила (спец. §8.1). Только поля, нужные правилам:
//! значений env, ConfigMap и Secret здесь нет (у Secret — только имена). Сборка — `collect.rs`,
//! правила — `rules/`. Время — секунды Unix.

use std::collections::{BTreeMap, BTreeSet};

use crate::k8s::pods::OwnerRef;
use crate::k8s::workloads::WorkloadKind;
use crate::platform::PlatformKind;

use super::model::TargetKind;

/// Условие объекта (`status.conditions`): статус, причина, сообщение и с какого момента.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Cond {
    pub status: String,
    pub reason: Option<String>,
    pub message: Option<String>,
    pub since: Option<i64>,
}

impl Cond {
    pub fn is(&self, status: &str) -> bool {
        self.status == status
    }
}

/// Ответ `/readyz?verbose`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Readyz {
    pub ok: bool,
    /// Непройденные проверки (`[-]…`).
    pub failed: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorSnap {
    pub name: String,
    pub degraded: bool,
    pub available: Option<bool>,
    pub message: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OpenShiftSnap {
    /// `None` — ClusterOperators не прочитаны.
    pub operators: Option<Vec<OperatorSnap>>,
    /// Условие `Failing` у ClusterVersion; внешний `None` — ClusterVersion не прочитан.
    pub failing: Option<Option<Cond>>,
    /// Деградировавшие MachineConfigPools; `None` — не прочитаны.
    pub degraded_pools: Option<Vec<String>>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NodeSnap {
    pub name: String,
    /// Условие `Ready`; `None` — его нет.
    pub ready: Option<Cond>,
    /// Условия давления, которые сейчас `True`: `MemoryPressure`, `DiskPressure`, `PIDPressure`.
    pub pressures: Vec<String>,
    pub unschedulable: bool,
    pub allocatable_cpu_milli: Option<i64>,
    pub allocatable_memory: Option<i64>,
}

/// Сумма requests подов ноды.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Requests {
    pub cpu_milli: i64,
    pub memory: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IngressClasses {
    pub names: BTreeSet<String>,
    pub has_default: bool,
}

/// Завершение контейнера: причина, код выхода, когда.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Exit {
    pub reason: Option<String>,
    pub exit_code: i32,
    pub finished: Option<i64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ContainerSnap {
    pub name: String,
    /// Init-контейнер (в том числе нативный sidecar).
    pub init: bool,
    /// Нативный sidecar: init-контейнер с `restartPolicy: Always`.
    pub sidecar: bool,
    pub image: String,
    /// В цикле перезапусков (`k8s::pods::in_crash_loop`).
    pub crash_loop: bool,
    pub waiting_reason: Option<String>,
    pub waiting_message: Option<String>,
    /// Завершение сейчас (`state.terminated`) и прошлое (`lastState.terminated`).
    pub exit: Option<Exit>,
    pub last_exit: Option<Exit>,
    pub restarts: i32,
    pub cpu_request: bool,
    pub memory_request: bool,
    pub memory_limit: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PodSnap {
    pub name: String,
    pub phase: String,
    pub created: Option<i64>,
    /// Владелец-workload или прямой владелец (`k8s::pods::owner_of`).
    pub owner: Option<OwnerRef>,
    pub node: Option<String>,
    pub ready: bool,
    /// Условие `Ready` = `False` — с какого момента.
    pub not_ready_since: Option<i64>,
    /// Условие `PodScheduled`.
    pub scheduled: Option<Cond>,
    pub terminating: bool,
    pub containers: Vec<ContainerSnap>,
}

/// Контейнер шаблона workload'а.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TemplateContainer {
    pub name: String,
    pub readiness_probe: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkloadSnap {
    pub kind: WorkloadKind,
    pub name: String,
    pub created: Option<i64>,
    pub desired: Option<i32>,
    pub available: Option<i32>,
    /// Условия `Progressing` и `Available` (Deployment, DeploymentConfig).
    pub progressing: Option<Cond>,
    pub available_cond: Option<Cond>,
    /// Основные контейнеры шаблона подов.
    pub containers: Vec<TemplateContainer>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HpaSnap {
    pub name: String,
    /// Вид и имя `scaleTargetRef`.
    pub target_kind: String,
    pub target_name: String,
    pub min_replicas: Option<i32>,
    pub max_replicas: i32,
    pub current_replicas: Option<i32>,
    pub conditions: BTreeMap<String, Cond>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServicePort {
    pub name: Option<String>,
    pub port: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceSnap {
    pub name: String,
    /// `ClusterIP`, `NodePort`, `LoadBalancer`, `ExternalName`.
    pub kind: String,
    pub has_selector: bool,
    pub ports: Vec<ServicePort>,
    /// Готовые endpoints по EndpointSlices; `None` — они не прочитаны.
    pub ready_endpoints: Option<i32>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RouteSnap {
    pub name: String,
    pub service: Option<String>,
    /// `None` — роутер не написал статус; иначе условие `Admitted`.
    pub admitted: Option<bool>,
    pub admitted_reason: Option<String>,
    pub admitted_message: Option<String>,
    /// Срок встроенного сертификата (`spec.tls.certificate`).
    pub cert_expires: Option<i64>,
    /// Ingress, из которого OpenShift создал Route.
    pub from_ingress: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IngressSnap {
    pub name: String,
    /// `spec.ingressClassName` или аннотация `kubernetes.io/ingress.class`.
    pub class: Option<String>,
    /// Бэкенды правил и `defaultBackend`: Service и порт (номер или имя).
    pub backends: Vec<(String, Option<String>)>,
    pub tls_secrets: Vec<String>,
    pub created: Option<i64>,
    /// OpenShift создал из него Routes.
    pub has_routes: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GatewaySnap {
    pub name: String,
    pub created: Option<i64>,
    pub accepted: Option<Cond>,
    pub programmed: Option<Cond>,
}

/// Родитель маршрута в `status.parents`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RouteParentSnap {
    /// `namespace/name` Gateway.
    pub gateway: String,
    pub accepted: Option<Cond>,
    pub resolved_refs: Option<Cond>,
}

/// HTTPRoute или GRPCRoute.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayRouteSnap {
    /// `Httproute` или `Grpcroute`.
    pub kind: TargetKind,
    pub name: String,
    pub created: Option<i64>,
    /// `namespace/name` из `spec.parentRefs`.
    pub parent_refs: Vec<String>,
    pub parents: Vec<RouteParentSnap>,
    /// Service в проекте маршрута из `backendRefs`.
    pub backends: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PvcSnap {
    pub name: String,
    pub phase: Option<String>,
    pub created: Option<i64>,
    pub class: Option<String>,
    /// Поды, которые его монтируют.
    pub mounted_by: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QuotaUse {
    pub resource: String,
    pub used: String,
    pub hard: String,
    pub ratio: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QuotaSnap {
    pub name: String,
    pub entries: Vec<QuotaUse>,
}

/// Проект: `None` у части — она не прочитана (причина — в `Coverage::gaps`) или её вида нет
/// в кластере (нет группы API).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ProjectSnap {
    pub namespace: String,
    pub pods: Option<Vec<PodSnap>>,
    pub workloads: Option<Vec<WorkloadSnap>>,
    pub hpas: Option<Vec<HpaSnap>>,
    pub services: Option<Vec<ServiceSnap>>,
    pub routes: Option<Vec<RouteSnap>>,
    pub ingresses: Option<Vec<IngressSnap>>,
    pub gateways: Option<Vec<GatewaySnap>>,
    pub gateway_routes: Option<Vec<GatewayRouteSnap>>,
    pub pvcs: Option<Vec<PvcSnap>>,
    pub quotas: Option<Vec<QuotaSnap>>,
    /// Имена Secret проекта (значения не читаются); `None` — нет прав на `list secrets`.
    pub secrets: Option<BTreeSet<String>>,
}

/// Часть, которую не удалось прочитать: что, в каком проекте и код ошибки (`FORBIDDEN`…).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Gap {
    pub part: String,
    pub namespace: Option<String>,
    pub code: String,
}

/// Что проверено и что нет: из этого строится `check.partial`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Coverage {
    /// Проектов в охвате до предела.
    pub projects_total: usize,
    pub projects_checked: usize,
    /// Подов больше предела: остальные не проверены.
    pub pod_limit: bool,
    /// Сбор остановлен дедлайном.
    pub timed_out: bool,
    pub gaps: Vec<Gap>,
}

impl Coverage {
    pub fn partial(&self) -> bool {
        self.projects_checked < self.projects_total
            || self.pod_limit
            || self.timed_out
            || !self.gaps.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClusterSnapshot {
    pub now: i64,
    pub platform: PlatformKind,
    /// `None` — `/readyz` не ответил.
    pub readyz: Option<Readyz>,
    /// Только у OpenShift.
    pub openshift: Option<OpenShiftSnap>,
    /// `None` — нет прав на ноды.
    pub nodes: Option<Vec<NodeSnap>>,
    /// Requests подов по нодам; `None` — поды всех проектов не читаются.
    pub node_requests: Option<BTreeMap<String, Requests>>,
    /// `None` — классы Ingress не прочитаны.
    pub ingress_classes: Option<IngressClasses>,
    pub projects: Vec<ProjectSnap>,
    pub coverage: Coverage,
}

impl ClusterSnapshot {
    pub fn empty(now: i64, platform: PlatformKind) -> Self {
        Self {
            now,
            platform,
            readyz: None,
            openshift: None,
            nodes: None,
            node_requests: None,
            ingress_classes: None,
            projects: Vec::new(),
            coverage: Coverage::default(),
        }
    }
}
