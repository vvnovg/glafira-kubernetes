//! Контракт крейта с приложением (спец. §10.4): операции чтения, их результаты, область доступа
//! LLM и трейт [`Backend`]. Типы ответов — собственные (стабильный контракт для LLM), а не DTO
//! интерфейса. Время — в секундах до момента ответа (`ageSeconds`, `…SecondsAgo`): модель не
//! знает, который час.

use std::collections::{BTreeMap, BTreeSet};

use glafira_mcp_core::{BackendError, BoxFuture, Hooks};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::health::{HealthOp, HealthOutput};

/// Нет такого подключения, доступ LLM к нему выключен или проект вне `llmNamespaces` — одна и та
/// же ошибка (спец. §10.2).
pub fn not_allowed() -> BackendError {
    BackendError::new(
        "not_allowed",
        "Unknown connection or project, or LLM access to it is off; call list_connections and list_projects",
    )
}

/// Логи подов запрещены политикой администратора (`mcpLogsEnabled = false`, спец. §10.2).
pub fn logs_disabled() -> BackendError {
    BackendError::new(
        "logs_disabled",
        "Reading pod logs is disabled by the organization's policy",
    )
}

/// У подключения уровень доступа `metadata`: логов и значений конфигурации модель не видит.
pub fn logs_not_allowed() -> BackendError {
    BackendError::new(
        "logs_not_allowed",
        "LLM access to this connection is limited to metadata. Ask the user to switch it to \"Metadata and logs\" in the connection settings in Glafira",
    )
}

/// Лог контейнера недоступен, хотя запрос верен: контейнер ещё не запускался, прошлого запуска
/// нет или среда выполнения узла уже удалила его лог (спец. §10.7). `message` — причина
/// по-английски и что делать дальше.
pub fn log_unavailable(message: impl Into<String>) -> BackendError {
    BackendError::new("log_unavailable", message)
}

/// Доступ к логам и значениям конфигурации (уровень `logs`, спец. §10.2).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum LogsAccess {
    /// Уровень `metadata`: логи и значения не видны.
    #[default]
    Metadata,
    /// Уровень `logs`.
    Allowed,
    /// Уровень `logs`, но политика запрещает логи: подключение работает как `metadata`.
    Disabled,
}

/// Что из подключения видит LLM (спец. §10.2): проекты из `llmNamespaces` или все доступные,
/// доступ к логам и имена полей для маскировки.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Scope {
    /// `None` — все проекты, доступные пользователю.
    pub namespaces: Option<BTreeSet<String>>,
    pub logs: LogsAccess,
    /// Имена полей для маскировки (спец. §10.3); `None` — стандартный список крейта.
    pub mask_fields: Option<Vec<String>>,
}

impl Scope {
    pub fn allows(&self, namespace: &str) -> bool {
        self.namespaces
            .as_ref()
            .is_none_or(|ns| ns.contains(namespace))
    }

    /// Маскировщик с именами полей подключения.
    pub fn masker(&self) -> crate::mask::Masker {
        match &self.mask_fields {
            Some(fields) => crate::mask::Masker::new(fields),
            None => crate::mask::Masker::default(),
        }
    }

    /// Логи доступны; иначе — `logs_disabled` (политика) или `logs_not_allowed` (уровень).
    pub fn require_logs(&self) -> Result<(), BackendError> {
        match self.logs {
            LogsAccess::Allowed => Ok(()),
            LogsAccess::Disabled => Err(logs_disabled()),
            LogsAccess::Metadata => Err(logs_not_allowed()),
        }
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
pub enum WorkloadKind {
    Deployment,
    StatefulSet,
    DaemonSet,
    DeploymentConfig,
    Job,
    CronJob,
}

/// Workload by kind and name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadRef {
    /// Deployment, StatefulSet, DaemonSet, DeploymentConfig, Job or CronJob.
    pub kind: WorkloadKind,
    pub name: String,
}

/// Object the events belong to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ObjectRef {
    /// Kind as in Kubernetes: Pod, Deployment, Service, Node…
    pub kind: String,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadOp {
    ListConnections,
    ClusterOverview {
        connection_id: String,
    },
    /// Все проекты, доступные пользователю; область LLM применяет крейт.
    ListProjects {
        connection_id: String,
    },
    ListNodes {
        connection_id: String,
    },
    DescribeNode {
        connection_id: String,
        name: String,
    },
    ListWorkloads {
        connection_id: String,
        namespace: String,
    },
    DescribeWorkload {
        connection_id: String,
        namespace: String,
        workload: WorkloadRef,
    },
    ListPods {
        connection_id: String,
        namespace: String,
        workload: Option<WorkloadRef>,
    },
    DescribePod {
        connection_id: String,
        namespace: String,
        name: String,
    },
    ListEvents {
        connection_id: String,
        namespace: String,
        object: Option<ObjectRef>,
    },
    Quotas {
        connection_id: String,
        namespace: String,
    },
    Network {
        connection_id: String,
        namespace: String,
    },
    /// Состояние workload'а для `observe_workload`: реплики, поды, предупреждения.
    WorkloadState {
        connection_id: String,
        namespace: String,
        workload: WorkloadRef,
    },
    /// Статистика потребления workload'а за окно (`get_resource_usage`).
    ResourceUsage {
        connection_id: String,
        namespace: String,
        workload: WorkloadRef,
        window_days: u32,
    },
    /// Рекомендация requests и limits (`recommend_resources`).
    RecommendResources {
        connection_id: String,
        namespace: String,
        workload: WorkloadRef,
        window_days: u32,
    },
}

impl ReadOp {
    pub fn connection_id(&self) -> Option<&str> {
        match self {
            ReadOp::ListConnections => None,
            ReadOp::ClusterOverview { connection_id }
            | ReadOp::ListProjects { connection_id }
            | ReadOp::ListNodes { connection_id }
            | ReadOp::DescribeNode { connection_id, .. }
            | ReadOp::ListWorkloads { connection_id, .. }
            | ReadOp::DescribeWorkload { connection_id, .. }
            | ReadOp::ListPods { connection_id, .. }
            | ReadOp::DescribePod { connection_id, .. }
            | ReadOp::ListEvents { connection_id, .. }
            | ReadOp::Quotas { connection_id, .. }
            | ReadOp::Network { connection_id, .. }
            | ReadOp::WorkloadState { connection_id, .. }
            | ReadOp::ResourceUsage { connection_id, .. }
            | ReadOp::RecommendResources { connection_id, .. } => Some(connection_id),
        }
    }

    /// Проект операции: он должен входить в область LLM.
    pub fn namespace(&self) -> Option<&str> {
        match self {
            ReadOp::ListWorkloads { namespace, .. }
            | ReadOp::DescribeWorkload { namespace, .. }
            | ReadOp::ListPods { namespace, .. }
            | ReadOp::DescribePod { namespace, .. }
            | ReadOp::ListEvents { namespace, .. }
            | ReadOp::Quotas { namespace, .. }
            | ReadOp::Network { namespace, .. }
            | ReadOp::WorkloadState { namespace, .. }
            | ReadOp::ResourceUsage { namespace, .. }
            | ReadOp::RecommendResources { namespace, .. } => Some(namespace),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ReadOutput {
    Connections(Vec<Connection>),
    Cluster(ClusterOverview),
    Projects(ProjectList),
    Nodes(NodeList),
    Node(NodeDetails),
    Workloads(WorkloadList),
    Workload(WorkloadDetails),
    Pods(PodList),
    Pod(PodDetails),
    Events(EventList),
    Quotas(Quotas),
    Network(Network),
    WorkloadState(WorkloadState),
    ResourceUsage(crate::resources::ResourceUsage),
    Recommendation(crate::resources::Recommendation),
}

/// Источник данных шлюза. Реализация приложения проверяет `llmAccess` подключения и в `read`,
/// и в `health`; крейт перед каждой операцией сверяет подключение и проект с [`Backend::scope`].
pub trait Backend: Hooks {
    /// Область LLM подключения; `None` — подключения нет или доступ LLM к нему выключен.
    fn scope(&self, connection_id: &str) -> Option<Scope>;
    fn read(&self, op: ReadOp) -> BoxFuture<'_, Result<ReadOutput, BackendError>>;
    /// Проверка кластера и замечания: кластер только читается, замечания пишутся в Glafira.
    fn health(&self, op: HealthOp) -> BoxFuture<'_, Result<HealthOutput, BackendError>>;
    /// Предложения изменений: кластер только читается (проверка предложения), предложение
    /// пишется в Glafira, применяет его человек.
    fn proposals(
        &self,
        op: crate::proposals::ProposalOp,
    ) -> BoxFuture<'_, Result<crate::proposals::ProposalOutput, BackendError>>;
    /// Логи подов (уровень `logs`): сырые строки, маскирует и фильтрует шлюз. Реализация
    /// приложения ещё раз проверяет уровень доступа и политику.
    fn logs(
        &self,
        op: crate::logs::LogsOp,
    ) -> BoxFuture<'_, Result<crate::logs::LogsOutput, BackendError>> {
        let _ = op;
        Box::pin(async {
            Err(BackendError::new(
                "not_supported",
                "Reading pod logs is not supported by this Glafira build",
            ))
        })
    }
}

/// Часть ответа: данные или причина, по которой их нет (права, ошибка чтения).
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub enum Part<T> {
    Data(T),
    Error { error: String },
}

impl<T> Part<T> {
    pub fn error(e: impl Into<String>) -> Self {
        Part::Error { error: e.into() }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum PlatformKind {
    Openshift,
    Dropapp,
    Kubernetes,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Platform {
    pub kind: PlatformKind,
    /// «OpenShift 4.18», «DropApp 5.1.2», «Kubernetes 1.31 (EKS)».
    pub label: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    /// eks, gke, aks, rancher, deckhouse, k3s, rke2.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub distribution: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Connection {
    pub id: String,
    pub name: String,
    pub environment: String,
    pub folder_path: Vec<String>,
    pub read_only: bool,
    pub llm_access: String,
    /// Известна, пока подключение открыто в Glafira; иначе — `null`.
    pub platform: Option<Platform>,
}

/// Условие объекта (`status.conditions`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Condition {
    #[serde(rename = "type")]
    pub kind: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_transition_seconds_ago: Option<i64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Capabilities {
    pub list_projects: bool,
    pub list_nodes: bool,
    pub list_all_pods: bool,
    pub read_cluster_operators: bool,
    pub read_cluster_version: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiServer {
    /// `null` — `/readyz` не ответил (причина в `error`).
    pub ready: Option<bool>,
    pub failed_checks: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RoleCount {
    pub role: String,
    pub total: usize,
    pub ready: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeCounts {
    pub total: usize,
    pub ready: usize,
    pub roles: Vec<RoleCount>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IngressClass {
    pub name: String,
    pub controller: Option<String>,
    pub default: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GatewayClass {
    pub name: String,
    pub controller: Option<String>,
    pub accepted: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Operator {
    pub name: String,
    pub version: Option<String>,
    pub available: Option<bool>,
    pub progressing: Option<bool>,
    pub degraded: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Operators {
    pub total: usize,
    /// Degraded, недоступные или обновляющиеся.
    pub not_ok: Vec<Operator>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MachineConfigPool {
    pub name: String,
    pub degraded: bool,
    pub updating: bool,
    pub machines: i64,
    pub ready: i64,
    pub updated: i64,
    pub degraded_machines: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterVersion {
    pub version: Option<String>,
    pub channel: Option<String>,
    pub available_updates: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failing: Option<Condition>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub progressing: Option<Condition>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenShift {
    pub cluster_version: Part<ClusterVersion>,
    pub infrastructure: Part<Option<String>>,
    pub operators: Part<Operators>,
    /// Только деградировавшие и обновляющиеся пулы.
    pub machine_config_pools_not_ok: Part<Vec<MachineConfigPool>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DropAppMetadata {
    pub cluster_name: Option<String>,
    pub version: Option<String>,
}

/// `Component` или `Tool` DropApp в сбое или обновлении.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DropAppComponent {
    /// `Component` или `Tool`.
    pub kind: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry: Option<i64>,
    /// `причина: сообщение` последнего события `Warning` по объекту.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_warning: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DropAppComponents {
    pub total: usize,
    /// В статусе `failed`, `error` или `upgrading`.
    pub not_ok: Vec<DropAppComponent>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DropAppNode {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DropAppNodes {
    pub total: usize,
    pub not_ready: Vec<DropAppNode>,
}

/// `IngressTLS`, у которого условие `Ready` не `True`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DropAppIngressTls {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DropAppIngressTlsList {
    pub total: usize,
    pub not_ready: Vec<DropAppIngressTls>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DropApp {
    pub metadata: Part<DropAppMetadata>,
    pub components: Part<DropAppComponents>,
    pub nodes: Part<DropAppNodes>,
    pub ingress_tls: Part<DropAppIngressTlsList>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterOverview {
    pub platform: Platform,
    pub server_version: String,
    pub api_server: ApiServer,
    pub nodes: Part<NodeCounts>,
    pub ingress_classes: Part<Vec<IngressClass>>,
    /// `null` — в кластере нет Gateway API.
    pub gateway_classes: Option<Part<Vec<GatewayClass>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub openshift: Option<OpenShift>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dropapp: Option<DropApp>,
    /// Что пользователю разрешено читать на уровне кластера.
    pub capabilities: Capabilities,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Project {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    pub phase: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectList {
    pub items: Vec<Project>,
    pub truncated: bool,
    /// Список проектов не читается — показан список из профиля подключения.
    pub error: Option<String>,
}

/// CPU и память строками quantity (`250m`, `512Mi`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Resources {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeItem {
    pub name: String,
    pub roles: Vec<String>,
    pub ready: bool,
    /// Условия не в норме: `Ready` ≠ True, давление памяти, диска, PID.
    pub conditions: Vec<String>,
    pub unschedulable: bool,
    pub allocatable: Resources,
    /// Сумма requests подов ноды; `null` — поды всех проектов не читаются.
    pub requested: Option<Resources>,
    pub taints: usize,
    pub kubelet_version: Option<String>,
    pub age_seconds: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeList {
    pub items: Vec<NodeItem>,
    pub truncated: bool,
    /// Почему нет `requested` (нет прав на поды всех проектов).
    pub requests_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeUsage {
    pub pods: usize,
    pub requests: Resources,
    pub limits: Resources,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeDetails {
    pub name: String,
    pub roles: Vec<String>,
    pub ready: bool,
    pub unschedulable: bool,
    pub conditions: Vec<Condition>,
    pub taints: Vec<String>,
    /// Метки ноды (их сверяют с `nodeSelector` и node affinity подов); после маскировки, как
    /// аннотации.
    pub labels: BTreeMap<String, String>,
    pub capacity: BTreeMap<String, String>,
    pub allocatable: BTreeMap<String, String>,
    /// Поды ноды и сумма их requests и limits; ошибка — поды всех проектов не читаются.
    pub usage: Part<NodeUsage>,
    pub kubelet_version: Option<String>,
    pub os_image: Option<String>,
    pub container_runtime: Option<String>,
    pub age_seconds: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadItem {
    pub kind: WorkloadKind,
    pub name: String,
    pub desired: Option<i32>,
    pub ready: Option<i32>,
    pub available: Option<i32>,
    pub updated: Option<i32>,
    pub images: Vec<String>,
    pub age_seconds: Option<i64>,
    /// unavailable, rolloutStuck, failed.
    pub problems: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schedule: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suspended: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KindError {
    pub kind: WorkloadKind,
    pub error: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkloadList {
    pub items: Vec<WorkloadItem>,
    /// Виды, которые не прочитались; остальные — в `items`.
    pub errors: Vec<KindError>,
    pub truncated: bool,
}

/// Переменная окружения: имя и источник; значение — только на уровне `logs` (спец. §10.2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Env {
    pub name: String,
    /// literal, configMapKeyRef, secretKeyRef, fieldRef, resourceFieldRef, configMapRef, secretRef.
    pub source: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reference: Option<String>,
    /// Литерал или значение ключа ConfigMap после маскировки (уровень `logs`). Значений Secret
    /// нет ни на каком уровне.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    /// Значение ConfigMap не прочиталось: нет прав, ключа или данные двоичные.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value_error: Option<String>,
    /// Значение обрезано до 4 КБ после маскировки.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub value_cut: bool,
}

/// Контейнер шаблона или пода. `command` и `args` — после маскировки (спец. §10.3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerSpec {
    pub name: String,
    pub image: String,
    pub init: bool,
    pub command: Vec<String>,
    pub args: Vec<String>,
    pub requests: BTreeMap<String, String>,
    pub limits: BTreeMap<String, String>,
    pub ports: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub liveness: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub readiness: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub startup: Option<String>,
    pub env: Vec<Env>,
}

/// Том: имя, вид (`configMap`, `secret`, `persistentVolumeClaim`, `emptyDir`…) и источник.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Volume {
    pub name: String,
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
}

/// Ограничения размещения пода или шаблона подов (спец. §10.4): по ним модель объясняет
/// `FailedScheduling`. Пустые поля и пустой объект в ответ не попадают. Термы affinity —
/// строками: `required: kubernetes.io/arch In [amd64], disktype Exists` (выражения терма —
/// «и», разные термы `required` — «или»), `preferred (weight 50): …`; у pod (anti)affinity —
/// селектор и `topologyKey`. Толерантности — как у `kubectl describe`:
/// `key=value:NoSchedule`, `key:NoExecute op=Exists for 300s`. После маскировки.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Scheduling {
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub node_selector: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub node_affinity: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub pod_affinity: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub pod_anti_affinity: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub tolerations: Vec<String>,
    /// `nodeName` шаблона подов; у пода нода — в поле `node`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub priority_class_name: Option<String>,
    /// Только если не `default-scheduler`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scheduler_name: Option<String>,
}

impl Scheduling {
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Hpa {
    pub name: String,
    pub min_replicas: Option<i32>,
    pub max_replicas: i32,
    pub current_replicas: Option<i32>,
    pub desired_replicas: i32,
    pub metrics: Vec<String>,
    pub conditions: Vec<Condition>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Pdb {
    pub name: String,
    pub min_available: Option<String>,
    pub max_unavailable: Option<String>,
    pub allowed_disruptions: i32,
    pub current_healthy: i32,
    pub desired_healthy: i32,
    pub expected_pods: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Revision {
    pub revision: i64,
    pub name: String,
    pub images: Vec<String>,
    pub age_seconds: Option<i64>,
    pub replicas: Option<i32>,
    pub current: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadDetails {
    #[serde(flatten)]
    pub workload: WorkloadItem,
    pub selector: Option<String>,
    pub strategy: Option<String>,
    pub conditions: Vec<Condition>,
    pub containers: Vec<ContainerSpec>,
    pub volumes: Vec<Volume>,
    /// Ограничения размещения шаблона подов.
    #[serde(skip_serializing_if = "Scheduling::is_empty")]
    pub scheduling: Scheduling,
    pub labels: BTreeMap<String, String>,
    /// После маскировки: без копий объекта, секретные значения — `***`.
    pub annotations: BTreeMap<String, String>,
    pub hpa: Part<Option<Hpa>>,
    pub pdb: Part<Option<Pdb>>,
    /// Ревизии выкладки (нет у Job и CronJob).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub history: Option<Part<Vec<Revision>>>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub active_jobs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Owner {
    pub kind: String,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PodItem {
    pub name: String,
    pub phase: String,
    /// Статус, как в `kubectl get pods`, если он не совпадает с фазой: `CrashLoopBackOff`, `Init:1/2`…
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// imagePull, oomKilled, crashLoop, pending, failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub problem: Option<String>,
    /// Готово / всего контейнеров: `1/2`.
    pub ready: String,
    pub restarts: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_restart_seconds_ago: Option<i64>,
    pub node: Option<String>,
    pub age_seconds: Option<i64>,
    pub owner: Option<Owner>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodList {
    pub items: Vec<PodItem>,
    pub truncated: bool,
}

/// Состояние контейнера: running, waiting или terminated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerState {
    pub state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_seconds_ago: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_seconds_ago: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PodContainer {
    #[serde(flatten)]
    pub spec: ContainerSpec,
    pub image_id: Option<String>,
    pub state: Option<ContainerState>,
    pub last_state: Option<ContainerState>,
    pub ready: bool,
    pub restarts: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Event {
    #[serde(rename = "type")]
    pub kind: String,
    pub reason: String,
    pub object: ObjectRef,
    pub message: String,
    pub count: i32,
    pub last_seen_seconds_ago: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PodDetails {
    #[serde(flatten)]
    pub pod: PodItem,
    pub qos: Option<String>,
    pub conditions: Vec<Condition>,
    pub pod_ip: Option<String>,
    pub service_account: Option<String>,
    pub containers: Vec<PodContainer>,
    pub volumes: Vec<Volume>,
    /// Ограничения размещения пода.
    #[serde(skip_serializing_if = "Scheduling::is_empty")]
    pub scheduling: Scheduling,
    pub labels: BTreeMap<String, String>,
    /// После маскировки: без копий объекта, секретные значения — `***`.
    pub annotations: BTreeMap<String, String>,
    /// Последние события пода, новые первыми.
    pub events: Part<Vec<Event>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventList {
    /// Новые первыми.
    pub items: Vec<Event>,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QuotaEntry {
    pub resource: String,
    pub used: Option<String>,
    pub hard: String,
    /// Доля использованного, 0..1 и больше.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ratio: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Quota {
    pub name: String,
    pub scopes: Vec<String>,
    pub entries: Vec<QuotaEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Limit {
    /// Container, Pod, PersistentVolumeClaim.
    pub kind: String,
    pub resource: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_request: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_limit_request_ratio: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LimitRange {
    pub name: String,
    pub limits: Vec<Limit>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Quotas {
    pub resource_quotas: Part<Vec<Quota>>,
    pub limit_ranges: Part<Vec<LimitRange>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Endpoints {
    pub ready: i32,
    pub total: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Service {
    pub name: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub cluster_ip: Option<String>,
    pub external: Vec<String>,
    pub ports: Vec<String>,
    pub selector: Option<String>,
    /// Готовые и все endpoints по EndpointSlices; `null` — они не читаются.
    pub endpoints: Option<Endpoints>,
    pub age_seconds: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Route {
    pub name: String,
    pub host: Option<String>,
    pub path: Option<String>,
    pub service: Option<String>,
    pub port: Option<String>,
    /// TLS-терминация: edge, passthrough, reencrypt.
    pub tls: Option<String>,
    pub admitted: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub admitted_message: Option<String>,
    /// Через сколько секунд истекает встроенный сертификат (отрицательно — уже истёк).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cert_expires_in_seconds: Option<i64>,
    /// Ingress, из которого OpenShift создал Route.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ingress: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IngressPath {
    pub host: Option<String>,
    pub path: Option<String>,
    pub service: Option<String>,
    pub port: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IngressTls {
    pub hosts: Vec<String>,
    pub secret: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Ingress {
    pub name: String,
    pub class: Option<String>,
    pub rules: Vec<IngressPath>,
    pub default_backend: Option<String>,
    pub tls: Vec<IngressTls>,
    pub addresses: Vec<String>,
    /// Routes, которые OpenShift создал из этого Ingress.
    pub routes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Listener {
    pub name: String,
    pub port: Option<i64>,
    pub protocol: Option<String>,
    pub hostname: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Gateway {
    pub name: String,
    pub class: Option<String>,
    pub listeners: Vec<Listener>,
    pub addresses: Vec<String>,
    pub accepted: Option<bool>,
    pub programmed: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RouteParent {
    pub name: String,
    pub namespace: Option<String>,
    pub section: Option<String>,
    pub accepted: Option<Condition>,
    pub resolved_refs: Option<Condition>,
}

/// HTTPRoute или GRPCRoute.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GatewayRoute {
    pub name: String,
    pub hostnames: Vec<String>,
    pub parents: Vec<RouteParent>,
    pub backends: Vec<String>,
}

/// Сеть проекта; вида без группы API в кластере нет в ответе (`null`).
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Network {
    pub services: Part<Vec<Service>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoints_error: Option<String>,
    pub routes: Option<Part<Vec<Route>>>,
    pub ingresses: Part<Vec<Ingress>>,
    pub gateways: Option<Part<Vec<Gateway>>>,
    pub http_routes: Option<Part<Vec<GatewayRoute>>>,
    pub grpc_routes: Option<Part<Vec<GatewayRoute>>>,
}

/// Контейнер пода для наблюдения: рестарты и причина последнего завершения.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerRestarts {
    pub name: String,
    pub restarts: i32,
    pub last_reason: Option<String>,
    pub last_exit_code: Option<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PodState {
    pub name: String,
    pub phase: String,
    pub ready: bool,
    pub containers: Vec<ContainerRestarts>,
}

/// Событие `Warning` объекта workload'а или его детей.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Warning {
    /// Ключ события в кластере: по нему повтор того же события не считается новым.
    pub key: String,
    pub reason: String,
    pub object: ObjectRef,
    pub message: String,
    pub count: i32,
}

/// Состояние workload'а в момент чтения (`observe_workload`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadState {
    pub desired: Option<i32>,
    pub ready: Option<i32>,
    pub available: Option<i32>,
    pub updated: Option<i32>,
    pub pods: Vec<PodState>,
    pub warnings: Vec<Warning>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logs_access_gives_its_own_error() {
        let scope = |logs| Scope {
            logs,
            ..Scope::default()
        };
        assert!(scope(LogsAccess::Allowed).require_logs().is_ok());
        assert_eq!(
            scope(LogsAccess::Metadata).require_logs().unwrap_err().code,
            "logs_not_allowed"
        );
        assert_eq!(
            scope(LogsAccess::Disabled).require_logs().unwrap_err().code,
            "logs_disabled"
        );
    }

    #[test]
    fn masker_uses_the_connection_names_or_the_default_list() {
        let own = Scope {
            mask_fields: Some(vec!["inn".into()]),
            ..Scope::default()
        };
        assert!(own.masker().is_secret_name("CLIENT_INN"));
        assert!(!own.masker().is_secret_name("PASSWORD"));
        assert!(Scope::default().masker().is_secret_name("PASSWORD"));
    }
}
