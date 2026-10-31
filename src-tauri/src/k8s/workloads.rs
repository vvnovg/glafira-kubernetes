//! Workloads проекта (спец. §6.3–6.4): Deployment, StatefulSet, DaemonSet, DeploymentConfig
//! (при группе `apps.openshift.io`), Job и CronJob — строка списка, подробности, HPA и PDB,
//! история rollout. Вид, который не читается, не скрывает остальные: его ошибка — в `errors`.

use std::collections::BTreeMap;

use k8s_openapi::api::apps::v1::{
    ControllerRevision, DaemonSet, Deployment, ReplicaSet, StatefulSet,
};
use k8s_openapi::api::autoscaling::v2::HorizontalPodAutoscaler;
use k8s_openapi::api::batch::v1::{CronJob, Job};
use k8s_openapi::api::core::v1::{PodTemplateSpec, ReplicationController};
use k8s_openapi::api::policy::v1::PodDisruptionBudget;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::api::{ApiResource, DynamicObject, GroupVersionKind, ListParams};
use serde::{Deserialize, Serialize};

use crate::error::{AppError, AppResult};
use crate::k8s::containers::{ContainerSpecView, containers_of, images_of};
use crate::k8s::errors::malformed;
use crate::k8s::read::{Listed, ReadClient, by_labels};
use crate::k8s::selectors::{map_selector_string, selector_matches, selector_string};
use crate::k8s::view::{
    ConditionView, Part, conditions, conditions_of, created, int_or_string, name, seconds,
};
use crate::k8s::yaml::redact_annotations;
use crate::platform::ApiGroups;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum WorkloadKind {
    Deployment,
    StatefulSet,
    DaemonSet,
    DeploymentConfig,
    Job,
    CronJob,
}

impl WorkloadKind {
    pub const ALL: [WorkloadKind; 6] = [
        WorkloadKind::Deployment,
        WorkloadKind::StatefulSet,
        WorkloadKind::DaemonSet,
        WorkloadKind::DeploymentConfig,
        WorkloadKind::Job,
        WorkloadKind::CronJob,
    ];

    /// Есть ли вид в кластере: DeploymentConfig — только при группе `apps.openshift.io`.
    pub fn available(self, groups: &ApiGroups) -> bool {
        self != WorkloadKind::DeploymentConfig || groups.deployment_configs
    }
}

pub fn deployment_configs() -> ApiResource {
    ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk("apps.openshift.io", "v1", "DeploymentConfig"),
        "deploymentconfigs",
    )
}

/// Почему workload не в норме.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum WorkloadProblem {
    /// Доступно меньше реплик, чем желаемо.
    Unavailable,
    /// Выкладка не уложилась в `progressDeadlineSeconds`.
    RolloutStuck,
    /// Job завершился ошибкой.
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadRow {
    pub kind: WorkloadKind,
    pub name: String,
    /// Желаемо: реплик, подов DaemonSet, завершений Job; у CronJob — нет.
    pub desired: Option<i32>,
    /// Готово; у Job — успешно завершено, у CronJob — идущих Job.
    pub ready: Option<i32>,
    pub available: Option<i32>,
    pub updated: Option<i32>,
    pub images: Vec<String>,
    pub created: Option<i64>,
    pub problem: Option<WorkloadProblem>,
    /// Только у CronJob: расписание, приостановлен ли, когда запускался последний раз.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schedule: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suspended: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_schedule: Option<i64>,
}

impl WorkloadRow {
    /// Ключ строки в списке и в watch: `Deployment/web`.
    pub fn key(&self) -> String {
        format!("{:?}/{}", self.kind, self.name)
    }
}

/// Всё, что нужно и строке, и подробностям: разбирается одинаково для каждого вида.
struct Parsed {
    row: WorkloadRow,
    meta: ObjectMeta,
    conditions: Vec<ConditionView>,
    /// Селектор подов строкой для `labelSelector`.
    selector: Option<String>,
    template: Option<PodTemplateSpec>,
    strategy: Option<String>,
    active_jobs: Vec<String>,
    /// DeploymentConfig: номер текущей версии.
    latest_version: Option<i64>,
    /// StatefulSet: ControllerRevision, на которой работают поды (`status.currentRevision`).
    current_revision: Option<String>,
    /// Сколько подов сверх желаемого создаёт выкладка (`maxSurge`); без выкладки с запасом — 0.
    surge: i32,
}

fn row(kind: WorkloadKind, meta: &ObjectMeta, template: Option<&PodTemplateSpec>) -> WorkloadRow {
    WorkloadRow {
        kind,
        name: name(meta),
        desired: None,
        ready: None,
        available: None,
        updated: None,
        images: template
            .and_then(|t| t.spec.as_ref())
            .map(images_of)
            .unwrap_or_default(),
        created: created(meta),
        problem: None,
        schedule: None,
        suspended: None,
        last_schedule: None,
    }
}

/// Доступно меньше желаемого (при желаемом больше нуля).
fn unavailable(desired: Option<i32>, available: Option<i32>) -> Option<WorkloadProblem> {
    let desired = desired.unwrap_or(0);
    (desired > 0 && available.unwrap_or(0) < desired).then_some(WorkloadProblem::Unavailable)
}

/// Проблема Deployment и DeploymentConfig: выкладка не уложилась в `progressDeadlineSeconds`
/// (`Progressing` = `False`, `ProgressDeadlineExceeded`), иначе — доступно меньше желаемого.
fn rollout_problem(
    conds: &[ConditionView],
    desired: Option<i32>,
    available: Option<i32>,
) -> Option<WorkloadProblem> {
    let stuck = conds.iter().any(|c| {
        c.kind == "Progressing"
            && c.status == "False"
            && c.reason.as_deref() == Some("ProgressDeadlineExceeded")
    });
    if stuck {
        Some(WorkloadProblem::RolloutStuck)
    } else {
        unavailable(desired, available)
    }
}

/// `maxSurge` подами: число как есть, проценты — от желаемых реплик с округлением вверх, как
/// у контроллера; по умолчанию 25 %.
fn surge_pods(max_surge: Option<&IntOrString>, replicas: i32) -> i32 {
    match max_surge {
        Some(IntOrString::Int(n)) => (*n).max(0),
        Some(IntOrString::String(s)) => s
            .strip_suffix('%')
            .and_then(|p| p.trim().parse::<i64>().ok())
            .map_or(0, |p| {
                // Процент задаёт автор workload'а и может быть любым: без переполнения и усечения.
                let pods = i64::from(replicas.max(0))
                    .saturating_mul(p.max(0))
                    .saturating_add(99)
                    / 100;
                i32::try_from(pods).unwrap_or(i32::MAX)
            }),
        None => (replicas.max(0) + 3) / 4,
    }
}

fn rolling(max_surge: Option<&IntOrString>, max_unavailable: Option<&IntOrString>) -> String {
    let v = |x: Option<&IntOrString>| x.map_or_else(|| "25%".into(), int_or_string);
    format!(
        "RollingUpdate (maxSurge {}, maxUnavailable {})",
        v(max_surge),
        v(max_unavailable)
    )
}

fn parse_deployment(d: &Deployment) -> Parsed {
    let spec = d.spec.as_ref();
    let status = d.status.as_ref();
    let template = spec.map(|s| s.template.clone());
    let mut r = row(WorkloadKind::Deployment, &d.metadata, template.as_ref());
    r.desired = Some(spec.and_then(|s| s.replicas).unwrap_or(1));
    r.ready = Some(status.and_then(|s| s.ready_replicas).unwrap_or(0));
    r.available = Some(status.and_then(|s| s.available_replicas).unwrap_or(0));
    r.updated = Some(status.and_then(|s| s.updated_replicas).unwrap_or(0));
    let conds = conditions_of(&status.and_then(|s| s.conditions.clone()));
    r.problem = rollout_problem(&conds, r.desired, r.available);
    let strategy = spec.and_then(|s| s.strategy.as_ref()).map(|st| {
        match st.type_.as_deref().unwrap_or("RollingUpdate") {
            "RollingUpdate" => {
                let ru = st.rolling_update.as_ref();
                rolling(
                    ru.and_then(|r| r.max_surge.as_ref()),
                    ru.and_then(|r| r.max_unavailable.as_ref()),
                )
            }
            other => other.to_string(),
        }
    });
    let selector = spec.map(|s| selector_string(&s.selector));
    let surge = match spec.and_then(|s| s.strategy.as_ref()) {
        Some(st) if st.type_.as_deref() == Some("Recreate") => 0,
        st => surge_pods(
            st.and_then(|s| s.rolling_update.as_ref())
                .and_then(|r| r.max_surge.as_ref()),
            r.desired.unwrap_or(1),
        ),
    };
    Parsed {
        row: r,
        meta: d.metadata.clone(),
        conditions: conds,
        selector,
        template,
        strategy,
        active_jobs: vec![],
        latest_version: None,
        current_revision: None,
        surge,
    }
}

fn parse_stateful_set(s: &StatefulSet) -> Parsed {
    let spec = s.spec.as_ref();
    let status = s.status.as_ref();
    let template = spec.map(|s| s.template.clone());
    let mut r = row(WorkloadKind::StatefulSet, &s.metadata, template.as_ref());
    r.desired = Some(spec.and_then(|s| s.replicas).unwrap_or(1));
    r.ready = Some(status.and_then(|s| s.ready_replicas).unwrap_or(0));
    r.available = Some(status.and_then(|s| s.available_replicas).unwrap_or(0));
    r.updated = Some(status.and_then(|s| s.updated_replicas).unwrap_or(0));
    r.problem = unavailable(r.desired, r.available);
    let selector = spec.map(|s| selector_string(&s.selector));
    Parsed {
        row: r,
        meta: s.metadata.clone(),
        conditions: conditions_of(&status.and_then(|s| s.conditions.clone())),
        selector,
        template,
        strategy: spec
            .and_then(|s| s.update_strategy.as_ref())
            .and_then(|u| u.type_.clone()),
        active_jobs: vec![],
        latest_version: None,
        current_revision: status.and_then(|s| s.current_revision.clone()),
        surge: 0,
    }
}

fn parse_daemon_set(d: &DaemonSet) -> Parsed {
    let spec = d.spec.as_ref();
    let status = d.status.as_ref();
    let template = spec.map(|s| s.template.clone());
    let mut r = row(WorkloadKind::DaemonSet, &d.metadata, template.as_ref());
    r.desired = status.map(|s| s.desired_number_scheduled);
    r.ready = status.map(|s| s.number_ready);
    r.available = Some(status.and_then(|s| s.number_available).unwrap_or(0));
    r.updated = Some(status.and_then(|s| s.updated_number_scheduled).unwrap_or(0));
    r.problem = unavailable(r.desired, r.available);
    let selector = spec.map(|s| selector_string(&s.selector));
    Parsed {
        row: r,
        meta: d.metadata.clone(),
        conditions: conditions_of(&status.and_then(|s| s.conditions.clone())),
        selector,
        template,
        strategy: spec
            .and_then(|s| s.update_strategy.as_ref())
            .and_then(|u| u.type_.clone()),
        active_jobs: vec![],
        latest_version: None,
        current_revision: None,
        surge: 0,
    }
}

fn parse_job(j: &Job) -> Parsed {
    let spec = j.spec.as_ref();
    let status = j.status.as_ref();
    let template = spec.map(|s| s.template.clone());
    let mut r = row(WorkloadKind::Job, &j.metadata, template.as_ref());
    r.desired = Some(spec.and_then(|s| s.completions).unwrap_or(1));
    r.ready = Some(status.and_then(|s| s.succeeded).unwrap_or(0));
    let conds = conditions_of(&status.and_then(|s| s.conditions.clone()));
    if conds.iter().any(|c| c.kind == "Failed" && c.is_true()) {
        r.problem = Some(WorkloadProblem::Failed);
    }
    let selector = spec.and_then(|s| s.selector.as_ref()).map(selector_string);
    Parsed {
        row: r,
        meta: j.metadata.clone(),
        conditions: conds,
        selector,
        template,
        strategy: None,
        active_jobs: vec![],
        latest_version: None,
        current_revision: None,
        surge: 0,
    }
}

fn parse_cron_job(c: &CronJob) -> Parsed {
    let spec = c.spec.as_ref();
    let status = c.status.as_ref();
    let template = spec.and_then(|s| s.job_template.spec.as_ref().map(|j| j.template.clone()));
    let mut r = row(WorkloadKind::CronJob, &c.metadata, template.as_ref());
    let active: Vec<String> = status
        .and_then(|s| s.active.as_ref())
        .map(|a| a.iter().filter_map(|o| o.name.clone()).collect())
        .unwrap_or_default();
    r.ready = Some(active.len() as i32);
    r.schedule = spec.map(|s| s.schedule.clone());
    r.suspended = Some(spec.and_then(|s| s.suspend) == Some(true));
    r.last_schedule = status
        .and_then(|s| s.last_schedule_time.as_ref())
        .map(seconds);
    Parsed {
        row: r,
        meta: c.metadata.clone(),
        conditions: vec![],
        selector: None,
        template,
        strategy: spec.and_then(|s| s.concurrency_policy.clone()),
        active_jobs: active,
        latest_version: None,
        current_revision: None,
        surge: 0,
    }
}

/// Поля DeploymentConfig, которые нужны Glafira (своего типа в `k8s-openapi` нет), — из
/// `DynamicObject::data`; метаданные уже разобраны в `DynamicObject::metadata`.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DeploymentConfigData {
    #[serde(default)]
    spec: DcSpec,
    #[serde(default)]
    status: DcStatus,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DcSpec {
    replicas: Option<i32>,
    selector: Option<BTreeMap<String, String>>,
    strategy: Option<DcStrategy>,
    template: Option<PodTemplateSpec>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DcStrategy {
    #[serde(rename = "type")]
    kind: Option<String>,
    rolling_params: Option<DcRollingParams>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DcRollingParams {
    max_surge: Option<IntOrString>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DcStatus {
    ready_replicas: Option<i32>,
    available_replicas: Option<i32>,
    updated_replicas: Option<i32>,
    latest_version: Option<i64>,
    #[serde(default)]
    conditions: serde_json::Value,
}

/// DeploymentConfig из `DynamicObject`. Поля не того вида (`"spec": null`, не тот тип
/// в шаблоне) — ошибка, а не строка из пустого объекта с ложной проблемой.
fn parse_deployment_config(o: &DynamicObject) -> AppResult<Parsed> {
    let dc = DeploymentConfigData::deserialize(&o.data)
        .map_err(|_| malformed("DeploymentConfig", &name(&o.metadata)))?;
    let mut r = row(
        WorkloadKind::DeploymentConfig,
        &o.metadata,
        dc.spec.template.as_ref(),
    );
    r.desired = Some(dc.spec.replicas.unwrap_or(1));
    r.ready = Some(dc.status.ready_replicas.unwrap_or(0));
    r.available = Some(dc.status.available_replicas.unwrap_or(0));
    r.updated = Some(dc.status.updated_replicas.unwrap_or(0));
    let conds = conditions(&dc.status.conditions);
    r.problem = rollout_problem(&conds, r.desired, r.available);
    let surge = match dc.spec.strategy.as_ref() {
        Some(st) if st.kind.as_deref() == Some("Recreate") => 0,
        st => surge_pods(
            st.and_then(|s| s.rolling_params.as_ref())
                .and_then(|p| p.max_surge.as_ref()),
            r.desired.unwrap_or(1),
        ),
    };
    Ok(Parsed {
        row: r,
        meta: o.metadata.clone(),
        conditions: conds,
        selector: dc.spec.selector.as_ref().map(map_selector_string),
        template: dc.spec.template,
        strategy: dc.spec.strategy.and_then(|s| s.kind),
        active_jobs: vec![],
        latest_version: dc.status.latest_version,
        current_revision: None,
        surge,
    })
}

pub fn deployment_row(d: &Deployment) -> WorkloadRow {
    parse_deployment(d).row
}
pub fn stateful_set_row(s: &StatefulSet) -> WorkloadRow {
    parse_stateful_set(s).row
}
pub fn daemon_set_row(d: &DaemonSet) -> WorkloadRow {
    parse_daemon_set(d).row
}
pub fn job_row(j: &Job) -> WorkloadRow {
    parse_job(j).row
}
pub fn cron_job_row(c: &CronJob) -> WorkloadRow {
    parse_cron_job(c).row
}
/// Строка DeploymentConfig. Объект, который не разбирается, — строка с именем и возрастом,
/// без чисел и без проблемы: так его показывает watch; список (`list_kind`) его пропускает
/// и называет в ошибке вида.
pub fn deployment_config_row(o: &DynamicObject) -> WorkloadRow {
    parse_deployment_config(o).map_or_else(
        |_| row(WorkloadKind::DeploymentConfig, &o.metadata, None),
        |p| p.row,
    )
}

/// Вид, который не прочитался (например, нет прав на `cronjobs`).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KindError {
    pub kind: WorkloadKind,
    pub error: AppError,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadList {
    pub items: Vec<WorkloadRow>,
    pub errors: Vec<KindError>,
    pub truncated: bool,
}

/// Строки одного вида и ошибка объекта, который не разобрался (только DeploymentConfig: его
/// поля читаются без типа из `k8s-openapi`). Такой объект пропускается, остальные — в списке;
/// если их несколько, ошибка — первого.
pub async fn list_kind(
    r: &ReadClient,
    namespace: &str,
    kind: WorkloadKind,
) -> AppResult<(Listed<WorkloadRow>, Option<AppError>)> {
    let (facts, skipped) = list_facts(r, namespace, kind).await?;
    Ok((facts.map(|f| f.row), skipped))
}

/// Workload вместе с условиями и шаблоном подов — для проверки (спец. §8).
#[derive(Debug, Clone)]
pub struct WorkloadFacts {
    pub row: WorkloadRow,
    pub conditions: Vec<ConditionView>,
    pub template: Option<PodTemplateSpec>,
    /// CronJob, из которого создан Job (`ownerReferences`); у остальных видов `None`.
    pub cron: Option<String>,
}

/// CronJob-владелец объекта: по нему поды Job относят к CronJob (вкладка «Ресурсы» проекта и
/// правила `resources.*`).
pub fn cron_owner(meta: &ObjectMeta) -> Option<&str> {
    meta.owner_references
        .iter()
        .flatten()
        .find(|o| o.kind == "CronJob")
        .map(|o| o.name.as_str())
}

impl From<Parsed> for WorkloadFacts {
    fn from(p: Parsed) -> Self {
        Self {
            cron: cron_owner(&p.meta).map(String::from),
            row: p.row,
            conditions: p.conditions,
            template: p.template,
        }
    }
}

/// Как `list_kind`, но с условиями и шаблоном подов каждого workload'а.
pub async fn list_facts(
    r: &ReadClient,
    namespace: &str,
    kind: WorkloadKind,
) -> AppResult<(Listed<WorkloadFacts>, Option<AppError>)> {
    let lp = ListParams::default();
    let ns = Some(namespace);
    macro_rules! typed {
        ($t:ty, $f:ident) => {
            (r.list::<$t>(ns, &lp).await?.map(|o| $f(&o).into()), None)
        };
    }
    Ok(match kind {
        WorkloadKind::Deployment => typed!(Deployment, parse_deployment),
        WorkloadKind::StatefulSet => typed!(StatefulSet, parse_stateful_set),
        WorkloadKind::DaemonSet => typed!(DaemonSet, parse_daemon_set),
        WorkloadKind::Job => typed!(Job, parse_job),
        WorkloadKind::CronJob => typed!(CronJob, parse_cron_job),
        WorkloadKind::DeploymentConfig => {
            let l = r.list_dynamic(&deployment_configs(), ns, &lp).await?;
            let (mut items, mut skipped) = (Vec::new(), None);
            for o in &l.items {
                match parse_deployment_config(o) {
                    Ok(p) => items.push(p.into()),
                    Err(e) => {
                        skipped.get_or_insert(e);
                    }
                }
            }
            let rows = Listed {
                items,
                truncated: l.truncated,
            };
            (rows, skipped)
        }
    })
}

/// Все workloads проекта; виды читаются параллельно, ошибка одного вида не скрывает остальные.
/// Вид с объектом, который не разобрался, приходит и строками, и ошибкой (`list_kind`).
pub async fn list_workloads(r: &ReadClient, namespace: &str, groups: &ApiGroups) -> WorkloadList {
    let kinds: Vec<WorkloadKind> = WorkloadKind::ALL
        .into_iter()
        .filter(|k| k.available(groups))
        .collect();
    let results =
        futures::future::join_all(kinds.iter().map(|k| list_kind(r, namespace, *k))).await;
    let mut list = WorkloadList {
        items: Vec::new(),
        errors: Vec::new(),
        truncated: false,
    };
    for (kind, result) in kinds.into_iter().zip(results) {
        match result {
            Ok((rows, skipped)) => {
                list.items.extend(rows.items);
                list.truncated |= rows.truncated;
                if let Some(error) = skipped {
                    list.errors.push(KindError { kind, error });
                }
            }
            Err(error) => list.errors.push(KindError { kind, error }),
        }
    }
    list.items
        .sort_by(|a, b| a.name.cmp(&b.name).then(a.kind.cmp(&b.kind)));
    list
}

/// HPA, который масштабирует workload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HpaView {
    pub name: String,
    pub min_replicas: Option<i32>,
    pub max_replicas: i32,
    pub current_replicas: Option<i32>,
    pub desired_replicas: i32,
    /// `cpu 80%`, `memory 70%`, `pods requests_per_second 100`…
    pub metrics: Vec<String>,
    pub conditions: Vec<ConditionView>,
}

fn hpa_view(h: &HorizontalPodAutoscaler) -> HpaView {
    let spec = h.spec.as_ref();
    let status = h.status.as_ref();
    let target = |t: &k8s_openapi::api::autoscaling::v2::MetricTarget| {
        if let Some(u) = t.average_utilization {
            format!("{u}%")
        } else if let Some(v) = &t.average_value {
            v.0.clone()
        } else if let Some(v) = &t.value {
            v.0.clone()
        } else {
            "—".into()
        }
    };
    let metrics = spec
        .and_then(|s| s.metrics.as_ref())
        .map(|ms| {
            ms.iter()
                .map(|m| {
                    if let Some(r) = &m.resource {
                        format!("{} {}", r.name, target(&r.target))
                    } else if let Some(c) = &m.container_resource {
                        format!("{} {} ({})", c.name, target(&c.target), c.container)
                    } else if let Some(p) = &m.pods {
                        format!("pods {} {}", p.metric.name, target(&p.target))
                    } else if let Some(o) = &m.object {
                        format!("object {} {}", o.metric.name, target(&o.target))
                    } else if let Some(e) = &m.external {
                        format!("external {} {}", e.metric.name, target(&e.target))
                    } else {
                        m.type_.clone()
                    }
                })
                .collect()
        })
        .unwrap_or_default();
    HpaView {
        name: name(&h.metadata),
        min_replicas: spec.and_then(|s| s.min_replicas),
        max_replicas: spec.map_or(0, |s| s.max_replicas),
        current_replicas: status.and_then(|s| s.current_replicas),
        desired_replicas: status.map_or(0, |s| s.desired_replicas),
        metrics,
        conditions: conditions_of(&status.and_then(|s| s.conditions.clone())),
    }
}

/// PDB, который защищает поды workload'а.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PdbView {
    pub name: String,
    pub min_available: Option<String>,
    pub max_unavailable: Option<String>,
    pub allowed_disruptions: i32,
    pub current_healthy: i32,
    pub desired_healthy: i32,
    pub expected_pods: i32,
}

fn pdb_view(p: &PodDisruptionBudget) -> PdbView {
    let spec = p.spec.as_ref();
    let status = p.status.as_ref();
    PdbView {
        name: name(&p.metadata),
        min_available: spec
            .and_then(|s| s.min_available.as_ref())
            .map(int_or_string),
        max_unavailable: spec
            .and_then(|s| s.max_unavailable.as_ref())
            .map(int_or_string),
        allowed_disruptions: status.map_or(0, |s| s.disruptions_allowed),
        current_healthy: status.map_or(0, |s| s.current_healthy),
        desired_healthy: status.map_or(0, |s| s.desired_healthy),
        expected_pods: status.map_or(0, |s| s.expected_pods),
    }
}

/// Ревизия выкладки: ReplicaSet, ControllerRevision или ReplicationController.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Revision {
    pub revision: i64,
    pub name: String,
    pub images: Vec<String>,
    pub created: Option<i64>,
    /// Подов у ревизии (ReplicaSet, ReplicationController); у ControllerRevision — `None`.
    pub replicas: Option<i32>,
    /// Ревизия, на которой работают поды, — там, где это известно. StatefulSet —
    /// `status.currentRevision` (при `OnDelete`, `partition` и во время выкладки это не самая
    /// новая ревизия), без него — самая новая. Deployment и DeploymentConfig — ревизия самого
    /// объекта, то есть последняя выкладка; на какой ревизии сколько подов, видно по `replicas`.
    /// DaemonSet — самая новая: на какой ревизии его поды, статус DaemonSet не говорит.
    pub current: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadDetails {
    pub workload: WorkloadRow,
    pub conditions: Vec<ConditionView>,
    pub selector: Option<String>,
    pub strategy: Option<String>,
    pub containers: Vec<ContainerSpecView>,
    pub labels: BTreeMap<String, String>,
    /// Как в YAML (`redact_annotations`): без копий объекта, `change-cause` с секретом — `***`.
    pub annotations: BTreeMap<String, String>,
    /// Только у CronJob: идущие Job.
    pub active_jobs: Vec<String>,
    pub hpa: Part<Option<HpaView>>,
    pub pdb: Part<Option<PdbView>>,
    /// История выкладки; у Job и CronJob её нет.
    pub history: Option<Part<Vec<Revision>>>,
    /// Тома шаблона подов — только для MCP (интерфейс показывает тома на вкладке пода).
    #[serde(skip)]
    pub volumes: Vec<crate::k8s::pods::VolumeView>,
    /// Ограничения размещения шаблона подов — только для MCP.
    #[serde(skip)]
    pub scheduling: crate::k8s::scheduling::SchedulingView,
}

fn owned_by(meta: &ObjectMeta, uid: Option<&str>) -> bool {
    uid.is_some_and(|uid| meta.owner_references.iter().flatten().any(|o| o.uid == uid))
}

fn annotation_i64(meta: &ObjectMeta, key: &str) -> Option<i64> {
    meta.annotations
        .as_ref()
        .and_then(|a| a.get(key))
        .and_then(|v| v.parse().ok())
}

async fn history(r: &ReadClient, namespace: &str, p: &Parsed) -> Option<Part<Vec<Revision>>> {
    let uid = p.meta.uid.as_deref();
    let lp = by_labels(p.selector.as_deref());
    let ns = Some(namespace);
    let sorted = |mut v: Vec<Revision>| {
        v.sort_by_key(|r| std::cmp::Reverse(r.revision));
        v
    };
    let result: AppResult<Vec<Revision>> = match p.row.kind {
        WorkloadKind::Deployment => {
            let current = annotation_i64(&p.meta, "deployment.kubernetes.io/revision");
            r.list::<ReplicaSet>(ns, &lp).await.map(|l| {
                sorted(
                    l.items
                        .iter()
                        .filter(|rs| owned_by(&rs.metadata, uid))
                        .map(|rs| {
                            let revision =
                                annotation_i64(&rs.metadata, "deployment.kubernetes.io/revision")
                                    .unwrap_or(0);
                            Revision {
                                revision,
                                name: name(&rs.metadata),
                                images: rs
                                    .spec
                                    .as_ref()
                                    .and_then(|s| s.template.as_ref())
                                    .and_then(|t| t.spec.as_ref())
                                    .map(images_of)
                                    .unwrap_or_default(),
                                created: created(&rs.metadata),
                                replicas: rs.status.as_ref().map(|s| s.replicas),
                                current: Some(revision) == current,
                            }
                        })
                        .collect(),
                )
            })
        }
        WorkloadKind::StatefulSet | WorkloadKind::DaemonSet => {
            r.list::<ControllerRevision>(ns, &lp).await.map(|l| {
                let mut v = sorted(
                    l.items
                        .iter()
                        .filter(|cr| owned_by(&cr.metadata, uid))
                        .map(|cr| Revision {
                            revision: cr.revision,
                            name: name(&cr.metadata),
                            images: cr
                                .data
                                .as_ref()
                                .and_then(|d| {
                                    d.0["spec"]["template"]["spec"]["containers"]
                                        .as_array()
                                        .cloned()
                                })
                                .map(|cs| {
                                    cs.iter()
                                        .filter_map(|c| c["image"].as_str().map(String::from))
                                        .collect()
                                })
                                .unwrap_or_default(),
                            created: created(&cr.metadata),
                            replicas: None,
                            current: false,
                        })
                        .collect(),
                );
                match &p.current_revision {
                    Some(current) => v.iter_mut().for_each(|r| r.current = r.name == *current),
                    // DaemonSet (такого поля в статусе нет) и StatefulSet без него: самая новая.
                    None => {
                        if let Some(first) = v.first_mut() {
                            first.current = true;
                        }
                    }
                }
                v
            })
        }
        WorkloadKind::DeploymentConfig => {
            let lp = ListParams::default().labels(&format!(
                "openshift.io/deployment-config.name={}",
                p.row.name
            ));
            r.list::<ReplicationController>(ns, &lp).await.map(|l| {
                sorted(
                    l.items
                        .iter()
                        .map(|rc| {
                            let revision = annotation_i64(
                                &rc.metadata,
                                "openshift.io/deployment-config.latest-version",
                            )
                            .unwrap_or(0);
                            Revision {
                                revision,
                                name: name(&rc.metadata),
                                images: rc
                                    .spec
                                    .as_ref()
                                    .and_then(|s| s.template.as_ref())
                                    .and_then(|t| t.spec.as_ref())
                                    .map(images_of)
                                    .unwrap_or_default(),
                                created: created(&rc.metadata),
                                replicas: rc.status.as_ref().map(|s| s.replicas),
                                current: Some(revision) == p.latest_version,
                            }
                        })
                        .collect(),
                )
            })
        }
        WorkloadKind::Job | WorkloadKind::CronJob => return None,
    };
    Some(result.into())
}

async fn parsed(
    r: &ReadClient,
    namespace: &str,
    kind: WorkloadKind,
    workload: &str,
) -> AppResult<Parsed> {
    Ok(match kind {
        WorkloadKind::Deployment => parse_deployment(&r.get(namespace, workload).await?),
        WorkloadKind::StatefulSet => parse_stateful_set(&r.get(namespace, workload).await?),
        WorkloadKind::DaemonSet => parse_daemon_set(&r.get(namespace, workload).await?),
        WorkloadKind::Job => parse_job(&r.get(namespace, workload).await?),
        WorkloadKind::CronJob => parse_cron_job(&r.get(namespace, workload).await?),
        WorkloadKind::DeploymentConfig => parse_deployment_config(
            &r.get_dynamic(&deployment_configs(), Some(namespace), workload)
                .await?,
        )?,
    })
}

/// Селектор подов workload'а строкой (для списка его подов).
pub async fn pod_selector(
    r: &ReadClient,
    namespace: &str,
    kind: WorkloadKind,
    workload: &str,
) -> AppResult<Option<String>> {
    Ok(parsed(r, namespace, kind, workload).await?.selector)
}

/// Подробности workload'а: сам объект, затем параллельно HPA, PDB и история.
pub async fn get_workload(
    r: &ReadClient,
    namespace: &str,
    kind: WorkloadKind,
    workload: &str,
) -> AppResult<WorkloadDetails> {
    let p = parsed(r, namespace, kind, workload).await?;
    let ns = Some(namespace);
    let hpa = async {
        r.list::<HorizontalPodAutoscaler>(ns, &ListParams::default())
            .await
            .map(|l| {
                l.items
                    .iter()
                    .find(|h| {
                        h.spec.as_ref().is_some_and(|s| {
                            s.scale_target_ref.kind == format!("{:?}", p.row.kind)
                                && s.scale_target_ref.name == p.row.name
                        })
                    })
                    .map(hpa_view)
            })
            .into()
    };
    let template_labels = p
        .template
        .as_ref()
        .and_then(|t| t.metadata.as_ref())
        .and_then(|m| m.labels.clone())
        .unwrap_or_default();
    let pdb = async {
        r.list::<PodDisruptionBudget>(ns, &ListParams::default())
            .await
            .map(|l| {
                l.items
                    .iter()
                    .find(|b| {
                        !template_labels.is_empty()
                            && b.spec
                                .as_ref()
                                .and_then(|s| s.selector.as_ref())
                                .is_some_and(|s| selector_matches(s, &template_labels))
                    })
                    .map(pdb_view)
            })
            .into()
    };
    let (hpa, pdb, history) = tokio::join!(hpa, pdb, history(r, namespace, &p));
    let mut annotations = p.meta.annotations.clone().unwrap_or_default();
    redact_annotations(&mut annotations);
    Ok(WorkloadDetails {
        conditions: p.conditions.clone(),
        selector: p.selector.clone(),
        strategy: p.strategy.clone(),
        containers: p
            .template
            .as_ref()
            .and_then(|t| t.spec.as_ref())
            .map(containers_of)
            .unwrap_or_default(),
        labels: p.meta.labels.clone().unwrap_or_default(),
        annotations,
        active_jobs: p.active_jobs.clone(),
        hpa,
        pdb,
        history,
        volumes: p
            .template
            .as_ref()
            .and_then(|t| t.spec.as_ref())
            .map(crate::k8s::pods::volumes_of)
            .unwrap_or_default(),
        scheduling: p
            .template
            .as_ref()
            .and_then(|t| t.spec.as_ref())
            .map(crate::k8s::scheduling::scheduling_of)
            .unwrap_or_default(),
        workload: p.row,
    })
}

/// Шаблон подов workload'а и то, что нужно анализу ресурсов и изменениям (спец. §7.3, §9).
#[derive(Debug, Clone)]
pub struct WorkloadSpec {
    pub row: WorkloadRow,
    /// Селектор подов строкой.
    pub selector: Option<String>,
    pub template: Option<PodTemplateSpec>,
    /// Подов сверх желаемого при выкладке (`maxSurge`); у StatefulSet, DaemonSet, Job и
    /// CronJob — 0.
    pub surge: i32,
    pub resource_version: Option<String>,
    /// `metadata.generation`: растёт только при изменении spec, не статуса.
    pub generation: Option<i64>,
}

pub async fn get_spec(
    r: &ReadClient,
    namespace: &str,
    kind: WorkloadKind,
    workload: &str,
) -> AppResult<WorkloadSpec> {
    let p = parsed(r, namespace, kind, workload).await?;
    Ok(WorkloadSpec {
        resource_version: p.meta.resource_version.clone(),
        generation: p.meta.generation,
        row: p.row,
        selector: p.selector,
        template: p.template,
        surge: p.surge,
    })
}

/// Строка workload'а и селектор его подов без HPA, PDB и истории — для частого чтения
/// (наблюдение MCP за workload'ом).
pub async fn get_row(
    r: &ReadClient,
    namespace: &str,
    kind: WorkloadKind,
    workload: &str,
) -> AppResult<(WorkloadRow, Option<String>)> {
    let p = parsed(r, namespace, kind, workload).await?;
    Ok((p.row, p.selector))
}

#[cfg(test)]
pub(crate) mod fixtures {
    pub fn template(app: &str, image: &str) -> serde_json::Value {
        serde_json::json!({"metadata": {"labels": {"app": app}},
            "spec": {"containers": [{"name": app, "image": image}]}})
    }

    pub fn deployment(name: &str, desired: i32, available: i32) -> serde_json::Value {
        serde_json::json!({"apiVersion": "apps/v1", "kind": "Deployment",
            "metadata": {"name": name, "namespace": "team-a", "uid": format!("uid-{name}"),
                "creationTimestamp": "2026-10-25T10:00:00Z",
                "annotations": {"deployment.kubernetes.io/revision": "3",
                    "kubectl.kubernetes.io/last-applied-configuration": "{…}"}},
            "spec": {"replicas": desired, "selector": {"matchLabels": {"app": name}},
                "strategy": {"type": "RollingUpdate", "rollingUpdate": {"maxSurge": 1, "maxUnavailable": "0"}},
                "template": template(name, "registry/web:3")},
            "status": {"replicas": desired, "readyReplicas": available, "availableReplicas": available,
                "updatedReplicas": desired,
                "conditions": [{"type": "Available", "status": "True"}]}})
    }

    pub fn list(kind: &str, items: Vec<serde_json::Value>) -> serde_json::Value {
        serde_json::json!({"apiVersion": "v1", "kind": format!("{kind}List"), "metadata": {}, "items": items})
    }
}

#[cfg(test)]
mod tests {
    use super::fixtures::*;
    use super::*;

    #[test]
    fn surge_counts_extra_pods_of_a_rollout() {
        let int = |n| IntOrString::Int(n);
        let pct = |p: &str| IntOrString::String(p.into());
        assert_eq!(surge_pods(None, 4), 1, "25 % по умолчанию");
        assert_eq!(surge_pods(None, 1), 1, "вверх");
        assert_eq!(surge_pods(Some(&pct("50%")), 3), 2);
        assert_eq!(surge_pods(Some(&pct("0%")), 3), 0);
        assert_eq!(surge_pods(Some(&int(2)), 10), 2);
        assert_eq!(surge_pods(Some(&pct("x")), 3), 0);
    }

    #[test]
    fn surge_does_not_overflow_on_an_odd_percentage() {
        let pct = |p: &str| IntOrString::String(p.into());
        assert_eq!(surge_pods(Some(&pct("200%")), 3), 6, "больше 100 % бывает");
        assert_eq!(surge_pods(Some(&pct("-50%")), 3), 0);
        // Произведение не помещается в i64, результат — в i32.
        for p in [
            "9223372036854775807%",
            "9223372036854775807 %",
            "99999999999%",
        ] {
            assert_eq!(surge_pods(Some(&pct(p)), 3), i32::MAX, "{p}");
        }
        assert_eq!(
            surge_pods(Some(&pct("9223372036854775807%")), i32::MAX),
            i32::MAX
        );
        assert_eq!(surge_pods(Some(&pct("100%")), i32::MAX), i32::MAX);
        assert_eq!(surge_pods(Some(&pct("5%")), -4), 0);
        assert_eq!(surge_pods(Some(&IntOrString::Int(i32::MIN)), 3), 0);
    }

    #[test]
    fn recreate_has_no_surge_even_with_a_huge_percentage() {
        let d: Deployment = serde_json::from_value(serde_json::json!({
            "metadata": {"name": "web"},
            "spec": {"replicas": 3, "selector": {"matchLabels": {"app": "web"}},
                "strategy": {"type": "Recreate",
                    "rollingUpdate": {"maxSurge": "9223372036854775807%"}},
                "template": fixtures::template("web", "web:1")}
        }))
        .unwrap();
        assert_eq!(parse_deployment(&d).surge, 0);
        let rolling: Deployment = serde_json::from_value(serde_json::json!({
            "metadata": {"name": "web"},
            "spec": {"replicas": 3, "selector": {"matchLabels": {"app": "web"}},
                "strategy": {"type": "RollingUpdate",
                    "rollingUpdate": {"maxSurge": "9223372036854775807%"}},
                "template": fixtures::template("web", "web:1")}
        }))
        .unwrap();
        assert_eq!(parse_deployment(&rolling).surge, i32::MAX);
    }

    #[tokio::test]
    async fn spec_carries_template_surge_and_resource_version() {
        let (client, _) = mock_client(|s| match s.path() {
            "/apis/apps/v1/namespaces/team-a/deployments/web" => (
                200,
                serde_json::json!({"apiVersion": "apps/v1", "kind": "Deployment",
                    "metadata": {"name": "web", "namespace": "team-a", "resourceVersion": "42", "generation": 3},
                    "spec": {"replicas": 4, "selector": {"matchLabels": {"app": "web"}},
                        "strategy": {"type": "RollingUpdate", "rollingUpdate": {"maxSurge": "50%"}},
                        "template": fixtures::template("web", "web:1")}}),
            ),
            _ => status(404, "NotFound", "no"),
        });
        let r = ReadClient::new(client, std::time::Duration::from_secs(5));
        let s = get_spec(&r, "team-a", WorkloadKind::Deployment, "web")
            .await
            .unwrap();
        assert_eq!(s.surge, 2);
        assert_eq!(s.resource_version.as_deref(), Some("42"));
        assert_eq!(s.generation, Some(3));
        assert_eq!(s.selector.as_deref(), Some("app=web"));
        assert_eq!(s.row.desired, Some(4));
        assert!(s.template.is_some());
    }
    use crate::k8s::testing::{mock_client, status};
    use std::time::Duration;

    fn reader(client: kube::Client) -> ReadClient {
        ReadClient::new(client, Duration::from_secs(5))
    }

    fn from<T: serde::de::DeserializeOwned>(v: serde_json::Value) -> T {
        serde_json::from_value(v).unwrap()
    }

    #[test]
    fn rows_of_every_kind() {
        let d = deployment_row(&from(deployment("web", 3, 2)));
        assert_eq!(
            (d.desired, d.ready, d.available, d.updated),
            (Some(3), Some(2), Some(2), Some(3))
        );
        assert_eq!(d.images, ["registry/web:3"]);
        assert_eq!(d.problem, Some(WorkloadProblem::Unavailable));
        assert_eq!(d.key(), "Deployment/web");
        assert_eq!(deployment_row(&from(deployment("ok", 2, 2))).problem, None);
        assert_eq!(
            deployment_row(&from(deployment("zero", 0, 0))).problem,
            None
        );

        let mut stuck = deployment("stuck", 1, 1);
        stuck["status"]["conditions"] = serde_json::json!([{"type": "Progressing", "status": "False",
            "reason": "ProgressDeadlineExceeded"}]);
        assert_eq!(
            deployment_row(&from(stuck)).problem,
            Some(WorkloadProblem::RolloutStuck)
        );

        let ds: DaemonSet = from(serde_json::json!({"metadata": {"name": "agent"},
            "spec": {"selector": {"matchLabels": {"app": "agent"}}, "template": template("agent", "agent:1")},
            "status": {"desiredNumberScheduled": 4, "numberReady": 3, "numberAvailable": 3,
                "currentNumberScheduled": 4, "numberMisscheduled": 0}}));
        let row = daemon_set_row(&ds);
        assert_eq!((row.desired, row.ready), (Some(4), Some(3)));
        assert_eq!(row.problem, Some(WorkloadProblem::Unavailable));

        let job: Job = from(serde_json::json!({"metadata": {"name": "migrate"},
            "spec": {"template": template("migrate", "app:1")},
            "status": {"failed": 3, "conditions": [{"type": "Failed", "status": "True", "reason": "BackoffLimitExceeded"}]}}));
        let row = job_row(&job);
        assert_eq!((row.desired, row.ready), (Some(1), Some(0)));
        assert_eq!(row.problem, Some(WorkloadProblem::Failed));

        let cron: CronJob = from(serde_json::json!({"metadata": {"name": "report"},
            "spec": {"schedule": "0 3 * * *", "suspend": true,
                "jobTemplate": {"spec": {"template": template("report", "report:2")}}},
            "status": {"active": [{"name": "report-1"}], "lastScheduleTime": "2026-10-25T10:00:00Z"}}));
        let row = cron_job_row(&cron);
        assert_eq!(row.schedule.as_deref(), Some("0 3 * * *"));
        assert_eq!((row.suspended, row.ready), (Some(true), Some(1)));
        assert_eq!(row.last_schedule, Some(1_792_922_400));
        assert_eq!(row.images, ["report:2"]);

        let dc: DynamicObject = from(serde_json::json!({"apiVersion": "apps.openshift.io/v1",
            "kind": "DeploymentConfig", "metadata": {"name": "legacy"},
            "spec": {"replicas": 2, "selector": {"app": "legacy"}, "strategy": {"type": "Rolling"},
                "template": template("legacy", "legacy:7")},
            "status": {"readyReplicas": 2, "availableReplicas": 2, "updatedReplicas": 2, "latestVersion": 7}}));
        let p = parse_deployment_config(&dc).unwrap();
        assert_eq!(p.row.kind, WorkloadKind::DeploymentConfig);
        assert_eq!((p.row.desired, p.row.available), (Some(2), Some(2)));
        assert_eq!(p.selector.as_deref(), Some("app=legacy"));
        assert_eq!(p.strategy.as_deref(), Some("Rolling"));
        assert_eq!(p.latest_version, Some(7));
    }

    #[tokio::test]
    async fn list_of_kinds_with_a_forbidden_one() {
        let (client, server) = mock_client(|s| match s.path() {
            "/apis/apps/v1/namespaces/team-a/deployments" => (
                200,
                list(
                    "Deployment",
                    vec![deployment("web", 1, 1), deployment("api", 1, 1)],
                ),
            ),
            "/apis/batch/v1/namespaces/team-a/cronjobs" => status(
                403,
                "Forbidden",
                r#"cronjobs.batch is forbidden: User "u" cannot list resource "cronjobs" in API group "batch" in the namespace "team-a""#,
            ),
            p if p.contains("deploymentconfigs") => panic!("без apps.openshift.io не спрашиваем"),
            _ => (200, list("X", vec![])),
        });
        let l = list_workloads(&reader(client), "team-a", &ApiGroups::default()).await;
        let names: Vec<_> = l.items.iter().map(|w| w.name.as_str()).collect();
        assert_eq!(names, ["api", "web"]);
        assert_eq!(l.errors.len(), 1);
        assert_eq!(l.errors[0].kind, WorkloadKind::CronJob);
        assert_eq!(l.errors[0].error.params["resource"], "cronjobs");
        assert_eq!(server.await.unwrap().len(), 5);
    }

    #[tokio::test]
    async fn deployment_details_with_hpa_pdb_and_history() {
        let (client, server) = mock_client(|s| match s.path() {
            "/apis/apps/v1/namespaces/team-a/deployments/web" => (200, deployment("web", 3, 3)),
            "/apis/autoscaling/v2/namespaces/team-a/horizontalpodautoscalers" => (
                200,
                list(
                    "HorizontalPodAutoscaler",
                    vec![serde_json::json!({"metadata": {"name": "web"},
                    "spec": {"scaleTargetRef": {"kind": "Deployment", "name": "web", "apiVersion": "apps/v1"},
                        "minReplicas": 2, "maxReplicas": 10,
                        "metrics": [{"type": "Resource", "resource": {"name": "cpu",
                            "target": {"type": "Utilization", "averageUtilization": 80}}}]},
                    "status": {"currentReplicas": 3, "desiredReplicas": 3,
                        "conditions": [{"type": "ScalingLimited", "status": "False"}]}})],
                ),
            ),
            "/apis/policy/v1/namespaces/team-a/poddisruptionbudgets" => (
                200,
                list(
                    "PodDisruptionBudget",
                    vec![
                        serde_json::json!({"metadata": {"name": "other"},
                        "spec": {"selector": {"matchLabels": {"app": "api"}}}}),
                        serde_json::json!({"metadata": {"name": "web"},
                        "spec": {"minAvailable": 2, "selector": {"matchLabels": {"app": "web"}}},
                        "status": {"disruptionsAllowed": 1, "currentHealthy": 3, "desiredHealthy": 2,
                            "expectedPods": 3}}),
                    ],
                ),
            ),
            "/apis/apps/v1/namespaces/team-a/replicasets" => {
                let rs = |name: &str, rev: &str, owner: &str, image: &str| {
                    serde_json::json!({"metadata": {"name": name,
                        "annotations": {"deployment.kubernetes.io/revision": rev},
                        "ownerReferences": [{"apiVersion": "apps/v1", "kind": "Deployment", "name": "web",
                            "uid": owner, "controller": true}]},
                        "spec": {"selector": {"matchLabels": {"app": "web"}},
                            "template": template("web", image)},
                        "status": {"replicas": if rev == "3" { 3 } else { 0 }}})
                };
                (
                    200,
                    list(
                        "ReplicaSet",
                        vec![
                            rs("web-a", "2", "uid-web", "registry/web:2"),
                            rs("web-b", "3", "uid-web", "registry/web:3"),
                            rs("web-foreign", "9", "uid-other", "x"),
                        ],
                    ),
                )
            }
            other => status(404, "NotFound", other),
        });
        let d = get_workload(&reader(client), "team-a", WorkloadKind::Deployment, "web")
            .await
            .unwrap();
        assert_eq!(d.selector.as_deref(), Some("app=web"));
        assert_eq!(
            d.strategy.as_deref(),
            Some("RollingUpdate (maxSurge 1, maxUnavailable 0)")
        );
        assert_eq!(d.containers.len(), 1);
        assert!(
            !d.annotations
                .contains_key("kubectl.kubernetes.io/last-applied-configuration")
        );
        assert_eq!(d.annotations["deployment.kubernetes.io/revision"], "3");
        let hpa = d.hpa.data.unwrap().unwrap();
        assert_eq!((hpa.min_replicas, hpa.max_replicas), (Some(2), 10));
        assert_eq!(hpa.metrics, ["cpu 80%"]);
        let pdb = d.pdb.data.unwrap().unwrap();
        assert_eq!(pdb.name, "web");
        assert_eq!(pdb.min_available.as_deref(), Some("2"));
        let history = d.history.unwrap().data.unwrap();
        let revs: Vec<_> = history.iter().map(|r| (r.revision, r.current)).collect();
        assert_eq!(
            revs,
            [(3, true), (2, false)],
            "чужой ReplicaSet не в истории"
        );
        assert_eq!(history[1].images, ["registry/web:2"]);
        let seen = server.await.unwrap();
        assert!(seen.iter().any(
            |s| s.path().ends_with("/replicasets") && s.uri.contains("labelSelector=app%3Dweb")
        ));
    }

    /// «Обзор» workload'а скрывает в аннотациях то же, что YAML: копии объекта с литеральными
    /// env и команду `--record` с секретом.
    #[tokio::test]
    async fn workload_annotations_without_copies_and_secret_change_cause() {
        let (client, _server) = mock_client(|s| match s.path() {
            "/apis/apps/v1/namespaces/team-a/deployments/web" => {
                let mut d = deployment("web", 1, 1);
                d["metadata"]["annotations"] = serde_json::json!({
                    "deployment.kubernetes.io/revision": "3",
                    "kapp.k14s.io/original": "{\"kind\":\"Deployment\",\"spec\":{\"template\":{\"spec\":{\"containers\":[{\"env\":[{\"name\":\"DB_PASSWORD\",\"value\":\"s3cr3t\"}]}]}}}}",
                    "objectset.rio.cattle.io/applied": "DB_PASSWORD=s3cr3t",
                    "kubernetes.io/change-cause": "kubectl set env deploy/web API_TOKEN=x --record=true"});
                (200, d)
            }
            p if p.ends_with("s") => (200, list("X", vec![])),
            other => status(404, "NotFound", other),
        });
        let d = get_workload(&reader(client), "team-a", WorkloadKind::Deployment, "web")
            .await
            .unwrap();
        let json = serde_json::to_string(&d).unwrap();
        assert!(
            !json.contains("s3cr3t") && !json.contains("API_TOKEN=x"),
            "{json}"
        );
        assert!(
            !d.annotations.contains_key("kapp.k14s.io/original")
                && !d
                    .annotations
                    .contains_key("objectset.rio.cattle.io/applied")
        );
        assert_eq!(d.annotations["kubernetes.io/change-cause"], "***");
        assert_eq!(d.annotations["deployment.kubernetes.io/revision"], "3");
    }

    #[tokio::test]
    async fn deployment_config_history_and_forbidden_parts() {
        let (client, _server) = mock_client(|s| match s.path() {
            "/apis/apps.openshift.io/v1/namespaces/team-a/deploymentconfigs/legacy" => (
                200,
                serde_json::json!({"apiVersion": "apps.openshift.io/v1", "kind": "DeploymentConfig",
                    "metadata": {"name": "legacy"},
                    "spec": {"replicas": 1, "selector": {"app": "legacy"}, "template": template("legacy", "legacy:7")},
                    "status": {"latestVersion": 7}}),
            ),
            "/api/v1/namespaces/team-a/replicationcontrollers" => {
                assert!(
                    s.uri
                        .contains("openshift.io%2Fdeployment-config.name%3Dlegacy"),
                    "{}",
                    s.uri
                );
                let rc = |n: i64| {
                    serde_json::json!({"metadata": {"name": format!("legacy-{n}"),
                        "annotations": {"openshift.io/deployment-config.latest-version": n.to_string()}},
                        "spec": {"template": template("legacy", &format!("legacy:{n}"))},
                        "status": {"replicas": if n == 7 { 1 } else { 0 }}})
                };
                (200, list("ReplicationController", vec![rc(6), rc(7)]))
            }
            _ => status(
                403,
                "Forbidden",
                r#"x is forbidden: User "u" cannot list resource "horizontalpodautoscalers" in API group "autoscaling" in the namespace "team-a""#,
            ),
        });
        let d = get_workload(
            &reader(client),
            "team-a",
            WorkloadKind::DeploymentConfig,
            "legacy",
        )
        .await
        .unwrap();
        assert!(d.hpa.error.is_some() && d.pdb.error.is_some());
        let history = d.history.unwrap().data.unwrap();
        assert_eq!(
            history
                .iter()
                .map(|r| (r.revision, r.current))
                .collect::<Vec<_>>(),
            [(7, true), (6, false)]
        );
    }

    #[tokio::test]
    async fn jobs_have_no_history_and_missing_workloads_are_not_found() {
        let (client, _server) = mock_client(|s| match s.path() {
            "/apis/batch/v1/namespaces/team-a/jobs/migrate" => (
                200,
                serde_json::json!({"apiVersion": "batch/v1", "kind": "Job", "metadata": {"name": "migrate"},
                    "spec": {"selector": {"matchLabels": {"batch.kubernetes.io/controller-uid": "u1"}},
                        "template": template("migrate", "app:1")}}),
            ),
            "/apis/apps/v1/namespaces/team-a/statefulsets/db" => {
                status(404, "NotFound", "not found")
            }
            _ => (200, list("X", vec![])),
        });
        let r = reader(client);
        let d = get_workload(&r, "team-a", WorkloadKind::Job, "migrate")
            .await
            .unwrap();
        assert!(d.history.is_none());
        assert_eq!(
            pod_selector(&r, "team-a", WorkloadKind::Job, "migrate")
                .await
                .unwrap()
                .as_deref(),
            Some("batch.kubernetes.io/controller-uid=u1")
        );
        let e = get_workload(&r, "team-a", WorkloadKind::StatefulSet, "db")
            .await
            .unwrap_err();
        assert_eq!(e.code, crate::error::ErrorCode::NotFound);
    }

    /// Строка StatefulSet: проблема — доступно меньше желаемого, как у остальных видов (готовый
    /// под до `minReadySeconds` ещё не доступен).
    #[test]
    fn stateful_set_row_counts_available_replicas() {
        let sts = |ready: i32, available: i32| -> StatefulSet {
            from(serde_json::json!({"metadata": {"name": "db",
                    "creationTimestamp": "2026-10-25T10:00:00Z"},
                "spec": {"replicas": 3, "serviceName": "db",
                    "selector": {"matchLabels": {"app": "db"}}, "template": template("db", "postgres:16")},
                "status": {"replicas": 3, "readyReplicas": ready, "availableReplicas": available,
                    "updatedReplicas": 2}}))
        };
        let row = stateful_set_row(&sts(3, 2));
        assert_eq!(
            (row.desired, row.ready, row.available, row.updated),
            (Some(3), Some(3), Some(2), Some(2))
        );
        assert_eq!(row.images, ["postgres:16"]);
        assert_eq!(row.created, Some(1_792_922_400));
        assert_eq!(row.key(), "StatefulSet/db");
        assert_eq!(row.problem, Some(WorkloadProblem::Unavailable));
        assert_eq!(stateful_set_row(&sts(3, 3)).problem, None);
        assert_eq!(
            stateful_set_row(&sts(2, 2)).problem,
            Some(WorkloadProblem::Unavailable)
        );
    }

    fn controller_revision(
        name: &str,
        revision: i64,
        owner: &str,
        image: &str,
    ) -> serde_json::Value {
        serde_json::json!({"apiVersion": "apps/v1", "kind": "ControllerRevision",
            "metadata": {"name": name, "creationTimestamp": "2026-10-25T10:00:00Z",
                "ownerReferences": [{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "db",
                    "uid": owner, "controller": true}]},
            "revision": revision,
            "data": {"spec": {"template": {"spec": {"containers": [
                {"name": "db", "image": image}, {"name": "exporter", "image": "exporter:1"}]}}}}})
    }

    /// История workload'а `db` вида `kind` со статусом `status` из ControllerRevisions: своих
    /// `db-1` и `db-2` и чужой `other-9` вперемешку.
    async fn controller_history(kind: WorkloadKind, status: serde_json::Value) -> Vec<Revision> {
        let (path, api_kind) = match kind {
            WorkloadKind::StatefulSet => ("statefulsets", "StatefulSet"),
            _ => ("daemonsets", "DaemonSet"),
        };
        let object = serde_json::json!({"apiVersion": "apps/v1", "kind": api_kind,
            "metadata": {"name": "db", "namespace": "team-a", "uid": "uid-db"},
            "spec": {"serviceName": "db", "selector": {"matchLabels": {"app": "db"}},
                "template": template("db", "postgres:17")},
            "status": status});
        let own = format!("/apis/apps/v1/namespaces/team-a/{path}/db");
        let (client, server) = mock_client(move |s| match s.path() {
            p if p == own => (200, object.clone()),
            "/apis/apps/v1/namespaces/team-a/controllerrevisions" => (
                200,
                list(
                    "ControllerRevision",
                    vec![
                        controller_revision("db-1", 1, "uid-db", "postgres:16"),
                        controller_revision("other-9", 9, "uid-other", "other:1"),
                        controller_revision("db-2", 2, "uid-db", "postgres:17"),
                    ],
                ),
            ),
            _ => (200, list("X", vec![])),
        });
        let d = get_workload(&reader(client), "team-a", kind, "db")
            .await
            .unwrap();
        let seen = server.await.unwrap();
        assert!(
            seen.iter()
                .any(|s| s.path().ends_with("/controllerrevisions")
                    && s.uri.contains("labelSelector=app%3Ddb"))
        );
        d.history
            .expect("история есть")
            .data
            .expect("история прочитана")
    }

    /// Ревизии StatefulSet и DaemonSet: только свои, новые первыми, образы из шаблона ревизии.
    /// Текущая у StatefulSet — `status.currentRevision` (при `OnDelete` поды остались на старой),
    /// без него и у DaemonSet — самая новая.
    #[tokio::test]
    async fn controller_revisions_are_the_history_of_stateful_and_daemon_sets() {
        let marks = |h: &[Revision]| -> Vec<(i64, String, bool)> {
            h.iter()
                .map(|r| (r.revision, r.name.clone(), r.current))
                .collect()
        };
        let newest = [
            (2, "db-2".to_string(), true),
            (1, "db-1".to_string(), false),
        ];

        let h = controller_history(
            WorkloadKind::StatefulSet,
            serde_json::json!({"replicas": 3, "currentRevision": "db-1", "updateRevision": "db-2"}),
        )
        .await;
        assert_eq!(
            marks(&h),
            [
                (2, "db-2".to_string(), false),
                (1, "db-1".to_string(), true)
            ],
            "чужой ревизии нет, поды — на db-1"
        );
        assert_eq!(h[0].images, ["postgres:17", "exporter:1"]);
        assert_eq!(h[1].images, ["postgres:16", "exporter:1"]);
        assert!(
            h.iter()
                .all(|r| r.replicas.is_none() && r.created == Some(1_792_922_400))
        );

        let h = controller_history(
            WorkloadKind::StatefulSet,
            serde_json::json!({"replicas": 3}),
        )
        .await;
        assert_eq!(marks(&h), newest, "без currentRevision — самая новая");

        let h = controller_history(
            WorkloadKind::DaemonSet,
            serde_json::json!({"currentNumberScheduled": 1, "desiredNumberScheduled": 1,
                "numberMisscheduled": 0, "numberReady": 1}),
        )
        .await;
        assert_eq!(marks(&h), newest);
    }

    /// DeploymentConfig, поля которого не того вида, — не строка из пустого объекта: список
    /// его пропускает и называет в ошибке вида (остальные строки на месте), подробности —
    /// ошибка, строка для watch — только имя и возраст, без чисел и проблемы.
    #[tokio::test]
    async fn malformed_deployment_config_is_an_error_not_an_empty_row() {
        let dc = |name: &str, spec: serde_json::Value| {
            serde_json::json!({"apiVersion": "apps.openshift.io/v1", "kind": "DeploymentConfig",
                "metadata": {"name": name, "creationTimestamp": "2026-10-25T10:00:00Z"},
                "spec": spec, "status": {"availableReplicas": 1}})
        };
        let good = dc(
            "legacy",
            serde_json::json!({"replicas": 1, "selector": {"app": "legacy"},
                "template": template("legacy", "legacy:7")}),
        );
        let null_spec = dc("broken", serde_json::Value::Null);
        let bad_template = dc(
            "odd",
            serde_json::json!({"template": {"spec": {"containers": "web"}}}),
        );
        let items = vec![good, null_spec.clone(), bad_template.clone()];
        let single = null_spec.clone();
        let (client, _server) = mock_client(move |s| match s.path() {
            "/apis/apps.openshift.io/v1/namespaces/team-a/deploymentconfigs" => {
                (200, list("DeploymentConfig", items.clone()))
            }
            "/apis/apps.openshift.io/v1/namespaces/team-a/deploymentconfigs/broken" => {
                (200, single.clone())
            }
            _ => (200, list("X", vec![])),
        });
        let r = reader(client);
        let groups = ApiGroups {
            deployment_configs: true,
            ..Default::default()
        };
        let l = list_workloads(&r, "team-a", &groups).await;
        let names: Vec<_> = l.items.iter().map(|w| w.name.as_str()).collect();
        assert_eq!(names, ["legacy"], "пустых строк нет");
        assert_eq!(l.errors.len(), 1, "одна ошибка на вид");
        let e = &l.errors[0];
        assert_eq!(e.kind, WorkloadKind::DeploymentConfig);
        assert_eq!(e.error.key, "errors.k8s.malformedObject");
        assert_eq!(
            (
                e.error.params["kind"].as_str(),
                e.error.params["name"].as_str()
            ),
            ("DeploymentConfig", "broken"),
            "ошибка первого из пропущенных"
        );
        assert_eq!(
            e.error.details, None,
            "текст serde (он цитирует значения полей) в ошибку не попадает"
        );

        let e = get_workload(&r, "team-a", WorkloadKind::DeploymentConfig, "broken")
            .await
            .unwrap_err();
        assert_eq!(e.key, "errors.k8s.malformedObject");

        for (o, name) in [(null_spec, "broken"), (bad_template, "odd")] {
            let o: DynamicObject = from(o);
            assert!(parse_deployment_config(&o).is_err(), "{name}");
            let row = deployment_config_row(&o);
            assert_eq!(row.name, name);
            assert_eq!(row.created, Some(1_792_922_400));
            assert_eq!(
                (row.desired, row.available, row.problem),
                (None, None, None),
                "{name}: без чисел и ложной проблемы"
            );
        }
    }

    #[test]
    fn kinds_serialize_as_in_kubernetes() {
        assert_eq!(
            serde_json::to_value(WorkloadKind::DeploymentConfig).unwrap(),
            "DeploymentConfig"
        );
        let k: WorkloadKind = serde_json::from_value("CronJob".into()).unwrap();
        assert_eq!(k, WorkloadKind::CronJob);
        assert!(!WorkloadKind::DeploymentConfig.available(&ApiGroups::default()));
    }
}
