//! Изменяющие операции (спец. §9): запросы записи в кластер есть только здесь. Порядок — как в
//! спеке: предпросмотр читает объект, строит «было → станет» и выполняет server-side dry-run;
//! применение после стражей (`guard`) перечитывает объект, сверяет версию предпросмотра
//! (`Facts::version`: изменившийся spec даёт `CONFLICT`) и пишет с
//! `fieldManager=glafira-kubernetes`. MCP-шлюз сюда не
//! доходит (сторожок `mcp::guard`): модель только предлагает изменения (`health::proposals`).

use std::time::Duration;

use futures::future::BoxFuture;
use k8s_openapi::api::core::v1::ResourceRequirements;
use serde::Serialize;
use serde_json::{Map, Value, json};

use crate::change::facts::{Facts, WorkloadFacts};
use crate::change::facts::{FieldChange, RESTARTED_AT, Warning, check, diff, read_facts};
use crate::change::{Change, ContainerResources, validate_form};
use crate::error::{AppError, AppResult, ErrorCode};
use crate::k8s::read::ReadClient;
use crate::k8s::workloads::WorkloadKind;

/// Кем изменено поле (`managedFields`).
pub const FIELD_MANAGER: &str = "glafira-kubernetes";

/// Вид запроса записи.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verb {
    /// Strategic merge patch (встроенные виды: контейнеры сливаются по имени).
    Strategic,
    /// JSON merge patch (подресурс `scale`, HPA).
    Merge,
    /// JSON patch (DeploymentConfig: контейнер — по индексу).
    Json,
    /// `instantiate` DeploymentConfig.
    Post,
    Delete,
}

impl Verb {
    fn method(self) -> http::Method {
        match self {
            Verb::Strategic | Verb::Merge | Verb::Json => http::Method::PATCH,
            Verb::Post => http::Method::POST,
            Verb::Delete => http::Method::DELETE,
        }
    }

    fn content_type(self) -> &'static str {
        match self {
            Verb::Strategic => "application/strategic-merge-patch+json",
            Verb::Merge => "application/merge-patch+json",
            Verb::Json => "application/json-patch+json",
            Verb::Post | Verb::Delete => "application/json",
        }
    }
}

/// Запрос записи: что уйдёт в кластер.
#[derive(Debug, Clone, PartialEq)]
pub struct Write {
    pub verb: Verb,
    /// Путь API без строки запроса.
    pub path: String,
    pub body: Value,
    /// API принимает `dryRun=All` (у `instantiate` DeploymentConfig — нет).
    pub dry_run: bool,
}

/// Запись в кластер: в тестах подменяется.
pub trait ClusterWriter: Send + Sync {
    /// `dry_run` — server-side dry-run: проверки и admission без изменения кластера.
    fn send<'a>(&'a self, w: &'a Write, dry_run: bool) -> BoxFuture<'a, AppResult<()>>;
}

/// Запись через клиент подключения (`Connected::writer`): запросы — только `Write`.
pub struct KubeWriter {
    client: kube::Client,
    timeout: Duration,
}

impl KubeWriter {
    pub fn new(client: kube::Client, timeout: Duration) -> Self {
        Self { client, timeout }
    }
}

/// Отказ admission-webhook — `ADMISSION_DENIED`, а не «нет прав»: 403 с текстом, который не
/// разобрался как отказ RBAC (`k8s::errors::forbidden`). `LimitRange` и квоту шаблона workload'а
/// API-сервер не проверяет: их проверяет `change::facts::check` до запроса.
fn admission(e: AppError) -> AppError {
    if e.key != "errors.k8s.forbidden" {
        return e;
    }
    let message = e.params.get("message").cloned().unwrap_or_default();
    crate::app_err!(
        ErrorCode::AdmissionDenied,
        "errors.ops.admissionDenied",
        "The cluster rejected the change: {{message}}",
        message = message
    )
}

impl ClusterWriter for KubeWriter {
    fn send<'a>(&'a self, w: &'a Write, dry_run: bool) -> BoxFuture<'a, AppResult<()>> {
        Box::pin(async move {
            // Вызов без dry-run (`instantiate` DeploymentConfig) `dryRun=All` может проигнорировать
            // и выполнить по-настоящему: такой запрос не уходит вовсе.
            if dry_run && !w.dry_run {
                return Err(AppError::internal_with(
                    "this request does not accept a dry-run",
                ));
            }
            // У DELETE с телом API-сервер берёт параметры только из `DeleteOptions`: строку
            // запроса он не читает, и `dryRun` в ней удалил бы объект по-настоящему.
            let mut body = w.body.clone();
            // Сериализатор не `Send`: строка запроса собирается до первого `await`.
            let query = {
                let mut q = url::form_urlencoded::Serializer::new(String::new());
                if w.verb == Verb::Delete {
                    if dry_run {
                        body["dryRun"] = json!(["All"]);
                    }
                } else {
                    q.append_pair("fieldManager", FIELD_MANAGER);
                    if dry_run {
                        q.append_pair("dryRun", "All");
                    }
                }
                q.finish()
            };
            let uri = match query.is_empty() {
                true => w.path.clone(),
                false => format!("{}?{query}", w.path),
            };
            let request = http::Request::builder()
                .method(w.verb.method())
                .uri(uri)
                .header(http::header::CONTENT_TYPE, w.verb.content_type())
                .body(
                    serde_json::to_vec(&body)
                        .map_err(|e| AppError::internal_with(e.to_string()))?,
                )
                .map_err(|e| AppError::internal_with(e.to_string()))?;
            crate::k8s::cluster::timed(self.timeout, self.client.request_text(request))
                .await
                .map(|_| ())
                .map_err(admission)
        })
    }
}

/// Путь workload'а в API.
fn workload_path(namespace: &str, kind: WorkloadKind, name: &str) -> String {
    let (prefix, plural) = match kind {
        WorkloadKind::Deployment => ("/apis/apps/v1", "deployments"),
        WorkloadKind::StatefulSet => ("/apis/apps/v1", "statefulsets"),
        WorkloadKind::DaemonSet => ("/apis/apps/v1", "daemonsets"),
        WorkloadKind::Job => ("/apis/batch/v1", "jobs"),
        WorkloadKind::CronJob => ("/apis/batch/v1", "cronjobs"),
        WorkloadKind::DeploymentConfig => ("/apis/apps.openshift.io/v1", "deploymentconfigs"),
    };
    format!("{prefix}/namespaces/{namespace}/{plural}/{name}")
}

/// `requests` или `limits` изменения для strategic merge patch: значение или `null` (убрать).
fn smp_values(v: &crate::change::ResourceValues) -> Value {
    let mut m = Map::new();
    for (k, x) in [("cpu", &v.cpu), ("memory", &v.memory)] {
        if let Some(x) = x {
            m.insert(k.into(), x.clone().map_or(Value::Null, Value::String));
        }
    }
    Value::Object(m)
}

/// Ресурсы контейнера после изменения целиком: прочие ресурсы (`ephemeral-storage`, GPU)
/// остаются — для JSON patch DeploymentConfig.
fn merged(current: Option<&ResourceRequirements>, c: &ContainerResources) -> Value {
    let mut out = serde_json::to_value(current.cloned().unwrap_or_default()).unwrap_or(json!({}));
    for (part, values) in [("requests", &c.requests), ("limits", &c.limits)] {
        let map = out
            .as_object_mut()
            .map(|o| o.entry(part).or_insert_with(|| json!({})));
        let Some(Value::Object(map)) = map else {
            continue;
        };
        for (k, x) in [("cpu", &values.cpu), ("memory", &values.memory)] {
            match x {
                None => {}
                Some(None) => {
                    map.remove(k);
                }
                Some(Some(q)) => {
                    map.insert(k.into(), Value::String(q.clone()));
                }
            }
        }
    }
    if let Value::Object(o) = &mut out {
        o.retain(|_, v| !v.as_object().is_some_and(Map::is_empty) && !v.is_null());
    }
    out
}

/// Ресурсы контейнеров: strategic merge patch по имени контейнера (у CronJob — шаблон Job),
/// у DeploymentConfig — JSON patch по индексу с `resourceVersion` в объекте.
fn set_resources(
    w: &WorkloadFacts,
    namespace: &str,
    kind: WorkloadKind,
    name: &str,
    containers: &[ContainerResources],
    rv: Option<&str>,
) -> AppResult<Write> {
    let path = workload_path(namespace, kind, name);
    let pod = w.spec.template.as_ref().and_then(|t| t.spec.as_ref());
    if kind == WorkloadKind::DeploymentConfig {
        let mut ops =
            vec![json!({"op": "replace", "path": "/metadata/resourceVersion", "value": rv})];
        for c in containers {
            let (list, idx, current) = pod
                .and_then(|p| {
                    let find = |list: &[k8s_openapi::api::core::v1::Container],
                                field: &'static str| {
                        list.iter()
                            .position(|x| x.name == c.name)
                            .map(|i| (field, i, list[i].resources.clone()))
                    };
                    find(&p.containers, "containers").or_else(|| {
                        p.init_containers
                            .as_deref()
                            .and_then(|l| find(l, "initContainers"))
                    })
                })
                .ok_or_else(|| AppError::internal_with("container is not in the template"))?;
            ops.push(json!({
                "op": "add",
                "path": format!("/spec/template/spec/{list}/{idx}/resources"),
                "value": merged(current.as_ref(), c),
            }));
        }
        return Ok(Write {
            verb: Verb::Json,
            path,
            body: Value::Array(ops),
            dry_run: true,
        });
    }
    if kind == WorkloadKind::Job {
        // `validate_form` такое изменение не пропускает: шаблон Job неизменяем.
        return Err(AppError::internal_with(
            "the pod template of a Job is immutable",
        ));
    }
    let init: std::collections::BTreeSet<&str> = pod
        .and_then(|p| p.init_containers.as_ref())
        .into_iter()
        .flatten()
        .map(|c| c.name.as_str())
        .collect();
    let item = |c: &ContainerResources| {
        let mut res = Map::new();
        for (part, v) in [("requests", &c.requests), ("limits", &c.limits)] {
            let values = smp_values(v);
            if values.as_object().is_some_and(|o| !o.is_empty()) {
                res.insert(part.into(), values);
            }
        }
        json!({"name": c.name, "resources": res})
    };
    let main: Vec<Value> = containers
        .iter()
        .filter(|c| !init.contains(c.name.as_str()))
        .map(item)
        .collect();
    let inits: Vec<Value> = containers
        .iter()
        .filter(|c| init.contains(c.name.as_str()))
        .map(item)
        .collect();
    let mut spec = Map::new();
    if !main.is_empty() {
        spec.insert("containers".into(), Value::Array(main));
    }
    if !inits.is_empty() {
        spec.insert("initContainers".into(), Value::Array(inits));
    }
    let template = json!({"spec": spec});
    let body = match kind {
        WorkloadKind::CronJob => json!({"spec": {"jobTemplate": {"spec": {"template": template}}}}),
        _ => json!({"spec": {"template": template}}),
    };
    Ok(Write {
        verb: Verb::Strategic,
        path,
        body,
        dry_run: true,
    })
}

/// Запросы записи изменения. Значения абсолютные, как у kubectl: `resourceVersion` прочитанного
/// объекта требует только JSON patch DeploymentConfig, адресующий контейнер по индексу (иначе 409);
/// под удаляется, только если это тот же объект (`uid`).
pub fn writes(change: &Change, facts: &Facts, now: &str) -> AppResult<Vec<Write>> {
    let ns = change.namespace();
    let rv = facts.resource_version();
    Ok(match (change, facts) {
        (
            Change::SetResources {
                workload,
                containers,
                ..
            },
            Facts::Workload(w),
        ) => vec![set_resources(
            w,
            ns,
            workload.kind,
            &workload.name,
            containers,
            rv,
        )?],
        (
            Change::Scale {
                workload, replicas, ..
            },
            Facts::Workload(_),
        ) => vec![Write {
            verb: Verb::Merge,
            path: format!("{}/scale", workload_path(ns, workload.kind, &workload.name)),
            body: json!({"spec": {"replicas": replicas}}),
            dry_run: true,
        }],
        (Change::RolloutRestart { workload, .. }, Facts::Workload(_)) => {
            let path = workload_path(ns, workload.kind, &workload.name);
            if workload.kind == WorkloadKind::DeploymentConfig {
                // `oc rollout latest`: новая выкладка; dry-run этот вызов не принимает.
                vec![Write {
                    verb: Verb::Post,
                    path: format!("{path}/instantiate"),
                    body: json!({"kind": "DeploymentRequest", "apiVersion": "apps.openshift.io/v1",
                        "name": workload.name, "latest": true, "force": true}),
                    dry_run: false,
                }]
            } else {
                vec![Write {
                    verb: Verb::Strategic,
                    path,
                    body: json!({"spec": {"template": {"metadata": {"annotations": {RESTARTED_AT: now}}}}}),
                    dry_run: true,
                }]
            }
        }
        (
            Change::SetHpaBounds {
                hpa,
                min_replicas,
                max_replicas,
                ..
            },
            Facts::Hpa(_),
        ) => {
            let mut spec = json!({"maxReplicas": max_replicas});
            if let Some(m) = min_replicas {
                spec["minReplicas"] = json!(m);
            }
            vec![Write {
                verb: Verb::Merge,
                path: format!(
                    "/apis/autoscaling/v2/namespaces/{ns}/horizontalpodautoscalers/{hpa}"
                ),
                body: json!({"spec": spec}),
                dry_run: true,
            }]
        }
        (Change::DeletePod { pod, .. }, Facts::Pod { uid, .. }) => vec![Write {
            verb: Verb::Delete,
            path: format!("/api/v1/namespaces/{ns}/pods/{pod}"),
            body: json!({"apiVersion": "v1", "kind": "DeleteOptions",
                "preconditions": {"uid": uid}}),
            dry_run: true,
        }],
        _ => return Err(AppError::internal_with("facts do not match the change")),
    })
}

/// Итог server-side dry-run.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum DryRun {
    Passed,
    /// API-сервер отклонил изменение: ошибка до подтверждения.
    Rejected {
        error: AppError,
    },
    /// Вызов dry-run не принимает (`instantiate` DeploymentConfig).
    Skipped,
}

/// Предпросмотр изменения для формы.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Preview {
    pub change: Change,
    /// Версия объекта (`Facts::version`): с ней идёт применение, объект изменился — `CONFLICT`.
    pub version: Option<String>,
    pub diff: Vec<FieldChange>,
    pub warnings: Vec<Warning>,
    pub dry_run: DryRun,
    /// Под без контроллера: при применении нужно ввести его имя.
    pub confirm_pod_name: bool,
}

/// Попыток записи: контроллер обновляет статус объекта и его `resourceVersion` между чтением и
/// записью (идущая выкладка), ответ 409 — повод перечитать объект.
const WRITE_ATTEMPTS: usize = 3;

/// Ответ 409: объект изменился между чтением и записью.
fn raced(e: &AppError) -> bool {
    e.key == "errors.k8s.conflict"
}

/// Ответ API-сервера, который предпросмотр показывает как результат dry-run: отказ по правам или
/// admission, неверное значение, гонка со статусом. Остальное (таймаут, обрыв связи, 401, 5xx) —
/// ошибка команды: `read_in` сбросит сломанный клиент и кэш прав.
fn rejects(e: &AppError) -> bool {
    matches!(
        e.code,
        ErrorCode::Forbidden
            | ErrorCode::AdmissionDenied
            | ErrorCode::InvalidInput
            | ErrorCode::Conflict
    )
}

/// Предпросмотр: форма, объект, проверка, «было → станет», dry-run.
pub async fn preview(
    r: &ReadClient,
    w: &dyn ClusterWriter,
    change: Change,
    now: &str,
) -> AppResult<Preview> {
    let change = change.trimmed();
    validate_form(&change)?;
    let mut attempt = 1;
    loop {
        // Чтение объекта и ограничений — большая future: в debug-сборке в куче.
        let facts = Box::pin(read_facts(r, &change)).await?;
        let warnings = check(&change, &facts)?;
        let mut dry_run = DryRun::Passed;
        for write in writes(&change, &facts, now)? {
            if !write.dry_run {
                dry_run = DryRun::Skipped;
                continue;
            }
            if let Err(error) = w.send(&write, true).await {
                if !rejects(&error) {
                    return Err(error);
                }
                dry_run = DryRun::Rejected { error };
                break;
            }
        }
        if let DryRun::Rejected { error } = &dry_run
            && raced(error)
            && attempt < WRITE_ATTEMPTS
        {
            attempt += 1;
            continue;
        }
        return Ok(Preview {
            diff: diff(&change, &facts, now),
            confirm_pod_name: warnings.contains(&Warning::Orphan),
            version: facts.version(),
            warnings,
            dry_run,
            change,
        });
    }
}

/// Применение после стражей подключения: объект перечитывается и сверяется с версией
/// предпросмотра `expected` (spec изменился — `CONFLICT`), под без контроллера требует ввода
/// имени (`confirm_pod`). Без версии предпросмотра (`None`) или без версии прочитанного объекта
/// запись не уходит: сверять не с чем, это тоже `CONFLICT` («Обновить и повторить»). Возвращает
/// применённое изменение.
pub async fn apply(
    r: &ReadClient,
    w: &dyn ClusterWriter,
    change: Change,
    expected: Option<&str>,
    confirm_pod: Option<&str>,
    now: &str,
) -> AppResult<Change> {
    let change = change.trimmed();
    validate_form(&change)?;
    let mut attempt = 1;
    loop {
        // Чтение объекта и ограничений — большая future: в debug-сборке в куче.
        let facts = Box::pin(read_facts(r, &change)).await?;
        let warnings = check(&change, &facts)?;
        if let (Change::DeletePod { pod, .. }, true) =
            (&change, warnings.contains(&Warning::Orphan))
        {
            crate::guard::ensure_name_confirmed(pod, confirm_pod)?;
        }
        // Версия обязательна с обеих сторон: иначе пересозданный под с тем же именем удалился бы
        // вслепую.
        match (expected, facts.version()) {
            (Some(e), Some(v)) if e == v => {}
            _ => return Err(changed()),
        }
        let mut sent = Ok(());
        for write in writes(&change, &facts, now)? {
            sent = w.send(&write, false).await;
            if sent.is_err() {
                break;
            }
        }
        match sent {
            Err(e) if raced(&e) && attempt < WRITE_ATTEMPTS => attempt += 1,
            sent => return sent.map(|()| change),
        }
    }
}

/// Объект изменился после предпросмотра.
fn changed() -> AppError {
    crate::app_err!(
        ErrorCode::Conflict,
        "errors.ops.changed",
        "The object has changed since the preview: refresh and retry"
    )
}

/// Сейчас в RFC 3339 (аннотация перезапуска).
pub fn now_rfc3339() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default()
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration as StdDuration;

    use super::*;
    use crate::k8s::pods::WorkloadRef;
    use crate::k8s::testing::{mock_client, status};

    /// Ответ записи для тестов.
    type Answer = Box<dyn Fn(&Write, bool) -> AppResult<()> + Send + Sync>;

    /// Запись для тестов: запоминает запросы, отвечает `answer`.
    pub struct FakeWriter {
        pub sent: Mutex<Vec<(Write, bool)>>,
        answer: Answer,
    }

    impl FakeWriter {
        pub fn new(answer: impl Fn(&Write, bool) -> AppResult<()> + Send + Sync + 'static) -> Self {
            Self {
                sent: Mutex::new(Vec::new()),
                answer: Box::new(answer),
            }
        }
    }

    impl ClusterWriter for FakeWriter {
        fn send<'a>(&'a self, w: &'a Write, dry_run: bool) -> BoxFuture<'a, AppResult<()>> {
            self.sent.lock().unwrap().push((w.clone(), dry_run));
            let r = (self.answer)(w, dry_run);
            Box::pin(async move { r })
        }
    }

    fn deployment() -> serde_json::Value {
        json!({"apiVersion": "apps/v1", "kind": "Deployment",
            "metadata": {"name": "web", "namespace": "team-a", "resourceVersion": "11", "generation": 2},
            "spec": {"replicas": 2, "selector": {"matchLabels": {"app": "web"}},
                "template": {"metadata": {"labels": {"app": "web"}}, "spec": {
                    "initContainers": [{"name": "proxy", "image": "p", "restartPolicy": "Always"}],
                    "containers": [{"name": "app", "image": "web:1", "resources": {
                        "requests": {"cpu": "500m", "ephemeral-storage": "1Gi"}, "limits": {"memory": "1Gi"}}}]}}}})
    }

    fn cluster(s: &crate::k8s::testing::Seen) -> (u16, serde_json::Value) {
        let list = |kind: &str| {
            (
                200,
                json!({"apiVersion": "v1", "kind": kind, "metadata": {}, "items": []}),
            )
        };
        match s.path() {
            "/apis/apps/v1/namespaces/team-a/deployments/web" => (200, deployment()),
            "/apis/apps.openshift.io/v1/namespaces/team-a/deploymentconfigs/web" => (
                200,
                json!({"apiVersion": "apps.openshift.io/v1", "kind": "DeploymentConfig",
                    "metadata": {"name": "web", "namespace": "team-a", "resourceVersion": "21", "generation": 7},
                    "spec": {"replicas": 1, "selector": {"app": "web"},
                        "template": {"metadata": {"labels": {"app": "web"}}, "spec": {"containers": [
                            {"name": "side", "image": "s"},
                            {"name": "app", "image": "web:1", "resources": {
                                "requests": {"cpu": "500m", "ephemeral-storage": "1Gi"}}}]}}},
                    "status": {}}),
            ),
            "/apis/batch/v1/namespaces/team-a/cronjobs/web" => (
                200,
                json!({"apiVersion": "batch/v1", "kind": "CronJob",
                    "metadata": {"name": "web", "namespace": "team-a", "resourceVersion": "31"},
                    "spec": {"schedule": "* * * * *", "jobTemplate": {"spec": {"template": {
                        "spec": {"containers": [{"name": "app", "image": "r"}]}}}}}}),
            ),
            "/api/v1/namespaces/team-a/pods/solo" => (
                200,
                json!({"apiVersion": "v1", "kind": "Pod",
                    "metadata": {"name": "solo", "resourceVersion": "3", "uid": "u-solo"}, "spec": {"containers": []}}),
            ),
            "/apis/autoscaling/v2/namespaces/team-a/horizontalpodautoscalers/web" => (
                200,
                json!({"apiVersion": "autoscaling/v2", "kind": "HorizontalPodAutoscaler",
                    "metadata": {"name": "web", "resourceVersion": "5", "generation": 1},
                    "spec": {"minReplicas": 2, "maxReplicas": 6, "scaleTargetRef": {"kind": "Deployment", "name": "web"}}}),
            ),
            p if p.ends_with("/horizontalpodautoscalers") => list("HorizontalPodAutoscalerList"),
            p if p.ends_with("/limitranges") => list("LimitRangeList"),
            p if p.ends_with("/resourcequotas") => list("ResourceQuotaList"),
            _ => status(404, "NotFound", "not found"),
        }
    }

    pub fn reader() -> ReadClient {
        ReadClient::new(mock_client(cluster).0, StdDuration::from_secs(5))
    }

    fn workload(kind: WorkloadKind) -> WorkloadRef {
        WorkloadRef {
            kind,
            name: "web".into(),
        }
    }

    fn set(kind: &str, containers: Value) -> Change {
        serde_json::from_value(json!({"kind": "setResources", "namespace": "team-a",
            "workload": {"kind": kind, "name": "web"}, "containers": containers}))
        .unwrap()
    }

    #[tokio::test]
    async fn resources_are_a_strategic_merge_patch_by_container_name() {
        let w = FakeWriter::new(|_, _| Ok(()));
        let c = set(
            "Deployment",
            json!([
                {"name": "app", "requests": {"cpu": "250m", "memory": null}, "limits": {"memory": "2Gi"}},
                {"name": "proxy", "requests": {"cpu": "50m"}}
            ]),
        );
        let p = preview(&reader(), &w, c, "now").await.unwrap();
        assert!(matches!(p.dry_run, DryRun::Passed));
        assert_eq!(
            p.version.as_deref(),
            Some("g2"),
            "generation, не resourceVersion"
        );
        let sent = w.sent.lock().unwrap();
        let (write, dry) = &sent[0];
        assert!(*dry);
        assert_eq!(
            (write.verb, write.path.as_str()),
            (
                Verb::Strategic,
                "/apis/apps/v1/namespaces/team-a/deployments/web"
            )
        );
        assert_eq!(
            write.body,
            json!({"spec": {"template": {"spec": {
            "containers": [{"name": "app", "resources": {"requests": {"cpu": "250m", "memory": null}, "limits": {"memory": "2Gi"}}}],
            "initContainers": [{"name": "proxy", "resources": {"requests": {"cpu": "50m"}}}]}}}})
        );
    }

    #[tokio::test]
    async fn deployment_config_resources_are_a_json_patch_that_keeps_other_resources() {
        let w = FakeWriter::new(|_, _| Ok(()));
        let c = set(
            "DeploymentConfig",
            json!([{"name": "app", "requests": {"cpu": null, "memory": "256Mi"}}]),
        );
        apply(&reader(), &w, c, Some("g7"), None, "now")
            .await
            .unwrap();
        let (write, dry) = w.sent.lock().unwrap()[0].clone();
        assert!(!dry);
        assert_eq!(write.verb, Verb::Json);
        assert_eq!(
            write.body,
            json!([
                {"op": "replace", "path": "/metadata/resourceVersion", "value": "21"},
                {"op": "add", "path": "/spec/template/spec/containers/1/resources",
                 "value": {"requests": {"ephemeral-storage": "1Gi", "memory": "256Mi"}}}
            ])
        );
    }

    #[tokio::test]
    async fn cron_job_template_and_job_refusal() {
        let w = FakeWriter::new(|_, _| Ok(()));
        let c = set("CronJob", json!([{"name": "app", "limits": {"cpu": "1"}}]));
        preview(&reader(), &w, c, "now").await.unwrap();
        let body = w.sent.lock().unwrap()[0].0.body.clone();
        assert_eq!(
            body["spec"]["jobTemplate"]["spec"]["template"]["spec"]["containers"][0]["resources"]["limits"]
                ["cpu"],
            "1"
        );
        assert_eq!(
            body.get("metadata"),
            None,
            "без resourceVersion: значения абсолютные"
        );
    }

    #[tokio::test]
    async fn scale_restart_hpa_and_delete() {
        let w = FakeWriter::new(|_, _| Ok(()));
        let r = reader();
        let scale = Change::Scale {
            namespace: "team-a".into(),
            workload: workload(WorkloadKind::Deployment),
            replicas: 4,
        };
        apply(&r, &w, scale, Some("g2"), None, "now").await.unwrap();
        let restart = Change::RolloutRestart {
            namespace: "team-a".into(),
            workload: workload(WorkloadKind::Deployment),
        };
        apply(&r, &w, restart, Some("g2"), None, "2026-10-28T12:00:00Z")
            .await
            .unwrap();
        let dc = Change::RolloutRestart {
            namespace: "team-a".into(),
            workload: workload(WorkloadKind::DeploymentConfig),
        };
        let p = preview(&r, &w, dc.clone(), "now").await.unwrap();
        assert!(
            matches!(p.dry_run, DryRun::Skipped),
            "instantiate без dry-run"
        );
        apply(&r, &w, dc, Some("g7"), None, "now").await.unwrap();
        let hpa = Change::SetHpaBounds {
            namespace: "team-a".into(),
            hpa: "web".into(),
            min_replicas: Some(3),
            max_replicas: 8,
        };
        apply(&r, &w, hpa, Some("g1"), None, "now").await.unwrap();
        let sent: Vec<(Verb, String, Value)> = w
            .sent
            .lock()
            .unwrap()
            .iter()
            .map(|(x, _)| (x.verb, x.path.clone(), x.body.clone()))
            .collect();
        assert_eq!(
            sent[0],
            (
                Verb::Merge,
                "/apis/apps/v1/namespaces/team-a/deployments/web/scale".into(),
                json!({"spec": {"replicas": 4}})
            )
        );
        assert_eq!(
            sent[1].2["spec"]["template"]["metadata"]["annotations"][RESTARTED_AT],
            "2026-10-28T12:00:00Z"
        );
        assert_eq!(
            sent[2].1,
            "/apis/apps.openshift.io/v1/namespaces/team-a/deploymentconfigs/web/instantiate"
        );
        assert_eq!(sent[2].2["latest"], true);
        assert_eq!(
            sent[3],
            (
                Verb::Merge,
                "/apis/autoscaling/v2/namespaces/team-a/horizontalpodautoscalers/web".into(),
                json!({"spec": {"minReplicas": 3, "maxReplicas": 8}})
            )
        );
        // Под без контроллера удаляется только с его именем.
        let pod = Change::DeletePod {
            namespace: "team-a".into(),
            pod: "solo".into(),
        };
        let p = preview(&r, &w, pod.clone(), "now").await.unwrap();
        assert!(p.confirm_pod_name);
        let before = w.sent.lock().unwrap().len();
        let e = apply(&r, &w, pod.clone(), Some("uu-solo"), None, "now")
            .await
            .unwrap_err();
        assert_eq!(e.key, "errors.guard.typeToConfirm");
        assert_eq!(
            w.sent.lock().unwrap().len(),
            before,
            "без имени в кластер ничего не ушло"
        );
        apply(&r, &w, pod, Some("uu-solo"), Some("solo"), "now")
            .await
            .unwrap();
        let last = w.sent.lock().unwrap().last().unwrap().0.clone();
        assert_eq!(
            (last.verb, last.body["preconditions"].clone()),
            (Verb::Delete, json!({"uid": "u-solo"}))
        );
    }

    #[tokio::test]
    async fn a_changed_object_conflicts_and_a_rejected_dry_run_is_shown() {
        let w = FakeWriter::new(|_, dry| {
            if dry {
                Err(admission(crate::k8s::errors::forbidden(
                    "admission webhook \"policy.example.com\" denied the request: no",
                )))
            } else {
                Ok(())
            }
        });
        let c = set(
            "Deployment",
            json!([{"name": "app", "requests": {"cpu": "1"}}]),
        );
        let p = preview(&reader(), &w, c.clone(), "now").await.unwrap();
        let DryRun::Rejected { error } = p.dry_run else {
            panic!("{:?}", p.dry_run)
        };
        assert_eq!(error.code, ErrorCode::AdmissionDenied);
        assert!(
            error.message.contains("policy.example.com"),
            "{}",
            error.message
        );
        let e = apply(&reader(), &w, c.clone(), Some("g10"), None, "now")
            .await
            .unwrap_err();
        assert_eq!(
            (e.code, e.key.as_str()),
            (ErrorCode::Conflict, "errors.ops.changed")
        );
        assert!(
            w.sent.lock().unwrap().iter().all(|(_, dry)| *dry),
            "после чужой версии запись не ушла"
        );
        // Гонка со статусом (409) — объект перечитывается, dry-run и запись повторяются; не больше
        // трёх раз.
        let racing = || {
            let tries = std::sync::atomic::AtomicUsize::new(0);
            FakeWriter::new(move |_, _| {
                match tries.fetch_add(1, std::sync::atomic::Ordering::SeqCst) {
                    0 => Err(crate::k8s::errors::from_status(
                        &kube::core::Status::failure("the object has been modified", "Conflict")
                            .with_code(409),
                    )),
                    _ => Ok(()),
                }
            })
        };
        let w = racing();
        let p = preview(&reader(), &w, c.clone(), "now").await.unwrap();
        assert!(matches!(p.dry_run, DryRun::Passed), "{:?}", p.dry_run);
        let w = racing();
        apply(&reader(), &w, c.clone(), Some("g2"), None, "now")
            .await
            .unwrap();
        assert_eq!(w.sent.lock().unwrap().len(), 2);
        let busy = FakeWriter::new(|_, _| {
            Err(crate::k8s::errors::from_status(
                &kube::core::Status::failure("the object has been modified", "Conflict")
                    .with_code(409),
            ))
        });
        let e = apply(&reader(), &busy, c, Some("g2"), None, "now")
            .await
            .unwrap_err();
        assert_eq!(
            (e.key.as_str(), busy.sent.lock().unwrap().len()),
            ("errors.k8s.conflict", WRITE_ATTEMPTS)
        );
        // Отказ RBAC остаётся «нет прав».
        let rbac = admission(crate::k8s::errors::forbidden(
            "deployments.apps \"web\" is forbidden: User \"u\" cannot patch resource \"deployments\" in API group \"apps\" in the namespace \"team-a\"",
        ));
        assert_eq!(rbac.code, ErrorCode::Forbidden);
    }

    #[tokio::test]
    async fn kube_writer_sends_the_field_manager_and_dry_run() {
        let (client, server) = mock_client(|_| (200, json!({})));
        let w = KubeWriter::new(client, StdDuration::from_secs(5));
        let write = Write {
            verb: Verb::Merge,
            path: "/apis/autoscaling/v2/namespaces/team-a/horizontalpodautoscalers/web".into(),
            body: json!({"spec": {"maxReplicas": 3}}),
            dry_run: true,
        };
        w.send(&write, true).await.unwrap();
        let delete = Write {
            verb: Verb::Delete,
            path: "/api/v1/namespaces/team-a/pods/web-1".into(),
            body: json!({"preconditions": {"uid": "u1"}}),
            dry_run: true,
        };
        w.send(&delete, true).await.unwrap();
        w.send(&delete, false).await.unwrap();
        drop(w);
        let seen = server.await.unwrap();
        assert_eq!(seen[0].method, http::Method::PATCH);
        assert!(
            seen[0]
                .uri
                .ends_with("?fieldManager=glafira-kubernetes&dryRun=All"),
            "{}",
            seen[0].uri
        );
        assert_eq!(seen[0].body, json!({"spec": {"maxReplicas": 3}}));
        // DELETE: dry-run — в теле `DeleteOptions`, строку запроса API-сервер не читает.
        assert_eq!(seen[1].method, http::Method::DELETE);
        assert_eq!(seen[1].uri, "/api/v1/namespaces/team-a/pods/web-1");
        assert_eq!(
            seen[1].body,
            json!({"preconditions": {"uid": "u1"}, "dryRun": ["All"]})
        );
        assert_eq!(seen[2].body, json!({"preconditions": {"uid": "u1"}}));
    }

    #[tokio::test]
    async fn a_call_without_dry_run_support_is_never_sent_as_a_dry_run() {
        let (client, server) = mock_client(|_| (200, json!({})));
        let w = KubeWriter::new(client, StdDuration::from_secs(5));
        let instantiate = Write {
            verb: Verb::Post,
            path: "/apis/apps.openshift.io/v1/namespaces/team-a/deploymentconfigs/web/instantiate"
                .into(),
            body: json!({"kind": "DeploymentRequest", "name": "web", "latest": true}),
            dry_run: false,
        };
        // `dryRun=All` OpenShift у этого вызова может проигнорировать и начать выкладку.
        let e = w.send(&instantiate, true).await.unwrap_err();
        assert_eq!(e.code, ErrorCode::Internal);
        w.send(&instantiate, false).await.unwrap();
        drop(w);
        let seen = server.await.unwrap();
        assert_eq!(seen.len(), 1, "dry-run в кластер не ушёл");
        assert_eq!(seen[0].method, http::Method::POST);
        assert!(!seen[0].uri.contains("dryRun"), "{}", seen[0].uri);
    }

    #[tokio::test]
    async fn apply_goes_only_with_the_version_of_the_preview() {
        let w = FakeWriter::new(|_, _| Ok(()));
        let r = reader();
        let scale = Change::Scale {
            namespace: "team-a".into(),
            workload: workload(WorkloadKind::Deployment),
            replicas: 4,
        };
        let pod = Change::DeletePod {
            namespace: "team-a".into(),
            pod: "solo".into(),
        };
        let cases = [
            // Без версии предпросмотра: под удаляется с его именем, чтобы упереться в версию.
            (scale.clone(), None, None),
            (pod.clone(), None, Some("solo")),
            // Другая версия: объект пересоздан или изменён.
            (pod.clone(), Some("uu-other"), Some("solo")),
        ];
        for (change, expected, confirm_pod) in cases {
            let e = apply(&r, &w, change, expected, confirm_pod, "now")
                .await
                .unwrap_err();
            assert_eq!(
                (e.code, e.key.as_str()),
                (ErrorCode::Conflict, "errors.ops.changed")
            );
        }
        assert!(
            w.sent.lock().unwrap().is_empty(),
            "в кластер ничего не ушло"
        );
        apply(&r, &w, scale, Some("g2"), None, "now").await.unwrap();
        assert_eq!(w.sent.lock().unwrap().len(), 1);
    }

    /// Клиент с ответами `cluster`, где запросы из `over` подменены.
    fn reader_over(
        over: impl Fn(&crate::k8s::testing::Seen) -> Option<(u16, Value)> + Send + 'static,
    ) -> ReadClient {
        let (client, _) = mock_client(move |s| over(s).unwrap_or_else(|| cluster(s)));
        ReadClient::new(client, StdDuration::from_secs(5))
    }

    /// Объект без `metadata.generation` (HPA до Kubernetes 1.37, CronJob старого кластера) версии
    /// по `generation` не имеет: версия — хеш spec. Контроллер при этом переписывает
    /// `resourceVersion` и статус при каждом чтении.
    #[tokio::test]
    async fn an_object_without_generation_is_applied_with_the_version_of_its_spec() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let max = Arc::new(AtomicUsize::new(6));
        let image = Arc::new(AtomicUsize::new(1));
        let reads = Arc::new(AtomicUsize::new(100));
        let (m, i, n) = (max.clone(), image.clone(), reads.clone());
        let r = reader_over(move |s| {
            let rv = n.fetch_add(1, Ordering::SeqCst).to_string();
            match s.path() {
                "/apis/autoscaling/v2/namespaces/team-a/horizontalpodautoscalers/web" => Some((
                    200,
                    json!({"apiVersion": "autoscaling/v2", "kind": "HorizontalPodAutoscaler",
                        "metadata": {"name": "web", "resourceVersion": rv},
                        "spec": {"minReplicas": 2, "maxReplicas": m.load(Ordering::SeqCst),
                            "scaleTargetRef": {"kind": "Deployment", "name": "web"}},
                        "status": {"currentReplicas": 2, "desiredReplicas": rv.len() + 1}}),
                )),
                "/apis/batch/v1/namespaces/team-a/cronjobs/web" => Some((
                    200,
                    json!({"apiVersion": "batch/v1", "kind": "CronJob",
                        "metadata": {"name": "web", "namespace": "team-a", "resourceVersion": rv},
                        "spec": {"schedule": "* * * * *", "jobTemplate": {"spec": {"template": {
                            "spec": {"containers": [{"name": "app",
                                "image": format!("r:{}", i.load(Ordering::SeqCst))}]}}}}}}),
                )),
                _ => None,
            }
        });
        let hpa = Change::SetHpaBounds {
            namespace: "team-a".into(),
            hpa: "web".into(),
            min_replicas: Some(3),
            max_replicas: 8,
        };
        let cron = set("CronJob", json!([{"name": "app", "limits": {"cpu": "1"}}]));
        for (change, spec_changes) in [(hpa, &max), (cron, &image)] {
            let w = FakeWriter::new(|_, _| Ok(()));
            let p = preview(&r, &w, change.clone(), "now").await.unwrap();
            assert!(matches!(p.dry_run, DryRun::Passed), "{:?}", p.dry_run);
            let version = p.version.clone().unwrap();
            assert!(version.starts_with('s'), "{version}");
            // Применение перечитывает объект: `resourceVersion` и статус другие, spec тот же.
            apply(&r, &w, change.clone(), p.version.as_deref(), None, "now")
                .await
                .unwrap();
            assert_eq!(w.sent.lock().unwrap().len(), 2, "dry-run и запись");
            // Spec изменился после предпросмотра: CONFLICT, записи нет.
            spec_changes.fetch_add(1, Ordering::SeqCst);
            let e = apply(&r, &w, change.clone(), p.version.as_deref(), None, "now")
                .await
                .unwrap_err();
            assert_eq!(
                (e.code, e.key.as_str()),
                (ErrorCode::Conflict, "errors.ops.changed")
            );
            assert_eq!(w.sent.lock().unwrap().len(), 2, "записи нет");
            // Новый предпросмотр даёт новую версию, с ней применение идёт.
            let again = preview(&r, &w, change.clone(), "now").await.unwrap();
            assert_ne!(again.version, p.version);
            apply(&r, &w, change, again.version.as_deref(), None, "now")
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn only_a_refusal_of_the_api_server_is_a_rejected_dry_run() {
        let c = set(
            "Deployment",
            json!([{"name": "app", "requests": {"cpu": "1"}}]),
        );
        let failure = |code: u16| {
            crate::k8s::errors::from_status(
                &kube::core::Status::failure("boom", "Reason").with_code(code),
            )
        };
        // Неверное значение и непрекращающаяся гонка со статусом показываются в предпросмотре.
        for (error, attempts) in [(failure(422), 1), (failure(409), WRITE_ATTEMPTS)] {
            let code = error.code;
            let w = FakeWriter::new(move |_, _| Err(error.clone()));
            let p = preview(&reader(), &w, c.clone(), "now").await.unwrap();
            let DryRun::Rejected { error } = p.dry_run else {
                panic!("{:?}", p.dry_run)
            };
            assert_eq!(error.code, code);
            assert_eq!(w.sent.lock().unwrap().len(), attempts);
        }
        // Сбой связи, таймаут, 401 и 5xx — ошибка команды: `read_in` сбросит сломанный клиент.
        for error in [
            failure(500),
            failure(401),
            crate::k8s::errors::timeout(),
            crate::k8s::errors::from_transport_text("connection refused"),
        ] {
            let (code, key) = (error.code, error.key.clone());
            let w = FakeWriter::new(move |_, _| Err(error.clone()));
            let e = preview(&reader(), &w, c.clone(), "now").await.unwrap_err();
            assert_eq!((e.code, e.key), (code, key));
            assert_eq!(w.sent.lock().unwrap().len(), 1, "без повторов");
        }
    }
}
