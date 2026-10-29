//! PromQL-шаблоны анализа ресурсов (спец. §7.2) — чистые функции: строка запроса из проекта,
//! workload'а и окна. Значения меток экранируются (`quote`), имена в регулярных выражениях —
//! ещё и как RE2 (`re_escape`): имя объекта не может дописать к запросу свои условия.
//!
//! Статистика — instant-запросы с подзапросом `[окно:5m]`: перцентиль по каждому поду за окно,
//! затем максимум по подам («худший под»). Несколько показателей одного семейства идут одним
//! запросом, у каждого ряда — метка `stat` (`label_replace`). Поды workload'а выбираются внутри
//! подзапроса, на каждом шаге: так в окно попадают и поды, удалённые до его конца.

use crate::k8s::workloads::WorkloadKind;

/// Шаг подзапросов и окно `rate`.
pub const STEP: &str = "5m";
/// Шаг подзапросов в секундах.
pub const STEP_SECONDS: u64 = 300;
/// Точек на графике окна (спец. §7.2): шаг графика — окно / 200, но не меньше `STEP`.
pub const CHART_POINTS: u64 = 200;

/// Значение в кавычках PromQL: `\`, `"` и переводы строк экранируются.
pub fn quote(v: &str) -> String {
    let mut out = String::with_capacity(v.len() + 2);
    out.push('"');
    for c in v.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// Литерал для регулярного выражения RE2: метасимволы — через `\`.
pub fn re_escape(v: &str) -> String {
    let mut out = String::with_capacity(v.len());
    for c in v.chars() {
        if "\\.+*?()|[]{}^$".contains(c) {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

/// Окно запроса: `7d`.
pub fn window(days: u32) -> String {
    format!("{days}d")
}

/// Шаг графика окна в секундах: окно / 200, не меньше 5 минут (спец. §7.2, §10.7). Range-запрос
/// идёт от `конец − окно + шаг` до конца: Prometheus отдаёт каждую точку `начало + i · шаг ≤ конец`,
/// так что точек ровно окно / шаг, то есть не больше 200 (от `конец − окно` их было бы 201).
pub fn chart_step_seconds(days: u32) -> u64 {
    (u64::from(days) * 86_400 / CHART_POINTS).max(STEP_SECONDS)
}

/// Алфавит случайных частей имён Kubernetes (`rand.String`): согласные и цифры без 0, 1 и 3.
const GENERATED: &str = "[bcdfghjklmnpqrstvwxz2456789]";

/// Поды workload'а в запросе.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Pods {
    /// Через kube-state-metrics (`kube_pod_owner` и владельцы ReplicaSet, Job,
    /// ReplicationController): точный выбор, в том числе подов индексированных Job.
    Owner {
        namespace: String,
        kind: WorkloadKind,
        name: String,
    },
    /// Без kube-state-metrics — по имени пода, которое порождает workload этого вида
    /// (`web-<hash>-<5>`, `db-<n>`…).
    Name {
        namespace: String,
        kind: WorkloadKind,
        name: String,
    },
}

impl Pods {
    pub fn namespace(&self) -> &str {
        match self {
            Pods::Owner { namespace, .. } | Pods::Name { namespace, .. } => namespace,
        }
    }

    /// Матчеры метрики: проект и, без kube-state-metrics, имя пода.
    fn matchers(&self) -> String {
        let ns = format!("namespace={}", quote(self.namespace()));
        match self {
            Pods::Owner { .. } => ns,
            Pods::Name { kind, name, .. } => {
                format!("{ns}, pod=~{}", quote(&pod_name_regex(*kind, name)))
            }
        }
    }

    /// Ограничение выражения с метками `namespace`, `pod` подами workload'а: умножение на
    /// единицы `owned_pods` (kube-state-metrics) или ничего (имя уже в матчерах).
    fn join(&self, expr: &str) -> String {
        match self {
            Pods::Owner {
                namespace,
                kind,
                name,
            } => format!(
                "({expr} * on (namespace, pod) group_left () {})",
                owned_pods(namespace, *kind, name)
            ),
            Pods::Name { .. } => expr.to_string(),
        }
    }
}

/// Регулярное выражение имени пода workload'а — те же формы, по которым находятся дети
/// workload'а в событиях (`k8s::events`). У DeploymentConfig поды выкладки и хуков
/// (`<dc>-<n>-deploy`, `-hook-pre`) не считаются: это не приложение.
pub fn pod_name_regex(kind: WorkloadKind, name: &str) -> String {
    let w = re_escape(name);
    let g = GENERATED;
    match kind {
        WorkloadKind::Deployment => format!("{w}-{g}{{5,10}}-{g}{{5}}"),
        WorkloadKind::StatefulSet => format!("{w}-[0-9]+"),
        WorkloadKind::DaemonSet | WorkloadKind::Job => format!("{w}-{g}{{5}}"),
        WorkloadKind::CronJob | WorkloadKind::DeploymentConfig => {
            format!("{w}-[0-9]+-{g}{{5}}")
        }
    }
}

/// Поды workload'а по kube-state-metrics: ряд со значением 1 и метками `namespace`, `pod`.
/// Deployment — через ReplicaSet (`kube_replicaset_owner`), CronJob — через Job
/// (`kube_job_owner`), DeploymentConfig — через ReplicationController
/// (`kube_replicationcontroller_owner`); `label_replace` кладёт имя посредника в `owner_name`.
pub fn owned_pods(namespace: &str, kind: WorkloadKind, name: &str) -> String {
    let ns = quote(namespace);
    let owner = quote(name);
    let via = |middle: &str, metric: &str, label: &str, parent: &str| {
        format!(
            "max by (namespace, pod) (kube_pod_owner{{namespace={ns}, owner_kind=\"{middle}\"}} \
             * on (namespace, owner_name) group_left () max by (namespace, owner_name) \
             (label_replace({metric}{{namespace={ns}, owner_kind=\"{parent}\", owner_name={owner}}}, \
             \"owner_name\", \"$1\", \"{label}\", \"(.*)\")))"
        )
    };
    match kind {
        WorkloadKind::Deployment => via(
            "ReplicaSet",
            "kube_replicaset_owner",
            "replicaset",
            "Deployment",
        ),
        WorkloadKind::CronJob => via("Job", "kube_job_owner", "job_name", "CronJob"),
        WorkloadKind::DeploymentConfig => via(
            "ReplicationController",
            "kube_replicationcontroller_owner",
            "replicationcontroller",
            "DeploymentConfig",
        ),
        WorkloadKind::StatefulSet | WorkloadKind::DaemonSet | WorkloadKind::Job => format!(
            "max by (namespace, pod) (kube_pod_owner{{namespace={ns}, owner_kind=\"{}\", owner_name={owner}}})",
            kind_name(kind)
        ),
    }
}

fn kind_name(kind: WorkloadKind) -> &'static str {
    match kind {
        WorkloadKind::Deployment => "Deployment",
        WorkloadKind::StatefulSet => "StatefulSet",
        WorkloadKind::DaemonSet => "DaemonSet",
        WorkloadKind::DeploymentConfig => "DeploymentConfig",
        WorkloadKind::Job => "Job",
        WorkloadKind::CronJob => "CronJob",
    }
}

/// Есть ли у проекта метрики kube-state-metrics: от него зависит выбор подов (`Pods`).
pub fn owner_metrics_present(namespace: &str) -> String {
    format!("count(kube_pod_owner{{namespace={}}})", quote(namespace))
}

/// Контейнеры приложения: без пустого имени и без `POD` (pause-контейнер).
const CONTAINERS: &str = "container!=\"\", container!=\"POD\"";

/// Потребление CPU в ядрах по поду и контейнеру (`rate` за 5 минут).
fn cpu(p: &Pods) -> String {
    p.join(&format!(
        "max by (namespace, pod, container) (rate(container_cpu_usage_seconds_total{{{}, {CONTAINERS}}}[{STEP}]))",
        p.matchers()
    ))
}

/// Рабочий набор памяти в байтах по поду и контейнеру.
fn memory(p: &Pods) -> String {
    p.join(&format!(
        "max by (namespace, pod, container) (container_memory_working_set_bytes{{{}, {CONTAINERS}}})",
        p.matchers()
    ))
}

/// Доля периодов CFS с троттлингом (0–1); периоды без работы не считаются.
fn throttling(p: &Pods) -> String {
    let m = p.matchers();
    p.join(&format!(
        "max by (namespace, pod, container) (rate(container_cpu_cfs_throttled_periods_total{{{m}, {CONTAINERS}}}[{STEP}]) \
         / (rate(container_cpu_cfs_periods_total{{{m}, {CONTAINERS}}}[{STEP}]) > 0))"
    ))
}

/// Перезапуски контейнеров за `range` (kube-state-metrics).
fn restarts(p: &Pods, range: &str) -> String {
    p.join(&format!(
        "max by (namespace, pod, container) (increase(kube_pod_container_status_restarts_total{{{}}}[{range}]))",
        p.matchers()
    ))
}

/// Перезапуски за `range`, после которых последняя причина завершения — OOMKilled.
fn ooms(p: &Pods, range: &str) -> String {
    format!(
        "({} > 0) and on (namespace, pod, container) \
         (max by (namespace, pod, container) (kube_pod_container_status_last_terminated_reason{{{}, reason=\"OOMKilled\"}}) == 1)",
        restarts(p, range),
        p.matchers()
    )
}

/// Показатель с меткой `stat`.
fn stat(name: &str, expr: &str) -> String {
    format!("label_replace({expr}, \"stat\", \"{name}\", \"\", \"\")")
}

fn either(parts: &[String]) -> String {
    parts.join(" or ")
}

/// Перцентиль по каждому поду за окно, затем максимум по подам — по контейнеру.
fn worst(expr: &str, f: &str, days: u32) -> String {
    format!(
        "max by (container) ({f}(({expr})[{}:{STEP}]))",
        window(days)
    )
}

fn quantile(expr: &str, q: &str, days: u32) -> String {
    format!(
        "max by (container) (quantile_over_time({q}, ({expr})[{}:{STEP}]))",
        window(days)
    )
}

/// CPU: p50, p95, p99 и максимум (ядра).
pub fn workload_cpu(p: &Pods, days: u32) -> String {
    let e = cpu(p);
    either(&[
        stat("cpu_p50", &quantile(&e, "0.5", days)),
        stat("cpu_p95", &quantile(&e, "0.95", days)),
        stat("cpu_p99", &quantile(&e, "0.99", days)),
        stat("cpu_max", &worst(&e, "max_over_time", days)),
    ])
}

/// Память: p95, p99 и максимум (байты).
pub fn workload_memory(p: &Pods, days: u32) -> String {
    let e = memory(p);
    either(&[
        stat("memory_p95", &quantile(&e, "0.95", days)),
        stat("memory_p99", &quantile(&e, "0.99", days)),
        stat("memory_max", &worst(&e, "max_over_time", days)),
    ])
}

/// Троттлинг: среднее и p95 (доля 0–1).
pub fn workload_throttling(p: &Pods, days: u32) -> String {
    let e = throttling(p);
    either(&[
        stat("throttling_mean", &worst(&e, "avg_over_time", days)),
        stat("throttling_p95", &quantile(&e, "0.95", days)),
    ])
}

/// OOM и рестарты по контейнеру (kube-state-metrics): число эпизодов OOM, время последнего
/// (Unix, секунды) и число рестартов за окно.
pub fn workload_restarts(p: &Pods, days: u32) -> String {
    let w = window(days);
    let oom = ooms(p, STEP);
    let restarts = restarts(p, STEP);
    either(&[
        stat(
            "oom_count",
            &format!("sum by (container) (count_over_time(({oom})[{w}:{STEP}]))"),
        ),
        stat(
            "oom_last",
            &format!("max by (container) (max_over_time(timestamp({oom})[{w}:{STEP}]))"),
        ),
        stat(
            "restarts",
            &format!("sum by (container) (sum_over_time(({restarts})[{w}:{STEP}]))"),
        ),
    ])
}

/// Покрытие окна: число шагов с данными хотя бы по одному поду (`steps`) и время первого такого
/// шага (`first`, Unix-секунды).
pub fn workload_coverage(p: &Pods, days: u32) -> String {
    let w = window(days);
    let any = format!("max({})", cpu(p));
    either(&[
        stat("steps", &format!("count_over_time(({any})[{w}:{STEP}])")),
        stat(
            "first",
            &format!("min_over_time(timestamp({any})[{w}:{STEP}])"),
        ),
    ])
}

/// Ряд графика CPU по контейнеру (range-запрос): худший под на каждом шаге.
pub fn workload_cpu_series(p: &Pods) -> String {
    format!("max by (container) ({})", cpu(p))
}

/// Ряд графика памяти по контейнеру.
pub fn workload_memory_series(p: &Pods) -> String {
    format!("max by (container) ({})", memory(p))
}

/// Отметки графика (range-запрос с шагом `step_seconds`): рестарты (`event="restart"`) и
/// OOM (`event="oom"`) по контейнеру за шаг.
pub fn workload_events_series(p: &Pods, step_seconds: u64) -> String {
    let range = format!("{step_seconds}s");
    let label = |expr: String, event: &str| {
        format!("label_replace({expr}, \"event\", \"{event}\", \"\", \"\")")
    };
    either(&[
        label(
            format!("sum by (container) ({} > 0)", restarts(p, &range)),
            "restart",
        ),
        label(format!("count by (container) ({})", ooms(p, &range)), "oom"),
    ])
}

/// Текущее потребление контейнеров пода (вкладка «Ресурсы» пода): CPU (`stat="cpu"`, ядра)
/// и память (`stat="memory"`, байты).
pub fn pod_now(namespace: &str, pod: &str) -> String {
    let m = format!(
        "namespace={}, pod={}, {CONTAINERS}",
        quote(namespace),
        quote(pod)
    );
    either(&[
        stat(
            "cpu",
            &format!("max by (container) (rate(container_cpu_usage_seconds_total{{{m}}}[{STEP}]))"),
        ),
        stat(
            "memory",
            &format!("max by (container) (container_memory_working_set_bytes{{{m}}})"),
        ),
    ])
}

/// Агрегаты проекта по поду и контейнеру (вкладка «Ресурсы» проекта и правила `resources.*`):
/// без выбора workload'а — поды сопоставляет с workload'ами Rust, по имени и владельцу.
fn by_pod(expr: &str) -> String {
    expr.replace(
        "max by (namespace, pod, container)",
        "max by (pod, container)",
    )
}

fn project_matchers(namespace: &str) -> String {
    format!("namespace={}, {CONTAINERS}", quote(namespace))
}

/// p95 CPU по поду и контейнеру за окно (ядра).
pub fn project_cpu_p95(namespace: &str, days: u32) -> String {
    let m = project_matchers(namespace);
    by_pod(&format!(
        "quantile_over_time(0.95, (max by (namespace, pod, container) (rate(container_cpu_usage_seconds_total{{{m}}}[{STEP}])))[{}:{STEP}])",
        window(days)
    ))
}

/// Максимум памяти по поду и контейнеру за окно (байты).
pub fn project_memory_max(namespace: &str, days: u32) -> String {
    let m = project_matchers(namespace);
    by_pod(&format!(
        "max_over_time((max by (namespace, pod, container) (container_memory_working_set_bytes{{{m}}}))[{}:{STEP}])",
        window(days)
    ))
}

/// Средний троттлинг по поду и контейнеру за окно (доля 0–1).
pub fn project_throttling_mean(namespace: &str, days: u32) -> String {
    let m = project_matchers(namespace);
    by_pod(&format!(
        "avg_over_time((max by (namespace, pod, container) (rate(container_cpu_cfs_throttled_periods_total{{{m}}}[{STEP}]) \
         / (rate(container_cpu_cfs_periods_total{{{m}}}[{STEP}]) > 0)))[{}:{STEP}])",
        window(days)
    ))
}

/// Число эпизодов OOM по поду и контейнеру за окно (kube-state-metrics).
pub fn project_oom_count(namespace: &str, days: u32) -> String {
    let ns = quote(namespace);
    by_pod(&format!(
        "count_over_time(((max by (namespace, pod, container) (increase(kube_pod_container_status_restarts_total{{namespace={ns}}}[{STEP}])) > 0) \
         and on (namespace, pod, container) (max by (namespace, pod, container) \
         (kube_pod_container_status_last_terminated_reason{{namespace={ns}, reason=\"OOMKilled\"}}) == 1))[{}:{STEP}])",
        window(days)
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn owner(kind: WorkloadKind, name: &str) -> Pods {
        Pods::Owner {
            namespace: "team-a".into(),
            kind,
            name: name.into(),
        }
    }

    fn by_name(kind: WorkloadKind, name: &str) -> Pods {
        Pods::Name {
            namespace: "team-a".into(),
            kind,
            name: name.into(),
        }
    }

    #[test]
    fn values_are_quoted_and_escaped() {
        assert_eq!(quote("web"), "\"web\"");
        assert_eq!(quote("a\"b\\c\nd"), "\"a\\\"b\\\\c\\nd\"");
        assert_eq!(re_escape("web.v2"), "web\\.v2");
        assert_eq!(re_escape("a+b(c)|[d]"), "a\\+b\\(c\\)\\|\\[d\\]");
        // Точка в имени — литерал и в RE2, и в строке PromQL.
        assert_eq!(
            pod_name_regex(WorkloadKind::StatefulSet, "db.v1"),
            "db\\.v1-[0-9]+"
        );
        assert!(
            by_name(WorkloadKind::StatefulSet, "db.v1")
                .matchers()
                .contains("pod=~\"db\\\\.v1-[0-9]+\"")
        );
    }

    #[test]
    fn a_name_cannot_add_its_own_conditions() {
        let evil = "x\"} or vector(1) or up{a=\"";
        for q in [
            workload_cpu(&owner(WorkloadKind::Deployment, evil), 7),
            workload_cpu(&by_name(WorkloadKind::Deployment, evil), 7),
            project_cpu_p95(evil, 1),
        ] {
            // Кавычка из имени экранирована: строка метки не закрывается раньше времени.
            assert!(!q.contains("\"x\"}"), "{q}");
        }
        assert!(
            owned_pods("team-a", WorkloadKind::Job, evil)
                .contains("owner_name=\"x\\\"} or vector(1) or up{a=\\\"\"")
        );
        assert!(
            by_name(WorkloadKind::Job, evil)
                .matchers()
                .contains("pod=~\"x\\\"\\\\} or vector\\\\(1\\\\)")
        );
    }

    #[test]
    fn pod_names_follow_the_workload_kind() {
        let g = GENERATED;
        for (kind, expected) in [
            (
                WorkloadKind::Deployment,
                format!("web-{g}{{5,10}}-{g}{{5}}"),
            ),
            (WorkloadKind::StatefulSet, "web-[0-9]+".to_string()),
            (WorkloadKind::DaemonSet, format!("web-{g}{{5}}")),
            (WorkloadKind::Job, format!("web-{g}{{5}}")),
            (WorkloadKind::CronJob, format!("web-[0-9]+-{g}{{5}}")),
            (
                WorkloadKind::DeploymentConfig,
                format!("web-[0-9]+-{g}{{5}}"),
            ),
        ] {
            assert_eq!(pod_name_regex(kind, "web"), expected, "{kind:?}");
        }
        let re = regex::Regex::new(&format!(
            "^(?:{})$",
            pod_name_regex(WorkloadKind::Deployment, "web")
        ))
        .unwrap();
        assert!(re.is_match("web-7d9f8c6b5d-x2x4z"));
        assert!(
            !re.is_match("web-api-7d9f8c6b5d-x2x4z"),
            "соседи не попадают"
        );
        let dc = regex::Regex::new(&format!(
            "^(?:{})$",
            pod_name_regex(WorkloadKind::DeploymentConfig, "legacy")
        ))
        .unwrap();
        assert!(dc.is_match("legacy-3-b2c4d"));
        assert!(
            !dc.is_match("legacy-3-deploy"),
            "под выкладки — не приложение"
        );
    }

    #[test]
    fn owners_go_through_their_intermediate_objects() {
        let d = owned_pods("team-a", WorkloadKind::Deployment, "web");
        assert_eq!(
            d,
            "max by (namespace, pod) (kube_pod_owner{namespace=\"team-a\", owner_kind=\"ReplicaSet\"} \
             * on (namespace, owner_name) group_left () max by (namespace, owner_name) \
             (label_replace(kube_replicaset_owner{namespace=\"team-a\", owner_kind=\"Deployment\", owner_name=\"web\"}, \
             \"owner_name\", \"$1\", \"replicaset\", \"(.*)\")))"
        );
        assert!(owned_pods("team-a", WorkloadKind::CronJob, "report")
            .contains("kube_job_owner{namespace=\"team-a\", owner_kind=\"CronJob\", owner_name=\"report\"}"));
        assert!(owned_pods("team-a", WorkloadKind::DeploymentConfig, "legacy")
            .contains("kube_replicationcontroller_owner{namespace=\"team-a\", owner_kind=\"DeploymentConfig\""));
        assert_eq!(
            owned_pods("team-a", WorkloadKind::StatefulSet, "db"),
            "max by (namespace, pod) (kube_pod_owner{namespace=\"team-a\", owner_kind=\"StatefulSet\", owner_name=\"db\"})"
        );
        assert_eq!(
            owner_metrics_present("team-a"),
            "count(kube_pod_owner{namespace=\"team-a\"})"
        );
    }

    #[test]
    fn workload_statistics_take_the_worst_pod_over_the_window() {
        let q = workload_cpu(&by_name(WorkloadKind::StatefulSet, "db"), 7);
        let cpu = "max by (namespace, pod, container) (rate(container_cpu_usage_seconds_total{namespace=\"team-a\", pod=~\"db-[0-9]+\", container!=\"\", container!=\"POD\"}[5m]))";
        assert_eq!(
            q,
            format!(
                "label_replace(max by (container) (quantile_over_time(0.5, ({cpu})[7d:5m])), \"stat\", \"cpu_p50\", \"\", \"\") \
                 or label_replace(max by (container) (quantile_over_time(0.95, ({cpu})[7d:5m])), \"stat\", \"cpu_p95\", \"\", \"\") \
                 or label_replace(max by (container) (quantile_over_time(0.99, ({cpu})[7d:5m])), \"stat\", \"cpu_p99\", \"\", \"\") \
                 or label_replace(max by (container) (max_over_time(({cpu})[7d:5m])), \"stat\", \"cpu_max\", \"\", \"\")"
            )
        );
        // С kube-state-metrics поды выбираются внутри подзапроса, на каждом шаге окна.
        let q = workload_memory(&owner(WorkloadKind::StatefulSet, "db"), 14);
        assert!(q.contains(
            "quantile_over_time(0.99, ((max by (namespace, pod, container) (container_memory_working_set_bytes{namespace=\"team-a\", container!=\"\", container!=\"POD\"}) \
             * on (namespace, pod) group_left () max by (namespace, pod) (kube_pod_owner{namespace=\"team-a\", owner_kind=\"StatefulSet\", owner_name=\"db\"})))[14d:5m])"
        ), "{q}");
        for (q, stats) in [
            (
                workload_memory(&owner(WorkloadKind::Job, "j"), 1),
                &["memory_p95", "memory_p99", "memory_max"][..],
            ),
            (
                workload_throttling(&owner(WorkloadKind::Job, "j"), 1),
                &["throttling_mean", "throttling_p95"][..],
            ),
            (
                workload_restarts(&owner(WorkloadKind::Job, "j"), 1),
                &["oom_count", "oom_last", "restarts"][..],
            ),
            (
                workload_coverage(&owner(WorkloadKind::Job, "j"), 1),
                &["steps", "first"][..],
            ),
        ] {
            for s in stats {
                assert!(q.contains(&format!("\"stat\", \"{s}\"")), "{s}: {q}");
            }
            assert_eq!(
                q.matches(" or label_replace(").count(),
                stats.len() - 1,
                "{q}"
            );
        }
        assert!(workload_throttling(&owner(WorkloadKind::Job, "j"), 1)
            .contains("/ (rate(container_cpu_cfs_periods_total{namespace=\"team-a\", container!=\"\", container!=\"POD\"}[5m]) > 0)"));
        assert!(workload_restarts(&owner(WorkloadKind::Job, "j"), 30)
            .contains("kube_pod_container_status_last_terminated_reason{namespace=\"team-a\", reason=\"OOMKilled\"}"));
    }

    #[test]
    fn charts_have_at_most_two_hundred_points_and_a_five_minute_step() {
        assert_eq!(chart_step_seconds(7), 3024);
        assert_eq!(chart_step_seconds(1), 432);
        assert_eq!(chart_step_seconds(30), 12_960);
        for days in [1, 7, 14, 30] {
            let step = chart_step_seconds(days);
            let window = u64::from(days) * 86_400;
            // Точки range-запроса `[конец − окно + шаг, конец]` с шагом `шаг`.
            let points = (window - step) / step + 1;
            assert!(
                step >= STEP_SECONDS && points <= CHART_POINTS,
                "{days}: {points}"
            );
        }
        let p = by_name(WorkloadKind::Deployment, "web");
        assert!(
            workload_cpu_series(&p)
                .starts_with("max by (container) (max by (namespace, pod, container) (rate(")
        );
        let e = workload_events_series(&p, 3024);
        assert!(
            e.contains("[3024s]")
                && e.contains("\"event\", \"restart\"")
                && e.contains("\"event\", \"oom\""),
            "{e}"
        );
    }

    #[test]
    fn pod_now_reads_cpu_and_memory_of_one_pod() {
        assert_eq!(
            pod_now("team-a", "web-1"),
            "label_replace(max by (container) (rate(container_cpu_usage_seconds_total{namespace=\"team-a\", pod=\"web-1\", container!=\"\", container!=\"POD\"}[5m])), \"stat\", \"cpu\", \"\", \"\") \
             or label_replace(max by (container) (container_memory_working_set_bytes{namespace=\"team-a\", pod=\"web-1\", container!=\"\", container!=\"POD\"}), \"stat\", \"memory\", \"\", \"\")"
        );
    }

    #[test]
    fn project_queries_group_by_pod_and_container() {
        assert_eq!(
            project_cpu_p95("team-a", 1),
            "quantile_over_time(0.95, (max by (pod, container) (rate(container_cpu_usage_seconds_total{namespace=\"team-a\", container!=\"\", container!=\"POD\"}[5m])))[1d:5m])"
        );
        assert_eq!(
            project_memory_max("team-a", 7),
            "max_over_time((max by (pod, container) (container_memory_working_set_bytes{namespace=\"team-a\", container!=\"\", container!=\"POD\"}))[7d:5m])"
        );
        for q in [
            project_throttling_mean("team-a", 1),
            project_oom_count("team-a", 1),
        ] {
            assert!(q.contains("max by (pod, container)"), "{q}");
            assert!(
                !q.contains(
                    "max by (namespace, pod, container) (kube_pod_container_status_restarts_total"
                ),
                "{q}"
            );
        }
    }
}
