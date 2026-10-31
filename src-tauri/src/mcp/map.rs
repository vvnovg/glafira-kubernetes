//! Данные чтения O1b → ответы для LLM (`glafira_mcp_kubernetes::model`, спец. §10.4): время —
//! в секундах до момента ответа, части экрана — данными или причиной, ошибки — кодом и
//! английским текстом. Маскировку `command`, `args` и аннотаций делает крейт шлюза.

use std::collections::BTreeMap;

use glafira_mcp_kubernetes::model as m;
use k8s_openapi::api::core::v1::Pod;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::AppError;
use crate::health::snapshot::Requests;
use crate::k8s::cluster::Capabilities;
use crate::k8s::containers::ContainerSpecView;
use crate::k8s::events::EventRow;
use crate::k8s::network::NetworkView;
use crate::k8s::nodes::{NodeDetails, NodeRow};
use crate::k8s::overview::ClusterOverview;
use crate::k8s::pods::{PodDetails, PodRow, StateView, VolumeView};
use crate::k8s::projects::ProjectRow;
use crate::k8s::quotas::QuotasView;
use crate::k8s::view::{ConditionView, Part};
use crate::k8s::workloads::{WorkloadDetails, WorkloadRow};
use crate::platform::adapters::PlatformOverview;
use crate::platform::{PlatformInfo, PlatformKind};

/// Ошибка для LLM: код и английский текст (`FORBIDDEN: …`).
pub fn error_text(e: &AppError) -> String {
    format!("{}: {}", code(e), llm_message(e))
}

/// Ключи ошибок, текст которых модель получает как есть: постоянная фраза без данных профиля
/// или только то, о чём модель спросила сама (проект, объект, id подключения), и ответ
/// API-сервера на её запрос (`{{message}}` у 404, 409, 400/422, 5xx), и ошибки проверки
/// изменения `errors.change.*`: в них только имена контейнеров, quantity и границы из самого
/// изменения и проекта пользователя, без адресов и учётных данных. Остальные ключи — и
/// любой новый — получают фиксированную фразу (`llm_message`): так текст с адресом сервера,
/// командой exec-плагина, адресом источника метрик или именем пользователя не уйдёт модели,
/// даже если о новом ключе здесь забыли.
pub const LLM_TEXT_KEYS: &[&str] = &[
    "errors.exec.notApproved",
    "errors.connections.loginRequired",
    "errors.connections.tokenMissing",
    "errors.connections.badNamespace",
    "errors.connections.notFound",
    "errors.oauth.notAvailable",
    "errors.oauth.cancelled",
    "errors.oauth.timeout",
    "errors.oauth.badTokenResponse",
    "errors.k8s.notFound",
    "errors.k8s.badName",
    "errors.k8s.forbiddenIn",
    "errors.k8s.forbiddenCluster",
    "errors.k8s.unauthorized",
    "errors.k8s.conflict",
    "errors.k8s.invalid",
    "errors.k8s.tooManyRequests",
    "errors.k8s.serverError",
    "errors.k8s.apiError",
    "errors.k8s.hostnameMismatch",
    "errors.k8s.certExpired",
    "errors.k8s.caUntrusted",
    "errors.k8s.tlsError",
    "errors.k8s.dns",
    "errors.k8s.timeout",
    "errors.k8s.connect",
    "errors.k8s.malformedObject",
    "errors.k8s.unexpectedResponse",
    "errors.health.annotationNotFound",
    "errors.change.badQuantity",
    "errors.change.containersCount",
    "errors.change.duplicateContainer",
    "errors.change.nothingToChange",
    "errors.change.scaleKind",
    "errors.change.replicas",
    "errors.change.restartKind",
    "errors.change.limitBelowRequest",
    "errors.change.hpaBounds",
    "errors.change.noContainer",
    "errors.change.limitRangeMin",
    "errors.change.limitRangeMax",
    "errors.change.resourcesKind",
    "errors.internal",
];

/// Длина текста ошибки для модели: ответ API-сервера бывает длинным (сообщение webhook'а).
pub const MAX_LLM_ERROR_CHARS: usize = 500;

/// Текст ошибки для LLM. Чтение открывает подключение, если оно ещё не открыто, и тексты
/// ошибок подключения несут данные профиля: команду exec-плагина и параметры её запуска
/// (`errors.exec.*`), адрес API-сервера и прокси (`errors.connections.*`), ответ OAuth-сервера
/// (`errors.oauth.*`), цепочку причин kube и сети, имя пользователя из ответа 403. Как есть
/// уходят только ключи `LLM_TEXT_KEYS` (не длиннее `MAX_LLM_ERROR_CHARS`); остальные получают
/// фиксированную фразу по группе ключа. Код ошибки остаётся прежним, подробности — только у
/// человека в интерфейсе.
pub fn llm_message(e: &AppError) -> String {
    let key = e.key.as_str();
    if LLM_TEXT_KEYS.contains(&key) {
        return cut(&e.message, MAX_LLM_ERROR_CHARS);
    }
    let hidden = if key.starts_with("errors.exec.") {
        "The connection's credential plugin failed; check the connection in Glafira"
    } else if key.starts_with("errors.oauth.") {
        "The sign-in was rejected; sign in again in Glafira"
    } else if key.starts_with("errors.connections.") {
        "The connection settings are invalid; check the connection in Glafira"
    } else {
        match key {
            "errors.k8s.credentials" => "The credentials were rejected; sign in again in Glafira",
            "errors.k8s.transport" => "The cluster could not be reached",
            "errors.k8s.forbidden" => "Access is forbidden",
            _ => "The operation failed; the user can see the details in Glafira",
        }
    };
    hidden.to_string()
}

/// Не длиннее `max` символов, с «…» на месте отрезанного.
fn cut(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_string();
    }
    let mut out: String = s.chars().take(max - 1).collect();
    out.push('…');
    out
}

pub fn code(e: &AppError) -> String {
    serde_json::to_value(e.code)
        .ok()
        .and_then(|v| v.as_str().map(String::from))
        .unwrap_or_else(|| "INTERNAL".into())
}

/// Перевод между типами с одинаковым JSON (виды workload'ов, объекты находок).
pub fn reshape<T: Serialize, U: DeserializeOwned>(t: &T) -> Option<U> {
    serde_json::to_value(t)
        .ok()
        .and_then(|v| serde_json::from_value(v).ok())
}

/// Значение перечисления так, как его видит JSON: `secretKeyRef`, `crashLoop`.
fn name_of<T: Serialize>(v: &T) -> String {
    serde_json::to_value(v)
        .ok()
        .and_then(|v| v.as_str().map(String::from))
        .unwrap_or_default()
}

fn ago(now: i64, t: Option<i64>) -> Option<i64> {
    t.map(|t| (now - t).max(0))
}

pub fn part<T, U>(p: Part<T>, f: impl FnOnce(T) -> U) -> m::Part<U> {
    match (p.data, p.error) {
        (Some(d), _) => m::Part::Data(f(d)),
        (None, Some(e)) => m::Part::error(error_text(&e)),
        (None, None) => m::Part::error("INTERNAL: no data"),
    }
}

/// CPU в милликорах строкой quantity: `250m`, `2`.
pub fn cpu_text(milli: i64) -> String {
    if milli % 1000 == 0 {
        format!("{}", milli / 1000)
    } else {
        format!("{milli}m")
    }
}

/// Память в байтах строкой quantity: `2Gi`, `512Mi`, иначе — округлённые мебибайты.
pub fn memory_text(bytes: i64) -> String {
    const MI: i64 = 1 << 20;
    const GI: i64 = 1 << 30;
    if bytes != 0 && bytes % GI == 0 {
        format!("{}Gi", bytes / GI)
    } else {
        format!("{}Mi", (bytes + MI / 2) / MI)
    }
}

pub fn resources(r: Requests) -> m::Resources {
    m::Resources {
        cpu: Some(cpu_text(r.cpu_milli)),
        memory: Some(memory_text(r.memory)),
    }
}

pub fn platform(p: &PlatformInfo) -> m::Platform {
    m::Platform {
        kind: match p.kind {
            PlatformKind::Openshift => m::PlatformKind::Openshift,
            PlatformKind::Kubernetes => m::PlatformKind::Kubernetes,
        },
        label: p.label.clone(),
        version: p.version.clone(),
        distribution: p.distribution.as_ref().map(name_of),
    }
}

pub fn condition(c: &ConditionView, now: i64) -> m::Condition {
    m::Condition {
        kind: c.kind.clone(),
        status: c.status.clone(),
        reason: c.reason.clone(),
        message: c.message.clone(),
        last_transition_seconds_ago: ago(now, c.last_transition),
    }
}

fn conditions(cs: &[ConditionView], now: i64) -> Vec<m::Condition> {
    cs.iter().map(|c| condition(c, now)).collect()
}

pub fn capabilities(c: &Capabilities) -> m::Capabilities {
    m::Capabilities {
        list_projects: c.list_projects,
        list_nodes: c.list_nodes,
        list_all_pods: c.list_all_pods,
        read_cluster_operators: c.read_cluster_operators,
        read_cluster_version: c.read_cluster_version,
    }
}

pub fn cluster(o: ClusterOverview, caps: &Capabilities, now: i64) -> m::ClusterOverview {
    m::ClusterOverview {
        platform: platform(&o.platform),
        server_version: o.server_version,
        api_server: m::ApiServer {
            ready: o.readyz.error.is_none().then_some(o.readyz.ok),
            failed_checks: o.readyz.failed,
            error: o.readyz.error.as_ref().map(error_text),
        },
        nodes: part(o.nodes, |n| m::NodeCounts {
            total: n.total,
            ready: n.ready,
            roles: n
                .roles
                .into_iter()
                .map(|r| m::RoleCount {
                    role: r.role,
                    total: r.total,
                    ready: r.ready,
                })
                .collect(),
        }),
        ingress_classes: part(o.ingress_classes, |v| {
            v.into_iter()
                .map(|c| m::IngressClass {
                    name: c.name,
                    controller: c.controller,
                    default: c.default,
                })
                .collect()
        }),
        gateway_classes: o.gateway_classes.map(|p| {
            part(p, |v| {
                v.into_iter()
                    .map(|c| m::GatewayClass {
                        name: c.name,
                        controller: c.controller,
                        accepted: c.accepted,
                    })
                    .collect()
            })
        }),
        openshift: o.platform_details.map(|d| match d {
            PlatformOverview::Openshift(os) => m::OpenShift {
                cluster_version: part(os.cluster_version, |v| m::ClusterVersion {
                    version: v.version,
                    channel: v.channel,
                    available_updates: v.available_updates,
                    failing: v.failing.as_ref().map(|c| condition(c, now)),
                    progressing: v.progressing.as_ref().map(|c| condition(c, now)),
                }),
                infrastructure: part(os.infrastructure, |i| i),
                operators: part(os.operators, |s| m::Operators {
                    total: s.total,
                    not_ok: s
                        .problems
                        .into_iter()
                        .map(|o| m::Operator {
                            name: o.name,
                            version: o.version,
                            available: o.available,
                            progressing: o.progressing,
                            degraded: o.degraded,
                            message: o.message,
                        })
                        .collect(),
                }),
                machine_config_pools_not_ok: part(os.machine_config_pools, |v| {
                    v.into_iter()
                        .filter(|p| p.degraded || p.updating)
                        .map(|p| m::MachineConfigPool {
                            name: p.name,
                            degraded: p.degraded,
                            updating: p.updating,
                            machines: p.machines,
                            ready: p.ready,
                            updated: p.updated,
                            degraded_machines: p.degraded_machines,
                        })
                        .collect()
                }),
            },
        }),
        capabilities: capabilities(caps),
    }
}

pub fn project(r: ProjectRow) -> m::Project {
    m::Project {
        name: r.name,
        display_name: r.display_name,
        phase: r.phase,
    }
}

pub fn node_item(r: NodeRow, requested: Option<Requests>, now: i64) -> m::NodeItem {
    m::NodeItem {
        name: r.name,
        roles: r.roles,
        ready: r.ready,
        conditions: r.problems,
        unschedulable: r.unschedulable,
        allocatable: m::Resources {
            cpu: r.cpu,
            memory: r.memory,
        },
        requested: requested.map(resources),
        taints: r.taints,
        kubelet_version: r.kubelet_version,
        age_seconds: ago(now, r.created),
    }
}

pub fn node_details(d: NodeDetails, usage: m::Part<m::NodeUsage>, now: i64) -> m::NodeDetails {
    m::NodeDetails {
        name: d.node.name,
        roles: d.node.roles,
        ready: d.node.ready,
        unschedulable: d.node.unschedulable,
        conditions: conditions(&d.conditions, now),
        taints: d.taints,
        capacity: d.capacity,
        allocatable: d.allocatable,
        usage,
        kubelet_version: d.node.kubelet_version,
        os_image: d.os_image,
        container_runtime: d.container_runtime,
        age_seconds: ago(now, d.node.created),
    }
}

/// Requests и limits подов ноды (основные контейнеры и нативные sidecar'ы).
pub fn node_usage(pods: &[Pod]) -> m::NodeUsage {
    let mut req = Requests::default();
    let mut lim = Requests::default();
    let running: Vec<&Pod> = pods
        .iter()
        .filter(|p| {
            !matches!(
                p.status.as_ref().and_then(|s| s.phase.as_deref()),
                Some("Succeeded" | "Failed")
            )
        })
        .collect();
    for p in &running {
        let spec = p.spec.as_ref();
        let containers = spec.into_iter().flat_map(|s| {
            s.containers.iter().chain(
                s.init_containers
                    .iter()
                    .flatten()
                    .filter(|c| c.restart_policy.as_deref() == Some("Always")),
            )
        });
        for c in containers {
            let r = c.resources.as_ref();
            let q = |m: Option<
                &BTreeMap<String, k8s_openapi::apimachinery::pkg::api::resource::Quantity>,
            >,
                     k: &str| {
                m.and_then(|m| m.get(k))
                    .and_then(|q| crate::k8s::quantity::parse_quantity(&q.0))
            };
            let (rq, lm) = (
                r.and_then(|r| r.requests.as_ref()),
                r.and_then(|r| r.limits.as_ref()),
            );
            req.cpu_milli += q(rq, "cpu").map_or(0, |v| (v * 1000.0).round() as i64);
            req.memory += q(rq, "memory").map_or(0, |v| v.round() as i64);
            lim.cpu_milli += q(lm, "cpu").map_or(0, |v| (v * 1000.0).round() as i64);
            lim.memory += q(lm, "memory").map_or(0, |v| v.round() as i64);
        }
    }
    m::NodeUsage {
        pods: running.len(),
        requests: resources(req),
        limits: resources(lim),
    }
}

pub fn workload_item(r: WorkloadRow, now: i64) -> m::WorkloadItem {
    m::WorkloadItem {
        kind: reshape(&r.kind).unwrap_or(m::WorkloadKind::Deployment),
        name: r.name,
        desired: r.desired,
        ready: r.ready,
        available: r.available,
        updated: r.updated,
        images: r.images,
        age_seconds: ago(now, r.created),
        problems: r.problem.as_ref().map(name_of).into_iter().collect(),
        schedule: r.schedule,
        suspended: r.suspended,
    }
}

pub fn container_spec(c: ContainerSpecView) -> m::ContainerSpec {
    m::ContainerSpec {
        name: c.name,
        image: c.image,
        init: c.init,
        command: c.command,
        args: c.args,
        requests: c.requests,
        limits: c.limits,
        ports: c.ports,
        liveness: c.liveness,
        readiness: c.readiness,
        startup: c.startup,
        env: c
            .env
            .into_iter()
            .map(|e| m::Env {
                source: name_of(&e.source),
                name: e.name,
                reference: e.reference,
                value: e.value,
                value_error: None,
            })
            .collect(),
    }
}

fn volume(v: VolumeView) -> m::Volume {
    m::Volume {
        name: v.name,
        kind: v.kind,
        source: v.source,
    }
}

pub fn workload_details(d: WorkloadDetails, now: i64) -> m::WorkloadDetails {
    m::WorkloadDetails {
        workload: workload_item(d.workload, now),
        selector: d.selector,
        strategy: d.strategy,
        conditions: conditions(&d.conditions, now),
        containers: d.containers.into_iter().map(container_spec).collect(),
        volumes: d.volumes.into_iter().map(volume).collect(),
        labels: d.labels,
        annotations: d.annotations,
        hpa: part(d.hpa, |h| {
            h.map(|h| m::Hpa {
                name: h.name,
                min_replicas: h.min_replicas,
                max_replicas: h.max_replicas,
                current_replicas: h.current_replicas,
                desired_replicas: h.desired_replicas,
                metrics: h.metrics,
                conditions: conditions(&h.conditions, now),
            })
        }),
        pdb: part(d.pdb, |p| {
            p.map(|p| m::Pdb {
                name: p.name,
                min_available: p.min_available,
                max_unavailable: p.max_unavailable,
                allowed_disruptions: p.allowed_disruptions,
                current_healthy: p.current_healthy,
                desired_healthy: p.desired_healthy,
                expected_pods: p.expected_pods,
            })
        }),
        history: d.history.map(|h| {
            part(h, |v| {
                v.into_iter()
                    .map(|r| m::Revision {
                        revision: r.revision,
                        name: r.name,
                        images: r.images,
                        age_seconds: ago(now, r.created),
                        replicas: r.replicas,
                        current: r.current,
                    })
                    .collect()
            })
        }),
        active_jobs: d.active_jobs,
    }
}

pub fn pod_item(r: PodRow, now: i64) -> m::PodItem {
    m::PodItem {
        reason: (r.status != r.phase).then(|| r.status.clone()),
        problem: r.problem.as_ref().map(name_of),
        ready: format!("{}/{}", r.ready, r.containers),
        restarts: r.restarts,
        last_restart_seconds_ago: ago(now, r.last_restart),
        node: r.node,
        age_seconds: ago(now, r.created),
        owner: r.owner.map(|o| m::Owner {
            kind: o.kind,
            name: o.name,
        }),
        phase: r.phase,
        name: r.name,
    }
}

fn state(s: StateView, now: i64) -> m::ContainerState {
    m::ContainerState {
        state: name_of(&s.state),
        reason: s.reason,
        message: s.message,
        exit_code: s.exit_code,
        started_seconds_ago: ago(now, s.started),
        finished_seconds_ago: ago(now, s.finished),
    }
}

pub fn event(e: EventRow, now: i64) -> m::Event {
    m::Event {
        kind: e.kind,
        reason: e.reason,
        object: m::ObjectRef {
            kind: e.object.kind,
            name: e.object.name,
        },
        message: e.message,
        count: e.count,
        last_seen_seconds_ago: ago(now, e.last_seen),
    }
}

pub fn pod_details(d: PodDetails, events: m::Part<Vec<m::Event>>, now: i64) -> m::PodDetails {
    m::PodDetails {
        qos: d.pod.qos.clone(),
        pod: pod_item(d.pod, now),
        conditions: conditions(&d.conditions, now),
        pod_ip: d.pod_ip,
        service_account: d.service_account,
        containers: d
            .containers
            .into_iter()
            .map(|c| m::PodContainer {
                spec: container_spec(c.spec),
                image_id: c.image_id,
                state: c.state.map(|s| state(s, now)),
                last_state: c.last_state.map(|s| state(s, now)),
                ready: c.ready,
                restarts: c.restarts,
            })
            .collect(),
        volumes: d.volumes.into_iter().map(volume).collect(),
        labels: d.labels,
        annotations: d.annotations,
        events,
    }
}

pub fn quotas(q: QuotasView) -> m::Quotas {
    m::Quotas {
        resource_quotas: part(q.quotas, |v| {
            v.into_iter()
                .map(|q| m::Quota {
                    name: q.name,
                    scopes: q.scopes,
                    entries: q
                        .entries
                        .into_iter()
                        .map(|e| m::QuotaEntry {
                            resource: e.resource,
                            used: e.used,
                            hard: e.hard,
                            ratio: e.ratio,
                        })
                        .collect(),
                })
                .collect()
        }),
        limit_ranges: part(q.limit_ranges, |v| {
            v.into_iter()
                .map(|l| m::LimitRange {
                    name: l.name,
                    limits: l
                        .limits
                        .into_iter()
                        .map(|x| m::Limit {
                            kind: x.kind,
                            resource: x.resource,
                            min: x.min,
                            max: x.max,
                            default: x.default,
                            default_request: x.default_request,
                            max_limit_request_ratio: x.max_limit_request_ratio,
                        })
                        .collect(),
                })
                .collect()
        }),
    }
}

fn gateway_route(r: crate::k8s::network::GatewayRouteRow, now: i64) -> m::GatewayRoute {
    m::GatewayRoute {
        name: r.name,
        hostnames: r.hostnames,
        parents: r
            .parents
            .into_iter()
            .map(|p| m::RouteParent {
                name: p.name,
                namespace: p.namespace,
                section: p.section,
                accepted: p.accepted.as_ref().map(|c| condition(c, now)),
                resolved_refs: p.resolved_refs.as_ref().map(|c| condition(c, now)),
            })
            .collect(),
        backends: r.backends,
    }
}

pub fn network(n: NetworkView, now: i64) -> m::Network {
    m::Network {
        services: part(n.services, |v| {
            v.into_iter()
                .map(|s| m::Service {
                    name: s.name,
                    kind: s.kind,
                    cluster_ip: s.cluster_ip,
                    external: s.external,
                    ports: s.ports,
                    selector: s.selector,
                    endpoints: s.endpoints.map(|e| m::Endpoints {
                        ready: e.ready,
                        total: e.total,
                    }),
                    age_seconds: ago(now, s.created),
                })
                .collect()
        }),
        endpoints_error: n.endpoints_error.as_ref().map(error_text),
        routes: n.routes.map(|p| {
            part(p, |v| {
                v.into_iter()
                    .map(|r| m::Route {
                        name: r.name,
                        host: r.host,
                        path: r.path,
                        service: r.service,
                        port: r.port,
                        tls: r.tls,
                        admitted: r.admitted,
                        admitted_message: r.admitted_message,
                        cert_expires_in_seconds: r.cert_expires.map(|t| t - now),
                        ingress: r.ingress,
                    })
                    .collect()
            })
        }),
        ingresses: part(n.ingresses, |v| {
            v.into_iter()
                .map(|i| m::Ingress {
                    name: i.name,
                    class: i.class,
                    rules: i
                        .rules
                        .into_iter()
                        .map(|p| m::IngressPath {
                            host: p.host,
                            path: p.path,
                            service: p.service,
                            port: p.port,
                        })
                        .collect(),
                    default_backend: i.default_backend,
                    tls: i
                        .tls
                        .into_iter()
                        .map(|t| m::IngressTls {
                            hosts: t.hosts,
                            secret: t.secret,
                        })
                        .collect(),
                    addresses: i.addresses,
                    routes: i.routes,
                })
                .collect()
        }),
        gateways: n.gateways.map(|p| {
            part(p, |v| {
                v.into_iter()
                    .map(|g| m::Gateway {
                        name: g.name,
                        class: g.class,
                        listeners: g
                            .listeners
                            .into_iter()
                            .map(|l| m::Listener {
                                name: l.name,
                                port: l.port,
                                protocol: l.protocol,
                                hostname: l.hostname,
                            })
                            .collect(),
                        addresses: g.addresses,
                        accepted: g.accepted,
                        programmed: g.programmed,
                    })
                    .collect()
            })
        }),
        http_routes: n.http_routes.map(|p| {
            part(p, |v| {
                v.into_iter().map(|r| gateway_route(r, now)).collect()
            })
        }),
        grpc_routes: n.grpc_routes.map(|p| {
            part(p, |v| {
                v.into_iter().map(|r| gateway_route(r, now)).collect()
            })
        }),
    }
}

/// Состояние workload'а для наблюдения: реплики, поды с рестартами по контейнерам,
/// события `Warning`.
pub fn workload_state(row: &WorkloadRow, pods: &[Pod], events: Vec<EventRow>) -> m::WorkloadState {
    let ready = |p: &Pod| {
        p.status
            .as_ref()
            .and_then(|s| s.conditions.as_ref())
            .is_some_and(|cs| cs.iter().any(|c| c.type_ == "Ready" && c.status == "True"))
    };
    m::WorkloadState {
        desired: row.desired,
        ready: row.ready,
        available: row.available,
        updated: row.updated,
        pods: pods
            .iter()
            .map(|p| {
                let status = p.status.as_ref();
                m::PodState {
                    name: p.metadata.name.clone().unwrap_or_default(),
                    phase: status.and_then(|s| s.phase.clone()).unwrap_or_default(),
                    ready: ready(p),
                    containers: status
                        .iter()
                        .flat_map(|s| {
                            s.init_container_statuses
                                .iter()
                                .flatten()
                                .chain(s.container_statuses.iter().flatten())
                        })
                        .map(|c| {
                            let last = c.last_state.as_ref().and_then(|s| s.terminated.as_ref());
                            m::ContainerRestarts {
                                name: c.name.clone(),
                                restarts: c.restart_count,
                                last_reason: last.and_then(|t| t.reason.clone()),
                                last_exit_code: last.map(|t| t.exit_code),
                            }
                        })
                        .collect(),
                }
            })
            .collect(),
        warnings: events
            .into_iter()
            .filter(|e| e.kind == "Warning")
            .map(|e| m::Warning {
                key: e.key,
                reason: e.reason,
                object: m::ObjectRef {
                    kind: e.object.kind,
                    name: e.object.name,
                },
                message: e.message,
                count: e.count,
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::k8s::pods::{OwnerRef, PodProblem};

    #[test]
    fn quantities_read_as_kubernetes_writes_them() {
        assert_eq!(cpu_text(2000), "2");
        assert_eq!(cpu_text(250), "250m");
        assert_eq!(memory_text(2 << 30), "2Gi");
        assert_eq!(memory_text(512 << 20), "512Mi");
        assert_eq!(memory_text((1 << 20) + 300_000), "1Mi");
        assert_eq!(memory_text(0), "0Mi");
    }

    #[test]
    fn pod_row_gets_ages_problem_and_kubectl_status() {
        let row = PodRow {
            name: "web-1".into(),
            phase: "Running".into(),
            status: "CrashLoopBackOff".into(),
            problem: Some(PodProblem::CrashLoop),
            ready: 0,
            containers: 2,
            restarts: 7,
            last_restart: Some(990),
            node: Some("w1".into()),
            created: Some(400),
            qos: None,
            owner: Some(OwnerRef {
                kind: "Deployment".into(),
                name: "web".into(),
            }),
        };
        let p = pod_item(row, 1000);
        assert_eq!(p.reason.as_deref(), Some("CrashLoopBackOff"));
        assert_eq!(p.problem.as_deref(), Some("crashLoop"));
        assert_eq!(p.ready, "0/2");
        assert_eq!(
            (p.age_seconds, p.last_restart_seconds_ago),
            (Some(600), Some(10))
        );
        let v = serde_json::to_value(&p).unwrap();
        assert_eq!(v["owner"]["kind"], "Deployment");
    }

    #[test]
    fn parts_carry_data_or_the_reason() {
        let ok: Part<i32> = Ok(1).into();
        assert_eq!(part(ok, |x| x + 1), m::Part::Data(2));
        let e = crate::k8s::errors::not_allowed("list", "nodes", None);
        let err: Part<i32> = Err(e).into();
        let m::Part::Error { error } = part(err, |x| x) else {
            panic!("ошибка")
        };
        assert!(error.starts_with("FORBIDDEN: "), "{error}");
    }

    #[test]
    fn unknown_error_keys_get_a_fixed_phrase() {
        let e = crate::app_err!(
            crate::error::ErrorCode::ConnectionFailed,
            "errors.someday.newKey",
            "Cannot reach {{url}}",
            url = "https://marker-host.example/prometheus"
        );
        assert_eq!(
            error_text(&e),
            "CONNECTION_FAILED: The operation failed; the user can see the details in Glafira"
        );
    }

    #[test]
    fn server_text_is_cut_for_the_model() {
        let long = "x".repeat(2000);
        let e = crate::k8s::errors::from_status(
            &kube::core::Status::failure(&long, "Invalid").with_code(422),
        );
        let text = llm_message(&e);
        assert_eq!(text.chars().count(), MAX_LLM_ERROR_CHARS);
        assert!(text.ends_with('…'), "{text}");
        let short = crate::k8s::errors::not_allowed("get", "pods", Some("team-a"));
        assert_eq!(llm_message(&short), short.message);
    }

    #[test]
    fn change_check_errors_reach_the_model_with_their_text() {
        let e = crate::change::check_limits("app", Some(1.0), Some(0.5), "CPU").unwrap_err();
        assert_eq!(e.key, "errors.change.limitBelowRequest");
        assert_eq!(
            llm_message(&e),
            "Container app: the CPU limit is below the request"
        );
    }

    #[test]
    fn the_job_resources_refusal_reaches_the_model_with_its_text() {
        let job: crate::change::Change = serde_json::from_value(serde_json::json!({
            "kind": "setResources", "namespace": "team-a",
            "workload": {"kind": "Job", "name": "once"},
            "containers": [{"name": "app", "requests": {"cpu": "1"}}]}))
        .unwrap();
        let e = crate::change::validate_form(&job).unwrap_err();
        assert_eq!(e.key, "errors.change.resourcesKind");
        assert_eq!(llm_message(&e), e.message);
    }

    #[test]
    fn llm_text_keys_are_dictionary_keys() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../src/locales/en.json");
        let dict: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        for key in LLM_TEXT_KEYS {
            let found = key
                .split('.')
                .try_fold(&dict, |v, part| v.get(part))
                .is_some_and(serde_json::Value::is_string);
            assert!(found, "{key}");
        }
    }
}
