//! Сбор снимка для правил (спец. §8.1): только чтение через `ReadClient`, не больше 4
//! параллельных запросов, 50 проектов и 5000 подов, дедлайн 45 с. Что не прочиталось, правила
//! не проверяют, а `check.partial` называет (`Coverage::gaps`).

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::Mutex;
use std::time::Duration;

use futures::future::BoxFuture;
use k8s_openapi::api::autoscaling::v2::HorizontalPodAutoscaler;
use k8s_openapi::api::core::v1::{
    Container, Node, PersistentVolumeClaim, Pod, ResourceQuota, Secret, Service,
};
use k8s_openapi::api::discovery::v1::EndpointSlice;
use k8s_openapi::api::networking::v1::Ingress;
use kube::api::{DynamicObject, ListParams};
use tokio::sync::Semaphore;
use tokio::time::Instant;

use super::model::TargetKind;
use super::snapshot::*;
use crate::error::{AppError, AppResult, ErrorCode};
use crate::k8s::cluster::ConnectionInfo;
use crate::k8s::network::{
    endpoints_by_service, gateway_route_row, gateways, grpc_routes, http_routes, ingress_row,
    route_row, routes,
};
use crate::k8s::openshift::{
    cluster_operators, cluster_versions, machine_config_pool_row, machine_config_pools,
    operator_row,
};
use crate::k8s::pods::{in_crash_loop, owner_of};
use crate::k8s::quantity::parse_quantity;
use crate::k8s::quotas::quota_view;
use crate::k8s::read::{Listed, ReadClient};
use crate::k8s::storage::mounts;
use crate::k8s::view::{ConditionView, conditions, conditions_of, created, name, seconds};
use crate::k8s::workloads::{WorkloadFacts, WorkloadKind, list_facts};
use crate::metrics::prometheus::MetricsSource;
use crate::platform::PlatformKind;
use crate::storage::profile::ConnectionProfile;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    pub projects: usize,
    pub pods: usize,
    pub parallel: usize,
    pub deadline: Duration,
}

/// Пределы проверки (спец. §8.1).
pub const LIMITS: Limits = Limits {
    projects: 50,
    pods: 5000,
    parallel: 4,
    deadline: Duration::from_secs(45),
};

/// Запросы к кластеру: не больше `parallel` одновременно и не позже дедлайна. Непрочитанное
/// записывается в разрывы снимка.
struct Reader<'a> {
    r: &'a ReadClient,
    slots: Semaphore,
    deadline: Instant,
    gaps: Mutex<Vec<Gap>>,
    timed_out: Mutex<bool>,
}

/// Код ошибки для `check.partial`: `FORBIDDEN`, `TIMEOUT`…
fn code(e: &AppError) -> String {
    serde_json::to_value(e.code)
        .ok()
        .and_then(|v| v.as_str().map(String::from))
        .unwrap_or_else(|| "INTERNAL".into())
}

impl Reader<'_> {
    fn gap(&self, part: &str, namespace: Option<&str>, code: String) {
        self.gaps
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(Gap {
                part: part.to_string(),
                namespace: namespace.map(String::from),
                code,
            });
    }

    fn time_out(&self, part: &str, namespace: Option<&str>) {
        *self.timed_out.lock().unwrap_or_else(|e| e.into_inner()) = true;
        self.gap(part, namespace, "TIMEOUT".into());
    }

    /// Запрос под слотом и дедлайном; ошибка — разрыв `part` и `None`. Ответ 404 — вида нет
    /// в кластере (GRPCRoute до Gateway API 1.1, IngressClass до 1.19): `None` без разрыва, как
    /// в интерфейсе (`k8s::network`). После дедлайна новые запросы не уходят. Запрос — в куче
    /// (`BoxFuture`): у проекта их десяток, и вместе они не помещались бы в стек потока runtime.
    fn read<'s, T: Send + 's>(
        &'s self,
        part: &'s str,
        namespace: Option<&'s str>,
        fut: impl Future<Output = AppResult<T>> + Send + 's,
    ) -> BoxFuture<'s, Option<T>> {
        self.run(part, namespace, true, fut)
    }

    /// Как `read`, но 404 — такой же разрыв, как остальные ошибки: у `/readyz` и списка проектов
    /// «вида нет» не бывает, а молчаливое `None` скрыло бы, что не прочитано ничего.
    fn read_required<'s, T: Send + 's>(
        &'s self,
        part: &'s str,
        namespace: Option<&'s str>,
        fut: impl Future<Output = AppResult<T>> + Send + 's,
    ) -> BoxFuture<'s, Option<T>> {
        self.run(part, namespace, false, fut)
    }

    fn run<'s, T: Send + 's>(
        &'s self,
        part: &'s str,
        namespace: Option<&'s str>,
        absent_is_none: bool,
        fut: impl Future<Output = AppResult<T>> + Send + 's,
    ) -> BoxFuture<'s, Option<T>> {
        Box::pin(async move {
            if Instant::now() >= self.deadline {
                self.time_out(part, namespace);
                return None;
            }
            let run = async {
                let _slot = self.slots.acquire().await;
                fut.await
            };
            match tokio::time::timeout_at(self.deadline, run).await {
                Ok(Ok(v)) => Some(v),
                Ok(Err(e)) if absent_is_none && e.code == ErrorCode::NotFound => None,
                Ok(Err(e)) => {
                    self.gap(part, namespace, code(&e));
                    None
                }
                Err(_) => {
                    self.time_out(part, namespace);
                    None
                }
            }
        })
    }

    /// Список: прочитано не всё (больше 10 000) — тоже разрыв.
    fn list<'s, T: Send + 's>(
        &'s self,
        part: &'s str,
        namespace: Option<&'s str>,
        fut: impl Future<Output = AppResult<Listed<T>>> + Send + 's,
    ) -> BoxFuture<'s, Option<Vec<T>>> {
        Box::pin(async move {
            let l = self.read(part, namespace, fut).await?;
            if l.truncated {
                self.gap(part, namespace, "TRUNCATED".into());
            }
            Some(l.items)
        })
    }
}

fn cond(c: &ConditionView) -> Cond {
    Cond {
        status: c.status.clone(),
        reason: c.reason.clone(),
        message: c.message.clone(),
        since: c.last_transition,
    }
}

fn find(conds: &[ConditionView], kind: &str) -> Option<Cond> {
    conds.iter().find(|c| c.kind == kind).map(cond)
}

fn milli(q: Option<&k8s_openapi::apimachinery::pkg::api::resource::Quantity>) -> Option<i64> {
    q.and_then(|q| parse_quantity(&q.0))
        .map(|v| (v * 1000.0).round() as i64)
}

fn bytes(q: Option<&k8s_openapi::apimachinery::pkg::api::resource::Quantity>) -> Option<i64> {
    q.and_then(|q| parse_quantity(&q.0))
        .map(|v| v.round() as i64)
}

fn node_snap(n: &Node) -> NodeSnap {
    let conds = conditions_of(&n.status.as_ref().and_then(|s| s.conditions.clone()));
    let alloc = n.status.as_ref().and_then(|s| s.allocatable.as_ref());
    NodeSnap {
        name: name(&n.metadata),
        ready: find(&conds, "Ready"),
        pressures: conds
            .iter()
            .filter(|c| c.kind.ends_with("Pressure") && c.is_true())
            .map(|c| c.kind.clone())
            .collect(),
        unschedulable: n.spec.as_ref().and_then(|s| s.unschedulable) == Some(true),
        allocatable_cpu_milli: milli(alloc.and_then(|a| a.get("cpu"))),
        allocatable_memory: bytes(alloc.and_then(|a| a.get("memory"))),
    }
}

/// Контейнеры, которые занимают ресурсы, пока под работает: основные и нативные sidecar'ы.
fn running_containers(p: &Pod) -> impl Iterator<Item = &Container> {
    p.spec.iter().flat_map(|s| {
        s.containers.iter().chain(
            s.init_containers
                .iter()
                .flatten()
                .filter(|c| c.restart_policy.as_deref() == Some("Always")),
        )
    })
}

/// Сумма requests подов по нодам: только размещённые и не завершённые поды.
pub fn requests_by_node(pods: &[Pod]) -> BTreeMap<String, Requests> {
    let mut out: BTreeMap<String, Requests> = BTreeMap::new();
    for p in pods {
        let phase = p.status.as_ref().and_then(|s| s.phase.as_deref());
        let Some(node) = p.spec.as_ref().and_then(|s| s.node_name.clone()) else {
            continue;
        };
        if matches!(phase, Some("Succeeded" | "Failed")) {
            continue;
        }
        let r = out.entry(node).or_default();
        for c in running_containers(p) {
            let req = c.resources.as_ref().and_then(|r| r.requests.as_ref());
            r.cpu_milli += milli(req.and_then(|m| m.get("cpu"))).unwrap_or(0);
            r.memory += bytes(req.and_then(|m| m.get("memory"))).unwrap_or(0);
        }
    }
    out
}

fn exit(s: Option<&k8s_openapi::api::core::v1::ContainerState>) -> Option<Exit> {
    s.and_then(|s| s.terminated.as_ref()).map(|t| Exit {
        reason: t.reason.clone(),
        exit_code: t.exit_code,
        finished: t.finished_at.as_ref().map(seconds),
    })
}

pub fn pod_snap(p: &Pod) -> PodSnap {
    let status = p.status.as_ref();
    let conds = conditions_of(&status.and_then(|s| s.conditions.clone()));
    let ready = find(&conds, "Ready");
    let statuses: BTreeMap<(bool, &str), &k8s_openapi::api::core::v1::ContainerStatus> = status
        .iter()
        .flat_map(|s| {
            let init = s
                .init_container_statuses
                .iter()
                .flatten()
                .map(|c| (true, c));
            let main = s.container_statuses.iter().flatten().map(|c| (false, c));
            init.chain(main)
        })
        .map(|(init, c)| ((init, c.name.as_str()), c))
        .collect();
    let spec_containers = p.spec.iter().flat_map(|s| {
        let init = s.init_containers.iter().flatten().map(|c| (true, c));
        let main = s.containers.iter().map(|c| (false, c));
        init.chain(main)
    });
    let containers = spec_containers
        .map(|(init, c)| {
            let st = statuses.get(&(init, c.name.as_str()));
            let res = c.resources.as_ref();
            let has = |m: Option<
                &BTreeMap<String, k8s_openapi::apimachinery::pkg::api::resource::Quantity>,
            >,
                       key: &str| m.is_some_and(|m| m.contains_key(key));
            let waiting = st
                .and_then(|s| s.state.as_ref())
                .and_then(|s| s.waiting.as_ref());
            ContainerSnap {
                name: c.name.clone(),
                init,
                sidecar: init && c.restart_policy.as_deref() == Some("Always"),
                image: c.image.clone().unwrap_or_default(),
                crash_loop: st.is_some_and(|s| in_crash_loop(p, s)),
                waiting_reason: waiting.and_then(|w| w.reason.clone()),
                waiting_message: waiting.and_then(|w| w.message.clone()),
                exit: exit(st.and_then(|s| s.state.as_ref())),
                last_exit: exit(st.and_then(|s| s.last_state.as_ref())),
                restarts: st.map_or(0, |s| s.restart_count),
                cpu_request: has(res.and_then(|r| r.requests.as_ref()), "cpu"),
                memory_request: has(res.and_then(|r| r.requests.as_ref()), "memory"),
                memory_limit: has(res.and_then(|r| r.limits.as_ref()), "memory"),
                amounts: ContainerAmounts {
                    cpu_request_milli: milli(
                        res.and_then(|r| r.requests.as_ref())
                            .and_then(|m| m.get("cpu")),
                    ),
                    memory_request: bytes(
                        res.and_then(|r| r.requests.as_ref())
                            .and_then(|m| m.get("memory")),
                    ),
                    cpu_limit_milli: milli(
                        res.and_then(|r| r.limits.as_ref())
                            .and_then(|m| m.get("cpu")),
                    ),
                    memory_limit: bytes(
                        res.and_then(|r| r.limits.as_ref())
                            .and_then(|m| m.get("memory")),
                    ),
                },
            }
        })
        .collect();
    PodSnap {
        name: name(&p.metadata),
        phase: status
            .and_then(|s| s.phase.clone())
            .unwrap_or_else(|| "Unknown".into()),
        created: created(&p.metadata),
        owner: owner_of(p),
        node: p.spec.as_ref().and_then(|s| s.node_name.clone()),
        ready: ready.as_ref().is_some_and(|c| c.is("True")),
        not_ready_since: ready.filter(|c| c.is("False")).and_then(|c| c.since),
        scheduled: find(&conds, "PodScheduled"),
        terminating: p.metadata.deletion_timestamp.is_some(),
        containers,
    }
}

fn workload_snap(f: WorkloadFacts) -> WorkloadSnap {
    // `Progressing` и `Available` разбирает `workload.rolloutStuck`, а он — про Deployment
    // и DeploymentConfig; у остальных видов одноимённое условие означало бы другое.
    let rollout = matches!(
        f.row.kind,
        WorkloadKind::Deployment | WorkloadKind::DeploymentConfig
    );
    WorkloadSnap {
        cron: f.cron,
        kind: f.row.kind,
        name: f.row.name,
        created: f.row.created,
        desired: f.row.desired,
        available: f.row.available,
        progressing: find(&f.conditions, "Progressing").filter(|_| rollout),
        available_cond: find(&f.conditions, "Available").filter(|_| rollout),
        containers: f
            .template
            .and_then(|t| t.spec)
            .map(|s| {
                s.containers
                    .into_iter()
                    .map(|c| TemplateContainer {
                        name: c.name,
                        readiness_probe: c.readiness_probe.is_some(),
                    })
                    .collect()
            })
            .unwrap_or_default(),
    }
}

fn hpa_snap(h: &HorizontalPodAutoscaler) -> HpaSnap {
    let spec = h.spec.as_ref();
    let status = h.status.as_ref();
    HpaSnap {
        name: name(&h.metadata),
        target_kind: spec
            .map(|s| s.scale_target_ref.kind.clone())
            .unwrap_or_default(),
        target_name: spec
            .map(|s| s.scale_target_ref.name.clone())
            .unwrap_or_default(),
        min_replicas: spec.and_then(|s| s.min_replicas),
        max_replicas: spec.map_or(0, |s| s.max_replicas),
        current_replicas: status.and_then(|s| s.current_replicas),
        conditions: conditions_of(&status.and_then(|s| s.conditions.clone()))
            .iter()
            .map(|c| (c.kind.clone(), cond(c)))
            .collect(),
    }
}

fn service_snap(
    s: &Service,
    ready: Option<&BTreeMap<String, crate::k8s::network::EndpointCount>>,
) -> ServiceSnap {
    let spec = s.spec.as_ref();
    let svc = name(&s.metadata);
    ServiceSnap {
        kind: spec
            .and_then(|s| s.type_.clone())
            .unwrap_or_else(|| "ClusterIP".into()),
        has_selector: spec
            .and_then(|s| s.selector.as_ref())
            .is_some_and(|m| !m.is_empty()),
        ports: spec
            .and_then(|s| s.ports.as_ref())
            .map(|ps| {
                ps.iter()
                    .map(|p| ServicePort {
                        name: p.name.clone(),
                        port: p.port,
                    })
                    .collect()
            })
            .unwrap_or_default(),
        ready_endpoints: ready.map(|m| m.get(&svc).map_or(0, |c| c.ready)),
        name: svc,
    }
}

/// `svc:port` → Service и порт (строка `default_backend` из `ingress_row`).
fn split_backend(b: &str) -> (String, Option<String>) {
    match b.rsplit_once(':') {
        Some((s, p)) => (s.to_string(), Some(p.to_string())),
        None => (b.to_string(), None),
    }
}

fn gateway_snap(o: &DynamicObject) -> GatewaySnap {
    let conds = conditions(&o.data["status"]["conditions"]);
    GatewaySnap {
        name: name(&o.metadata),
        created: created(&o.metadata),
        accepted: find(&conds, "Accepted"),
        programmed: find(&conds, "Programmed"),
    }
}

fn gateway_route_snap(kind: TargetKind, o: &DynamicObject) -> GatewayRouteSnap {
    let route_ns = o.metadata.namespace.clone().unwrap_or_default();
    let row = gateway_route_row(
        if kind == TargetKind::Grpcroute {
            "GRPCRoute"
        } else {
            "HTTPRoute"
        },
        o,
    );
    let gw =
        |name: &str, ns: &Option<String>| format!("{}/{name}", ns.as_deref().unwrap_or(&route_ns));
    GatewayRouteSnap {
        kind,
        name: row.name.clone(),
        created: row.created,
        parent_refs: row
            .parents
            .iter()
            .map(|p| gw(&p.name, &p.namespace))
            .collect(),
        parents: row
            .parents
            .iter()
            .filter(|p| p.accepted.is_some() || p.resolved_refs.is_some())
            .map(|p| RouteParentSnap {
                gateway: gw(&p.name, &p.namespace),
                accepted: p.accepted.as_ref().map(cond),
                resolved_refs: p.resolved_refs.as_ref().map(cond),
            })
            .collect(),
        backends: row.backends.iter().map(|b| split_backend(b).0).collect(),
    }
}

/// Проекты проверки: названные (`Some`) или все, что видит пользователь, — избранные первыми,
/// затем по имени; не больше `limit`. Второе значение — сколько было до предела.
async fn projects(
    rd: &Reader<'_>,
    info: &ConnectionInfo,
    profile: &ConnectionProfile,
    requested: Option<&[String]>,
    limit: usize,
) -> (Vec<String>, usize) {
    let mut names: Vec<String> = match requested {
        Some(list) => {
            let mut v: Vec<String> = Vec::new();
            for n in list {
                if !v.contains(n) {
                    v.push(n.clone());
                }
            }
            v
        }
        None => {
            let list = rd
                .read_required(
                    "projects",
                    None,
                    crate::k8s::projects::list_projects(
                        rd.r,
                        info.api_groups.project_source(),
                        profile,
                    ),
                )
                .await;
            let Some(list) = list else {
                return (Vec::new(), 0);
            };
            if let Some(e) = &list.error {
                rd.gap("projects", None, code(e));
            }
            let mut rows = list.items;
            rows.sort_by(|a, b| b.favorite.cmp(&a.favorite).then(a.name.cmp(&b.name)));
            rows.into_iter().map(|p| p.name).collect()
        }
    };
    let total = names.len();
    names.truncate(limit);
    (names, total)
}

/// Окно правил `resources.*`, дни (спец. §8.2).
pub const RESOURCES_WINDOW_DAYS: u32 = 1;

async fn project(
    rd: &Reader<'_>,
    info: &ConnectionInfo,
    ns: &str,
    metrics: Option<&dyn MetricsSource>,
    now: i64,
) -> ProjectSnap {
    let lp = ListParams::default();
    let n = Some(ns);
    let groups = &info.api_groups;
    let kinds: Vec<WorkloadKind> = WorkloadKind::ALL
        .into_iter()
        .filter(|k| k.available(groups))
        .collect();
    // Вид, который не прочитался, — разрыв; остальные виды проверяются.
    let workloads = async {
        let mut out = Vec::new();
        for kind in &kinds {
            let part = format!("{kind:?}");
            if let Some((l, skipped)) = rd.read(&part, n, list_facts(rd.r, ns, *kind)).await {
                if l.truncated {
                    rd.gap(&part, n, "TRUNCATED".into());
                }
                if let Some(e) = skipped {
                    rd.gap(&part, n, code(&e));
                }
                out.extend(l.items.into_iter().map(workload_snap));
            }
        }
        Some(out)
    };
    let dynamic = |part: &'static str, ar: kube::api::ApiResource| {
        let lp = lp.clone();
        async move { rd.list(part, n, rd.r.list_dynamic(&ar, n, &lp)).await }
    };
    let (pods, workloads, hpas, services, slices, ingresses, pvcs, quotas, secrets) = tokio::join!(
        rd.list("pods", n, rd.r.list::<Pod>(n, &lp)),
        workloads,
        rd.list(
            "horizontalpodautoscalers",
            n,
            rd.r.list::<HorizontalPodAutoscaler>(n, &lp)
        ),
        rd.list("services", n, rd.r.list::<Service>(n, &lp)),
        rd.list("endpointslices", n, rd.r.list::<EndpointSlice>(n, &lp)),
        rd.list("ingresses", n, rd.r.list::<Ingress>(n, &lp)),
        rd.list(
            "persistentvolumeclaims",
            n,
            rd.r.list::<PersistentVolumeClaim>(n, &lp)
        ),
        rd.list("resourcequotas", n, rd.r.list::<ResourceQuota>(n, &lp)),
        rd.list("secrets", n, rd.r.names::<Secret>(ns)),
    );
    let (route_objs, gateway_objs, http_objs, grpc_objs, usage) = tokio::join!(
        async {
            match groups.routes {
                true => dynamic("routes", routes()).await,
                false => None,
            }
        },
        async {
            match groups.gateway_api {
                true => dynamic("gateways", gateways()).await,
                false => None,
            }
        },
        async {
            match groups.gateway_api {
                true => dynamic("httproutes", http_routes()).await,
                false => None,
            }
        },
        async {
            match groups.gateway_api {
                true => dynamic("grpcroutes", grpc_routes()).await,
                false => None,
            }
        },
        // Потребление за 24 ч — агрегирующими запросами по проекту (три запроса под одним
        // слотом). Медленный источник держит только это чтение, а не чтение маршрутов. Ответ 404
        // источника или прокси API-сервера — такой же разрыв, как остальные ошибки: непрочитанное
        // не проверено, а не «вида нет» (`read_required`, не `read`).
        async {
            match metrics {
                Some(src) => {
                    rd.read_required("resources", n, async move {
                        crate::resources::project::pod_container_use(
                            src,
                            ns,
                            RESOURCES_WINDOW_DAYS,
                            now,
                            false,
                        )
                        .await
                        .map_err(|f| f.error)
                    })
                    .await
                }
                None => None,
            }
        },
    );
    let route_rows: Vec<_> = route_objs.iter().flatten().map(route_row).collect();
    let mut gateway_routes: Option<Vec<GatewayRouteSnap>> = None;
    for (kind, objs) in [
        (TargetKind::Httproute, &http_objs),
        (TargetKind::Grpcroute, &grpc_objs),
    ] {
        if let Some(objs) = objs {
            gateway_routes
                .get_or_insert_with(Vec::new)
                .extend(objs.iter().map(|o| gateway_route_snap(kind, o)));
        }
    }
    // Route ожидались (группа API есть), но не прочитались: какой Ingress OpenShift обслуживает
    // через Routes, неизвестно, и класс такого Ingress правило не проверяет.
    let routes_unread = groups.routes && route_objs.is_none();
    let ready = slices.as_deref().map(endpoints_by_service);
    let mounted = pods.as_deref().map(mounts);
    ProjectSnap {
        namespace: ns.to_string(),
        workloads,
        hpas: hpas.map(|v| v.iter().map(hpa_snap).collect()),
        services: services.map(|v| v.iter().map(|s| service_snap(s, ready.as_ref())).collect()),
        routes: route_objs.as_ref().map(|_| {
            route_rows
                .iter()
                .map(|r| RouteSnap {
                    name: r.name.clone(),
                    service: r.service.clone(),
                    admitted: r.admitted,
                    admitted_reason: None,
                    admitted_message: r.admitted_message.clone(),
                    cert_expires: r.cert_expires,
                    from_ingress: r.ingress.clone(),
                })
                .collect()
        }),
        ingresses: ingresses.map(|v| {
            v.iter()
                .map(|i| {
                    let row = ingress_row(i, &route_rows);
                    let mut backends: Vec<(String, Option<String>)> = row
                        .rules
                        .iter()
                        .filter_map(|p| p.service.clone().map(|s| (s, p.port.clone())))
                        .collect();
                    backends.extend(row.default_backend.as_deref().map(split_backend));
                    IngressSnap {
                        name: row.name.clone(),
                        class: row.class.clone(),
                        backends,
                        tls_secrets: row.tls.iter().filter_map(|t| t.secret.clone()).collect(),
                        created: row.created,
                        has_routes: routes_unread || !row.routes.is_empty(),
                    }
                })
                .collect()
        }),
        gateways: gateway_objs.map(|v| v.iter().map(gateway_snap).collect()),
        gateway_routes,
        pvcs: pvcs.map(|v| {
            v.iter()
                .map(|c| {
                    let pvc = name(&c.metadata);
                    PvcSnap {
                        phase: c.status.as_ref().and_then(|s| s.phase.clone()),
                        created: created(&c.metadata),
                        class: c.spec.as_ref().and_then(|s| s.storage_class_name.clone()),
                        mounted_by: mounted
                            .as_ref()
                            .and_then(|m| m.get(&pvc).cloned())
                            .unwrap_or_default(),
                        name: pvc,
                    }
                })
                .collect()
        }),
        quotas: quotas.map(|v| {
            v.iter()
                .map(|q| {
                    let view = quota_view(q);
                    QuotaSnap {
                        name: view.name,
                        entries: view
                            .entries
                            .into_iter()
                            .filter_map(|e| {
                                Some(QuotaUse {
                                    ratio: e.ratio?,
                                    used: e.used?,
                                    hard: e.hard,
                                    resource: e.resource,
                                })
                            })
                            .collect(),
                    }
                })
                .collect()
        }),
        secrets: secrets.map(|v| v.into_iter().collect::<BTreeSet<_>>()),
        pods: pods.map(|v| v.iter().map(pod_snap).collect()),
        usage,
    }
}

/// Уровень кластера: `/readyz`, ноды и requests подов на них, классы Ingress, у OpenShift —
/// операторы, версия и пулы машин. Без прав — разрыв без запроса.
async fn cluster(rd: &Reader<'_>, info: &ConnectionInfo, s: &mut ClusterSnapshot) {
    let caps = &info.capabilities;
    let lp = ListParams::default();
    // Как любое чтение: под слотом и дедлайном, ошибка — разрыв `readyz`.
    let readyz = rd.read_required("readyz", None, async {
        let r = crate::k8s::overview::readyz(rd.r).await;
        match r.error {
            None => Ok(Readyz {
                ok: r.ok,
                failed: r.failed,
            }),
            Some(e) => Err(e),
        }
    });
    let nodes = async {
        match caps.list_nodes {
            true => rd.list("nodes", None, rd.r.list_cluster::<Node>(&lp)).await,
            false => {
                rd.gap("nodes", None, "FORBIDDEN".into());
                None
            }
        }
    };
    let all_pods = async {
        match caps.list_all_pods {
            true => {
                rd.list("pods (all projects)", None, rd.r.list::<Pod>(None, &lp))
                    .await
            }
            false => {
                rd.gap("pods (all projects)", None, "FORBIDDEN".into());
                None
            }
        }
    };
    let classes = rd.read(
        "ingressclasses",
        None,
        crate::k8s::overview::ingress_classes(rd.r),
    );
    let openshift = async {
        if info.platform.kind != PlatformKind::Openshift {
            return None;
        }
        let operators = async {
            match caps.read_cluster_operators {
                true => rd
                    .list(
                        "clusteroperators",
                        None,
                        rd.r.list_dynamic(&cluster_operators(), None, &lp),
                    )
                    .await
                    .map(|v| {
                        v.iter()
                            .map(|o| {
                                let row = operator_row(o);
                                OperatorSnap {
                                    name: row.name,
                                    degraded: row.degraded == Some(true),
                                    available: row.available,
                                    message: row.message,
                                }
                            })
                            .collect()
                    }),
                false => {
                    rd.gap("clusteroperators", None, "FORBIDDEN".into());
                    None
                }
            }
        };
        let version = async {
            match caps.read_cluster_version {
                true => rd
                    .read(
                        "clusterversion",
                        None,
                        rd.r.get_dynamic(&cluster_versions(), None, "version"),
                    )
                    .await
                    .map(|o| find(&conditions(&o.data["status"]["conditions"]), "Failing")),
                false => {
                    rd.gap("clusterversion", None, "FORBIDDEN".into());
                    None
                }
            }
        };
        let pools_ar = machine_config_pools();
        let pools = rd.list(
            "machineconfigpools",
            None,
            rd.r.list_dynamic(&pools_ar, None, &lp),
        );
        let (operators, failing, pools) = tokio::join!(operators, version, pools);
        Some(OpenShiftSnap {
            operators,
            failing,
            degraded_pools: pools.map(|v| {
                v.iter()
                    .map(machine_config_pool_row)
                    .filter(|p| p.degraded)
                    .map(|p| p.name)
                    .collect()
            }),
        })
    };
    let (readyz, nodes, all_pods, classes, openshift) =
        tokio::join!(readyz, nodes, all_pods, classes, openshift);
    s.readyz = readyz;
    s.nodes = nodes.map(|v| v.iter().map(node_snap).collect());
    s.node_requests = all_pods.map(|p| requests_by_node(&p));
    s.ingress_classes = classes.map(|rows| IngressClasses {
        names: rows.iter().map(|c| c.name.clone()).collect(),
        has_default: rows.iter().any(|c| c.default),
    });
    s.openshift = openshift;
}

/// Снимок кластера и проектов. `requested` — проекты проверки (`None` — все доступные);
/// `metrics` — источник истории метрик для правил `resources.*` (без него — разрыв
/// `resources`).
pub async fn collect(
    r: &ReadClient,
    info: &ConnectionInfo,
    profile: &ConnectionProfile,
    requested: Option<&[String]>,
    limits: Limits,
    now: i64,
    metrics: Option<&dyn MetricsSource>,
) -> ClusterSnapshot {
    let rd = Reader {
        r,
        slots: Semaphore::new(limits.parallel),
        deadline: Instant::now() + limits.deadline,
        gaps: Mutex::new(Vec::new()),
        timed_out: Mutex::new(false),
    };
    let mut s = ClusterSnapshot::empty(now, info.platform.kind);
    let (names, total) = projects(&rd, info, profile, requested, limits.projects).await;
    let ((), projects) = tokio::join!(
        cluster(&rd, info, &mut s),
        futures::future::join_all(
            names
                .iter()
                .map(|ns| Box::pin(project(&rd, info, ns, metrics, now)))
        )
    );
    if metrics.is_none() && !names.is_empty() {
        rd.gap("resources", None, "METRICS_UNAVAILABLE".into());
    }
    s.projects = projects;
    // Не больше `limits.pods` подов: проекты по порядку. Проект, чей список не поместился,
    // и все после него не прочитаны (`None`): обрезанный список правила приняли бы за полный
    // (`workload.unavailable` для workload'а, чьи поды отрезаны). Об этом говорит `pod_limit`.
    let mut budget = limits.pods;
    let mut pod_limit = false;
    for p in &mut s.projects {
        let count = p.pods.as_ref().map_or(0, Vec::len);
        if count > budget {
            p.pods = None;
            pod_limit = true;
            budget = 0;
        } else {
            budget -= count;
        }
    }
    // Разрывы приходят в порядке завершения запросов; в `check.partial` — по порядку: сначала
    // кластер, затем проекты по имени, внутри — по части.
    let mut gaps = std::mem::take(&mut *rd.gaps.lock().unwrap_or_else(|e| e.into_inner()));
    gaps.sort_by(|a, b| (&a.namespace, &a.part, &a.code).cmp(&(&b.namespace, &b.part, &b.code)));
    s.coverage = Coverage {
        projects_total: total,
        projects_checked: s.projects.len(),
        pod_limit,
        timed_out: *rd.timed_out.lock().unwrap_or_else(|e| e.into_inner()),
        gaps,
    };
    s
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;
    use crate::k8s::cluster::Capabilities;
    use crate::k8s::pods::fixtures::{crash_looping, running};
    use crate::k8s::testing::{Seen, mock_raw};
    use crate::metrics::prometheus::fake::{FakeSource, vector};
    use crate::platform::ApiGroups;
    use crate::storage::profile::sample_profile;

    const NOW: i64 = 1_793_016_000;

    fn info(caps: Capabilities) -> ConnectionInfo {
        ConnectionInfo {
            user: "u".into(),
            groups: vec![],
            server_version: "v1.37.0".into(),
            platform: crate::platform::detect(&Default::default()),
            api_groups: ApiGroups::default(),
            capabilities: caps,
            token_expires_at: None,
        }
    }

    fn list(kind: &str, items: Vec<Value>) -> (u16, Vec<u8>) {
        let v = json!({"apiVersion": "v1", "kind": kind, "metadata": {}, "items": items});
        (200, serde_json::to_vec(&v).unwrap())
    }

    fn forbidden() -> (u16, Vec<u8>) {
        let (code, v) = crate::k8s::testing::status(403, "Forbidden", "forbidden");
        (code, serde_json::to_vec(&v).unwrap())
    }

    /// Мок кластера: проект team-a с подом в цикле перезапусков, Deployment, Service без готовых
    /// endpoints и Ingress; Secret в team-a не читаются (403).
    fn cluster(s: &Seen) -> (u16, Vec<u8>) {
        let path = s.path();
        match path {
            "/readyz" => (
                200,
                b"[+]ping ok\n[-]etcd failed: reason withheld\n".to_vec(),
            ),
            "/api/v1/namespaces" => list(
                "NamespaceList",
                ["team-b", "team-a"]
                    .map(|n| json!({"metadata": {"name": n}, "status": {"phase": "Active"}}))
                    .to_vec(),
            ),
            "/apis/networking.k8s.io/v1/ingressclasses" => list(
                "IngressClassList",
                vec![json!({"metadata": {"name": "nginx",
                    "annotations": {"ingressclass.kubernetes.io/is-default-class": "true"}},
                    "spec": {"controller": "k8s.io/ingress-nginx"}})],
            ),
            "/api/v1/namespaces/team-a/pods" => {
                list("PodList", vec![crash_looping("web-1"), running("web-2")])
            }
            "/apis/apps/v1/namespaces/team-a/deployments" => list(
                "DeploymentList",
                vec![
                    json!({"metadata": {"name": "web", "creationTimestamp": "2026-10-25T10:00:00Z"},
                    "spec": {"replicas": 2, "selector": {"matchLabels": {"app": "web"}},
                        "template": {"metadata": {"labels": {"app": "web"}},
                            "spec": {"containers": [{"name": "web", "image": "web:1",
                                "readinessProbe": {"httpGet": {"path": "/", "port": 80}}}]}}},
                    "status": {"availableReplicas": 1, "conditions": [{"type": "Progressing",
                        "status": "False", "reason": "ProgressDeadlineExceeded",
                        "lastTransitionTime": "2026-10-26T11:00:00Z"}]}}),
                ],
            ),
            "/api/v1/namespaces/team-a/services" => list(
                "ServiceList",
                vec![
                    json!({"metadata": {"name": "web"}, "spec": {"selector": {"app": "web"},
                    "ports": [{"name": "http", "port": 80}]}}),
                ],
            ),
            "/apis/networking.k8s.io/v1/namespaces/team-a/ingresses" => list(
                "IngressList",
                vec![
                    json!({"metadata": {"name": "web", "creationTimestamp": "2026-10-25T10:00:00Z"},
                    "spec": {"tls": [{"secretName": "web-tls"}], "rules": [{"host": "web.test",
                        "http": {"paths": [{"path": "/", "pathType": "Prefix",
                            "backend": {"service": {"name": "web", "port": {"name": "http"}}}}]}}]}}),
                ],
            ),
            "/api/v1/namespaces/team-a/secrets" => forbidden(),
            p if p.starts_with("/api/v1/namespaces/team-b/") || p.starts_with("/apis/") => {
                list("List", vec![])
            }
            _ => list("List", vec![]),
        }
    }

    #[tokio::test]
    async fn snapshot_of_a_project_and_what_was_not_read() {
        let (client, _server) = mock_raw(cluster);
        let r = ReadClient::new(client, Duration::from_secs(5));
        let s = collect(
            &r,
            &info(Capabilities {
                list_projects: true,
                ..Default::default()
            }),
            &sample_profile("c1"),
            None,
            LIMITS,
            NOW,
            None,
        )
        .await;
        assert_eq!(
            s.readyz,
            Some(Readyz {
                ok: false,
                failed: vec!["etcd".into()]
            })
        );
        let names: Vec<&str> = s.projects.iter().map(|p| p.namespace.as_str()).collect();
        assert_eq!(names, ["team-a", "team-b"]);
        let a = &s.projects[0];
        let pods = a.pods.as_ref().unwrap();
        assert!(pods[0].containers[0].crash_loop, "{:?}", pods[0]);
        assert_eq!(pods[0].owner.as_ref().unwrap().name, "web");
        assert!(!pods[1].containers[0].crash_loop);
        let w = &a.workloads.as_ref().unwrap()[0];
        assert_eq!(
            (w.kind, w.desired, w.available),
            (WorkloadKind::Deployment, Some(2), Some(1))
        );
        assert_eq!(
            w.progressing.as_ref().unwrap().reason.as_deref(),
            Some("ProgressDeadlineExceeded")
        );
        assert!(w.containers[0].readiness_probe);
        let svc = &a.services.as_ref().unwrap()[0];
        assert_eq!((svc.has_selector, svc.ready_endpoints), (true, Some(0)));
        let ing = &a.ingresses.as_ref().unwrap()[0];
        assert_eq!(
            ing.backends,
            [("web".to_string(), Some("http".to_string()))]
        );
        assert_eq!(ing.tls_secrets, ["web-tls"]);
        assert_eq!(a.secrets, None, "403 — Secret не прочитаны");
        assert!(a.routes.is_none() && a.gateways.is_none(), "групп API нет");
        assert!(s.ingress_classes.as_ref().unwrap().has_default);
        let gaps: Vec<String> = s
            .coverage
            .gaps
            .iter()
            .map(|g| {
                format!(
                    "{} {} {}",
                    g.namespace.as_deref().unwrap_or("-"),
                    g.part,
                    g.code
                )
            })
            .collect();
        assert!(gaps.contains(&"- nodes FORBIDDEN".to_string()), "{gaps:?}");
        assert!(
            gaps.contains(&"- pods (all projects) FORBIDDEN".to_string()),
            "{gaps:?}"
        );
        assert!(
            gaps.contains(&"team-a secrets FORBIDDEN".to_string()),
            "{gaps:?}"
        );
        assert_eq!(
            (s.coverage.projects_total, s.coverage.projects_checked),
            (2, 2)
        );
        let findings: Vec<String> =
            crate::health::rules::evaluate(&s, crate::storage::profile::Environment::Prod)
                .iter()
                .map(|f| f.rule_id.clone())
                .collect();
        for rule in [
            "cluster.apiNotReady",
            "pod.crashLoop",
            "workload.rolloutStuck",
            "service.noEndpoints",
            "check.partial",
        ] {
            assert!(findings.iter().any(|f| f == rule), "{rule}: {findings:?}");
        }
    }

    #[tokio::test]
    async fn project_and_pod_limits() {
        let (client, server) = mock_raw(|s| match s.path() {
            p if p.ends_with("/pods") => list("PodList", vec![running("a"), running("b")]),
            _ => list("List", vec![]),
        });
        let r = ReadClient::new(client, Duration::from_secs(5));
        let requested = ["x", "y", "z", "x"].map(String::from);
        let limits = Limits {
            projects: 2,
            pods: 3,
            ..LIMITS
        };
        let s = collect(
            &r,
            &info(Capabilities::default()),
            &sample_profile("c1"),
            Some(&requested),
            limits,
            NOW,
            None,
        )
        .await;
        let names: Vec<&str> = s.projects.iter().map(|p| p.namespace.as_str()).collect();
        assert_eq!(
            names,
            ["x", "y"],
            "названные проекты по порядку, без повторов"
        );
        assert_eq!(
            (s.coverage.projects_total, s.coverage.projects_checked),
            (3, 2)
        );
        let pods: Vec<Option<usize>> = s
            .projects
            .iter()
            .map(|p| p.pods.as_ref().map(Vec::len))
            .collect();
        assert_eq!(
            pods,
            [Some(2), None],
            "список, не поместившийся в предел, не прочитан целиком — поды проекта `None`"
        );
        assert!(s.coverage.pod_limit && s.coverage.partial());
        drop(r);
        let seen = server.await.unwrap();
        assert!(
            !seen
                .iter()
                .any(|s| s.path().starts_with("/api/v1/namespaces/z/")),
            "проект сверх предела не читается"
        );
    }

    #[tokio::test]
    async fn deadline_stops_the_reads() {
        let (client, _server) = mock_raw(|_| list("List", vec![]));
        let r = ReadClient::new(client, Duration::from_secs(5));
        let limits = Limits {
            deadline: Duration::ZERO,
            ..LIMITS
        };
        let s = collect(
            &r,
            &info(Capabilities::default()),
            &sample_profile("c1"),
            Some(&["a".to_string()]),
            limits,
            NOW,
            None,
        )
        .await;
        assert!(s.coverage.timed_out);
        assert!(s.coverage.gaps.iter().any(|g| g.code == "TIMEOUT"));
        assert!(s.projects[0].pods.is_none());
    }

    fn not_found() -> (u16, Vec<u8>) {
        let (code, v) = crate::k8s::testing::status(404, "NotFound", "not found");
        (code, serde_json::to_vec(&v).unwrap())
    }

    fn gap_lines(s: &ClusterSnapshot) -> Vec<String> {
        s.coverage
            .gaps
            .iter()
            .map(|g| {
                format!(
                    "{} {} {}",
                    g.namespace.as_deref().unwrap_or("-"),
                    g.part,
                    g.code
                )
            })
            .collect()
    }

    /// Права на всё есть: разрывов не бывает, пока кластер не откажет.
    fn admin() -> Capabilities {
        Capabilities {
            list_projects: true,
            list_nodes: true,
            list_all_pods: true,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn usage_of_the_day_is_read_per_project_from_the_metrics_source() {
        let (client, _server) = mock_raw(|_| list("List", vec![]));
        let r = ReadClient::new(client, Duration::from_secs(5));
        let metrics = FakeSource::new(|_, q| {
            assert!(q.contains("[1d:5m]"), "окно правил — 24 ч: {q}");
            let v = if q.contains("container_cpu_usage_seconds_total") {
                0.4
            } else {
                1e8
            };
            Ok(vector(&[(
                &[("pod", "web-1"), ("container", "app")][..],
                v,
            )]))
        });
        let s = collect(
            &r,
            &info(admin()),
            &sample_profile("c1"),
            Some(&["a".to_string()]),
            LIMITS,
            NOW,
            Some(&metrics),
        )
        .await;
        let usage = s.projects[0].usage.as_ref().unwrap();
        let u = usage[&("web-1".to_string(), "app".to_string())];
        assert_eq!(u.cpu_p95, Some(0.4));
        assert_eq!(u.memory_max, Some(1e8));
        assert_eq!(u.oom_count, None, "OOM — правило pod.oomKilled");
        assert_eq!(
            metrics.queries().len(),
            3,
            "не больше трёх запросов на проект"
        );
        let failing =
            FakeSource::new(|_, _| Err(crate::metrics::prometheus::Failure::bad_response("x")));
        let s = collect(
            &r,
            &info(admin()),
            &sample_profile("c1"),
            Some(&["a".to_string()]),
            LIMITS,
            NOW,
            Some(&failing),
        )
        .await;
        assert!(s.projects[0].usage.is_none());
        assert!(
            gap_lines(&s).contains(&"a resources CONNECTION_FAILED".to_string()),
            "{:?}",
            gap_lines(&s)
        );
    }

    #[tokio::test]
    async fn metrics_not_found_is_a_gap_not_an_absent_kind() {
        // 404 источника (HTTP) или прокси API-сервера к сервису, которого нет: «вида нет» тут
        // не бывает, правила `resources.*` не проверены, и проверка частичная.
        let (client, _server) = mock_raw(|_| list("List", vec![]));
        let r = ReadClient::new(client, Duration::from_secs(5));
        let missing = FakeSource::new(|_, _| {
            Err(crate::metrics::prometheus::Failure::from_error(
                crate::k8s::errors::from_status(
                    &kube::core::Status::failure(
                        "services \"prometheus-k8s\" not found",
                        "NotFound",
                    )
                    .with_code(404),
                ),
            ))
        });
        let s = collect(
            &r,
            &info(admin()),
            &sample_profile("c1"),
            Some(&["a".to_string()]),
            LIMITS,
            NOW,
            Some(&missing),
        )
        .await;
        assert!(s.projects[0].usage.is_none());
        assert!(
            gap_lines(&s).contains(&"a resources NOT_FOUND".to_string()),
            "{:?}",
            gap_lines(&s)
        );
        assert!(s.coverage.partial());
    }

    #[tokio::test]
    async fn a_job_remembers_the_cronjob_it_came_from() {
        let job = |name: &str, owners: Value| {
            json!({"metadata": {"name": name, "ownerReferences": owners},
                "spec": {"template": {"spec": {"containers": []}}}})
        };
        let (client, _server) = mock_raw(move |s| match s.path() {
            p if p.ends_with("/jobs") => list(
                "JobList",
                vec![
                    job(
                        "report-29000",
                        json!([{"apiVersion": "batch/v1", "kind": "CronJob",
                            "name": "report", "uid": "u1", "controller": true}]),
                    ),
                    job("once", json!([])),
                ],
            ),
            _ => list("List", vec![]),
        });
        let r = ReadClient::new(client, Duration::from_secs(5));
        let s = collect(
            &r,
            &info(admin()),
            &sample_profile("c1"),
            Some(&["a".to_string()]),
            LIMITS,
            NOW,
            None,
        )
        .await;
        let cron: Vec<_> = s.projects[0]
            .workloads
            .as_ref()
            .unwrap()
            .iter()
            .map(|w| (w.name.as_str(), w.cron.as_deref()))
            .collect();
        assert_eq!(cron, [("report-29000", Some("report")), ("once", None)]);
    }

    #[tokio::test]
    async fn an_absent_kind_is_not_a_gap() {
        // Gateway API 1.0 без GRPCRoute v1 и кластер без IngressClass отвечают 404.
        let (client, _server) = mock_raw(|s| match s.path() {
            p if p.ends_with("/grpcroutes") || p.ends_with("/ingressclasses") => not_found(),
            _ => list("List", vec![]),
        });
        let r = ReadClient::new(client, Duration::from_secs(5));
        let mut i = info(admin());
        i.api_groups.gateway_api = true;
        let metrics = FakeSource::new(|_, _| Ok(vector(&[])));
        let s = collect(
            &r,
            &i,
            &sample_profile("c1"),
            Some(&["a".to_string()]),
            LIMITS,
            NOW,
            Some(&metrics),
        )
        .await;
        assert_eq!(gap_lines(&s), Vec::<String>::new());
        assert!(!s.coverage.partial());
        let a = &s.projects[0];
        assert_eq!(
            a.gateway_routes.as_deref().map(<[_]>::len),
            Some(0),
            "HTTPRoute прочитан, GRPCRoute — вида нет"
        );
        assert!(a.gateways.is_some());
        assert!(s.ingress_classes.is_none(), "нет вида — нет классов");
    }

    #[tokio::test]
    async fn readyz_and_projects_are_required_so_not_found_is_a_gap() {
        let (client, _server) = mock_raw(|s| match s.path() {
            "/readyz" | "/api/v1/namespaces" => not_found(),
            _ => list("List", vec![]),
        });
        let r = ReadClient::new(client, Duration::from_secs(5));
        let s = collect(
            &r,
            &info(admin()),
            &sample_profile("c1"),
            None,
            LIMITS,
            NOW,
            None,
        )
        .await;
        assert!(s.readyz.is_none());
        assert_eq!(
            gap_lines(&s),
            ["- projects NOT_FOUND", "- readyz NOT_FOUND"],
            "у /readyz и списка проектов вида, которого может не быть, нет"
        );
    }

    #[tokio::test]
    async fn readyz_goes_through_the_slot_and_the_deadline() {
        let (client, server) = mock_raw(cluster);
        let r = ReadClient::new(client, Duration::from_secs(5));
        // Свободных слотов нет: ни один запрос, и `/readyz` тоже, не уходит до дедлайна.
        let rd = Reader {
            r: &r,
            slots: Semaphore::new(0),
            deadline: Instant::now() + Duration::from_millis(100),
            gaps: Mutex::new(Vec::new()),
            timed_out: Mutex::new(false),
        };
        let mut s = ClusterSnapshot::empty(NOW, PlatformKind::Kubernetes);
        super::cluster(&rd, &info(Capabilities::default()), &mut s).await;
        assert!(s.readyz.is_none());
        let gaps = std::mem::take(&mut *rd.gaps.lock().unwrap());
        assert!(
            gaps.iter()
                .any(|g| g.part == "readyz" && g.code == "TIMEOUT"),
            "{gaps:?}"
        );
        assert!(*rd.timed_out.lock().unwrap());
        drop(rd);
        drop(r);
        let seen = server.await.unwrap();
        assert!(
            !seen.iter().any(|s| s.path() == "/readyz"),
            "запрос ушёл мимо слота: {seen:?}"
        );
    }

    /// Ingress `web` с классом, которого в кластере нет; Routes проекта отвечают по `routes`.
    fn ingress_with_routes(routes: fn() -> (u16, Vec<u8>)) -> impl Fn(&Seen) -> (u16, Vec<u8>) {
        move |s| match s.path() {
            "/apis/networking.k8s.io/v1/ingressclasses" => list(
                "IngressClassList",
                vec![json!({"metadata": {"name": "nginx"},
                    "spec": {"controller": "k8s.io/ingress-nginx"}})],
            ),
            "/apis/networking.k8s.io/v1/namespaces/a/ingresses" => list(
                "IngressList",
                vec![json!({"metadata": {"name": "web"},
                    "spec": {"ingressClassName": "traefik",
                        "defaultBackend": {"service": {"name": "web", "port": {"number": 80}}}}})],
            ),
            "/apis/route.openshift.io/v1/namespaces/a/routes" => routes(),
            _ => list("List", vec![]),
        }
    }

    async fn class_missing(routes: fn() -> (u16, Vec<u8>)) -> (Vec<String>, ClusterSnapshot) {
        let (client, _server) = mock_raw(ingress_with_routes(routes));
        let r = ReadClient::new(client, Duration::from_secs(5));
        let mut i = info(admin());
        i.api_groups.routes = true;
        let s = collect(
            &r,
            &i,
            &sample_profile("c1"),
            Some(&["a".to_string()]),
            LIMITS,
            NOW,
            None,
        )
        .await;
        let findings =
            crate::health::rules::evaluate(&s, crate::storage::profile::Environment::Prod)
                .into_iter()
                .filter(|f| f.rule_id == "ingress.classMissing")
                .map(|f| f.target.name.clone().unwrap_or_default())
                .collect();
        (findings, s)
    }

    #[tokio::test]
    async fn unread_routes_do_not_flag_the_ingress_class() {
        let (found, s) = class_missing(forbidden).await;
        assert!(s.projects[0].routes.is_none());
        assert!(gap_lines(&s).contains(&"a routes FORBIDDEN".to_string()));
        assert_eq!(
            found,
            Vec::<String>::new(),
            "Routes не прочитаны — Ingress может обслуживаться ими"
        );
    }

    #[tokio::test]
    async fn read_routes_without_the_ingress_still_flag_the_class() {
        let (found, s) = class_missing(|| list("RouteList", vec![])).await;
        assert!(s.projects[0].routes.is_some());
        assert_eq!(found, ["web"]);
    }

    #[tokio::test]
    async fn gaps_come_out_in_a_stable_order() {
        let (client, _server) = mock_raw(|s| match s.path() {
            p if p.ends_with("/secrets") || p == "/api/v1/namespaces/a/pods" => forbidden(),
            _ => list("List", vec![]),
        });
        let r = ReadClient::new(client, Duration::from_secs(5));
        let requested = ["b", "a"].map(String::from);
        let s = collect(
            &r,
            &info(Capabilities::default()),
            &sample_profile("c1"),
            Some(&requested),
            LIMITS,
            NOW,
            None,
        )
        .await;
        assert_eq!(
            gap_lines(&s),
            [
                "- nodes FORBIDDEN",
                "- pods (all projects) FORBIDDEN",
                "- resources METRICS_UNAVAILABLE",
                "a pods FORBIDDEN",
                "a secrets FORBIDDEN",
                "b secrets FORBIDDEN",
            ],
            "сначала кластер, затем проект, затем часть"
        );
    }

    #[tokio::test]
    async fn a_cut_pod_list_does_not_invent_an_unavailable_workload() {
        let (client, _server) = mock_raw(|s| match s.path() {
            p if p.ends_with("/pods") => list("PodList", vec![running("web-1"), running("web-2")]),
            p if p.ends_with("/deployments") => list(
                "DeploymentList",
                vec![
                    json!({"metadata": {"name": "web", "creationTimestamp": "2026-10-25T10:00:00Z"},
                    "spec": {"replicas": 2, "selector": {"matchLabels": {"app": "web"}},
                        "template": {"spec": {"containers": [{"name": "web", "image": "web:1"}]}}},
                    "status": {"availableReplicas": 0}}),
                ],
            ),
            _ => list("List", vec![]),
        });
        let r = ReadClient::new(client, Duration::from_secs(5));
        let limits = Limits { pods: 1, ..LIMITS };
        let s = collect(
            &r,
            &info(admin()),
            &sample_profile("c1"),
            Some(&["x".to_string()]),
            limits,
            NOW,
            None,
        )
        .await;
        assert!(s.coverage.pod_limit && s.projects[0].pods.is_none());
        let findings: Vec<String> =
            crate::health::rules::evaluate(&s, crate::storage::profile::Environment::Prod)
                .iter()
                .map(|f| f.rule_id.clone())
                .collect();
        assert!(
            !findings.iter().any(|f| f == "workload.unavailable"),
            "{findings:?}"
        );
        assert!(
            findings.iter().any(|f| f == "check.partial"),
            "{findings:?}"
        );
    }

    #[test]
    fn rollout_conditions_are_only_for_deployments() {
        let facts = |kind: WorkloadKind| WorkloadFacts {
            row: crate::k8s::workloads::WorkloadRow {
                kind,
                name: "w".into(),
                desired: Some(1),
                ready: Some(1),
                available: Some(1),
                updated: None,
                images: vec![],
                created: None,
                problem: None,
                schedule: None,
                suspended: None,
                last_schedule: None,
            },
            conditions: ["Progressing", "Available"]
                .map(|k| ConditionView {
                    kind: k.into(),
                    status: "False".into(),
                    reason: None,
                    message: None,
                    last_transition: None,
                })
                .to_vec(),
            template: None,
            cron: None,
        };
        for kind in WorkloadKind::ALL {
            let w = workload_snap(facts(kind));
            let deploys = matches!(
                kind,
                WorkloadKind::Deployment | WorkloadKind::DeploymentConfig
            );
            assert_eq!(
                (w.progressing.is_some(), w.available_cond.is_some()),
                (deploys, deploys),
                "{kind:?}"
            );
        }
    }

    #[test]
    fn requests_are_summed_by_node_for_running_pods() {
        let pod = |name: &str, node: &str, phase: &str, cpu: &str, mem: &str| -> Pod {
            serde_json::from_value(json!({"metadata": {"name": name},
                "spec": {"nodeName": node, "containers": [{"name": "c",
                    "resources": {"requests": {"cpu": cpu, "memory": mem}}}],
                    "initContainers": [{"name": "proxy", "restartPolicy": "Always",
                        "resources": {"requests": {"cpu": "100m"}}},
                        {"name": "init", "resources": {"requests": {"cpu": "4"}}}]},
                "status": {"phase": phase}}))
            .unwrap()
        };
        let m = requests_by_node(&[
            pod("a", "w1", "Running", "250m", "128Mi"),
            pod("b", "w1", "Pending", "1", "1Gi"),
            pod("c", "w1", "Succeeded", "8", "8Gi"),
            pod("d", "w2", "Running", "500m", "0"),
        ]);
        assert_eq!(
            m["w1"],
            Requests {
                cpu_milli: 250 + 100 + 1000 + 100,
                memory: (128 << 20) + (1 << 30)
            }
        );
        assert_eq!(m["w2"].cpu_milli, 600);
    }
}
