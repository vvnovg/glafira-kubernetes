//! Сеть проекта (спец. §6.3): Services с готовыми endpoints по EndpointSlices, Routes (при
//! `route.openshift.io`), Ingress, а при Gateway API — Gateway, HTTPRoute и GRPCRoute. Каждый вид
//! — своей частью: без прав на один вид остальные показываются. Ключ TLS из Route не читается.

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::Service;
use k8s_openapi::api::discovery::v1::EndpointSlice;
use k8s_openapi::api::networking::v1::{Ingress, IngressBackend};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::api::{ApiResource, DynamicObject, GroupVersionKind, ListParams};
use serde::Serialize;

use crate::der::read_tlv;
use crate::error::{AppError, AppResult, ErrorCode};
use crate::k8s::read::ReadClient;
use crate::k8s::selectors::map_selector_string;
use crate::k8s::view::{ConditionView, Part, condition, condition_flag, conditions, created, name};
use crate::platform::ApiGroups;

pub fn routes() -> ApiResource {
    ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk("route.openshift.io", "v1", "Route"),
        "routes",
    )
}

fn gateway_api(kind: &str, plural: &str) -> ApiResource {
    ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk("gateway.networking.k8s.io", "v1", kind),
        plural,
    )
}

pub fn gateways() -> ApiResource {
    gateway_api("Gateway", "gateways")
}

pub fn http_routes() -> ApiResource {
    gateway_api("HTTPRoute", "httproutes")
}

pub fn grpc_routes() -> ApiResource {
    gateway_api("GRPCRoute", "grpcroutes")
}

/// Срок (`notAfter`, секунды Unix) первого сертификата в PEM — встроенного сертификата Route.
pub fn certificate_expiry(pem_text: &str) -> Option<i64> {
    let block = pem::parse_many(pem_text)
        .ok()?
        .into_iter()
        .find(|p| p.tag() == "CERTIFICATE")?;
    let (_, cert, _) = read_tlv(block.contents())?;
    let (_, tbs, _) = read_tlv(cert)?;
    let mut rest = tbs;
    // Версия — необязательное поле [0].
    if rest.first() == Some(&0xA0) {
        rest = read_tlv(rest)?.2;
    }
    // Серийный номер, алгоритм подписи, издатель.
    for _ in 0..3 {
        rest = read_tlv(rest)?.2;
    }
    let (_, validity, _) = read_tlv(rest)?;
    let (_, _, after) = read_tlv(validity)?;
    let (tag, not_after, _) = read_tlv(after)?;
    asn1_time(tag, not_after)
}

/// UTCTime (`YYMMDDHHMMSSZ`, годы 1950–2049) или GeneralizedTime (`YYYYMMDDHHMMSSZ`).
fn asn1_time(tag: u8, v: &[u8]) -> Option<i64> {
    let s = std::str::from_utf8(v).ok()?;
    let (year, rest) = match tag {
        0x17 => {
            let yy: i32 = s.get(0..2)?.parse().ok()?;
            (if yy >= 50 { 1900 + yy } else { 2000 + yy }, s.get(2..)?)
        }
        0x18 => (s.get(0..4)?.parse().ok()?, s.get(4..)?),
        _ => return None,
    };
    let num = |from: usize| rest.get(from..from + 2)?.parse::<u8>().ok();
    let date =
        time::Date::from_calendar_date(year, time::Month::try_from(num(0)?).ok()?, num(2)?).ok()?;
    let t = time::Time::from_hms(num(4)?, num(6)?, num(8)?).ok()?;
    Some(
        time::PrimitiveDateTime::new(date, t)
            .assume_utc()
            .unix_timestamp(),
    )
}

fn port_text(p: &IntOrString) -> String {
    match p {
        IntOrString::Int(i) => i.to_string(),
        IntOrString::String(s) => s.clone(),
    }
}

/// Готовые endpoints и все.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EndpointCount {
    pub ready: i32,
    pub total: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceRow {
    pub name: String,
    /// `ClusterIP`, `NodePort`, `LoadBalancer`, `ExternalName`.
    #[serde(rename = "type")]
    pub kind: String,
    pub cluster_ip: Option<String>,
    /// Внешние адреса: балансировщик, `externalIPs`, `externalName`.
    pub external: Vec<String>,
    /// `80/TCP → 8080`, `80:30080/TCP → http`.
    pub ports: Vec<String>,
    pub selector: Option<String>,
    /// `None` — у сервиса нет селектора или EndpointSlices не читаются.
    pub endpoints: Option<EndpointCount>,
    pub created: Option<i64>,
}

pub fn service_row(s: &Service, endpoints: Option<EndpointCount>) -> ServiceRow {
    let spec = s.spec.as_ref();
    let mut external: Vec<String> = s
        .status
        .as_ref()
        .and_then(|st| st.load_balancer.as_ref())
        .and_then(|lb| lb.ingress.as_ref())
        .map(|ing| {
            ing.iter()
                .filter_map(|i| i.ip.clone().or_else(|| i.hostname.clone()))
                .collect()
        })
        .unwrap_or_default();
    external.extend(
        spec.and_then(|s| s.external_ips.clone())
            .unwrap_or_default(),
    );
    external.extend(spec.and_then(|s| s.external_name.clone()));
    let selector = spec
        .and_then(|s| s.selector.as_ref())
        .filter(|m| !m.is_empty())
        .map(map_selector_string);
    ServiceRow {
        name: name(&s.metadata),
        kind: spec
            .and_then(|s| s.type_.clone())
            .unwrap_or_else(|| "ClusterIP".into()),
        cluster_ip: spec.and_then(|s| s.cluster_ip.clone()),
        external,
        ports: spec
            .and_then(|s| s.ports.as_ref())
            .map(|ps| {
                ps.iter()
                    .map(|p| {
                        let proto = p.protocol.as_deref().unwrap_or("TCP");
                        let port = match p.node_port {
                            Some(n) => format!("{}:{n}/{proto}", p.port),
                            None => format!("{}/{proto}", p.port),
                        };
                        match &p.target_port {
                            Some(t) => format!("{port} → {}", port_text(t)),
                            None => port,
                        }
                    })
                    .collect()
            })
            .unwrap_or_default(),
        endpoints: endpoints.filter(|_| selector.is_some()),
        selector,
        created: created(&s.metadata),
    }
}

/// Endpoints по сервисам: метка `kubernetes.io/service-name` у EndpointSlice. Endpoint без
/// условия `ready` считается готовым (так читает его kube-proxy).
pub fn endpoints_by_service(slices: &[EndpointSlice]) -> BTreeMap<String, EndpointCount> {
    let mut out: BTreeMap<String, EndpointCount> = BTreeMap::new();
    for s in slices {
        let Some(service) = s
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get("kubernetes.io/service-name"))
        else {
            continue;
        };
        let e = out
            .entry(service.clone())
            .or_insert(EndpointCount { ready: 0, total: 0 });
        for ep in &s.endpoints {
            e.total += 1;
            if ep.conditions.as_ref().and_then(|c| c.ready) != Some(false) {
                e.ready += 1;
            }
        }
    }
    out
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RouteRow {
    pub name: String,
    pub host: Option<String>,
    pub path: Option<String>,
    pub service: Option<String>,
    pub port: Option<String>,
    /// TLS-терминация: `edge`, `passthrough`, `reencrypt`.
    pub tls: Option<String>,
    pub insecure_policy: Option<String>,
    /// Принят ли роутером; `None` — статуса роутера нет.
    pub admitted: Option<bool>,
    /// Почему не принят.
    pub admitted_message: Option<String>,
    /// Срок встроенного сертификата `spec.tls.certificate`.
    pub cert_expires: Option<i64>,
    /// Ingress, из которого OpenShift создал Route.
    pub ingress: Option<String>,
    pub created: Option<i64>,
}

pub fn route_row(o: &DynamicObject) -> RouteRow {
    let spec = &o.data["spec"];
    let text = |v: &serde_json::Value| v.as_str().filter(|s| !s.is_empty()).map(String::from);
    let ingresses = o.data["status"]["ingress"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let admitted: Vec<ConditionView> = ingresses
        .iter()
        .filter_map(|i| {
            conditions(&i["conditions"])
                .into_iter()
                .find(|c| c.kind == "Admitted")
        })
        .collect();
    let accepted = admitted.iter().any(ConditionView::is_true);
    RouteRow {
        name: o.metadata.name.clone().unwrap_or_default(),
        host: text(&spec["host"]),
        path: text(&spec["path"]),
        service: text(&spec["to"]["name"]),
        port: match &spec["port"]["targetPort"] {
            serde_json::Value::Number(n) => Some(n.to_string()),
            v => text(v),
        },
        tls: text(&spec["tls"]["termination"]),
        insecure_policy: text(&spec["tls"]["insecureEdgeTerminationPolicy"]),
        admitted: (!admitted.is_empty()).then_some(accepted),
        admitted_message: if accepted {
            None
        } else {
            admitted.iter().find_map(|c| c.message.clone())
        },
        cert_expires: spec["tls"]["certificate"]
            .as_str()
            .and_then(certificate_expiry),
        ingress: o
            .metadata
            .owner_references
            .iter()
            .flatten()
            .find(|r| r.kind == "Ingress")
            .map(|r| r.name.clone()),
        created: created(&o.metadata),
    }
}

/// Правило Ingress: хост и путь → Service и порт.
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
pub struct IngressRow {
    pub name: String,
    /// `spec.ingressClassName` или аннотация `kubernetes.io/ingress.class`.
    pub class: Option<String>,
    pub rules: Vec<IngressPath>,
    pub default_backend: Option<String>,
    pub tls: Vec<IngressTls>,
    /// Адрес из `status.loadBalancer` (заполняют не все контроллеры).
    pub addresses: Vec<String>,
    /// Routes, которые OpenShift создал из этого Ingress.
    pub routes: Vec<String>,
    pub created: Option<i64>,
}

fn backend(b: &IngressBackend) -> (Option<String>, Option<String>) {
    match &b.service {
        Some(s) => (
            Some(s.name.clone()),
            s.port
                .as_ref()
                .and_then(|p| p.number.map(|n| n.to_string()).or_else(|| p.name.clone())),
        ),
        None => (
            b.resource
                .as_ref()
                .map(|r| format!("{}/{}", r.kind, r.name)),
            None,
        ),
    }
}

pub fn ingress_row(i: &Ingress, routes: &[RouteRow]) -> IngressRow {
    let spec = i.spec.as_ref();
    let name = name(&i.metadata);
    let rules = spec
        .and_then(|s| s.rules.as_ref())
        .map(|rules| {
            rules
                .iter()
                .flat_map(|r| {
                    let host = r.host.clone();
                    r.http
                        .as_ref()
                        .map(|h| h.paths.clone())
                        .unwrap_or_default()
                        .into_iter()
                        .map(move |p| {
                            let (service, port) = backend(&p.backend);
                            IngressPath {
                                host: host.clone(),
                                path: p.path.clone(),
                                service,
                                port,
                            }
                        })
                })
                .collect()
        })
        .unwrap_or_default();
    IngressRow {
        class: spec.and_then(|s| s.ingress_class_name.clone()).or_else(|| {
            i.metadata
                .annotations
                .as_ref()
                .and_then(|a| a.get("kubernetes.io/ingress.class"))
                .cloned()
        }),
        rules,
        default_backend: spec
            .and_then(|s| s.default_backend.as_ref())
            .map(backend)
            .and_then(|(s, p)| s.map(|s| p.map_or(s.clone(), |p| format!("{s}:{p}")))),
        tls: spec
            .and_then(|s| s.tls.as_ref())
            .map(|ts| {
                ts.iter()
                    .map(|t| IngressTls {
                        hosts: t.hosts.clone().unwrap_or_default(),
                        secret: t.secret_name.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default(),
        addresses: i
            .status
            .as_ref()
            .and_then(|s| s.load_balancer.as_ref())
            .and_then(|lb| lb.ingress.as_ref())
            .map(|ing| {
                ing.iter()
                    .filter_map(|x| x.ip.clone().or_else(|| x.hostname.clone()))
                    .collect()
            })
            .unwrap_or_default(),
        routes: routes
            .iter()
            .filter(|r| r.ingress.as_deref() == Some(name.as_str()))
            .map(|r| r.name.clone())
            .collect(),
        created: created(&i.metadata),
        name,
    }
}

/// Listener Gateway.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ListenerView {
    pub name: String,
    pub port: Option<i64>,
    pub protocol: Option<String>,
    pub hostname: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GatewayRow {
    pub name: String,
    pub class: Option<String>,
    pub listeners: Vec<ListenerView>,
    pub addresses: Vec<String>,
    pub accepted: Option<bool>,
    pub programmed: Option<bool>,
    pub created: Option<i64>,
}

pub fn gateway_row(o: &DynamicObject) -> GatewayRow {
    let conds = conditions(&o.data["status"]["conditions"]);
    GatewayRow {
        name: o.metadata.name.clone().unwrap_or_default(),
        class: o.data["spec"]["gatewayClassName"]
            .as_str()
            .map(String::from),
        listeners: o.data["spec"]["listeners"]
            .as_array()
            .map(|ls| {
                ls.iter()
                    .map(|l| ListenerView {
                        name: l["name"].as_str().unwrap_or_default().to_string(),
                        port: l["port"].as_i64(),
                        protocol: l["protocol"].as_str().map(String::from),
                        hostname: l["hostname"].as_str().map(String::from),
                    })
                    .collect()
            })
            .unwrap_or_default(),
        addresses: o.data["status"]["addresses"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|x| x["value"].as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default(),
        accepted: condition_flag(&conds, "Accepted"),
        programmed: condition_flag(&conds, "Programmed"),
        created: created(&o.metadata),
    }
}

/// Родительский Gateway маршрута и его условия для этого маршрута.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ParentView {
    pub name: String,
    pub namespace: Option<String>,
    pub section: Option<String>,
    /// `None` — статуса от этого родителя ещё нет.
    pub accepted: Option<ConditionView>,
    pub resolved_refs: Option<ConditionView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GatewayRouteRow {
    /// `HTTPRoute` или `GRPCRoute`.
    pub kind: String,
    pub name: String,
    pub hostnames: Vec<String>,
    pub parents: Vec<ParentView>,
    /// `service:port`.
    pub backends: Vec<String>,
    pub created: Option<i64>,
}

pub fn gateway_route_row(kind: &str, o: &DynamicObject) -> GatewayRouteRow {
    let spec = &o.data["spec"];
    let statuses = o.data["status"]["parents"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let same = |a: &serde_json::Value, b: &serde_json::Value| {
        a["name"] == b["name"]
            && a["namespace"] == b["namespace"]
            && a["sectionName"] == b["sectionName"]
    };
    let parents = spec["parentRefs"]
        .as_array()
        .map(|refs| {
            refs.iter()
                .map(|p| {
                    let conds = statuses
                        .iter()
                        .find(|s| same(&s["parentRef"], p))
                        .map(|s| conditions(&s["conditions"]))
                        .unwrap_or_default();
                    ParentView {
                        name: p["name"].as_str().unwrap_or_default().to_string(),
                        namespace: p["namespace"].as_str().map(String::from),
                        section: p["sectionName"].as_str().map(String::from),
                        accepted: condition(&conds, "Accepted").cloned(),
                        resolved_refs: condition(&conds, "ResolvedRefs").cloned(),
                    }
                })
                .collect()
        })
        .unwrap_or_default();
    let backends = spec["rules"]
        .as_array()
        .map(|rules| {
            rules
                .iter()
                .flat_map(|r| r["backendRefs"].as_array().cloned().unwrap_or_default())
                .map(|b| match b["port"].as_i64() {
                    Some(p) => format!("{}:{p}", b["name"].as_str().unwrap_or_default()),
                    None => b["name"].as_str().unwrap_or_default().to_string(),
                })
                .collect()
        })
        .unwrap_or_default();
    GatewayRouteRow {
        kind: kind.into(),
        name: o.metadata.name.clone().unwrap_or_default(),
        hostnames: spec["hostnames"]
            .as_array()
            .map(|h| {
                h.iter()
                    .filter_map(|x| x.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default(),
        parents,
        backends,
        created: created(&o.metadata),
    }
}

/// Сеть проекта. Вид без группы API в кластере — `None`; GRPCRoute появился в Gateway API 1.1,
/// и у более старой установки его нет (404 — тоже `None`).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NetworkView {
    pub services: Part<Vec<ServiceRow>>,
    /// Почему не видно endpoints сервисов (нет прав на EndpointSlices).
    pub endpoints_error: Option<AppError>,
    pub routes: Option<Part<Vec<RouteRow>>>,
    pub ingresses: Part<Vec<IngressRow>>,
    pub gateways: Option<Part<Vec<GatewayRow>>>,
    pub http_routes: Option<Part<Vec<GatewayRouteRow>>>,
    pub grpc_routes: Option<Part<Vec<GatewayRouteRow>>>,
}

pub async fn get_network(r: &ReadClient, namespace: &str, groups: &ApiGroups) -> NetworkView {
    let ns = Some(namespace);
    let lp = ListParams::default();
    let dynamic = |ar: ApiResource| {
        let lp = lp.clone();
        async move { r.list_dynamic(&ar, ns, &lp).await.map(|l| l.items) }
    };
    let routes = async {
        match groups.routes {
            true => Some(
                dynamic(routes())
                    .await
                    .map(|items| items.iter().map(route_row).collect::<Vec<_>>()),
            ),
            false => None,
        }
    };
    let gateway_api = |ar: ApiResource| async move {
        match groups.gateway_api {
            true => Some(dynamic(ar).await),
            false => None,
        }
    };
    let (services, slices, routes, ingresses, gateways, http, grpc) = tokio::join!(
        r.list::<Service>(ns, &lp),
        r.list::<EndpointSlice>(ns, &lp),
        routes,
        r.list::<Ingress>(ns, &lp),
        gateway_api(gateways()),
        gateway_api(http_routes()),
        gateway_api(grpc_routes()),
    );
    let (endpoints, endpoints_error) = match slices {
        Ok(l) => (Some(endpoints_by_service(&l.items)), None),
        Err(e) => (None, Some(e)),
    };
    let services: AppResult<Vec<ServiceRow>> = services.map(|l| {
        l.items
            .iter()
            .map(|s| {
                let count = endpoints.as_ref().map(|m| {
                    m.get(&name(&s.metadata))
                        .copied()
                        .unwrap_or(EndpointCount { ready: 0, total: 0 })
                });
                service_row(s, count)
            })
            .collect()
    });
    let route_rows = routes
        .as_ref()
        .and_then(|r| r.as_ref().ok())
        .cloned()
        .unwrap_or_default();
    let gateway_routes = |kind: &str, r: Option<AppResult<Vec<DynamicObject>>>| match r {
        None => None,
        Some(Err(e)) if e.code == ErrorCode::NotFound => None,
        Some(r) => Some(
            r.map(|items| items.iter().map(|o| gateway_route_row(kind, o)).collect())
                .into(),
        ),
    };
    NetworkView {
        services: services.into(),
        endpoints_error,
        routes: routes.map(Part::from),
        ingresses: ingresses
            .map(|l| {
                l.items
                    .iter()
                    .map(|i| ingress_row(i, &route_rows))
                    .collect()
            })
            .into(),
        gateways: gateways.map(|r| {
            r.map(|items| items.iter().map(gateway_row).collect())
                .into()
        }),
        http_routes: gateway_routes("HTTPRoute", http),
        grpc_routes: gateway_routes("GRPCRoute", grpc),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::k8s::testing::{Seen, mock_client, status};
    use std::time::Duration;

    const ROUTE_CERT: &str = include_str!("testdata/route-cert.pem");

    fn from<T: serde::de::DeserializeOwned>(v: serde_json::Value) -> T {
        serde_json::from_value(v).unwrap()
    }

    fn list(items: Vec<serde_json::Value>) -> (u16, serde_json::Value) {
        (
            200,
            serde_json::json!({"apiVersion": "v1", "kind": "List", "metadata": {}, "items": items}),
        )
    }

    #[test]
    fn certificate_expiry_in_utc_and_generalized_time() {
        assert_eq!(
            certificate_expiry(ROUTE_CERT),
            Some(1_795_537_583),
            "UTCTime"
        );
        assert_eq!(
            certificate_expiry(crate::storage::profile::TEST_CERT_PEM),
            Some(4_946_471_467),
            "GeneralizedTime после 2049 года"
        );
        assert_eq!(certificate_expiry("not a pem"), None);
    }

    #[test]
    fn services_with_ports_external_addresses_and_endpoints() {
        let svc: Service = from(serde_json::json!({"metadata": {"name": "web"},
            "spec": {"type": "LoadBalancer", "clusterIP": "10.0.0.10", "selector": {"app": "web"},
                "externalIPs": ["192.0.2.1"],
                "ports": [{"port": 80, "targetPort": 8080, "protocol": "TCP", "nodePort": 30080},
                    {"port": 443, "targetPort": "https"}]},
            "status": {"loadBalancer": {"ingress": [{"hostname": "lb.example.com"}]}}}));
        let slices: Vec<EndpointSlice> = vec![
            from(
                serde_json::json!({"metadata": {"name": "web-1", "labels": {"kubernetes.io/service-name": "web"}},
                "addressType": "IPv4", "endpoints": [
                    {"addresses": ["10.1.0.1"], "conditions": {"ready": true}},
                    {"addresses": ["10.1.0.2"], "conditions": {"ready": false}},
                    {"addresses": ["10.1.0.3"]}]}),
            ),
            from(
                serde_json::json!({"metadata": {"name": "x"}, "addressType": "IPv4", "endpoints": []}),
            ),
        ];
        let counts = endpoints_by_service(&slices);
        assert_eq!(counts["web"], EndpointCount { ready: 2, total: 3 });
        let row = service_row(&svc, counts.get("web").copied());
        assert_eq!(row.kind, "LoadBalancer");
        assert_eq!(row.ports, ["80:30080/TCP → 8080", "443/TCP → https"]);
        assert_eq!(row.external, ["lb.example.com", "192.0.2.1"]);
        assert_eq!(row.selector.as_deref(), Some("app=web"));
        assert_eq!(row.endpoints, Some(EndpointCount { ready: 2, total: 3 }));
        let manual: Service = from(serde_json::json!({"metadata": {"name": "db"},
            "spec": {"type": "ExternalName", "externalName": "db.example.com"}}));
        let row = service_row(&manual, Some(EndpointCount { ready: 0, total: 0 }));
        assert_eq!(row.endpoints, None, "без селектора endpoints не считаются");
        assert_eq!(row.external, ["db.example.com"]);
    }

    fn route(name: &str, admitted: Option<&str>, owner: Option<&str>) -> serde_json::Value {
        let mut r = serde_json::json!({"apiVersion": "route.openshift.io/v1", "kind": "Route",
            "metadata": {"name": name},
            "spec": {"host": format!("{name}.apps.example.com"), "path": "/api",
                "to": {"kind": "Service", "name": "web"}, "port": {"targetPort": 8080},
                "tls": {"termination": "edge", "insecureEdgeTerminationPolicy": "Redirect",
                    "certificate": ROUTE_CERT, "key": "-----BEGIN PRIVATE KEY-----"}}});
        if let Some(a) = admitted {
            r["status"] = serde_json::json!({"ingress": [{"routerName": "default",
                "conditions": [{"type": "Admitted", "status": a, "message": "host already claimed"}]}]});
        }
        if let Some(o) = owner {
            r["metadata"]["ownerReferences"] = serde_json::json!([{"apiVersion": "networking.k8s.io/v1",
                "kind": "Ingress", "name": o, "uid": "i"}]);
        }
        r
    }

    #[test]
    fn routes_admission_certificate_and_owner() {
        let ok = route_row(&from(route("web", Some("True"), None)));
        assert_eq!(ok.host.as_deref(), Some("web.apps.example.com"));
        assert_eq!(
            (ok.service.as_deref(), ok.port.as_deref()),
            (Some("web"), Some("8080"))
        );
        assert_eq!(ok.tls.as_deref(), Some("edge"));
        assert_eq!(
            (ok.admitted, ok.admitted_message.as_deref()),
            (Some(true), None)
        );
        assert_eq!(ok.cert_expires, Some(1_795_537_583));
        let json = serde_json::to_string(&ok).unwrap();
        assert!(
            !json.contains("PRIVATE KEY"),
            "ключ Route не уходит: {json}"
        );
        let rejected = route_row(&from(route("dup", Some("False"), Some("web"))));
        assert_eq!(rejected.admitted, Some(false));
        assert_eq!(
            rejected.admitted_message.as_deref(),
            Some("host already claimed")
        );
        assert_eq!(rejected.ingress.as_deref(), Some("web"));
        assert_eq!(route_row(&from(route("new", None, None))).admitted, None);
    }

    #[test]
    fn ingress_rules_tls_class_and_routes() {
        let ing: Ingress = from(serde_json::json!({"metadata": {"name": "web",
                "annotations": {"kubernetes.io/ingress.class": "nginx"}},
            "spec": {"rules": [{"host": "web.example.com", "http": {"paths": [
                    {"path": "/", "pathType": "Prefix", "backend": {"service": {"name": "web", "port": {"number": 80}}}},
                    {"path": "/api", "pathType": "Prefix", "backend": {"service": {"name": "api", "port": {"name": "http"}}}}
                ]}}],
                "defaultBackend": {"service": {"name": "fallback", "port": {"number": 8080}}},
                "tls": [{"hosts": ["web.example.com"], "secretName": "web-tls"}]},
            "status": {"loadBalancer": {"ingress": [{"ip": "203.0.113.5"}]}}}));
        let routes = vec![
            route_row(&from(route("web-abcde", Some("True"), Some("web")))),
            route_row(&from(route("other", Some("True"), None))),
        ];
        let row = ingress_row(&ing, &routes);
        assert_eq!(row.class.as_deref(), Some("nginx"));
        assert_eq!(
            row.rules[1],
            IngressPath {
                host: Some("web.example.com".into()),
                path: Some("/api".into()),
                service: Some("api".into()),
                port: Some("http".into())
            }
        );
        assert_eq!(row.default_backend.as_deref(), Some("fallback:8080"));
        assert_eq!(row.tls[0].secret.as_deref(), Some("web-tls"));
        assert_eq!(row.addresses, ["203.0.113.5"]);
        assert_eq!(row.routes, ["web-abcde"]);
    }

    fn gateway_objects(s: &Seen) -> Option<(u16, serde_json::Value)> {
        Some(match s.path() {
            "/apis/gateway.networking.k8s.io/v1/namespaces/team-a/gateways" => {
                list(vec![serde_json::json!({
                "metadata": {"name": "gw"},
                "spec": {"gatewayClassName": "cilium", "listeners": [
                    {"name": "https", "port": 443, "protocol": "HTTPS", "hostname": "*.example.com"}]},
                "status": {"addresses": [{"type": "IPAddress", "value": "198.51.100.7"}],
                    "conditions": [{"type": "Accepted", "status": "True"},
                        {"type": "Programmed", "status": "False", "reason": "AddressNotAssigned"}]}})])
            }
            "/apis/gateway.networking.k8s.io/v1/namespaces/team-a/httproutes" => {
                list(vec![serde_json::json!({
                "metadata": {"name": "web"},
                "spec": {"hostnames": ["web.example.com"],
                    "parentRefs": [{"name": "gw", "sectionName": "https"}, {"name": "other"}],
                    "rules": [{"backendRefs": [{"name": "web", "port": 80}, {"name": "canary", "port": 80}]}]},
                "status": {"parents": [{"parentRef": {"name": "gw", "sectionName": "https"},
                    "controllerName": "io.cilium/gateway-controller",
                    "conditions": [{"type": "Accepted", "status": "True"},
                        {"type": "ResolvedRefs", "status": "False", "reason": "BackendNotFound"}]}]}})])
            }
            "/apis/gateway.networking.k8s.io/v1/namespaces/team-a/grpcroutes" => status(
                404,
                "NotFound",
                "the server could not find the requested resource",
            ),
            _ => return None,
        })
    }

    #[tokio::test]
    async fn network_of_a_kubernetes_project_with_gateway_api() {
        let (client, server) = mock_client(|s| {
            gateway_objects(s).unwrap_or_else(|| match s.path() {
                "/api/v1/namespaces/team-a/services" => list(vec![serde_json::json!({
                    "metadata": {"name": "web"}, "spec": {"selector": {"app": "web"}, "ports": [{"port": 80}]}})]),
                "/apis/discovery.k8s.io/v1/namespaces/team-a/endpointslices" => status(
                    403,
                    "Forbidden",
                    r#"endpointslices.discovery.k8s.io is forbidden: User "u" cannot list resource "endpointslices" in API group "discovery.k8s.io" in the namespace "team-a""#,
                ),
                "/apis/networking.k8s.io/v1/namespaces/team-a/ingresses" => list(vec![]),
                other => panic!("лишний запрос {other}"),
            })
        });
        let r = ReadClient::new(client, Duration::from_secs(5));
        let groups = ApiGroups {
            gateway_api: true,
            ..Default::default()
        };
        let n = get_network(&r, "team-a", &groups).await;
        let services = n.services.data.unwrap();
        assert_eq!(services[0].endpoints, None, "EndpointSlices не читаются");
        assert_eq!(
            n.endpoints_error.unwrap().params["resource"],
            "endpointslices"
        );
        assert!(
            n.routes.is_none(),
            "нет route.openshift.io — Routes не спрашиваем"
        );
        assert_eq!(n.ingresses.data.unwrap().len(), 0);
        let gw = &n.gateways.unwrap().data.unwrap()[0];
        assert_eq!((gw.accepted, gw.programmed), (Some(true), Some(false)));
        assert_eq!(gw.listeners[0].port, Some(443));
        assert_eq!(gw.addresses, ["198.51.100.7"]);
        let http = &n.http_routes.unwrap().data.unwrap()[0];
        assert_eq!(http.backends, ["web:80", "canary:80"]);
        assert_eq!(http.parents.len(), 2);
        assert!(http.parents[0].accepted.as_ref().unwrap().is_true());
        assert_eq!(
            http.parents[0]
                .resolved_refs
                .as_ref()
                .unwrap()
                .reason
                .as_deref(),
            Some("BackendNotFound")
        );
        assert!(
            http.parents[1].accepted.is_none(),
            "статуса от родителя нет"
        );
        assert!(
            n.grpc_routes.is_none(),
            "GRPCRoute нет в этой версии Gateway API"
        );
        drop(r);
        assert_eq!(server.await.unwrap().len(), 6);
    }

    #[tokio::test]
    async fn network_of_openshift_without_gateway_api() {
        let (client, server) = mock_client(|s| match s.path() {
            "/apis/route.openshift.io/v1/namespaces/team-a/routes" => {
                list(vec![route("web-abcde", Some("True"), Some("web"))])
            }
            "/apis/networking.k8s.io/v1/namespaces/team-a/ingresses" => {
                list(vec![serde_json::json!({
                "metadata": {"name": "web"}, "spec": {"ingressClassName": "openshift-default"}})])
            }
            p if p.contains("gateway.networking.k8s.io") => panic!("Gateway API не спрашиваем"),
            _ => list(vec![]),
        });
        let r = ReadClient::new(client, Duration::from_secs(5));
        let groups = ApiGroups {
            routes: true,
            ..Default::default()
        };
        let n = get_network(&r, "team-a", &groups).await;
        assert_eq!(n.routes.unwrap().data.unwrap().len(), 1);
        assert_eq!(n.ingresses.data.unwrap()[0].routes, ["web-abcde"]);
        assert!(n.gateways.is_none() && n.http_routes.is_none() && n.grpc_routes.is_none());
        assert!(n.endpoints_error.is_none());
        drop(r);
        assert_eq!(server.await.unwrap().len(), 4);
    }
}
