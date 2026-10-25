//! Сеть проекта (спец. §6.3): Services с готовыми endpoints по EndpointSlices, Routes (при
//! `route.openshift.io`), Ingress, а при Gateway API — Gateway, HTTPRoute и GRPCRoute. Каждый вид
//! — своей частью: без прав на один вид остальные показываются. Ключ TLS из Route не читается.

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::Service;
use k8s_openapi::api::discovery::v1::EndpointSlice;
use k8s_openapi::api::networking::v1::{Ingress, IngressBackend};
use kube::api::{ApiResource, DynamicObject, GroupVersionKind, ListParams};
use serde::Serialize;

use crate::der::read_tlv;
use crate::error::{AppError, AppResult, ErrorCode};
use crate::k8s::read::ReadClient;
use crate::k8s::selectors::map_selector_string;
use crate::k8s::view::{
    ConditionView, Part, condition, condition_flag, conditions, created, int_or_string, name,
};
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
                            Some(t) => format!("{port} → {}", int_or_string(t)),
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
/// условия `ready` считается готовым (так читает его kube-proxy). На dual-stack кластере у
/// сервиса по EndpointSlice на семейство адресов (`addressType`: IPv4, IPv6), и каждый под
/// есть в обоих: считаем по семействам и берём то, где endpoints больше.
pub fn endpoints_by_service(slices: &[EndpointSlice]) -> BTreeMap<String, EndpointCount> {
    let mut families: BTreeMap<(&str, &str), EndpointCount> = BTreeMap::new();
    for s in slices {
        let Some(service) = s
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get("kubernetes.io/service-name"))
        else {
            continue;
        };
        let e = families
            .entry((service.as_str(), s.address_type.as_str()))
            .or_insert(EndpointCount { ready: 0, total: 0 });
        for ep in &s.endpoints {
            e.total += 1;
            if ep.conditions.as_ref().and_then(|c| c.ready) != Some(false) {
                e.ready += 1;
            }
        }
    }
    let mut out: BTreeMap<String, EndpointCount> = BTreeMap::new();
    for ((service, _), count) in families {
        out.entry(service.to_string())
            .and_modify(|best| {
                if (count.total, count.ready) > (best.total, best.ready) {
                    *best = count;
                }
            })
            .or_insert(count);
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
        name: name(&o.metadata),
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
        name: name(&o.metadata),
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
    // Пропущенный `namespace` в parentRef — namespace самого маршрута (Gateway API).
    let route_ns = o.metadata.namespace.as_deref();
    let parent_ns = |p: &serde_json::Value| {
        p["namespace"]
            .as_str()
            .filter(|n| !n.is_empty())
            .or(route_ns)
            .map(String::from)
    };
    let same = |a: &serde_json::Value, b: &serde_json::Value| {
        a["name"] == b["name"]
            && parent_ns(a) == parent_ns(b)
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
        name: name(&o.metadata),
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
    use crate::storage::profile::TEST_CERT_PEM;
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
            certificate_expiry(TEST_CERT_PEM),
            Some(4_946_471_467),
            "GeneralizedTime после 2049 года"
        );
        assert_eq!(certificate_expiry("not a pem"), None);
    }

    fn pem_of(tag: &str, der: &[u8]) -> String {
        pem::encode(&pem::Pem::new(tag, der.to_vec()))
    }

    fn der_of(pem_text: &str) -> Vec<u8> {
        pem::parse(pem_text).unwrap().into_contents()
    }

    #[test]
    fn certificate_expiry_of_truncated_and_malformed_input_is_none() {
        for text in [ROUTE_CERT, TEST_CERT_PEM] {
            let der = der_of(text);
            assert!(certificate_expiry(&pem_of("CERTIFICATE", &der)).is_some());
            for n in 0..der.len() {
                assert_eq!(
                    certificate_expiry(&pem_of("CERTIFICATE", &der[..n])),
                    None,
                    "первые {n} из {} байт",
                    der.len()
                );
            }
        }
        // Неверный тег срока: второе время в сертификате — `notAfter`.
        let der = der_of(ROUTE_CERT);
        let not_after = der
            .windows(2)
            .enumerate()
            .filter(|(_, w)| *w == [0x17, 13])
            .map(|(i, _)| i)
            .nth(1)
            .unwrap();
        for tag in [0x04, 0x13, 0x30] {
            let mut bad = der.clone();
            bad[not_after] = tag;
            assert_eq!(
                certificate_expiry(&pem_of("CERTIFICATE", &bad)),
                None,
                "тег {tag:#x}"
            );
        }
        let mut bad = der.clone();
        bad[not_after + 2] = b'x';
        assert_eq!(certificate_expiry(&pem_of("CERTIFICATE", &bad)), None);
        // Блок не сертификат.
        assert_eq!(
            certificate_expiry(&pem_of("TRUSTED CERTIFICATE", &der)),
            None
        );
        assert_eq!(certificate_expiry(""), None);
    }

    #[test]
    fn certificate_expiry_skips_private_key_blocks() {
        // Блок «ключа» со срезом настоящего сертификата: прочитай его, срок вышел бы другим.
        let key = pem_of("PRIVATE KEY", &der_of(TEST_CERT_PEM));
        assert_eq!(certificate_expiry(&key), None);
        assert_eq!(
            certificate_expiry(&format!("{key}{ROUTE_CERT}")),
            Some(1_795_537_583),
            "ключ раньше сертификата"
        );
        assert_eq!(
            certificate_expiry(&format!("{ROUTE_CERT}{key}")),
            Some(1_795_537_583)
        );
    }

    #[test]
    fn utc_time_pivots_between_1950_and_2049() {
        assert_eq!(asn1_time(0x17, b"500101000000Z"), Some(-631_152_000), "50");
        assert_eq!(asn1_time(0x17, b"991231235959Z"), Some(946_684_799), "99");
        assert_eq!(asn1_time(0x17, b"000101000000Z"), Some(946_684_800), "00");
        assert_eq!(asn1_time(0x17, b"491231235959Z"), Some(2_524_607_999), "49");
        assert_eq!(
            asn1_time(0x18, b"20500101000000Z"),
            Some(2_524_608_000),
            "GeneralizedTime"
        );
        assert_eq!(asn1_time(0x17, b"501301000000Z"), None, "месяц 13");
        assert_eq!(asn1_time(0x17, b"5001"), None, "короткое значение");
        assert_eq!(asn1_time(0x04, b"500101000000Z"), None, "не время");
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

    #[test]
    fn dual_stack_endpoints_are_not_counted_twice() {
        let slice = |name: &str, family: &str, endpoints: serde_json::Value| -> EndpointSlice {
            from(serde_json::json!({"metadata": {"name": name,
                "labels": {"kubernetes.io/service-name": "web"}},
                "addressType": family, "endpoints": endpoints}))
        };
        let v4 = slice(
            "web-v4",
            "IPv4",
            serde_json::json!([{"addresses": ["10.1.0.1"], "conditions": {"ready": true}}]),
        );
        let v6 = slice(
            "web-v6",
            "IPv6",
            serde_json::json!([{"addresses": ["fd00::1"], "conditions": {"ready": true}}]),
        );
        assert_eq!(
            endpoints_by_service(&[v4, v6])["web"],
            EndpointCount { ready: 1, total: 1 },
            "один под в обоих семействах"
        );
        // Слайсы одного семейства складываются, из семейств берётся то, где endpoints больше.
        let a = slice(
            "web-a",
            "IPv4",
            serde_json::json!([{"addresses": ["10.1.0.1"], "conditions": {"ready": true}}]),
        );
        let b = slice(
            "web-b",
            "IPv4",
            serde_json::json!([{"addresses": ["10.1.0.2"], "conditions": {"ready": false}}]),
        );
        let six = slice(
            "web-6",
            "IPv6",
            serde_json::json!([{"addresses": ["fd00::1"], "conditions": {"ready": true}}]),
        );
        assert_eq!(
            endpoints_by_service(&[six, a, b])["web"],
            EndpointCount { ready: 1, total: 2 }
        );
    }

    #[test]
    fn route_parents_match_status_with_namespace_defaulting_to_the_routes_own() {
        let accepted = |spec_ref: serde_json::Value, status_ref: serde_json::Value| {
            let route = gateway_route_row(
                "HTTPRoute",
                &from(serde_json::json!({
                    "metadata": {"name": "web", "namespace": "team-a"},
                    "spec": {"parentRefs": [spec_ref]},
                    "status": {"parents": [{"parentRef": status_ref,
                        "conditions": [{"type": "Accepted", "status": "True"}]}]}})),
            );
            route.parents[0].accepted.is_some()
        };
        let gw = || serde_json::json!({"name": "gw"});
        let gw_in = |ns: &str| serde_json::json!({"name": "gw", "namespace": ns});
        assert!(accepted(gw(), gw_in("team-a")), "в spec namespace опущен");
        assert!(accepted(gw_in("team-a"), gw()), "в status namespace опущен");
        assert!(accepted(gw(), gw()));
        assert!(accepted(gw_in("infra"), gw_in("infra")));
        assert!(!accepted(gw(), gw_in("infra")), "Gateway в другом проекте");
        assert!(!accepted(gw_in("infra"), gw()));
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

    /// Сеть проекта, где на путь, оканчивающийся на `denied`, API-сервер отвечает 403.
    async fn network_forbidding(denied: &'static str) -> NetworkView {
        let (client, server) = mock_client(move |s| {
            if s.path().ends_with(denied) {
                let resource = denied.rsplit('/').next().unwrap();
                return status(
                    403,
                    "Forbidden",
                    &format!(
                        r#"{resource} is forbidden: User "u" cannot list resource "{resource}" in API group "" in the namespace "team-a""#
                    ),
                );
            }
            gateway_objects(s).unwrap_or_else(|| match s.path() {
                "/api/v1/namespaces/team-a/services" => list(vec![serde_json::json!({
                    "metadata": {"name": "web"}, "spec": {"selector": {"app": "web"}, "ports": [{"port": 80}]}})]),
                "/apis/discovery.k8s.io/v1/namespaces/team-a/endpointslices" => list(vec![serde_json::json!({
                    "metadata": {"name": "web-1", "labels": {"kubernetes.io/service-name": "web"}},
                    "addressType": "IPv4", "endpoints": [{"addresses": ["10.1.0.1"], "conditions": {"ready": true}}]})]),
                "/apis/route.openshift.io/v1/namespaces/team-a/routes" => {
                    list(vec![route("web-abcde", Some("True"), Some("web"))])
                }
                "/apis/networking.k8s.io/v1/namespaces/team-a/ingresses" => list(vec![serde_json::json!({
                    "metadata": {"name": "web"}, "spec": {"ingressClassName": "openshift-default"}})]),
                other => panic!("лишний запрос {other}"),
            })
        });
        let r = ReadClient::new(client, Duration::from_secs(5));
        let groups = ApiGroups {
            routes: true,
            gateway_api: true,
            ..Default::default()
        };
        let n = get_network(&r, "team-a", &groups).await;
        drop(r);
        assert_eq!(server.await.unwrap().len(), 7, "все виды спрошены");
        n
    }

    fn forbidden_resource<T>(part: &Part<T>) -> &str {
        let e = part.error.as_ref().expect("часть без данных");
        assert_eq!(e.code, ErrorCode::Forbidden);
        e.params["resource"].as_str()
    }

    #[tokio::test]
    async fn forbidden_services_leave_the_other_parts_visible() {
        let n = network_forbidding("/namespaces/team-a/services").await;
        assert_eq!(forbidden_resource(&n.services), "services");
        assert!(n.endpoints_error.is_none());
        assert_eq!(n.routes.unwrap().data.unwrap().len(), 1);
        assert_eq!(n.ingresses.data.unwrap()[0].routes, ["web-abcde"]);
        assert_eq!(n.gateways.unwrap().data.unwrap().len(), 1);
        assert_eq!(n.http_routes.unwrap().data.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn forbidden_routes_leave_the_other_parts_visible() {
        let n = network_forbidding("/namespaces/team-a/routes").await;
        assert_eq!(forbidden_resource(n.routes.as_ref().unwrap()), "routes");
        let services = n.services.data.unwrap();
        assert_eq!(
            services[0].endpoints,
            Some(EndpointCount { ready: 1, total: 1 })
        );
        let ingresses = n.ingresses.data.unwrap();
        assert_eq!(ingresses.len(), 1);
        assert!(ingresses[0].routes.is_empty(), "Routes не прочитаны");
        assert_eq!(n.gateways.unwrap().data.unwrap().len(), 1);
        assert_eq!(n.http_routes.unwrap().data.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn forbidden_gateways_leave_the_other_parts_visible() {
        let n = network_forbidding("/namespaces/team-a/gateways").await;
        assert_eq!(forbidden_resource(n.gateways.as_ref().unwrap()), "gateways");
        assert_eq!(n.services.data.unwrap().len(), 1);
        assert_eq!(n.routes.unwrap().data.unwrap().len(), 1);
        assert_eq!(n.ingresses.data.unwrap()[0].routes, ["web-abcde"]);
        let http = &n.http_routes.unwrap().data.unwrap()[0];
        assert_eq!(http.backends, ["web:80", "canary:80"]);
    }
}
