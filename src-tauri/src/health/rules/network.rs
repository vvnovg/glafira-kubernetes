//! Правила сети проекта (спец. §8.2): Service без endpoints, Route (допуск роутером, срок
//! встроенного сертификата), Ingress (класс, бэкенды, Secret TLS), Gateway API (маршруты
//! и Gateway).

use std::collections::BTreeMap;

use serde_json::json;

use super::{CERT_EXPIRED, Findings, GRACE_S, listed};
use crate::health::model::{Target, TargetKind};
use crate::health::snapshot::{ClusterSnapshot, Cond, ProjectSnap, ServicePort};

/// Встроенный сертификат Route истекает меньше чем через столько.
pub const CERT_WARN_S: i64 = 30 * 86_400;
const DAY_S: i64 = 86_400;

/// Кто ссылается на Service: Routes, Ingresses и маршруты Gateway API проекта.
fn referenced_by(p: &ProjectSnap, service: &str) -> Vec<String> {
    let mut out = Vec::new();
    for r in p.routes.iter().flatten() {
        if r.service.as_deref() == Some(service) {
            out.push(format!("Route/{}", r.name));
        }
    }
    for i in p.ingresses.iter().flatten() {
        if i.backends.iter().any(|(s, _)| s == service) {
            out.push(format!("Ingress/{}", i.name));
        }
    }
    for r in p.gateway_routes.iter().flatten() {
        if r.backends.iter().any(|b| b == service) {
            let kind = match r.kind {
                TargetKind::Grpcroute => "GRPCRoute",
                _ => "HTTPRoute",
            };
            out.push(format!("{kind}/{}", r.name));
        }
    }
    out
}

/// Порт Ingress (номер или имя) есть у Service.
fn has_port(ports: &[ServicePort], port: &str) -> bool {
    match port.parse::<i32>() {
        Ok(n) => ports.iter().any(|p| p.port == n),
        Err(_) => ports.iter().any(|p| p.name.as_deref() == Some(port)),
    }
}

/// Условие `False` дольше `GRACE_S`: с момента перехода, а без него — с создания объекта.
fn false_long(c: Option<&Cond>, created: Option<i64>, now: i64) -> bool {
    c.is_some_and(|c| c.is("False") && c.since.or(created).is_some_and(|t| now - t >= GRACE_S))
}

pub fn check(s: &ClusterSnapshot, out: &mut Findings) {
    for p in &s.projects {
        let ns = p.namespace.as_str();
        for svc in p.services.iter().flatten() {
            if svc.has_selector && svc.kind != "ExternalName" && svc.ready_endpoints == Some(0) {
                out.emit(
                    "service.noEndpoints",
                    Target::object(TargetKind::Service, ns, &svc.name),
                    None,
                    json!({ "referencedBy": listed(&referenced_by(p, &svc.name)) }),
                );
            }
        }
        for r in p.routes.iter().flatten() {
            let target = || Target::object(TargetKind::Route, ns, &r.name);
            match r.admitted {
                Some(true) => {}
                None => out.emit(
                    "route.notAdmitted",
                    target(),
                    None,
                    json!({ "reason": "noStatus" }),
                ),
                Some(false) => out.emit(
                    "route.notAdmitted",
                    target(),
                    None,
                    json!({ "reason": r.admitted_reason, "message": r.admitted_message }),
                ),
            }
            if let Some(t) = r.cert_expires {
                let left = t - s.now;
                if left <= 0 {
                    // Истёк хоть час назад — «истёк 1 день назад», а не «0 дней».
                    let days = -((-left + DAY_S - 1) / DAY_S);
                    out.emit_at(
                        "route.certExpiring",
                        CERT_EXPIRED,
                        target(),
                        None,
                        json!({ "days": days, "expired": true }),
                    );
                } else if left < CERT_WARN_S {
                    out.emit(
                        "route.certExpiring",
                        target(),
                        None,
                        json!({ "days": left / DAY_S, "expired": false }),
                    );
                }
            }
        }
        let services: Option<BTreeMap<&str, &[ServicePort]>> = p.services.as_ref().map(|v| {
            v.iter()
                .map(|s| (s.name.as_str(), s.ports.as_slice()))
                .collect()
        });
        for i in p.ingresses.iter().flatten() {
            let target = || Target::object(TargetKind::Ingress, ns, &i.name);
            // Из такого Ingress OpenShift создал Routes: их и проверяют правила `route.*`.
            if !i.has_routes
                && let Some(classes) = &s.ingress_classes
            {
                let missing = match &i.class {
                    Some(c) => !classes.names.contains(c),
                    None => !classes.has_default,
                };
                if missing {
                    out.emit(
                        "ingress.classMissing",
                        target(),
                        None,
                        json!({ "class": i.class }),
                    );
                }
            }
            if let Some(services) = &services {
                let mut absent: Vec<String> = Vec::new();
                let mut ports: Vec<String> = Vec::new();
                for (svc, port) in &i.backends {
                    match services.get(svc.as_str()) {
                        None => {
                            if !absent.contains(svc) {
                                absent.push(svc.clone());
                            }
                        }
                        Some(sp) => {
                            if let Some(port) = port
                                && !has_port(sp, port)
                            {
                                let p = format!("{svc}:{port}");
                                if !ports.contains(&p) {
                                    ports.push(p);
                                }
                            }
                        }
                    }
                }
                if !absent.is_empty() || !ports.is_empty() {
                    out.emit(
                        "ingress.backendMissing",
                        target(),
                        None,
                        json!({ "services": listed(&absent), "ports": listed(&ports) }),
                    );
                }
            }
            if let Some(secrets) = &p.secrets
                && i.created.is_some_and(|c| s.now - c >= GRACE_S)
            {
                let missing: Vec<String> = i
                    .tls_secrets
                    .iter()
                    .filter(|name| !secrets.contains(*name))
                    .cloned()
                    .collect();
                if !missing.is_empty() {
                    out.emit(
                        "ingress.tlsSecretMissing",
                        target(),
                        None,
                        json!({ "secrets": listed(&missing) }),
                    );
                }
            }
        }
        for r in p.gateway_routes.iter().flatten() {
            let target = || Target::object(r.kind, ns, &r.name);
            let old = r.created.is_some_and(|c| s.now - c >= GRACE_S);
            let mut rejected: Vec<String> = Vec::new();
            let mut why: Option<&Cond> = None;
            for parent in &r.parent_refs {
                match r.parents.iter().find(|x| &x.gateway == parent) {
                    None if old => rejected.push(parent.clone()),
                    Some(x) if x.accepted.as_ref().is_some_and(|c| c.is("False")) => {
                        rejected.push(parent.clone());
                        why = why.or(x.accepted.as_ref());
                    }
                    _ => {}
                }
            }
            if !rejected.is_empty() {
                out.emit(
                    "gatewayRoute.notAccepted",
                    target(),
                    None,
                    json!({
                        "parents": listed(&rejected),
                        "reason": why.and_then(|c| c.reason.clone()),
                        "message": why.and_then(|c| c.message.clone()),
                    }),
                );
            }
            let unresolved: Vec<_> = r
                .parents
                .iter()
                .filter(|x| x.resolved_refs.as_ref().is_some_and(|c| c.is("False")))
                .collect();
            if let Some(first) = unresolved.first() {
                let c = first.resolved_refs.as_ref();
                out.emit(
                    "gatewayRoute.refsUnresolved",
                    target(),
                    None,
                    json!({
                        "parents": listed(&unresolved.iter().map(|x| x.gateway.clone()).collect::<Vec<_>>()),
                        "reason": c.and_then(|c| c.reason.clone()),
                        "message": c.and_then(|c| c.message.clone()),
                    }),
                );
            }
        }
        for g in p.gateways.iter().flatten() {
            let failing = [
                ("Accepted", g.accepted.as_ref()),
                ("Programmed", g.programmed.as_ref()),
            ]
            .into_iter()
            .find(|(_, c)| false_long(*c, g.created, s.now));
            if let Some((name, c)) = failing {
                let since = c.and_then(|c| c.since).or(g.created).unwrap_or(s.now);
                out.emit(
                    "gateway.notProgrammed",
                    Target::object(TargetKind::Gateway, ns, &g.name),
                    None,
                    json!({
                        "condition": name,
                        "reason": c.and_then(|c| c.reason.clone()),
                        "message": c.and_then(|c| c.message.clone()),
                        "minutes": (s.now - since) / 60,
                    }),
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::super::fixtures::NOW;
    use super::*;
    use crate::health::model::{Finding, Severity};
    use crate::health::snapshot::*;
    use crate::platform::PlatformKind;
    use crate::storage::profile::Environment;

    fn run(s: &ClusterSnapshot, env: Environment) -> Vec<Finding> {
        crate::health::rules::evaluate(s, env)
    }

    fn ids(v: &[Finding]) -> Vec<String> {
        v.iter()
            .map(|f| format!("{} {}", f.rule_id, f.target.name.as_deref().unwrap_or("-")))
            .collect()
    }

    fn snap(p: ProjectSnap) -> ClusterSnapshot {
        let mut s = ClusterSnapshot::empty(NOW, PlatformKind::Kubernetes);
        s.ingress_classes = Some(IngressClasses {
            names: BTreeSet::from(["nginx".to_string()]),
            has_default: false,
        });
        s.projects = vec![p];
        s
    }

    fn service(name: &str, ready: Option<i32>) -> ServiceSnap {
        ServiceSnap {
            name: name.into(),
            kind: "ClusterIP".into(),
            has_selector: true,
            ports: vec![ServicePort {
                name: Some("http".into()),
                port: 80,
            }],
            ready_endpoints: ready,
        }
    }

    fn cond(status: &str, reason: &str, since: i64) -> Cond {
        Cond {
            status: status.into(),
            reason: Some(reason.into()),
            message: Some(format!("{reason} message")),
            since: Some(since),
        }
    }

    #[test]
    fn service_without_ready_endpoints_names_who_uses_it() {
        let mut external = service("ext", Some(0));
        external.kind = "ExternalName".into();
        let mut headless = service("raw", Some(0));
        headless.has_selector = false;
        let p = ProjectSnap {
            namespace: "team-a".into(),
            services: Some(vec![
                service("web", Some(0)),
                service("api", Some(2)),
                service("db", None),
                external,
                headless,
            ]),
            routes: Some(vec![RouteSnap {
                name: "web".into(),
                service: Some("web".into()),
                admitted: Some(true),
                ..Default::default()
            }]),
            ingresses: Some(vec![IngressSnap {
                name: "web".into(),
                class: Some("nginx".into()),
                backends: vec![("web".into(), Some("80".into()))],
                ..Default::default()
            }]),
            gateway_routes: Some(vec![GatewayRouteSnap {
                kind: TargetKind::Httproute,
                name: "web".into(),
                created: Some(NOW),
                parent_refs: vec![],
                parents: vec![],
                backends: vec!["web".into()],
            }]),
            ..Default::default()
        };
        let v = run(&snap(p), Environment::Prod);
        assert_eq!(ids(&v), ["service.noEndpoints web"]);
        assert_eq!(
            v[0].params["referencedBy"],
            json!(["Route/web", "Ingress/web", "HTTPRoute/web"])
        );
    }

    #[test]
    fn routes_not_admitted_and_certificates() {
        let route = |name: &str, admitted: Option<bool>, cert: Option<i64>| RouteSnap {
            name: name.into(),
            admitted,
            admitted_reason: admitted.filter(|a| !a).map(|_| "HostAlreadyClaimed".into()),
            cert_expires: cert,
            ..Default::default()
        };
        let p = ProjectSnap {
            namespace: "team-a".into(),
            routes: Some(vec![
                route("new", None, None),
                route("taken", Some(false), None),
                route("soon", Some(true), Some(NOW + 29 * DAY_S)),
                route("later", Some(true), Some(NOW + CERT_WARN_S)),
                route("old", Some(true), Some(NOW - 3600)),
            ]),
            ..Default::default()
        };
        let s = snap(p);
        let v = run(&s, Environment::Prod);
        assert_eq!(
            ids(&v),
            [
                "route.notAdmitted new",
                "route.notAdmitted taken",
                "route.certExpiring old",
                "route.certExpiring soon"
            ]
        );
        assert_eq!(v[0].params["reason"], "noStatus");
        assert_eq!(v[1].params["reason"], "HostAlreadyClaimed");
        assert_eq!(
            (
                v[2].severity,
                v[2].params["days"].as_i64(),
                v[2].params["expired"].as_bool()
            ),
            (Severity::Critical, Some(-1), Some(true))
        );
        assert_eq!(
            (v[3].severity, v[3].params["days"].as_i64()),
            (Severity::Warning, Some(29))
        );
        let dev = run(&s, Environment::Dev);
        let cert: Vec<_> = dev
            .iter()
            .filter(|f| f.rule_id == "route.certExpiring")
            .map(|f| f.severity)
            .collect();
        assert_eq!(
            cert,
            [Severity::Warning, Severity::Info],
            "истёк — П у прочих, скоро — И"
        );
    }

    #[test]
    fn ingresses_with_missing_class_backends_and_tls_secrets() {
        let ingress = |name: &str, class: Option<&str>| IngressSnap {
            name: name.into(),
            class: class.map(String::from),
            backends: vec![("web".into(), Some("80".into()))],
            created: Some(NOW - GRACE_S),
            ..Default::default()
        };
        let mut broken = ingress("broken", Some("traefik"));
        broken.backends = vec![
            ("api".into(), Some("80".into())),
            ("web".into(), Some("https".into())),
            ("web".into(), Some("http".into())),
            ("api".into(), None),
        ];
        broken.tls_secrets = vec!["web-tls".into(), "present".into()];
        let mut fresh = ingress("fresh", Some("nginx"));
        fresh.tls_secrets = vec!["pending-tls".into()];
        fresh.created = Some(NOW - 60);
        let mut openshift = ingress("routed", Some("traefik"));
        openshift.has_routes = true;
        let p = ProjectSnap {
            namespace: "team-a".into(),
            services: Some(vec![service("web", Some(1))]),
            ingresses: Some(vec![broken, fresh, ingress("default", None), openshift]),
            secrets: Some(BTreeSet::from(["present".to_string()])),
            ..Default::default()
        };
        let mut s = snap(p);
        let v = run(&s, Environment::Prod);
        assert_eq!(
            ids(&v),
            [
                "ingress.classMissing broken",
                "ingress.classMissing default",
                "ingress.backendMissing broken",
                "ingress.tlsSecretMissing broken"
            ]
        );
        assert_eq!(v[0].params["class"], "traefik");
        assert!(
            v[1].params["class"].is_null(),
            "без класса и без класса по умолчанию"
        );
        assert_eq!(v[2].params["services"], json!(["api"]));
        assert_eq!(v[2].params["ports"], json!(["web:https"]));
        assert_eq!(v[3].params["secrets"], json!(["web-tls"]));
        s.ingress_classes = None;
        s.projects[0].secrets = None;
        let v = run(&s, Environment::Prod);
        assert_eq!(
            ids(&v),
            ["ingress.backendMissing broken"],
            "классы и Secret не прочитаны"
        );
    }

    #[test]
    fn gateway_routes_and_gateways() {
        let parent = |gw: &str, accepted: Option<Cond>, resolved: Option<Cond>| RouteParentSnap {
            gateway: gw.into(),
            accepted,
            resolved_refs: resolved,
        };
        let p = ProjectSnap {
            namespace: "team-a".into(),
            gateway_routes: Some(vec![
                GatewayRouteSnap {
                    kind: TargetKind::Httproute,
                    name: "web".into(),
                    created: Some(NOW - GRACE_S),
                    parent_refs: vec![
                        "infra/public".into(),
                        "infra/internal".into(),
                        "infra/late".into(),
                    ],
                    parents: vec![
                        parent(
                            "infra/public",
                            Some(cond("True", "Accepted", NOW)),
                            Some(cond("False", "BackendNotFound", NOW)),
                        ),
                        parent(
                            "infra/internal",
                            Some(cond("False", "NotAllowedByListeners", NOW)),
                            None,
                        ),
                    ],
                    backends: vec![],
                },
                GatewayRouteSnap {
                    kind: TargetKind::Grpcroute,
                    name: "rpc".into(),
                    created: Some(NOW - 10),
                    parent_refs: vec!["infra/public".into()],
                    parents: vec![],
                    backends: vec![],
                },
            ]),
            gateways: Some(vec![
                GatewaySnap {
                    name: "edge".into(),
                    created: Some(NOW - 3600),
                    accepted: Some(cond("True", "Accepted", NOW - 3600)),
                    programmed: Some(cond("False", "AddressNotAssigned", NOW - GRACE_S)),
                },
                GatewaySnap {
                    name: "new".into(),
                    created: Some(NOW - 60),
                    accepted: Some(cond("False", "Pending", NOW - 60)),
                    programmed: None,
                },
            ]),
            ..Default::default()
        };
        let v = run(&snap(p), Environment::Prod);
        assert_eq!(
            ids(&v),
            [
                "gatewayRoute.notAccepted web",
                "gatewayRoute.refsUnresolved web",
                "gateway.notProgrammed edge"
            ]
        );
        assert_eq!(
            v[0].params["parents"],
            json!(["infra/internal", "infra/late"])
        );
        assert_eq!(v[0].params["reason"], "NotAllowedByListeners");
        assert_eq!(v[1].params["reason"], "BackendNotFound");
        assert_eq!(v[1].params["parents"], json!(["infra/public"]));
        assert_eq!(
            (
                v[2].params["condition"].as_str(),
                v[2].params["minutes"].as_i64()
            ),
            (Some("Programmed"), Some(5))
        );
    }
}
