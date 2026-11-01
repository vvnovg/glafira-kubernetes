//! Автопоиск источника метрик платформы (спец. §7.1, §4.3). У `openshift` — Thanos Querier:
//! маршрут `thanos-querier` в `openshift-monitoring`, иначе адрес из домена приложений
//! (`ingresses.config.openshift.io/cluster`), иначе из хоста OAuth-сервера. У остальных —
//! Prometheus через прокси API-сервера по списку известных сервисов, кандидаты дистрибутива
//! первыми; у `dropapp` перед ними — vmselect VictoriaMetrics мониторинга DropApp.

use futures::future::BoxFuture;
use kube::api::{ApiResource, GroupVersionKind};

use super::prometheus::{Failure, MetricsSource, PromApi, SourceInfo, SourceKind, SourceOrigin};
use crate::k8s::read::{ReadClient, ServiceProxy};
use crate::platform::PlatformKind;

/// Известные сервисы Prometheus: проект, имя, порт и дистрибутив, у которого он стоит обычно.
pub const PROMETHEUS_SERVICES: [(&str, &str, u16, Option<&str>); 4] = [
    // Rancher Monitoring.
    (
        "cattle-monitoring-system",
        "rancher-monitoring-prometheus",
        9090,
        Some("rancher"),
    ),
    // kube-prometheus.
    ("monitoring", "prometheus-k8s", 9090, None),
    // Prometheus Operator.
    ("monitoring", "prometheus-operated", 9090, None),
    ("observability", "prometheus-operated", 9090, None),
];

/// vmselect VictoriaMetrics мониторинга DropApp: проект, имя, порт и префикс API Prometheus.
pub const DROPAPP_VMSELECT: (&str, &str, u16, &str) = (
    "dapp-monitoring",
    "vmselect-victoria-metrics-k8s-stack",
    8481,
    "/select/0/prometheus",
);

/// Кандидаты по порядку проверки: у `dropapp` — vmselect DropApp, затем, как у остальных,
/// сервисы дистрибутива, общие и чужих дистрибутивов.
pub fn candidates(platform: PlatformKind, distribution: Option<&str>) -> Vec<ServiceProxy> {
    let mut out = Vec::new();
    if platform == PlatformKind::Dropapp {
        let (ns, name, port, prefix) = DROPAPP_VMSELECT;
        out.push(ServiceProxy {
            namespace: ns.into(),
            name: name.into(),
            port,
            path_prefix: prefix.into(),
        });
    }
    let rank = |d: Option<&str>| match d {
        Some(x) if Some(x) == distribution => 0,
        None => 1,
        Some(_) => 2,
    };
    let mut list: Vec<_> = PROMETHEUS_SERVICES.iter().enumerate().collect();
    list.sort_by_key(|(i, (_, _, _, d))| (rank(*d), *i));
    out.extend(
        list.into_iter()
            .map(|(_, (ns, name, port, _))| ServiceProxy {
                namespace: (*ns).into(),
                name: (*name).into(),
                port: *port,
                path_prefix: String::new(),
            }),
    );
    out
}

/// Prometheus за прокси API-сервера: запросы — `ReadClient::prometheus`.
pub struct ProxySource {
    reader: ReadClient,
    service: ServiceProxy,
    info: SourceInfo,
}

impl ProxySource {
    pub fn new(reader: ReadClient, service: ServiceProxy) -> Self {
        let info = SourceInfo {
            kind: SourceKind::Prometheus,
            origin: SourceOrigin::Auto,
            location: service.label(),
            history: true,
        };
        Self {
            reader,
            service,
            info,
        }
    }
}

impl MetricsSource for ProxySource {
    fn info(&self) -> &SourceInfo {
        &self.info
    }

    fn get<'a>(
        &'a self,
        api: PromApi,
        params: Vec<(String, String)>,
    ) -> BoxFuture<'a, Result<String, Failure>> {
        Box::pin(async move {
            self.reader
                .prometheus(&self.service, api, &params)
                .await
                .map_err(Failure::from_error)
        })
    }
}

/// Маршрут `thanos-querier` в `openshift-monitoring` — для подписи источника.
pub const THANOS_ROUTE: &str = "openshift-monitoring/thanos-querier";

/// `ingresses.config.openshift.io`: домен приложений кластера.
fn ingress_config() -> ApiResource {
    ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk("config.openshift.io", "v1", "Ingress"),
        "ingresses",
    )
}

/// Адрес Thanos Querier по домену приложений.
fn from_domain(domain: &str) -> Option<url::Url> {
    let domain = domain.trim().trim_end_matches('.');
    if domain.is_empty() {
        return None;
    }
    url::Url::parse(&format!(
        "https://thanos-querier-openshift-monitoring.{domain}"
    ))
    .ok()
}

/// Адрес Thanos Querier OpenShift: маршрут, если его можно прочитать, иначе по домену
/// приложений — из настроек Ingress кластера или из хоста OAuth-сервера
/// (`oauth-openshift.<домен>`). Ошибка — причина последней попытки.
pub async fn thanos_url(r: &ReadClient) -> Result<url::Url, Failure> {
    let route = r
        .get_dynamic(
            &crate::k8s::network::routes(),
            Some("openshift-monitoring"),
            "thanos-querier",
        )
        .await;
    if let Ok(o) = &route
        && let Some(host) = o.data["spec"]["host"].as_str()
        && let Ok(u) = url::Url::parse(&format!("https://{host}"))
    {
        return Ok(u);
    }
    if let Ok(o) = r.get_dynamic(&ingress_config(), None, "cluster").await
        && let Some(u) = o.data["spec"]["domain"].as_str().and_then(from_domain)
    {
        return Ok(u);
    }
    let meta = r
        .get_text("/.well-known/oauth-authorization-server")
        .await
        .map_err(Failure::from_error)?;
    let issuer: serde_json::Value =
        serde_json::from_str(&meta).map_err(|_| Failure::bad_response("oauth metadata"))?;
    issuer["issuer"]
        .as_str()
        .and_then(|i| url::Url::parse(i).ok())
        .and_then(|u| {
            u.host_str()
                .and_then(|h| h.strip_prefix("oauth-openshift."))
                .and_then(from_domain)
        })
        .ok_or_else(|| match route {
            Err(e) => Failure::from_error(e),
            Ok(_) => Failure::bad_response("no thanos-querier address"),
        })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::k8s::testing::{mock_client, status};
    use crate::metrics::prometheus::{Reason, probe};
    use crate::platform::PlatformKind;

    fn reader(
        handler: impl Fn(&crate::k8s::testing::Seen) -> (u16, serde_json::Value) + Send + 'static,
    ) -> ReadClient {
        ReadClient::new(mock_client(handler).0, Duration::from_secs(5))
    }

    #[test]
    fn distribution_candidates_go_first() {
        let names = |d| {
            candidates(PlatformKind::Kubernetes, d)
                .iter()
                .map(ServiceProxy::label)
                .collect::<Vec<_>>()
        };
        assert_eq!(
            names(None),
            [
                "monitoring/prometheus-k8s:9090",
                "monitoring/prometheus-operated:9090",
                "observability/prometheus-operated:9090",
                "cattle-monitoring-system/rancher-monitoring-prometheus:9090"
            ]
        );
        assert_eq!(
            names(Some("rancher"))[0],
            "cattle-monitoring-system/rancher-monitoring-prometheus:9090"
        );
        assert_eq!(names(Some("eks")), names(None));
    }

    #[test]
    fn dropapp_asks_victoria_metrics_first() {
        let names = |p, d| {
            candidates(p, d)
                .iter()
                .map(ServiceProxy::label)
                .collect::<Vec<_>>()
        };
        for d in [None, Some("rancher")] {
            let dropapp = names(PlatformKind::Dropapp, d);
            assert_eq!(
                dropapp[0],
                "dapp-monitoring/vmselect-victoria-metrics-k8s-stack:8481"
            );
            assert_eq!(dropapp[1..], names(PlatformKind::Kubernetes, d), "{d:?}");
        }
        assert_eq!(
            candidates(PlatformKind::Dropapp, None)[0].path_prefix,
            "/select/0/prometheus"
        );
        assert!(
            candidates(PlatformKind::Kubernetes, None)
                .iter()
                .all(|c| c.path_prefix.is_empty())
        );
    }

    #[tokio::test]
    async fn prometheus_is_asked_through_the_service_proxy() {
        let r = reader(|s| {
            assert_eq!(
                s.path(),
                "/api/v1/namespaces/monitoring/services/http:prometheus-k8s:9090/proxy/api/v1/query"
            );
            assert!(s.uri.contains("query=vector%281%29"), "{}", s.uri);
            (
                200,
                serde_json::json!({"status": "success", "data": {"resultType": "vector",
                    "result": [{"metric": {}, "value": [1, "1"]}]}}),
            )
        });
        let src = ProxySource::new(r, candidates(PlatformKind::Kubernetes, None)[0].clone());
        probe(&src, 1).await.unwrap();
        assert_eq!(src.info().location, "monitoring/prometheus-k8s:9090");
    }

    #[tokio::test]
    async fn victoria_metrics_is_asked_through_the_service_proxy_with_the_prefix() {
        let r = reader(|s| {
            assert_eq!(
                s.path(),
                "/api/v1/namespaces/dapp-monitoring/services/http:vmselect-victoria-metrics-k8s-stack:8481/proxy/select/0/prometheus/api/v1/query"
            );
            (
                200,
                serde_json::json!({"status": "success", "data": {"resultType": "vector",
                    "result": [{"metric": {}, "value": [1, "1"]}]}}),
            )
        });
        let src = ProxySource::new(r, candidates(PlatformKind::Dropapp, None)[0].clone());
        probe(&src, 1).await.unwrap();
        assert_eq!(
            src.info().location,
            "dapp-monitoring/vmselect-victoria-metrics-k8s-stack:8481"
        );
    }

    #[tokio::test]
    async fn proxy_answers_become_reasons() {
        for (code, reason) in [
            (403, Reason::Forbidden),
            (404, Reason::NotFound),
            (503, Reason::Unavailable),
        ] {
            let r = reader(move |_| status(code, "X", "no"));
            let src = ProxySource::new(r, candidates(PlatformKind::Kubernetes, None)[0].clone());
            assert_eq!(probe(&src, 1).await.unwrap_err().reason, reason, "{code}");
        }
        // Ответ Prometheus с ошибкой запроса — не Status: «ответ не тот».
        let r = reader(|_| {
            (
                400,
                serde_json::json!({"status": "error", "errorType": "bad_data", "error": "parse error"}),
            )
        });
        let src = ProxySource::new(r, candidates(PlatformKind::Kubernetes, None)[0].clone());
        assert_eq!(
            probe(&src, 1).await.unwrap_err().reason,
            Reason::BadResponse
        );
    }

    #[tokio::test]
    async fn thanos_address_from_the_route_the_domain_or_the_oauth_host() {
        let route = reader(|s| match s.path() {
            "/apis/route.openshift.io/v1/namespaces/openshift-monitoring/routes/thanos-querier" => {
                (
                    200,
                    serde_json::json!({"apiVersion": "route.openshift.io/v1", "kind": "Route",
                    "metadata": {"name": "thanos-querier"},
                    "spec": {"host": "thanos-querier-openshift-monitoring.apps.a.example.com"}}),
                )
            }
            _ => status(404, "NotFound", "no"),
        });
        assert_eq!(
            thanos_url(&route).await.unwrap().as_str(),
            "https://thanos-querier-openshift-monitoring.apps.a.example.com/"
        );
        let domain = reader(|s| match s.path() {
            "/apis/config.openshift.io/v1/ingresses/cluster" => (
                200,
                serde_json::json!({"apiVersion": "config.openshift.io/v1", "kind": "Ingress",
                    "metadata": {"name": "cluster"}, "spec": {"domain": "apps.b.example.com"}}),
            ),
            _ => status(403, "Forbidden", "no"),
        });
        assert_eq!(
            thanos_url(&domain).await.unwrap().host_str(),
            Some("thanos-querier-openshift-monitoring.apps.b.example.com")
        );
        let oauth = reader(|s| match s.path() {
            "/.well-known/oauth-authorization-server" => (
                200,
                serde_json::json!({"issuer": "https://oauth-openshift.apps.c.example.com"}),
            ),
            _ => status(403, "Forbidden", "no"),
        });
        assert_eq!(
            thanos_url(&oauth).await.unwrap().host_str(),
            Some("thanos-querier-openshift-monitoring.apps.c.example.com")
        );
        let nothing = reader(|_| status(403, "Forbidden", "no"));
        assert_eq!(
            thanos_url(&nothing).await.unwrap_err().reason,
            Reason::Forbidden
        );
    }
}
