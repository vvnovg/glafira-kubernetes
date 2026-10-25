//! Платформа кластера (спец. §4.3) — то, что у вендоров устроено по-разному и из групп API
//! не выводится. Определяется чистой функцией `detect` по discovery при подключении; ручного выбора
//! в профиле нет. Какие объекты читать и показывать, решают группы API (`ApiGroups`), а не
//! платформа. Поведение, которое зависит от платформы (обзор кластера, автопоиск метрик), добавляют
//! следующие этапы.

use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum PlatformKind {
    Openshift,
    /// Любой другой Kubernetes; Platform V DropApp — тоже, пока нет его адаптера (этап O5).
    Kubernetes,
}

/// Справочный дистрибутив платформы `kubernetes`: виден в интерфейсе и MCP и влияет только
/// на порядок кандидатов автопоиска метрик (этап O3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Distribution {
    Eks,
    Gke,
    Aks,
    Rancher,
    Deckhouse,
    K3s,
    Rke2,
}

impl Distribution {
    /// Название в интерфейсе (не переводится).
    pub fn title(self) -> &'static str {
        match self {
            Distribution::Eks => "EKS",
            Distribution::Gke => "GKE",
            Distribution::Aks => "AKS",
            Distribution::Rancher => "Rancher",
            Distribution::Deckhouse => "Deckhouse",
            Distribution::K3s => "k3s",
            Distribution::Rke2 => "RKE2",
        }
    }
}

/// Что кластер сообщил о себе: группы API, `/version`, версия ClusterVersion и метки ноды.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Discovery {
    /// `group/version` всех групп API из `/apis`, например `route.openshift.io/v1`.
    pub group_versions: Vec<String>,
    /// `gitVersion` из `/version`, например `v1.31.4-eks-a737599`.
    pub git_version: String,
    /// `status.desired.version` объекта ClusterVersion OpenShift, если он читается:
    /// `4.18.3`, `4.15.0-0.okd-2024-03-10-010116`.
    pub cluster_version: Option<String>,
    /// Имена меток первой ноды, если ноды читаются.
    pub node_labels: Vec<String>,
}

impl Discovery {
    pub fn has_group(&self, group: &str) -> bool {
        self.group_versions
            .iter()
            .any(|gv| gv.split_once('/').is_some_and(|(g, _)| g == group))
    }

    fn has_group_matching(&self, f: impl Fn(&str) -> bool) -> bool {
        self.group_versions
            .iter()
            .any(|gv| gv.split_once('/').is_some_and(|(g, _)| f(g)))
    }
}

/// Группы API, от которых зависит, какие объекты Glafira читает и показывает (спец. §4.3).
/// Правило одно для всех платформ: кластер Kubernetes с CRD `Route` покажет Route.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiGroups {
    /// `route.openshift.io` — Route.
    pub routes: bool,
    /// `project.openshift.io` — проекты вместо списка namespace.
    pub projects: bool,
    /// `apps.openshift.io` — DeploymentConfig.
    pub deployment_configs: bool,
    /// `gateway.networking.k8s.io/v1` — Gateway API (Gateway, HTTPRoute, GRPCRoute).
    pub gateway_api: bool,
}

impl ApiGroups {
    pub fn of(d: &Discovery) -> Self {
        Self {
            routes: d.has_group("route.openshift.io"),
            projects: d.has_group("project.openshift.io"),
            deployment_configs: d.has_group("apps.openshift.io"),
            gateway_api: d
                .group_versions
                .iter()
                .any(|gv| gv == "gateway.networking.k8s.io/v1"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PlatformInfo {
    pub kind: PlatformKind,
    pub distribution: Option<Distribution>,
    /// Версия платформы «major.minor»: OpenShift — из ClusterVersion (если он читается),
    /// Kubernetes — из `/version`.
    pub version: Option<String>,
    /// Название в интерфейсе и MCP: «OpenShift 4.18», «OKD 4.15», «Kubernetes 1.31 (EKS)».
    pub label: String,
}

/// «major.minor» из строки версии: `v1.31.4-eks-a737599` → `1.31`, `4.18.3` → `4.18`.
fn major_minor(v: &str) -> Option<String> {
    let mut parts = v.trim().trim_start_matches('v').split('.');
    let major = parts
        .next()
        .filter(|p| p.chars().all(|c| c.is_ascii_digit()) && !p.is_empty())?;
    let minor: String = parts
        .next()?
        .chars()
        .take_while(char::is_ascii_digit)
        .collect();
    (!minor.is_empty()).then(|| format!("{major}.{minor}"))
}

fn distribution(d: &Discovery) -> Option<Distribution> {
    let v = d.git_version.as_str();
    if v.contains("-eks-") {
        Some(Distribution::Eks)
    } else if v.contains("-gke.") {
        Some(Distribution::Gke)
    } else if d
        .node_labels
        .iter()
        .any(|l| l.starts_with("kubernetes.azure.com/"))
    {
        Some(Distribution::Aks)
    } else if d.has_group_matching(|g| g.ends_with(".cattle.io")) {
        Some(Distribution::Rancher)
    } else if d.has_group_matching(|g| g == "deckhouse.io" || g.ends_with(".deckhouse.io")) {
        Some(Distribution::Deckhouse)
    } else if v.contains("+k3s") {
        Some(Distribution::K3s)
    } else if v.contains("+rke2") {
        Some(Distribution::Rke2)
    } else {
        None
    }
}

/// Платформа по discovery (спец. §4.3). Признаки проверяются по порядку: группа
/// `config.openshift.io` → `openshift`; признак DropApp — этап O5; иначе `kubernetes`.
pub fn detect(d: &Discovery) -> PlatformInfo {
    if d.has_group("config.openshift.io") {
        let full = d.cluster_version.as_deref().unwrap_or("");
        let version = major_minor(full);
        let name = if full.contains("okd") {
            "OKD"
        } else {
            "OpenShift"
        };
        let label = match &version {
            Some(v) => format!("{name} {v}"),
            None => name.to_string(),
        };
        return PlatformInfo {
            kind: PlatformKind::Openshift,
            distribution: None,
            version,
            label,
        };
    }
    let distribution = distribution(d);
    let version = major_minor(&d.git_version);
    let mut label = match &version {
        Some(v) => format!("Kubernetes {v}"),
        None => "Kubernetes".to_string(),
    };
    if let Some(dist) = distribution {
        label.push_str(&format!(" ({})", dist.title()));
    }
    PlatformInfo {
        kind: PlatformKind::Kubernetes,
        distribution,
        version,
        label,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const KUBE: &[&str] = &[
        "apps/v1",
        "batch/v1",
        "networking.k8s.io/v1",
        "authentication.k8s.io/v1",
        "authorization.k8s.io/v1",
    ];
    const OPENSHIFT: &[&str] = &[
        "config.openshift.io/v1",
        "route.openshift.io/v1",
        "project.openshift.io/v1",
        "apps.openshift.io/v1",
        "oauth.openshift.io/v1",
    ];

    fn snapshot(extra: &[&str], git_version: &str) -> Discovery {
        Discovery {
            group_versions: KUBE.iter().chain(extra).map(|s| s.to_string()).collect(),
            git_version: git_version.into(),
            ..Default::default()
        }
    }

    /// Снимки discovery и `/version` (спец. §17): платформа, дистрибутив, название.
    #[test]
    fn detect_table() {
        let mut ocp = snapshot(OPENSHIFT, "v1.31.6");
        ocp.cluster_version = Some("4.18.3".into());
        let mut okd = snapshot(OPENSHIFT, "v1.28.7+6e2789b");
        okd.cluster_version = Some("4.15.0-0.okd-2024-03-10-010116".into());
        let ocp_no_cv = snapshot(OPENSHIFT, "v1.31.6");
        let mut aks = snapshot(&[], "v1.30.5");
        aks.node_labels = vec![
            "kubernetes.azure.com/cluster".into(),
            "kubernetes.io/os".into(),
        ];
        let cases: Vec<(&str, Discovery, PlatformKind, Option<Distribution>, &str)> = vec![
            ("OCP", ocp, PlatformKind::Openshift, None, "OpenShift 4.18"),
            ("OKD", okd, PlatformKind::Openshift, None, "OKD 4.15"),
            (
                "OCP без прав на ClusterVersion",
                ocp_no_cv,
                PlatformKind::Openshift,
                None,
                "OpenShift",
            ),
            (
                "Kubernetes",
                snapshot(&[], "v1.31.4"),
                PlatformKind::Kubernetes,
                None,
                "Kubernetes 1.31",
            ),
            (
                "Kubernetes с CRD Route",
                snapshot(&["route.openshift.io/v1"], "v1.31.4"),
                PlatformKind::Kubernetes,
                None,
                "Kubernetes 1.31",
            ),
            (
                "Gateway API",
                snapshot(
                    &[
                        "gateway.networking.k8s.io/v1",
                        "gateway.networking.k8s.io/v1beta1",
                    ],
                    "v1.32.1",
                ),
                PlatformKind::Kubernetes,
                None,
                "Kubernetes 1.32",
            ),
            (
                "EKS",
                snapshot(&[], "v1.30.4-eks-a737599"),
                PlatformKind::Kubernetes,
                Some(Distribution::Eks),
                "Kubernetes 1.30 (EKS)",
            ),
            (
                "GKE",
                snapshot(&[], "v1.31.1-gke.1678000"),
                PlatformKind::Kubernetes,
                Some(Distribution::Gke),
                "Kubernetes 1.31 (GKE)",
            ),
            (
                "AKS",
                aks,
                PlatformKind::Kubernetes,
                Some(Distribution::Aks),
                "Kubernetes 1.30 (AKS)",
            ),
            (
                "Rancher",
                snapshot(
                    &["management.cattle.io/v3", "catalog.cattle.io/v1"],
                    "v1.30.6+k3s1",
                ),
                PlatformKind::Kubernetes,
                Some(Distribution::Rancher),
                "Kubernetes 1.30 (Rancher)",
            ),
            (
                "Deckhouse",
                snapshot(&["deckhouse.io/v1alpha1"], "v1.30.8"),
                PlatformKind::Kubernetes,
                Some(Distribution::Deckhouse),
                "Kubernetes 1.30 (Deckhouse)",
            ),
            (
                "k3s",
                snapshot(&[], "v1.31.2+k3s1"),
                PlatformKind::Kubernetes,
                Some(Distribution::K3s),
                "Kubernetes 1.31 (k3s)",
            ),
            (
                "RKE2",
                snapshot(&[], "v1.31.2+rke2r1"),
                PlatformKind::Kubernetes,
                Some(Distribution::Rke2),
                "Kubernetes 1.31 (RKE2)",
            ),
        ];
        for (name, d, kind, dist, label) in cases {
            let p = detect(&d);
            assert_eq!(
                (p.kind, p.distribution, p.label.as_str()),
                (kind, dist, label),
                "{name}"
            );
        }
    }

    #[test]
    fn api_groups_decide_objects_not_platform() {
        let routes_only = snapshot(&["route.openshift.io/v1"], "v1.31.4");
        assert_eq!(
            ApiGroups::of(&routes_only),
            ApiGroups {
                routes: true,
                ..Default::default()
            }
        );
        assert_eq!(
            ApiGroups::of(&snapshot(OPENSHIFT, "v1.31.6")),
            ApiGroups {
                routes: true,
                projects: true,
                deployment_configs: true,
                gateway_api: false,
            }
        );
        let beta_only = snapshot(&["gateway.networking.k8s.io/v1beta1"], "v1.29.0");
        assert!(!ApiGroups::of(&beta_only).gateway_api, "нужна версия v1");
    }

    #[test]
    fn versions_and_json() {
        assert_eq!(major_minor("v1.31.4-eks-a737599").as_deref(), Some("1.31"));
        assert_eq!(major_minor("4.18.3").as_deref(), Some("4.18"));
        assert_eq!(major_minor("v1.29+").as_deref(), Some("1.29"));
        assert_eq!(major_minor(""), None);
        assert_eq!(major_minor("garbage"), None);
        let p = detect(&snapshot(&[], "v1.30.4-eks-a737599"));
        assert_eq!(
            serde_json::to_value(&p).unwrap(),
            serde_json::json!({"kind": "kubernetes", "distribution": "eks", "version": "1.30",
                "label": "Kubernetes 1.30 (EKS)"})
        );
        let p = detect(&snapshot(&["config.openshift.io/v1"], "garbage"));
        assert_eq!(
            serde_json::to_value(&p).unwrap(),
            serde_json::json!({"kind": "openshift", "distribution": null, "version": null,
                "label": "OpenShift"})
        );
    }
}
