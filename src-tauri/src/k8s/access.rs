//! Права пользователя в проекте (спец. §5.4): `SelfSubjectRulesReview` при открытии проекта,
//! кэш на 5 минут, сброс при ответе 403. По ним интерфейс не показывает вкладки и разделы,
//! которые не читаются; окончательно решает API-сервер.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::time::Duration;

use k8s_openapi::api::authorization::v1::ResourceRule;
use serde::Serialize;
use tokio::time::Instant;

use crate::error::AppResult;
use crate::k8s::read::ReadClient;
use crate::sync::MutexExt;

/// Что интерфейс читает в проекте: группа API и ресурс.
pub const RESOURCES: &[(&str, &str)] = &[
    ("", "pods"),
    ("", "pods/log"),
    ("", "events"),
    ("", "services"),
    ("discovery.k8s.io", "endpointslices"),
    ("route.openshift.io", "routes"),
    ("networking.k8s.io", "ingresses"),
    ("gateway.networking.k8s.io", "gateways"),
    ("gateway.networking.k8s.io", "httproutes"),
    ("gateway.networking.k8s.io", "grpcroutes"),
    ("", "configmaps"),
    ("", "secrets"),
    ("", "persistentvolumeclaims"),
    ("", "resourcequotas"),
    ("", "limitranges"),
    ("autoscaling", "horizontalpodautoscalers"),
    ("policy", "poddisruptionbudgets"),
    ("apps", "deployments"),
    ("apps", "statefulsets"),
    ("apps", "daemonsets"),
    ("apps", "replicasets"),
    ("apps", "controllerrevisions"),
    ("batch", "jobs"),
    ("batch", "cronjobs"),
    ("apps.openshift.io", "deploymentconfigs"),
    ("", "replicationcontrollers"),
];

/// Сколько живут права проекта в кэше.
pub const ACCESS_TTL: Duration = Duration::from_secs(5 * 60);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Verbs {
    pub get: bool,
    pub list: bool,
    pub watch: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NamespaceAccess {
    /// `ресурс.группа`, как в kubectl (`pods`, `deployments.apps`, `pods/log`) → права.
    pub resources: BTreeMap<String, Verbs>,
    /// Сервер перечислил не все права (например, решает вебхук авторизации): интерфейс тогда
    /// ничего не скрывает.
    pub incomplete: bool,
}

/// Ключ ресурса: `pods`, `deployments.apps`.
pub fn resource_key(group: &str, resource: &str) -> String {
    if group.is_empty() {
        resource.to_string()
    } else {
        format!("{resource}.{group}")
    }
}

fn names(list: &Option<Vec<String>>, value: &str) -> bool {
    list.iter().flatten().any(|x| x == "*" || x == value)
}

/// Подходит ли ресурс правила — как в RBAC Kubernetes (`ResourceMatches`): точное имя, `*` или
/// для подресурса `pods/log` — `*/log`. `pods/*` RBAC не раскрывает: это имя ресурса, а не шаблон.
fn resource_matches(list: &Option<Vec<String>>, resource: &str) -> bool {
    list.iter().flatten().any(|x| {
        x == "*"
            || x == resource
            || resource
                .split_once('/')
                .is_some_and(|(_, sub)| *x == format!("*/{sub}"))
    })
}

/// Разрешено ли `verb` на ресурс по правилам. Правила с `resourceNames` дают доступ только к
/// отдельным объектам — для списков и вкладок они не считаются.
pub fn allows(rules: &[ResourceRule], verb: &str, group: &str, resource: &str) -> bool {
    rules.iter().any(|r| {
        r.resource_names.as_ref().is_none_or(Vec::is_empty)
            && r.verbs.iter().any(|v| v == "*" || v == verb)
            && names(&r.api_groups, group)
            && resource_matches(&r.resources, resource)
    })
}

pub fn access_from_rules(rules: &[ResourceRule], incomplete: bool) -> NamespaceAccess {
    NamespaceAccess {
        resources: RESOURCES
            .iter()
            .map(|(group, resource)| {
                let can = |verb| allows(rules, verb, group, resource);
                (
                    resource_key(group, resource),
                    Verbs {
                        get: can("get"),
                        list: can("list"),
                        watch: can("watch"),
                    },
                )
            })
            .collect(),
        incomplete,
    }
}

/// Права пользователя в проекте.
pub async fn namespace_access(r: &ReadClient, namespace: &str) -> AppResult<NamespaceAccess> {
    let status = r.rules_review(namespace).await?;
    Ok(access_from_rules(&status.resource_rules, status.incomplete))
}

/// Права проектов по подключениям: живут `ACCESS_TTL`, сбрасываются при 403 в проекте и при
/// сбросе клиента подключения. Сброс подключения меняет его эпоху: права, проверка которых
/// началась до сброса, в кэш не попадают (`put`).
#[derive(Default)]
pub struct AccessCache {
    inner: Mutex<CacheInner>,
}

#[derive(Default)]
struct CacheInner {
    rights: HashMap<(String, String), (Instant, NamespaceAccess)>,
    /// Растёт при каждом `forget_connection`.
    epochs: HashMap<String, u64>,
}

impl CacheInner {
    fn epoch(&self, connection_id: &str) -> u64 {
        self.epochs.get(connection_id).copied().unwrap_or(0)
    }
}

impl AccessCache {
    pub fn get(&self, connection_id: &str, namespace: &str) -> Option<NamespaceAccess> {
        let key = (connection_id.to_string(), namespace.to_string());
        let mut inner = self.inner.lock_safe();
        match inner.rights.get(&key) {
            Some((at, a)) if at.elapsed() < ACCESS_TTL => Some(a.clone()),
            Some(_) => {
                inner.rights.remove(&key);
                None
            }
            None => None,
        }
    }

    /// Эпоха подключения: читается до проверки прав и передаётся в `put`.
    pub fn epoch(&self, connection_id: &str) -> u64 {
        self.inner.lock_safe().epoch(connection_id)
    }

    /// Кладёт права, если подключение не сбрасывали с эпохи `epoch`. `false` — сбрасывали
    /// (вход, новый токен, 401), и права, прочитанные прежним клиентом, не кэшируются.
    pub fn put(
        &self,
        connection_id: &str,
        namespace: &str,
        epoch: u64,
        access: NamespaceAccess,
    ) -> bool {
        let mut inner = self.inner.lock_safe();
        if inner.epoch(connection_id) != epoch {
            return false;
        }
        inner.rights.insert(
            (connection_id.to_string(), namespace.to_string()),
            (Instant::now(), access),
        );
        true
    }

    pub fn forget(&self, connection_id: &str, namespace: &str) {
        self.inner
            .lock_safe()
            .rights
            .remove(&(connection_id.to_string(), namespace.to_string()));
    }

    pub fn forget_connection(&self, connection_id: &str) {
        let mut inner = self.inner.lock_safe();
        inner.rights.retain(|(c, _), _| c != connection_id);
        *inner.epochs.entry(connection_id.to_string()).or_default() += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::k8s::testing::mock_client;

    fn rule(verbs: &[&str], groups: &[&str], resources: &[&str]) -> ResourceRule {
        let list = |v: &[&str]| Some(v.iter().map(|s| s.to_string()).collect());
        ResourceRule {
            verbs: verbs.iter().map(|s| s.to_string()).collect(),
            api_groups: list(groups),
            resources: list(resources),
            resource_names: None,
        }
    }

    #[test]
    fn rules_with_wildcards_subresources_and_names() {
        let rules = [
            rule(&["get", "list", "watch"], &[""], &["pods", "pods/log"]),
            rule(&["list"], &["apps"], &["*"]),
            rule(&["*"], &["batch"], &["jobs"]),
            rule(&["get"], &["*"], &["*/status"]),
            ResourceRule {
                resource_names: Some(vec!["only-this".into()]),
                ..rule(&["get", "list"], &[""], &["secrets"])
            },
        ];
        assert!(allows(&rules, "watch", "", "pods"));
        assert!(allows(&rules, "get", "", "pods/log"));
        assert!(allows(&rules, "list", "apps", "deployments"));
        assert!(!allows(&rules, "watch", "apps", "deployments"));
        assert!(allows(&rules, "watch", "batch", "jobs"));
        assert!(allows(&rules, "get", "apps", "deployments/status"));
        assert!(
            !allows(&rules, "list", "", "secrets"),
            "только отдельный объект"
        );
        assert!(!allows(&rules, "list", "", "configmaps"));
        // `*` подходит и подресурсу, а `pods/*` — нет: RBAC сравнивает его как имя.
        assert!(allows(&rules, "list", "apps", "deployments/scale"));
        let only_star = [rule(&["get"], &[""], &["pods/*"])];
        assert!(!allows(&only_star, "get", "", "pods/log"));
        assert!(!allows(&only_star, "get", "", "pods"));
        let a = access_from_rules(&rules, false);
        assert_eq!(a.resources.len(), RESOURCES.len());
        assert_eq!(
            a.resources["deployments.apps"],
            Verbs {
                get: false,
                list: true,
                watch: false
            }
        );
        assert!(a.resources["pods/log"].get);
        assert!(!a.resources["secrets"].list);
    }

    #[tokio::test]
    async fn review_of_a_namespace() {
        let (client, server) = mock_client(|_| {
            (
                201,
                serde_json::json!({"apiVersion": "authorization.k8s.io/v1",
                    "kind": "SelfSubjectRulesReview", "metadata": {}, "spec": {"namespace": "team-a"},
                    "status": {"incomplete": true, "nonResourceRules": [],
                        "resourceRules": [{"verbs": ["list"], "apiGroups": [""], "resources": ["pods"]}]}}),
            )
        });
        let r = ReadClient::new(client, Duration::from_secs(5));
        let a = namespace_access(&r, "team-a").await.unwrap();
        assert!(a.incomplete);
        assert!(a.resources["pods"].list && !a.resources["pods"].watch);
        drop(r);
        let seen = server.await.unwrap();
        assert_eq!(
            seen[0].path(),
            "/apis/authorization.k8s.io/v1/selfsubjectrulesreviews"
        );
        assert_eq!(seen[0].method, http::Method::POST);
        assert_eq!(seen[0].body["spec"]["namespace"], "team-a");
    }

    /// Сервер не вернул статус проверки — о правах ничего не известно, интерфейс ничего не
    /// скрывает.
    #[tokio::test]
    async fn review_without_a_status_hides_nothing() {
        let (client, _) = mock_client(|_| {
            (
                201,
                serde_json::json!({"apiVersion": "authorization.k8s.io/v1",
                    "kind": "SelfSubjectRulesReview", "metadata": {}, "spec": {"namespace": "team-a"}}),
            )
        });
        let r = ReadClient::new(client, Duration::from_secs(5));
        let a = namespace_access(&r, "team-a").await.unwrap();
        assert!(a.incomplete);
    }

    #[tokio::test(start_paused = true)]
    async fn cache_lives_five_minutes_and_can_be_forgotten() {
        let c = AccessCache::default();
        let a = NamespaceAccess {
            incomplete: true,
            ..Default::default()
        };
        assert!(c.put("c", "team-a", 0, a.clone()));
        assert!(c.put("c", "team-b", 0, a.clone()));
        assert!(c.put("d", "team-a", 0, a.clone()));
        assert_eq!(c.get("c", "team-a"), Some(a.clone()));
        c.forget("c", "team-b");
        assert_eq!(c.get("c", "team-b"), None);
        assert_eq!(c.epoch("c"), 0, "403 в проекте эпоху не меняет");
        tokio::time::advance(ACCESS_TTL).await;
        assert_eq!(c.get("c", "team-a"), None, "устарело");
        assert!(c.put("c", "team-a", 0, a.clone()));
        c.forget_connection("c");
        assert_eq!(c.get("c", "team-a"), None);
        assert!(
            c.inner.lock_safe().rights.len() == 1,
            "права другого подключения остались"
        );
    }

    /// Права, проверка которых началась до сброса подключения, в кэш не попадают; права
    /// другого подключения и новая проверка — попадают.
    #[test]
    fn rights_read_before_a_reset_are_not_cached() {
        let c = AccessCache::default();
        let a = NamespaceAccess::default();
        let (before, other) = (c.epoch("c"), c.epoch("d"));
        c.forget_connection("c");
        assert!(!c.put("c", "team-a", before, a.clone()));
        assert_eq!(c.get("c", "team-a"), None);
        assert!(c.put("d", "team-a", other, a.clone()));
        assert!(c.put("c", "team-a", c.epoch("c"), a.clone()));
        assert_eq!(c.get("c", "team-a"), Some(a));
    }
}
