//! Чтение кластера (спец. §4.1): только get, list, watch, логи, GET отдельных путей API-сервера
//! и проверки прав.
//! Внутренний `kube::Client` наружу не отдаётся — через `ReadClient` кластер не изменить. Им же
//! будет пользоваться MCP-шлюз (этап O2). Каждый запрос ограничен таймаутом профиля; списки
//! читаются страницами (спец. §6.7).

use std::fmt::Debug;
use std::time::Duration;

use futures::{AsyncBufRead, Stream};
use k8s_openapi::api::authorization::v1::{
    SelfSubjectRulesReview, SelfSubjectRulesReviewSpec, SubjectRulesReviewStatus,
};
use k8s_openapi::{ClusterResourceScope, NamespaceResourceScope};
use kube::api::{Api, ApiResource, DynamicObject, ListParams, LogParams, ObjectList, PostParams};
use kube::core::Resource;
use kube::runtime::watcher;
use serde::de::DeserializeOwned;

use crate::error::AppResult;
use crate::k8s::cluster::timed;

/// Сколько объектов на странице списка.
pub const PAGE_SIZE: u32 = 500;
/// Сверх страниц, которых хватило бы на весь список, читается не больше стольких: сервер,
/// который отдаёт `continue` с пустыми или неполными страницами, не зациклит чтение.
const EXTRA_PAGES: usize = 5;
/// Больше стольких объектов один список не читает: остальное — `truncated`.
pub const MAX_ITEMS: usize = 10_000;

/// Прочитанный список: объекты и признак, что прочитано не всё (`MAX_ITEMS`).
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Listed<K> {
    pub items: Vec<K>,
    pub truncated: bool,
}

impl<K> Listed<K> {
    pub fn map<T>(self, f: impl FnMut(K) -> T) -> Listed<T> {
        Listed {
            items: self.items.into_iter().map(f).collect(),
            truncated: self.truncated,
        }
    }
}

/// Клиент только для чтения поверх `kube::Client` подключения.
#[derive(Clone)]
pub struct ReadClient {
    client: kube::Client,
    timeout: Duration,
}

impl ReadClient {
    pub fn new(client: kube::Client, timeout: Duration) -> Self {
        Self { client, timeout }
    }

    /// Таймаут одного запроса (из профиля).
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Объекты в namespace (`Some`) или во всех namespace (`None`).
    pub async fn list<K>(
        &self,
        namespace: Option<&str>,
        params: &ListParams,
    ) -> AppResult<Listed<K>>
    where
        K: Resource<Scope = NamespaceResourceScope, DynamicType = ()>
            + Clone
            + DeserializeOwned
            + Debug,
    {
        let api = match namespace {
            Some(ns) => Api::namespaced(self.client.clone(), ns),
            None => Api::all(self.client.clone()),
        };
        self.pages(&api, params, MAX_ITEMS).await
    }

    /// Объекты уровня кластера (ноды, классы Ingress).
    pub async fn list_cluster<K>(&self, params: &ListParams) -> AppResult<Listed<K>>
    where
        K: Resource<Scope = ClusterResourceScope, DynamicType = ()>
            + Clone
            + DeserializeOwned
            + Debug,
    {
        self.pages(&Api::all(self.client.clone()), params, MAX_ITEMS)
            .await
    }

    /// Объекты вида, которого нет в `k8s-openapi` (OpenShift, Gateway API): в namespace или,
    /// при `None`, во всех namespace или на уровне кластера.
    pub async fn list_dynamic(
        &self,
        ar: &ApiResource,
        namespace: Option<&str>,
        params: &ListParams,
    ) -> AppResult<Listed<DynamicObject>> {
        self.pages(&self.dynamic(ar, namespace), params, MAX_ITEMS)
            .await
    }

    fn dynamic(&self, ar: &ApiResource, namespace: Option<&str>) -> Api<DynamicObject> {
        match namespace {
            Some(ns) => Api::namespaced_with(self.client.clone(), ns, ar),
            None => Api::all_with(self.client.clone(), ar),
        }
    }

    /// Страницы по `PAGE_SIZE`, пока сервер отдаёт `continue` и не набрано `max` объектов, —
    /// но не больше, чем нужно на `max` объектов, и `EXTRA_PAGES` сверх того: дальше список
    /// `truncated`.
    async fn pages<K>(&self, api: &Api<K>, params: &ListParams, max: usize) -> AppResult<Listed<K>>
    where
        K: Resource + Clone + DeserializeOwned + Debug,
    {
        let mut items = Vec::new();
        let mut token: Option<String> = None;
        for _ in 0..max.div_ceil(PAGE_SIZE as usize) + EXTRA_PAGES {
            let mut page = params.clone().limit(PAGE_SIZE);
            if let Some(t) = &token {
                page = page.continue_token(t);
            }
            let list: ObjectList<K> = timed(self.timeout, api.list(&page)).await?;
            items.extend(list.items);
            token = list.metadata.continue_.filter(|t| !t.is_empty());
            if items.len() >= max {
                let truncated = items.len() > max || token.is_some();
                items.truncate(max);
                return Ok(Listed { items, truncated });
            }
            if token.is_none() {
                return Ok(Listed {
                    items,
                    truncated: false,
                });
            }
        }
        // Страниц прочитано с запасом, а сервер всё ещё отдаёт `continue`.
        Ok(Listed {
            items,
            truncated: true,
        })
    }

    pub async fn get<K>(&self, namespace: &str, name: &str) -> AppResult<K>
    where
        K: Resource<Scope = NamespaceResourceScope, DynamicType = ()>
            + Clone
            + DeserializeOwned
            + Debug,
    {
        let api: Api<K> = Api::namespaced(self.client.clone(), namespace);
        timed(self.timeout, api.get(name)).await
    }

    /// Объект или `None`, если его нет (404).
    pub async fn get_opt<K>(&self, namespace: &str, name: &str) -> AppResult<Option<K>>
    where
        K: Resource<Scope = NamespaceResourceScope, DynamicType = ()>
            + Clone
            + DeserializeOwned
            + Debug,
    {
        let api: Api<K> = Api::namespaced(self.client.clone(), namespace);
        timed(self.timeout, api.get_opt(name)).await
    }

    pub async fn get_cluster<K>(&self, name: &str) -> AppResult<K>
    where
        K: Resource<Scope = ClusterResourceScope, DynamicType = ()>
            + Clone
            + DeserializeOwned
            + Debug,
    {
        let api: Api<K> = Api::all(self.client.clone());
        timed(self.timeout, api.get(name)).await
    }

    pub async fn get_dynamic(
        &self,
        ar: &ApiResource,
        namespace: Option<&str>,
        name: &str,
    ) -> AppResult<DynamicObject> {
        timed(self.timeout, self.dynamic(ar, namespace).get(name)).await
    }

    /// Тело ответа на GET к пути API-сервера, например `/readyz?verbose`. Ответ с ошибкой
    /// приходит ошибкой, текст тела — в её сообщении.
    pub async fn get_text(&self, path_and_query: &str) -> AppResult<String> {
        let request = http::Request::get(path_and_query)
            .body(Vec::new())
            .map_err(|e| crate::error::AppError::internal_with(e.to_string()))?;
        timed(self.timeout, self.client.request_text(request)).await
    }

    /// Изменения объектов namespace (`kube::runtime::watcher`): список, затем watch; после
    /// разрыва watcher сам читает список заново. Таймаут к потоку не применяется.
    pub fn watch<K>(
        &self,
        namespace: &str,
        config: watcher::Config,
    ) -> impl Stream<Item = watcher::Result<watcher::Event<K>>> + Send + use<K>
    where
        K: Resource<Scope = NamespaceResourceScope, DynamicType = ()>
            + Clone
            + DeserializeOwned
            + Debug
            + Send
            + 'static,
    {
        watcher(Api::namespaced(self.client.clone(), namespace), config)
    }

    /// То же для вида без типа в `k8s-openapi`.
    pub fn watch_dynamic(
        &self,
        ar: &ApiResource,
        namespace: &str,
        config: watcher::Config,
    ) -> impl Stream<Item = watcher::Result<watcher::Event<DynamicObject>>> + Send + use<> {
        watcher(self.dynamic(ar, Some(namespace)), config)
    }

    /// Права пользователя в namespace (`SelfSubjectRulesReview`). Проверка создаётся запросом
    /// POST, но в кластере ничего не меняет.
    pub async fn rules_review(&self, namespace: &str) -> AppResult<SubjectRulesReviewStatus> {
        let api: Api<SelfSubjectRulesReview> = Api::all(self.client.clone());
        let review = SelfSubjectRulesReview {
            spec: SelfSubjectRulesReviewSpec {
                namespace: Some(namespace.to_string()),
            },
            ..Default::default()
        };
        let r = timed(self.timeout, api.create(&PostParams::default(), &review)).await?;
        // Без статуса о правах ничего не известно: `incomplete`, и интерфейс ничего не скрывает.
        Ok(r.status.unwrap_or_else(|| SubjectRulesReviewStatus {
            incomplete: true,
            ..Default::default()
        }))
    }

    /// Имена проектов (или namespace): первые 500 и признак «есть ещё».
    pub async fn project_names(&self, projects_api: bool) -> AppResult<(Vec<String>, bool)> {
        crate::k8s::cluster::list_project_names(&self.client, self.timeout, projects_api).await
    }

    /// Поток строк лога контейнера; таймаут — только на открытие потока.
    pub async fn log_stream(
        &self,
        namespace: &str,
        pod: &str,
        params: &LogParams,
    ) -> AppResult<impl AsyncBufRead + use<>> {
        let api: Api<k8s_openapi::api::core::v1::Pod> =
            Api::namespaced(self.client.clone(), namespace);
        timed(self.timeout, api.log_stream(pod, params)).await
    }
}

/// Пустой `ListParams` с селектором меток.
pub fn by_labels(selector: Option<&str>) -> ListParams {
    match selector.filter(|s| !s.is_empty()) {
        Some(s) => ListParams::default().labels(s),
        None => ListParams::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;
    use crate::k8s::testing::{mock_client, mock_raw, status};
    use k8s_openapi::api::core::v1::{ConfigMap, Node, Pod};

    fn reader(client: kube::Client) -> ReadClient {
        ReadClient::new(client, Duration::from_secs(5))
    }

    fn pods(names: &[&str], next: Option<&str>) -> (u16, serde_json::Value) {
        let items: Vec<_> = names
            .iter()
            .map(|n| serde_json::json!({"metadata": {"name": n, "namespace": "team-a"}}))
            .collect();
        (
            200,
            serde_json::json!({"apiVersion": "v1", "kind": "PodList",
                "metadata": {"continue": next.unwrap_or("")}, "items": items}),
        )
    }

    fn names(l: &Listed<Pod>) -> Vec<String> {
        l.items
            .iter()
            .map(|p| p.metadata.name.clone().unwrap())
            .collect()
    }

    #[tokio::test]
    async fn list_reads_every_page() {
        let (client, server) = mock_client(|s| {
            if s.uri.contains("continue=t1") {
                pods(&["c"], None)
            } else {
                pods(&["a", "b"], Some("t1"))
            }
        });
        let l: Listed<Pod> = reader(client)
            .list(Some("team-a"), &by_labels(Some("app=web")))
            .await
            .unwrap();
        assert_eq!(names(&l), ["a", "b", "c"]);
        assert!(!l.truncated);
        let seen = server.await.unwrap();
        assert_eq!(seen.len(), 2);
        for s in &seen {
            assert_eq!(s.path(), "/api/v1/namespaces/team-a/pods");
            assert!(s.uri.contains("limit=500"), "{}", s.uri);
            assert!(s.uri.contains("labelSelector=app%3Dweb"), "{}", s.uri);
        }
    }

    #[tokio::test]
    async fn list_stops_at_the_limit() {
        let (client, server) = mock_client(|s| {
            if s.uri.contains("continue=t1") {
                pods(&["c", "d"], Some("t2"))
            } else {
                pods(&["a", "b"], Some("t1"))
            }
        });
        let api: Api<Pod> = Api::all(client.clone());
        let l = reader(client)
            .pages(&api, &ListParams::default(), 3)
            .await
            .unwrap();
        assert_eq!(names(&l), ["a", "b", "c"]);
        assert!(l.truncated);
        drop(api);
        assert_eq!(server.await.unwrap().len(), 2, "третью страницу не читали");
        let (client, _) = mock_client(|_| pods(&["a", "b", "c"], None));
        let api: Api<Pod> = Api::all(client.clone());
        let l = reader(client)
            .pages(&api, &ListParams::default(), 3)
            .await
            .unwrap();
        assert!(!l.truncated, "ровно столько, сколько можно");
    }

    #[tokio::test]
    async fn endless_continue_stops_after_spare_pages() {
        let (client, server) = mock_client(|_| pods(&[], Some("again")));
        let (r, params) = (reader(client), ListParams::default());
        let l = tokio::time::timeout(Duration::from_secs(10), r.list::<Pod>(None, &params))
            .await
            .expect("чтение списка остановилось")
            .unwrap();
        drop(r);
        assert!(l.items.is_empty());
        assert!(l.truncated, "прочитано не всё");
        assert_eq!(
            server.await.unwrap().len(),
            MAX_ITEMS / PAGE_SIZE as usize + EXTRA_PAGES
        );
    }

    #[tokio::test]
    async fn cluster_lists_and_missing_objects() {
        let (client, server) = mock_client(|s| match s.path() {
            "/api/v1/nodes" => (
                200,
                serde_json::json!({"apiVersion": "v1", "kind": "NodeList", "metadata": {},
                    "items": [{"metadata": {"name": "n1"}}]}),
            ),
            _ => status(404, "NotFound", "configmaps \"x\" not found"),
        });
        let r = reader(client);
        let nodes: Listed<Node> = r.list_cluster(&ListParams::default()).await.unwrap();
        assert_eq!(nodes.items.len(), 1);
        assert!(
            r.get_opt::<ConfigMap>("team-a", "x")
                .await
                .unwrap()
                .is_none()
        );
        let e = r.get::<ConfigMap>("team-a", "x").await.unwrap_err();
        assert_eq!(e.code, ErrorCode::NotFound);
        drop(r);
        let seen = server.await.unwrap();
        assert_eq!(seen[1].path(), "/api/v1/namespaces/team-a/configmaps/x");
    }

    #[tokio::test]
    async fn dynamic_objects_of_a_namespace_and_of_all() {
        let ar = ApiResource::from_gvk_with_plural(
            &kube::api::GroupVersionKind::gvk("route.openshift.io", "v1", "Route"),
            "routes",
        );
        let (client, server) = mock_client(|_| {
            (
                200,
                serde_json::json!({"apiVersion": "route.openshift.io/v1", "kind": "RouteList",
                    "metadata": {}, "items": [{"metadata": {"name": "web"}}]}),
            )
        });
        let r = reader(client);
        assert_eq!(
            r.list_dynamic(&ar, Some("team-a"), &ListParams::default())
                .await
                .unwrap()
                .items
                .len(),
            1
        );
        r.list_dynamic(&ar, None, &ListParams::default())
            .await
            .unwrap();
        drop(r);
        let seen = server.await.unwrap();
        assert_eq!(
            seen[0].path(),
            "/apis/route.openshift.io/v1/namespaces/team-a/routes"
        );
        assert_eq!(seen[1].path(), "/apis/route.openshift.io/v1/routes");
    }

    #[tokio::test]
    async fn text_of_a_path_and_the_text_of_its_error() {
        let (client, _) = mock_raw(|s| match s.uri.as_str() {
            "/readyz?verbose" => (200, b"[+]ping ok\nreadyz check passed\n".to_vec()),
            _ => (
                500,
                b"[+]ping ok\n[-]etcd failed: reason withheld\n".to_vec(),
            ),
        });
        let r = reader(client);
        assert!(
            r.get_text("/readyz?verbose")
                .await
                .unwrap()
                .contains("passed")
        );
        let e = r.get_text("/livez?verbose").await.unwrap_err();
        assert_eq!(e.code, ErrorCode::ConnectionFailed);
        assert!(e.params["message"].contains("[-]etcd failed"), "{e:?}");
    }
}
