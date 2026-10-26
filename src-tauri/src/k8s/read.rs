//! Чтение кластера (спец. §4.1): только get, list, watch, логи, GET отдельных путей API-сервера
//! и проверки прав.
//! Внутренний `kube::Client` наружу не отдаётся — через `ReadClient` кластер не изменить. Им же
//! будет пользоваться MCP-шлюз (этап O2). Каждый запрос ограничен таймаутом профиля; списки
//! читаются страницами (спец. §6.7). Ошибки запросов собирает `observe`: так чтение команды
//! (`AppState::read`) узнаёт и о тех, что легли в часть экрана, а не в её итог.

use std::cell::RefCell;
use std::fmt::Debug;
use std::future::Future;
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

use crate::error::{AppError, AppResult, ErrorCode};
use crate::k8s::cluster::timed;
use crate::k8s::errors::breaks_client;

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

/// Ошибки запросов `ReadClient` за время `observe`: те, по которым чтение команды что-то
/// делает. Части экрана (`Part`) и виды списка, которые не прочитались, приходят данными,
/// а не ошибкой команды; без этого их 403 не сбросил бы кэш прав проекта, а 401 — клиент.
#[derive(Debug, Default)]
pub struct Observed {
    /// Был ответ 403.
    pub forbidden: bool,
    /// Первая ошибка, с которой клиент дальше не работает (`breaks_client`).
    pub broken: Option<AppError>,
}

impl Observed {
    fn note(&mut self, e: &AppError) {
        self.forbidden |= e.code == ErrorCode::Forbidden;
        if self.broken.is_none() && breaks_client(e.code) {
            self.broken = Some(e.clone());
        }
    }
}

tokio::task_local! {
    static OBSERVED: RefCell<Observed>;
}

/// `fut` и ошибки запросов `ReadClient`, которые в нём были. Видны запросы той же задачи tokio
/// (`join_all`, `tokio::join!`), а не запущенных из неё через `tokio::spawn`. Вложенный
/// `observe` собирает свои ошибки сам, внешний их не видит.
pub async fn observe<F: Future>(fut: F) -> (F::Output, Observed) {
    OBSERVED
        .scope(RefCell::new(Observed::default()), async {
            let out = fut.await;
            (out, OBSERVED.with(RefCell::take))
        })
        .await
}

/// Отмечает ошибку запроса для `observe`; вне его ничего не делает. Ответ 404 у `get_opt` —
/// не ошибка, а 404 и остальные коды, кроме 403 и `breaks_client`, не отмечаются.
fn noted<T>(r: AppResult<T>) -> AppResult<T> {
    if let Err(e) = &r {
        let _ = OBSERVED.try_with(|o| o.borrow_mut().note(e));
    }
    r
}

/// Проект (namespace) запроса — имя DNS-1123 (`is_valid_namespace`). `kube` подставляет его в
/// путь как есть, без кодирования: `a/../b` увёл бы запрос по другому пути, а прокси,
/// который нормализует пути, — к другому объекту.
pub fn check_namespace(namespace: &str) -> AppResult<()> {
    if crate::storage::profile::is_valid_namespace(namespace) {
        return Ok(());
    }
    Err(crate::app_err!(
        ErrorCode::InvalidInput,
        "errors.connections.badNamespace",
        "“{{name}}” is not a valid project name: use lowercase letters, digits and “-”",
        name = namespace.escape_debug()
    ))
}

/// Проект, проверенный `check_namespace`: потоки watch открываются только с ним — вернуть ошибку
/// до первого запроса поток не может, поэтому проверка — при создании `Namespace`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Namespace(String);

impl Namespace {
    pub fn new(name: &str) -> AppResult<Self> {
        check_namespace(name)?;
        Ok(Self(name.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Путь API-сервера для `get_text`: абсолютный, печатаемый ASCII без `..`, `//`, `\`, `%` и `#`
/// (запрос после `?` разрешён). Пути задаёт код (`/readyz?verbose`), а не пользователь: проверка
/// страхует, что путь не уведёт запрос к другому ресурсу.
pub fn check_path(path_and_query: &str) -> AppResult<()> {
    let path = path_and_query
        .split_once('?')
        .map_or(path_and_query, |(p, _)| p);
    let ok = path.starts_with('/')
        && !path.contains("..")
        && !path.contains("//")
        && path_and_query
            .bytes()
            .all(|b| b.is_ascii_graphic() && !b"\\%#".contains(&b));
    if ok {
        return Ok(());
    }
    Err(AppError::internal_with(format!(
        "refused API path {}",
        path_and_query.escape_debug()
    )))
}

/// Имя объекта запроса: не пустое и не `.`, печатаемый ASCII без `/`, `\`, `%`, `?`, `#`, `;`
/// и без `..`. `ReadClient` вида не знает, поэтому правило — самое мягкое из правил имён
/// Kubernetes (`path.IsValidPathSegmentName`: не `.` и `..`, без `/` и `%`; под него подходит и
/// `sha256~…` у `UserOAuthAccessToken`, который DNS-1123 не пропустил бы) и сверх него то, что
/// по-своему понимают URL (`?`, `#`) и нормализующие прокси (`\`, `..;`, пробелы, не-ASCII).
pub fn check_name(name: &str) -> AppResult<()> {
    let ok = !name.is_empty()
        && name != "."
        && !name.contains("..")
        && name
            .bytes()
            .all(|b| b.is_ascii_graphic() && !b"/\\%?#;".contains(&b));
    if ok {
        return Ok(());
    }
    Err(bad_name(name))
}

/// Вид или имя объекта для `fieldSelector` (`involvedObject.name=…`): имя по `check_name` и без
/// `,` и `=` — они добавили бы к выборке свои условия.
pub fn check_field_value(value: &str) -> AppResult<()> {
    if value.contains([',', '=']) {
        return Err(bad_name(value));
    }
    check_name(value)
}

fn bad_name(name: &str) -> AppError {
    crate::app_err!(
        ErrorCode::InvalidInput,
        "errors.k8s.badName",
        "“{{name}}” is not a valid object name",
        name = name.escape_debug()
    )
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

    /// Запрос с таймаутом профиля; его ошибка отмечается для `observe`. Через него идут все
    /// запросы клиента, кроме watch: сессии watch идут в своих задачах, вне `observe`.
    async fn call<T>(&self, fut: impl Future<Output = kube::Result<T>>) -> AppResult<T> {
        noted(timed(self.timeout, fut).await)
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
        if let Some(ns) = namespace {
            check_namespace(ns)?;
        }
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
        if let Some(ns) = namespace {
            check_namespace(ns)?;
        }
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
            let list: ObjectList<K> = self.call(api.list(&page)).await?;
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
        check_namespace(namespace)?;
        check_name(name)?;
        let api: Api<K> = Api::namespaced(self.client.clone(), namespace);
        self.call(api.get(name)).await
    }

    /// Объект или `None`, если его нет (404).
    pub async fn get_opt<K>(&self, namespace: &str, name: &str) -> AppResult<Option<K>>
    where
        K: Resource<Scope = NamespaceResourceScope, DynamicType = ()>
            + Clone
            + DeserializeOwned
            + Debug,
    {
        check_namespace(namespace)?;
        check_name(name)?;
        let api: Api<K> = Api::namespaced(self.client.clone(), namespace);
        self.call(api.get_opt(name)).await
    }

    pub async fn get_cluster<K>(&self, name: &str) -> AppResult<K>
    where
        K: Resource<Scope = ClusterResourceScope, DynamicType = ()>
            + Clone
            + DeserializeOwned
            + Debug,
    {
        check_name(name)?;
        let api: Api<K> = Api::all(self.client.clone());
        self.call(api.get(name)).await
    }

    pub async fn get_dynamic(
        &self,
        ar: &ApiResource,
        namespace: Option<&str>,
        name: &str,
    ) -> AppResult<DynamicObject> {
        if let Some(ns) = namespace {
            check_namespace(ns)?;
        }
        check_name(name)?;
        self.call(self.dynamic(ar, namespace).get(name)).await
    }

    /// Тело ответа на GET к пути API-сервера, например `/readyz?verbose`. Ответ с ошибкой
    /// приходит ошибкой, текст тела — в её сообщении. Путь проверяется до запроса (`check_path`).
    pub async fn get_text(&self, path_and_query: &str) -> AppResult<String> {
        check_path(path_and_query)?;
        let request = http::Request::get(path_and_query)
            .body(Vec::new())
            .map_err(|e| crate::error::AppError::internal_with(e.to_string()))?;
        self.call(self.client.request_text(request)).await
    }

    /// Изменения объектов namespace (`kube::runtime::watcher`): список, затем watch; после
    /// разрыва watcher сам читает список заново. Таймаут к потоку не применяется. Ошибку
    /// поток вернуть не может: namespace проверен заранее (`Namespace`).
    pub fn watch<K>(
        &self,
        namespace: &Namespace,
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
        watcher(
            Api::namespaced(self.client.clone(), namespace.as_str()),
            config,
        )
    }

    /// То же для вида без типа в `k8s-openapi`.
    pub fn watch_dynamic(
        &self,
        ar: &ApiResource,
        namespace: &Namespace,
        config: watcher::Config,
    ) -> impl Stream<Item = watcher::Result<watcher::Event<DynamicObject>>> + Send + use<> {
        watcher(self.dynamic(ar, Some(namespace.as_str())), config)
    }

    /// Права пользователя в namespace (`SelfSubjectRulesReview`). Проверка создаётся запросом
    /// POST, но в кластере ничего не меняет.
    pub async fn rules_review(&self, namespace: &str) -> AppResult<SubjectRulesReviewStatus> {
        check_namespace(namespace)?;
        let api: Api<SelfSubjectRulesReview> = Api::all(self.client.clone());
        let review = SelfSubjectRulesReview {
            spec: SelfSubjectRulesReviewSpec {
                namespace: Some(namespace.to_string()),
            },
            ..Default::default()
        };
        let r = self
            .call(api.create(&PostParams::default(), &review))
            .await?;
        // Без статуса о правах ничего не известно: `incomplete`, и интерфейс ничего не скрывает.
        Ok(r.status.unwrap_or_else(|| SubjectRulesReviewStatus {
            incomplete: true,
            ..Default::default()
        }))
    }

    /// Имена проектов (или namespace): первые 500 и признак «есть ещё».
    pub async fn project_names(&self, projects_api: bool) -> AppResult<(Vec<String>, bool)> {
        noted(
            crate::k8s::cluster::list_project_names(&self.client, self.timeout, projects_api).await,
        )
    }

    /// Поток строк лога контейнера; таймаут — только на открытие потока.
    pub async fn log_stream(
        &self,
        namespace: &str,
        pod: &str,
        params: &LogParams,
    ) -> AppResult<impl AsyncBufRead + use<>> {
        check_namespace(namespace)?;
        check_name(pod)?;
        let api: Api<k8s_openapi::api::core::v1::Pod> =
            Api::namespaced(self.client.clone(), namespace);
        self.call(api.log_stream(pod, params)).await
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

    /// Ошибки запросов собирает `observe`, и параллельных (`tokio::join!`) тоже; 404 и `None`
    /// у `get_opt` не отмечаются, а вне `observe` отметка ничего не делает.
    #[tokio::test]
    async fn errors_of_requests_are_observed_only_inside_observe() {
        let (client, _server) = mock_client(|s| match s.path() {
            "/api/v1/namespaces/team-a/pods" => status(
                403,
                "Forbidden",
                r#"pods is forbidden: User "u" cannot list resource "pods" in API group "" in the namespace "team-a""#,
            ),
            "/api/v1/namespaces/team-a/configmaps" => status(401, "Unauthorized", "Unauthorized"),
            _ => status(404, "NotFound", "configmaps \"x\" not found"),
        });
        let (r, lp) = (reader(client), ListParams::default());
        let e = r.list::<Pod>(Some("team-a"), &lp).await.unwrap_err();
        assert_eq!(e.code, ErrorCode::Forbidden, "вне observe — просто ошибка");

        let ((pods, maps, missing), seen) = observe(async {
            tokio::join!(
                r.list::<Pod>(Some("team-a"), &lp),
                r.list::<ConfigMap>(Some("team-a"), &lp),
                r.get_opt::<ConfigMap>("team-a", "x"),
            )
        })
        .await;
        assert!(pods.is_err() && maps.is_err());
        assert!(missing.unwrap().is_none());
        assert!(seen.forbidden);
        assert_eq!(seen.broken.unwrap().code, ErrorCode::SessionExpired);

        let (missing, seen) = observe(r.get::<ConfigMap>("team-a", "x")).await;
        assert_eq!(missing.unwrap_err().code, ErrorCode::NotFound);
        assert!(
            !seen.forbidden && seen.broken.is_none(),
            "404 не отмечается"
        );
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

    #[test]
    fn names_and_namespaces_that_may_go_into_a_path() {
        for ok in ["team-a", "a", "0abc"] {
            assert!(check_namespace(ok).is_ok(), "{ok}");
        }
        for bad in [
            "",
            "team-a/../kube-system",
            "a/b",
            "..",
            "Team",
            "a.b",
            "a%2Fb",
            "a?b",
            &"a".repeat(64),
        ] {
            let e = check_namespace(bad).unwrap_err();
            assert_eq!(e.code, ErrorCode::InvalidInput, "{bad}");
        }
        for ok in [
            "web",
            "web-7d9f-x",
            "app.v1",
            "Web",
            "system:node",
            "sha256~AbC-_x",
        ] {
            assert!(check_name(ok).is_ok(), "{ok}");
        }
        for bad in [
            "",
            ".",
            "..",
            "a..b",
            "../secrets/db",
            "a/b",
            "a\\b",
            "a%2Fb",
            "a?watch=1",
            "a#b",
            "x;y",
            "a b",
            "a\nb",
            "a\u{0}",
            "веб",
            "a\u{ff0f}b",
        ] {
            let e = check_name(bad).unwrap_err();
            assert_eq!(
                (e.code, e.key.as_str()),
                (ErrorCode::InvalidInput, "errors.k8s.badName"),
                "{bad:?}"
            );
        }
        assert_eq!(check_name("a\nb").unwrap_err().params["name"], "a\\nb");
    }

    /// Имя или namespace, которые не прошли проверку, в запрос не уходят ни одним методом;
    /// правильные доходят до API-сервера как есть.
    #[tokio::test]
    async fn rejected_names_send_no_request() {
        let (client, server) = mock_client(|_| {
            (
                200,
                serde_json::json!({"apiVersion": "v1", "kind": "ConfigMap",
                    "metadata": {"name": "web", "namespace": "team-a"}}),
            )
        });
        let r = reader(client);
        let cm = ApiResource::erase::<ConfigMap>(&());
        let params = ListParams::default();
        let codes = [
            r.get::<ConfigMap>("team-a", "../secrets/db").await.err(),
            r.get::<ConfigMap>("team-a/../kube-system", "web")
                .await
                .err(),
            r.get_opt::<ConfigMap>("team-a", "..").await.err(),
            r.get_cluster::<Node>("a/b").await.err(),
            r.get_dynamic(&cm, Some("a/b"), "web").await.err(),
            r.get_dynamic(&cm, None, "a?b").await.err(),
            r.list::<Pod>(Some("a/b"), &params).await.err(),
            r.list_dynamic(&cm, Some(".."), &params).await.err(),
            r.log_stream("team-a", "../x", &LogParams::default())
                .await
                .err(),
            r.rules_review("a/b").await.err(),
            r.get_text("/api/v1/namespaces/a/../../secrets").await.err(),
            r.get_text("readyz").await.err(),
            r.get_text("/readyz?x#y").await.err(),
        ]
        .map(|e| e.map(|e| e.code));
        assert!(
            codes[..10]
                .iter()
                .all(|c| *c == Some(ErrorCode::InvalidInput)),
            "{codes:?}"
        );
        assert!(
            codes[10..].iter().all(|c| *c == Some(ErrorCode::Internal)),
            "пути задаёт код: отказ — внутренняя ошибка, {codes:?}"
        );
        assert!(Namespace::new("team-a/../kube-system").is_err());
        assert_eq!(Namespace::new("team-a").unwrap().as_str(), "team-a");
        let ok: ConfigMap = r.get("team-a", "web").await.unwrap();
        assert_eq!(ok.metadata.name.as_deref(), Some("web"));
        drop(r);
        let seen = server.await.unwrap();
        assert_eq!(seen.len(), 1, "{seen:?}");
        assert_eq!(seen[0].path(), "/api/v1/namespaces/team-a/configmaps/web");
    }
}
