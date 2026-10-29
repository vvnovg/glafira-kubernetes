//! Запуск проверки (спец. §8.1): одна проверка на подключение одновременно — второй вызов ждёт
//! идущую и получает её результат. После проверки и любого изменения замечаний — уведомление
//! интерфейсу (`health://changed`).

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use futures::future::BoxFuture;
use tokio::sync::watch;

use super::collect::{LIMITS, collect};
use super::rules;
use super::snapshot::ClusterSnapshot;
use super::store::{CheckRun, HealthStore, Scope, Trigger, connection_not_found};
use crate::commands::blocking;
use crate::error::{AppError, AppResult, ErrorCode};
use crate::state::AppState;
use crate::storage::profile::Environment;
use crate::sync::{MutexExt, RwLockExt};

pub type Notify = Arc<dyn Fn(&str) + Send + Sync>;

/// Сбор снимка по проектам проверки (`None` — все доступные). В приложении — чтение кластера
/// подключения (`collector`), в тестах — подделка.
pub type Collector = Arc<
    dyn Fn(Option<Vec<String>>) -> BoxFuture<'static, AppResult<ClusterSnapshot>> + Send + Sync,
>;

type Outcome = Option<Result<Arc<CheckRun>, AppError>>;

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

/// Прошлая проверка охватывает проекты: все (`None`) — только проверка всех; названные — если
/// каждый из них проверен.
pub fn covers(scope: &Scope, requested: Option<&[String]>) -> bool {
    match requested {
        None => scope.requested.is_none(),
        Some(list) => list.iter().all(|ns| scope.projects.contains(ns)),
    }
}

/// Идущая проверка: окружение, под которым считается строгость, её проекты и канал результата.
struct Running {
    env: Environment,
    requested: Option<Vec<String>>,
    rx: watch::Receiver<Outcome>,
}

/// Снимает проверку с учёта `running`, но только свою запись (по каналу): запись проверки,
/// зарегистрированной следом с тем же id, не трогается. Сбрасывается явно перед отправкой
/// результата, а при панике задачи — при раскрутке стека.
struct Unregister {
    service: Arc<HealthService>,
    id: String,
    rx: watch::Receiver<Outcome>,
}

impl Drop for Unregister {
    fn drop(&mut self) {
        let mut running = self.service.running.lock_safe();
        if running
            .get(&self.id)
            .is_some_and(|r| r.rx.same_channel(&self.rx))
        {
            running.remove(&self.id);
        }
    }
}

pub struct HealthService {
    pub store: HealthStore,
    running: Mutex<HashMap<String, Running>>,
    /// Поколение данных подключения: растёт при удалении (`forget`), нет записи — 0. Идущая проверка
    /// сохраняет результат, только если поколение с её начала не изменилось.
    generations: Mutex<HashMap<String, u64>>,
    notify: RwLock<Option<Notify>>,
}

impl HealthService {
    pub fn open(data_dir: &Path) -> Self {
        Self {
            store: HealthStore::open(data_dir),
            running: Mutex::new(HashMap::new()),
            generations: Mutex::new(HashMap::new()),
            notify: RwLock::new(None),
        }
    }

    pub fn set_notify(&self, f: Notify) {
        *self.notify.write_safe() = Some(f);
    }

    /// Подключение удалено: данные проверки стираются, идущая проверка своё не сохранит.
    pub fn forget(&self, connection_id: &str) -> AppResult<()> {
        let mut generations = self.generations.lock_safe();
        *generations.entry(connection_id.to_string()).or_insert(0) += 1;
        self.store.delete_connection(connection_id)
    }

    /// Поколение данных подключения: читается до проверки существования подключения, чтобы
    /// `write` отказал, если подключение удалили в промежутке.
    pub fn generation(&self, connection_id: &str) -> u64 {
        self.generations
            .lock_safe()
            .get(connection_id)
            .copied()
            .unwrap_or(0)
    }

    /// Единственный путь записи в `health/<id>.json`: под тем же замком, что и `forget`, и только
    /// если подключение не удаляли с момента чтения `generation` — иначе файл удалённого
    /// подключения появился бы заново.
    pub fn write<T>(
        &self,
        connection_id: &str,
        generation: u64,
        f: impl FnOnce(&HealthStore) -> AppResult<T>,
    ) -> AppResult<T> {
        let generations = self.generations.lock_safe();
        if generations.get(connection_id).copied().unwrap_or(0) != generation {
            return Err(connection_not_found(connection_id));
        }
        let out = f(&self.store);
        drop(generations);
        out
    }

    /// Состояние подключения изменилось (проверка, скрытие, замечания).
    pub fn changed(&self, connection_id: &str) {
        let f = self.notify.read_safe().clone();
        if let Some(f) = f {
            f(connection_id);
        }
    }

    /// Новая проверка подключения (или ожидание уже идущей с теми же проектами).
    pub async fn run(
        self: &Arc<Self>,
        state: &AppState,
        connection_id: &str,
        trigger: Trigger,
        requested: Option<Vec<String>>,
    ) -> AppResult<Arc<CheckRun>> {
        // Поколение — до проверки подключения, как в `commands::health::change`: подключение,
        // удалённое в промежутке, своего файла обратно не получит.
        let generation = self.generation(connection_id);
        let env = state.connections.get(connection_id)?.environment;
        self.run_with(
            connection_id,
            generation,
            env,
            trigger,
            requested,
            collector(state, connection_id),
        )
        .await
    }

    /// Последняя проверка, если ей меньше `max_age_ms`, она сделана в текущем окружении
    /// подключения и охватывает проекты (`covers`), иначе новая. `true` — использована прошлая.
    pub async fn latest_or_run(
        self: &Arc<Self>,
        state: &AppState,
        connection_id: &str,
        trigger: Trigger,
        max_age_ms: i64,
        requested: Option<Vec<String>>,
    ) -> AppResult<(Arc<CheckRun>, bool)> {
        let now = now_ms();
        let env = state.connections.get(connection_id)?.environment;
        let (this, id) = (self.clone(), connection_id.to_string());
        let last = blocking(move || Ok(this.store.load(&id, now)?.last_check)).await?;
        // Окружение подключения сменили (например, dev → prod): у прошлой проверки другая строгость.
        if let Some(last) = last
            && last.environment == env
            && now - last.started_at_ms < max_age_ms
            && covers(&last.scope, requested.as_deref())
        {
            return Ok((Arc::new(last), true));
        }
        Ok((
            self.run(state, connection_id, trigger, requested).await?,
            false,
        ))
    }

    /// Ядро запуска на любом сборщике (тесты — на подделке). `generation` прочитано до проверки
    /// подключения: результат сохраняется, только если подключение с тех пор не удаляли.
    pub async fn run_with(
        self: &Arc<Self>,
        connection_id: &str,
        generation: u64,
        env: Environment,
        trigger: Trigger,
        requested: Option<Vec<String>>,
        collector: Collector,
    ) -> AppResult<Arc<CheckRun>> {
        loop {
            let (rx, same) = {
                let mut running = self.running.lock_safe();
                match running.get(connection_id) {
                    Some(r) => (r.rx.clone(), r.env == env && r.requested == requested),
                    None => {
                        let (tx, rx) = watch::channel(None);
                        running.insert(
                            connection_id.to_string(),
                            Running {
                                env,
                                requested: requested.clone(),
                                rx: rx.clone(),
                            },
                        );
                        let this = self.clone();
                        let id = connection_id.to_string();
                        let (trigger, requested, collector) =
                            (trigger.clone(), requested.clone(), collector.clone());
                        let own = rx.clone();
                        // Проверка доводится до конца, даже если вызвавший перестал ждать.
                        tauri::async_runtime::spawn(async move {
                            let unregister = Unregister {
                                service: this.clone(),
                                id: id.clone(),
                                rx: own,
                            };
                            let r = this
                                .check(&id, generation, env, trigger, requested, collector)
                                .await
                                .map(Arc::new);
                            // Запись снимается ровно один раз — до уведомления и отправки результата,
                            // чтобы разбуженный ждущий не увидел свою же завершённую проверку.
                            drop(unregister);
                            if r.is_ok() {
                                this.changed(&id);
                            }
                            let _ = tx.send(Some(r));
                        });
                        (rx, true)
                    }
                }
            };
            if same {
                return wait(rx).await;
            }
            // Идущая проверка — под другим окружением (его сменили в форме подключения) или по
            // другим проектам: дождаться её и запустить свою.
            let _ = wait(rx).await;
        }
    }

    async fn check(
        self: &Arc<Self>,
        connection_id: &str,
        generation: u64,
        env: Environment,
        trigger: Trigger,
        requested: Option<Vec<String>>,
        collector: Collector,
    ) -> AppResult<CheckRun> {
        let started_at = now_ms();
        let snap = collector(requested.clone()).await?;
        let findings = rules::evaluate(&snap, env);
        let c = &snap.coverage;
        let run = CheckRun {
            run_id: uuid::Uuid::new_v4().to_string(),
            started_at_ms: started_at,
            duration_ms: now_ms() - started_at,
            trigger,
            environment: env,
            incomplete: c.partial(),
            scope: Scope {
                projects: snap.projects.iter().map(|p| p.namespace.clone()).collect(),
                projects_total: c.projects_total,
                pods: snap
                    .projects
                    .iter()
                    .map(|p| p.pods.as_ref().map_or(0, Vec::len))
                    .sum(),
                nodes: snap.nodes.as_ref().map(Vec::len),
                requested,
            },
            findings,
        };
        // Файл пишется на blocking-потоке: `write` держит замок поколений на время fsync.
        // Подключение, удалённое за время проверки, не получит обратно свой файл.
        let (this, id, saved) = (self.clone(), connection_id.to_string(), run.clone());
        blocking(move || this.write(&id, generation, |s| s.save_check(&id, saved, now_ms())))
            .await?;
        Ok(run)
    }
}

/// Чтение кластера подключения для проверки: клиент из пула или новое подключение, как у
/// команд интерфейса (`AppState::read`).
fn collector(state: &AppState, connection_id: &str) -> Collector {
    let (st, id) = (state.clone(), connection_id.to_string());
    Arc::new(move |requested| {
        let (st, id) = (st.clone(), id.clone());
        Box::pin(async move {
            let profile = st.connections.get(&id)?;
            let (st2, id2) = (st.clone(), id.clone());
            st.read(&id, |c| async move {
                let now = now_ms() / 1000;
                // Источник истории метрик — для правил `resources.*`; без него — разрыв.
                let sel = crate::metrics::select::for_connection(&st2, &id2, &c, false).await?;
                Ok(collect(
                    &c.reader(),
                    &c.info,
                    &profile,
                    requested.as_deref(),
                    LIMITS,
                    now,
                    sel.history.as_deref(),
                )
                .await)
            })
            .await
        })
    })
}

async fn wait(mut rx: watch::Receiver<Outcome>) -> AppResult<Arc<CheckRun>> {
    loop {
        if let Some(r) = rx.borrow().clone() {
            return r;
        }
        if rx.changed().await.is_err() {
            return Err(app_err!(
                ErrorCode::Internal,
                "errors.app.taskPanicked",
                "Background task failed: {{error}}",
                error = "health check"
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::*;
    use crate::health::rules::pods::fixtures::{pod, project};
    use crate::platform::PlatformKind;

    /// Сборщик-подделка: снимок с подом в цикле перезапусков; считает вызовы, ждёт `delay`.
    fn counting(calls: Arc<AtomicUsize>, delay: Duration) -> Collector {
        Arc::new(move |requested: Option<Vec<String>>| {
            calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                tokio::time::sleep(delay).await;
                let mut s = ClusterSnapshot::empty(1_793_016_000, PlatformKind::Kubernetes);
                let mut p = pod("web-1", Some(("Deployment", "web")));
                p.containers[0].crash_loop = true;
                let names = requested.unwrap_or_else(|| vec!["team-a".to_string()]);
                s.projects = names
                    .iter()
                    .map(|ns| project(ns, vec![p.clone()]))
                    .collect();
                s.coverage.projects_total = names.len();
                s.coverage.projects_checked = names.len();
                Ok(s)
            })
        })
    }

    /// Сборщик, который ждёт разрешения; о входе в сбор сообщает `entered`.
    fn gated(entered: Arc<tokio::sync::Notify>, gate: Arc<tokio::sync::Notify>) -> Collector {
        let inner = counting(Arc::new(AtomicUsize::new(0)), Duration::ZERO);
        Arc::new(move |requested| {
            let (entered, gate, next) = (entered.clone(), gate.clone(), inner(requested));
            Box::pin(async move {
                entered.notify_one();
                gate.notified().await;
                next.await
            })
        })
    }

    fn panicking() -> Collector {
        Arc::new(|_| Box::pin(async { panic!("collect exploded") }))
    }

    #[tokio::test]
    async fn concurrent_callers_share_one_check() {
        let dir = tempfile::tempdir().unwrap();
        let svc = Arc::new(HealthService::open(dir.path()));
        let notified = Arc::new(AtomicUsize::new(0));
        let n = notified.clone();
        svc.set_notify(Arc::new(move |_| {
            n.fetch_add(1, Ordering::SeqCst);
        }));
        let calls = Arc::new(AtomicUsize::new(0));
        let src = counting(calls.clone(), Duration::from_millis(50));
        let g = svc.generation("c1");
        let (a, b) = tokio::join!(
            svc.run_with("c1", g, Environment::Prod, Trigger::Ui, None, src.clone()),
            svc.run_with(
                "c1",
                g,
                Environment::Prod,
                Trigger::Mcp { client: None },
                None,
                src.clone()
            ),
        );
        let (a, b) = (a.unwrap(), b.unwrap());
        assert_eq!(a.run_id, b.run_id);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(notified.load(Ordering::SeqCst), 1);
        let ids: Vec<_> = a.findings.iter().map(|f| f.rule_id.as_str()).collect();
        assert_eq!(ids, ["pod.crashLoop"]);
        assert_eq!(a.scope.projects, ["team-a"]);
        assert_eq!((a.scope.pods, a.scope.requested.clone()), (1, None));
        // Сохранена в файл; следующая проверка — отдельная.
        let saved = svc.store.load("c1", now_ms()).unwrap().last_check.unwrap();
        assert_eq!(saved.run_id, a.run_id);
        let c = svc
            .run_with("c1", g, Environment::Prod, Trigger::Ui, None, src)
            .await
            .unwrap();
        assert_ne!(c.run_id, a.run_id);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn check_of_other_projects_or_environment_waits_and_runs_its_own() {
        let dir = tempfile::tempdir().unwrap();
        let svc = Arc::new(HealthService::open(dir.path()));
        let (entered, gate) = (
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Notify::new()),
        );
        let all = tokio::spawn({
            let svc = svc.clone();
            let src = gated(entered.clone(), gate.clone());
            async move {
                svc.run_with("c1", 0, Environment::Prod, Trigger::Ui, None, src)
                    .await
            }
        });
        entered.notified().await;
        let calls = Arc::new(AtomicUsize::new(0));
        let some = tokio::spawn({
            let (svc, src) = (svc.clone(), counting(calls.clone(), Duration::ZERO));
            async move {
                svc.run_with(
                    "c1",
                    0,
                    Environment::Prod,
                    Trigger::Mcp { client: None },
                    Some(vec!["team-b".into()]),
                    src,
                )
                .await
            }
        });
        // Пока проверка всех проектов не закончилась, проверка team-b ждёт и сама не читает.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        gate.notify_one();
        let all = all.await.unwrap().unwrap();
        let some = some.await.unwrap().unwrap();
        assert_ne!(all.run_id, some.run_id);
        assert_eq!(some.scope.projects, ["team-b"]);
        assert_eq!(some.scope.requested, Some(vec!["team-b".to_string()]));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn reuse_needs_the_same_projects() {
        let scope = |projects: &[&str], requested: Option<&[&str]>| Scope {
            projects: projects.iter().map(|s| s.to_string()).collect(),
            requested: requested.map(|r| r.iter().map(|s| s.to_string()).collect()),
            ..Default::default()
        };
        let all = scope(&["a", "b"], None);
        assert!(covers(&all, None));
        assert!(covers(&all, Some(&["a".to_string()])));
        assert!(
            !covers(&all, Some(&["c".to_string()])),
            "c сверх предела 50 не проверен"
        );
        let some = scope(&["a"], Some(&["a"]));
        assert!(
            !covers(&some, None),
            "проверка одного проекта — не проверка всех"
        );
        assert!(covers(&some, Some(&["a".to_string()])));
    }

    #[tokio::test]
    async fn check_of_deleted_connection_is_not_saved() {
        let dir = tempfile::tempdir().unwrap();
        let svc = Arc::new(HealthService::open(dir.path()));
        let file = dir.path().join("health").join("c1.json");
        let src = || counting(Arc::new(AtomicUsize::new(0)), Duration::ZERO);
        svc.run_with(
            "c1",
            svc.generation("c1"),
            Environment::Prod,
            Trigger::Ui,
            None,
            src(),
        )
        .await
        .unwrap();
        assert!(file.exists());

        // Подключение удаляют, пока проверка ещё читает кластер.
        let (entered, gate) = (
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Notify::new()),
        );
        let running = tokio::spawn({
            let svc = svc.clone();
            let src = gated(entered.clone(), gate.clone());
            async move {
                svc.run_with(
                    "c1",
                    svc.generation("c1"),
                    Environment::Prod,
                    Trigger::Ui,
                    None,
                    src,
                )
                .await
            }
        });
        entered.notified().await;
        svc.forget("c1").unwrap();
        assert!(!file.exists());
        gate.notify_one();
        let err = running.await.unwrap().unwrap_err();
        assert_eq!(err.code, ErrorCode::NotFound);
        assert_eq!(err.key, "errors.connections.notFound");
        assert!(!file.exists(), "результат удалённого подключения сохранён");

        // Подключение с тем же id, созданное заново, сохраняется как обычно.
        svc.run_with(
            "c1",
            svc.generation("c1"),
            Environment::Prod,
            Trigger::Ui,
            None,
            src(),
        )
        .await
        .unwrap();
        assert!(svc.store.load("c1", now_ms()).unwrap().last_check.is_some());
    }

    #[tokio::test]
    async fn panicking_check_does_not_trap_the_next_callers() {
        let dir = tempfile::tempdir().unwrap();
        let svc = Arc::new(HealthService::open(dir.path()));
        let prod = svc
            .run_with("c1", 0, Environment::Prod, Trigger::Ui, None, panicking())
            .await;
        assert_eq!(prod.unwrap_err().key, "errors.app.taskPanicked");
        let next = tokio::time::timeout(
            Duration::from_secs(5),
            svc.run_with(
                "c1",
                0,
                Environment::Dev,
                Trigger::Ui,
                None,
                counting(Arc::new(AtomicUsize::new(0)), Duration::ZERO),
            ),
        )
        .await
        .expect("run_with must finish")
        .unwrap();
        assert_eq!(next.environment, Environment::Dev);
        assert!(svc.running.lock_safe().is_empty());
    }

    #[test]
    fn write_after_forget_is_refused_and_leaves_no_file() {
        let dir = tempfile::tempdir().unwrap();
        let svc = HealthService::open(dir.path());
        let file = dir.path().join("health").join("c1.json");
        let ignore = |s: &HealthStore| s.set_ignored("c1", "k", true, now_ms());
        let generation = svc.generation("c1");
        svc.write("c1", generation, ignore).unwrap();
        assert!(file.exists());
        svc.forget("c1").unwrap();
        assert!(!file.exists());
        let called = std::cell::Cell::new(false);
        let err = svc
            .write("c1", generation, |s| {
                called.set(true);
                ignore(s)
            })
            .unwrap_err();
        assert_eq!(err.key, "errors.connections.notFound");
        assert!(!called.get());
        assert!(!file.exists(), "запись удалённого подключения создала файл");
        let fresh = svc.generation("c1");
        svc.write("c1", fresh, ignore).unwrap();
        assert!(svc.store.load("c1", now_ms()).unwrap().is_ignored("k"));
    }
}
