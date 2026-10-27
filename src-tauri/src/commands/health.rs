//! Команды проверки кластера и вкладки «Замечания» (спец. §8.1, §4.2). Каждое изменение шлёт
//! событие `health://changed`: значки и блоки других вкладок перечитывают данные.

use serde::Serialize;
use tauri::State;

use super::blocking;
use crate::error::AppResult;
use crate::health::service::now_ms;
use crate::health::store::{Annotation, AnnotationStatus, CheckRun, HealthStore, Trigger};
use crate::state::AppState;

/// Состояние подключения для интерфейса: последняя проверка, скрытые находки и замечания LLM.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthView {
    pub last_check: Option<CheckRun>,
    pub ignored: Vec<String>,
    pub annotations: Vec<Annotation>,
}

fn view(st: &AppState, connection_id: &str) -> AppResult<HealthView> {
    st.connections.get(connection_id)?;
    let f = st.health.store.load(connection_id, now_ms())?;
    Ok(HealthView {
        last_check: f.last_check,
        ignored: f.ignored.into_iter().map(|i| i.key).collect(),
        annotations: f.annotations,
    })
}

/// Изменение файла подключения на blocking-потоке, затем событие и свежее состояние. Запись идёт
/// через `HealthService::write`: поколение читается до проверки подключения, и подключение,
/// удалённое в промежутке, файла не получит.
async fn change(
    st: &AppState,
    connection_id: String,
    op: impl FnOnce(&HealthStore, &str) -> AppResult<()> + Send + 'static,
) -> AppResult<HealthView> {
    let st = st.clone();
    blocking(move || {
        let generation = st.health.generation(&connection_id);
        st.connections.get(&connection_id)?;
        st.health
            .write(&connection_id, generation, |s| op(s, &connection_id))?;
        st.health.changed(&connection_id);
        view(&st, &connection_id)
    })
    .await
}

/// «Проверить»: все доступные проекты (не больше 50) и уровень кластера.
#[tauri::command]
pub async fn run_health_check(
    state: State<'_, AppState>,
    connection_id: String,
) -> AppResult<HealthView> {
    let st = state.inner().clone();
    st.health
        .clone()
        .run(&st, &connection_id, Trigger::Ui, None)
        .await?;
    blocking(move || view(&st, &connection_id)).await
}

#[tauri::command]
pub async fn get_health(
    state: State<'_, AppState>,
    connection_id: String,
) -> AppResult<HealthView> {
    let st = state.inner().clone();
    blocking(move || view(&st, &connection_id)).await
}

#[tauri::command]
pub async fn set_finding_ignored(
    state: State<'_, AppState>,
    connection_id: String,
    key: String,
    ignored: bool,
) -> AppResult<HealthView> {
    change(&state, connection_id, move |s, id| {
        s.set_ignored(id, &key, ignored, now_ms())
    })
    .await
}

#[tauri::command]
pub async fn set_annotation_status(
    state: State<'_, AppState>,
    connection_id: String,
    annotation_id: String,
    status: AnnotationStatus,
) -> AppResult<HealthView> {
    change(&state, connection_id, move |s, id| {
        s.set_status(id, &annotation_id, status, now_ms())
    })
    .await
}

#[tauri::command]
pub async fn delete_annotation(
    state: State<'_, AppState>,
    connection_id: String,
    annotation_id: String,
) -> AppResult<HealthView> {
    change(&state, connection_id, move |s, id| {
        s.delete_annotation(id, &annotation_id, now_ms())
    })
    .await
}

#[tauri::command]
pub async fn delete_all_annotations(
    state: State<'_, AppState>,
    connection_id: String,
) -> AppResult<HealthView> {
    change(&state, connection_id, |s, id| {
        s.delete_all_annotations(id, now_ms()).map(|_| ())
    })
    .await
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::health::model::{Severity, Target};
    use crate::health::store::{Annotated, Author, NewAnnotation};
    use crate::secrets::MemoryStore;
    use crate::storage::profile::sample_profile;

    fn state(dir: &std::path::Path) -> AppState {
        let st = AppState::new(dir, Arc::new(MemoryStore::default())).unwrap();
        st.connections.upsert(sample_profile("c1")).unwrap();
        st
    }

    #[tokio::test]
    async fn changes_notify_and_return_fresh_view() {
        let dir = tempfile::tempdir().unwrap();
        let st = state(dir.path());
        let n = Arc::new(AtomicUsize::new(0));
        let n2 = n.clone();
        st.health.set_notify(Arc::new(move |id| {
            assert_eq!(id, "c1");
            n2.fetch_add(1, Ordering::SeqCst);
        }));
        let v = change(&st, "c1".into(), |s, id| {
            s.set_ignored(id, "k", true, now_ms())
        })
        .await
        .unwrap();
        assert_eq!(v.ignored, ["k"]);
        let Annotated::Created(aid) = st
            .health
            .store
            .annotate(
                "c1",
                NewAnnotation {
                    target: Target::cluster(),
                    severity: Severity::Info,
                    title: "t".into(),
                    body: "b".into(),
                    related_rule_id: None,
                    author: Author {
                        client: Some("claude-ai".into()),
                        session_id: "s1".into(),
                    },
                },
                now_ms(),
            )
            .unwrap()
        else {
            panic!("created")
        };
        let v = change(&st, "c1".into(), move |s, id| {
            s.set_status(id, &aid, AnnotationStatus::Resolved, now_ms())
        })
        .await
        .unwrap();
        assert_eq!(v.annotations[0].status, AnnotationStatus::Resolved);
        assert_eq!(n.load(Ordering::SeqCst), 2);
        let v = change(&st, "c1".into(), |s, id| {
            s.delete_all_annotations(id, now_ms()).map(|_| ())
        })
        .await
        .unwrap();
        assert!(v.annotations.is_empty());
    }

    #[tokio::test]
    async fn unknown_connection_gets_no_file() {
        let dir = tempfile::tempdir().unwrap();
        let st = state(dir.path());
        let e = change(&st, "nope".into(), |s, id| {
            s.set_ignored(id, "k", true, now_ms())
        })
        .await
        .unwrap_err();
        assert_eq!(e.key, "errors.connections.notFound");
        assert!(!dir.path().join("health").join("nope.json").exists());
    }
}
