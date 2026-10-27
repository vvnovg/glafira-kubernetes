//! Файл `health/<connectionId>.json` (спец. §8.1, §13): последняя проверка, скрытые находки и
//! замечания LLM. Срок замечания — 7 дней с создания; открытых — не больше 200 на подключение.
//! Предложения LLM (этап O3) добавятся в этот же файл.

use std::path::{Path, PathBuf};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use super::model::{Finding, Severity, Target};
use crate::error::{AppError, AppResult, ErrorCode};
use crate::storage::fs::{read_json_or_quarantine, write_json};
use crate::storage::profile::Environment;
use crate::sync::MutexExt;

pub const SCHEMA_VERSION: u32 = 1;
pub const DIR_NAME: &str = "health";
pub const ANNOTATION_TTL_MS: i64 = 7 * 24 * 60 * 60 * 1000;
pub const MAX_OPEN_ANNOTATIONS: usize = 200;
pub const MAX_TITLE_CHARS: usize = 120;
pub const MAX_BODY_CHARS: usize = 2000;

/// Кто запустил проверку.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum Trigger {
    Ui,
    Mcp {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        client: Option<String>,
    },
}

/// Что проверено: проекты (не больше 50), поды (не больше 5000), ноды.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Scope {
    /// Проверенные проекты.
    pub projects: Vec<String>,
    /// Сколько проектов было в охвате до предела.
    pub projects_total: usize,
    pub pods: usize,
    /// `None` — ноды не читаются.
    pub nodes: Option<usize>,
    /// Какие проекты просили проверить; `None` — все доступные (так проверяет интерфейс).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckRun {
    pub run_id: String,
    pub started_at_ms: i64,
    pub duration_ms: i64,
    pub trigger: Trigger,
    pub environment: Environment,
    pub incomplete: bool,
    pub scope: Scope,
    pub findings: Vec<Finding>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Ignored {
    pub key: String,
    pub at_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AnnotationStatus {
    Open,
    /// «Решено».
    Resolved,
    /// «Не актуально».
    Dismissed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Annotation {
    pub id: String,
    pub target: Target,
    pub severity: Severity,
    pub title: String,
    pub body: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub related_rule_id: Option<String>,
    /// Имя клиента, которое человек видел в диалоге подтверждения; `None` — безымянный.
    #[serde(default)]
    pub client: Option<String>,
    /// Сессия безымянного клиента: снять замечание можно только из неё.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub status: AnnotationStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub closed_at_ms: Option<i64>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthFile {
    pub schema_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_check: Option<CheckRun>,
    #[serde(default)]
    pub ignored: Vec<Ignored>,
    #[serde(default)]
    pub annotations: Vec<Annotation>,
}

impl HealthFile {
    pub fn is_ignored(&self, key: &str) -> bool {
        self.ignored.iter().any(|i| i.key == key)
    }

    fn purge(&mut self, now_ms: i64) {
        self.annotations
            .retain(|a| now_ms - a.created_at_ms < ANNOTATION_TTL_MS);
    }
}

/// Автор замечания: имя клиента или сессия безымянного.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Author {
    pub client: Option<String>,
    pub session_id: String,
}

impl Author {
    fn owns(&self, a: &Annotation) -> bool {
        self.owns_record(&a.client, a.session_id.as_deref())
    }

    /// Именованный клиент владеет записями с тем же именем; безымянный — только своей сессии.
    fn owns_record(&self, client: &Option<String>, session_id: Option<&str>) -> bool {
        match (&self.client, client) {
            (Some(mine), Some(theirs)) => mine == theirs,
            (None, None) => session_id == Some(self.session_id.as_str()),
            _ => false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct NewAnnotation {
    pub target: Target,
    pub severity: Severity,
    pub title: String,
    pub body: String,
    pub related_rule_id: Option<String>,
    pub author: Author,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Annotated {
    Created(String),
    /// То же замечание того же автора (объект и заголовок) обновлено.
    Updated(String),
    LimitReached,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Removed {
    Removed,
    NotFound,
    NotOwner,
}

/// Текст замечания: без управляющих символов (кроме перевода строки), не длиннее `max` символов.
pub fn sanitize(text: &str, max: usize) -> String {
    let clean: String = text
        .chars()
        .filter(|c| *c == '\n' || !c.is_control())
        .collect();
    let clean = clean.trim();
    if clean.chars().count() <= max {
        return clean.to_string();
    }
    let mut cut: String = clean.chars().take(max.saturating_sub(1)).collect();
    cut.push('…');
    cut
}

fn same_title(a: &str, b: &str) -> bool {
    a.trim().to_lowercase() == b.trim().to_lowercase()
}

/// Самый длинный id подключения, у которого есть файл: hex-имя (`h-` + 2 символа на байт + `.json`)
/// должно укладываться в 255 байт имени файла.
pub const MAX_ID_BYTES: usize = 100;

/// Имя файла по id подключения. Id, который создаёт приложение (uuid в нижнем регистре), — как
/// есть; любой другой (импортированный файл подключений может принести что угодно) — `h-<hex байтов id>`:
/// такое имя безопасно для файловой системы и однозначно. Простое имя состоит только из строчных
/// букв, цифр, «-» и «_»: файловые системы macOS и Windows по умолчанию не различают регистр, и `Prod-1`
/// с `prod-1` делили бы один файл (проверка одного подключения досталась бы другому, в том числе
/// подключению с выключенным доступом LLM), поэтому id с заглавной буквой кодируется. Пустой и слишком
/// длинный id файла не имеют. Id, который сам начинается с «h-», тоже кодируется — иначе он совпал бы
/// с hex-именем другого id (hex пишется строчными, а «H-…» кодируется как любой id с заглавной буквой).
pub(crate) fn file_stem(id: &str) -> Option<String> {
    if id.is_empty() || id.len() > MAX_ID_BYTES {
        return None;
    }
    let plain = !id.starts_with("h-")
        && id
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_');
    Some(if plain {
        id.to_string()
    } else {
        format!("h-{}", hex(id))
    })
}

fn hex(id: &str) -> String {
    id.bytes().map(|b| format!("{b:02x}")).collect()
}

/// Подключения нет (или его id не годится для имени файла).
pub(crate) fn connection_not_found(connection_id: &str) -> AppError {
    app_err!(
        ErrorCode::NotFound,
        "errors.connections.notFound",
        "Connection {{id}} not found",
        id = connection_id
    )
}

pub struct HealthStore {
    dir: PathBuf,
    lock: Mutex<()>,
}

impl HealthStore {
    pub fn open(data_dir: &Path) -> Self {
        Self {
            dir: data_dir.join(DIR_NAME),
            lock: Mutex::new(()),
        }
    }

    fn path(&self, connection_id: &str) -> AppResult<PathBuf> {
        let stem = file_stem(connection_id).ok_or_else(|| connection_not_found(connection_id))?;
        Ok(self.dir.join(format!("{stem}.json")))
    }

    /// Каталог `health/` — только для владельца (файлы — 0600, см. `atomic_write`).
    fn ensure_dir(&self) -> AppResult<()> {
        std::fs::create_dir_all(&self.dir)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if let Err(e) =
                std::fs::set_permissions(&self.dir, std::fs::Permissions::from_mode(0o700))
            {
                tracing::warn!(
                    "failed to restrict permissions on {}: {e}",
                    self.dir.display()
                );
            }
        }
        Ok(())
    }

    fn read(&self, connection_id: &str, now_ms: i64) -> AppResult<HealthFile> {
        let mut f =
            read_json_or_quarantine::<HealthFile>(&self.path(connection_id)?, SCHEMA_VERSION)?
                .unwrap_or_default();
        f.schema_version = SCHEMA_VERSION;
        f.purge(now_ms);
        Ok(f)
    }

    /// Состояние подключения с учётом срока замечаний.
    pub fn load(&self, connection_id: &str, now_ms: i64) -> AppResult<HealthFile> {
        let _g = self.lock.lock_safe();
        self.read(connection_id, now_ms)
    }

    fn update<T>(
        &self,
        connection_id: &str,
        now_ms: i64,
        f: impl FnOnce(&mut HealthFile) -> T,
    ) -> AppResult<T> {
        self.update_if(connection_id, now_ms, |file| (f(file), true))
    }

    /// Как `update`, но файл пишется, только если операция сообщила об изменении (второй элемент).
    fn update_if<T>(
        &self,
        connection_id: &str,
        now_ms: i64,
        f: impl FnOnce(&mut HealthFile) -> (T, bool),
    ) -> AppResult<T> {
        let _g = self.lock.lock_safe();
        let mut file = self.read(connection_id, now_ms)?;
        let (out, changed) = f(&mut file);
        if changed {
            self.ensure_dir()?;
            write_json(&self.path(connection_id)?, &file)?;
        }
        Ok(out)
    }

    pub fn save_check(&self, connection_id: &str, run: CheckRun, now_ms: i64) -> AppResult<()> {
        self.update(connection_id, now_ms, |f| f.last_check = Some(run))
    }

    pub fn set_ignored(
        &self,
        connection_id: &str,
        key: &str,
        ignored: bool,
        now_ms: i64,
    ) -> AppResult<()> {
        self.update(connection_id, now_ms, |f| {
            f.ignored.retain(|i| i.key != key);
            if ignored {
                f.ignored.push(Ignored {
                    key: key.to_string(),
                    at_ms: now_ms,
                });
            }
        })
    }

    pub fn annotate(
        &self,
        connection_id: &str,
        new: NewAnnotation,
        now_ms: i64,
    ) -> AppResult<Annotated> {
        let title = sanitize(&new.title, MAX_TITLE_CHARS);
        let body = sanitize(&new.body, MAX_BODY_CHARS);
        self.update(connection_id, now_ms, |f| {
            if let Some(a) = f.annotations.iter_mut().find(|a| {
                a.status == AnnotationStatus::Open
                    && new.author.owns(a)
                    && a.target == new.target
                    && same_title(&a.title, &title)
            }) {
                a.severity = new.severity;
                a.body = body;
                a.related_rule_id = new.related_rule_id;
                a.updated_at_ms = now_ms;
                return Annotated::Updated(a.id.clone());
            }
            let open = f
                .annotations
                .iter()
                .filter(|a| a.status == AnnotationStatus::Open)
                .count();
            if open >= MAX_OPEN_ANNOTATIONS {
                return Annotated::LimitReached;
            }
            let id = uuid::Uuid::new_v4().to_string();
            let session_id = new
                .author
                .client
                .is_none()
                .then(|| new.author.session_id.clone());
            f.annotations.push(Annotation {
                id: id.clone(),
                target: new.target,
                severity: new.severity,
                title,
                body,
                related_rule_id: new.related_rule_id,
                client: new.author.client,
                session_id,
                created_at_ms: now_ms,
                updated_at_ms: now_ms,
                status: AnnotationStatus::Open,
                closed_at_ms: None,
            });
            Annotated::Created(id)
        })
    }

    fn not_found() -> crate::error::AppError {
        app_err!(
            ErrorCode::NotFound,
            "errors.health.annotationNotFound",
            "The note was deleted or has expired"
        )
    }

    /// «Решено» / «Не актуально» — или снова открыть (`Open`).
    pub fn set_status(
        &self,
        connection_id: &str,
        annotation_id: &str,
        status: AnnotationStatus,
        now_ms: i64,
    ) -> AppResult<()> {
        let found = self.update(connection_id, now_ms, |f| {
            let Some(a) = f.annotations.iter_mut().find(|a| a.id == annotation_id) else {
                return false;
            };
            a.status = status;
            a.closed_at_ms = (status != AnnotationStatus::Open).then_some(now_ms);
            true
        })?;
        found.then_some(()).ok_or_else(Self::not_found)
    }

    pub fn delete_annotation(
        &self,
        connection_id: &str,
        annotation_id: &str,
        now_ms: i64,
    ) -> AppResult<()> {
        let found = self.update(connection_id, now_ms, |f| {
            let n = f.annotations.len();
            f.annotations.retain(|a| a.id != annotation_id);
            f.annotations.len() < n
        })?;
        found.then_some(()).ok_or_else(Self::not_found)
    }

    /// «Удалить все замечания LLM» у подключения.
    pub fn delete_all_annotations(&self, connection_id: &str, now_ms: i64) -> AppResult<usize> {
        self.update(connection_id, now_ms, |f| {
            let n = f.annotations.len();
            f.annotations.clear();
            n
        })
    }

    /// `remove_annotation` из MCP: только свои замечания.
    pub fn remove_own(
        &self,
        connection_id: &str,
        annotation_id: &str,
        author: &Author,
        now_ms: i64,
    ) -> AppResult<Removed> {
        self.update(connection_id, now_ms, |f| {
            let Some(pos) = f.annotations.iter().position(|a| a.id == annotation_id) else {
                return Removed::NotFound;
            };
            if !author.owns(&f.annotations[pos]) {
                return Removed::NotOwner;
            }
            f.annotations.remove(pos);
            Removed::Removed
        })
    }

    /// Удаление вместе с подключением.
    pub fn delete_connection(&self, connection_id: &str) -> AppResult<()> {
        let _g = self.lock.lock_safe();
        match std::fs::remove_file(self.path(connection_id)?) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::health::model::TargetKind;

    const ID: &str = "c1";
    const T0: i64 = 1_800_000_000_000;

    fn store() -> (tempfile::TempDir, HealthStore) {
        let dir = tempfile::tempdir().unwrap();
        let s = HealthStore::open(dir.path());
        (dir, s)
    }

    fn author(client: Option<&str>, session: &str) -> Author {
        Author {
            client: client.map(String::from),
            session_id: session.into(),
        }
    }

    fn note(title: &str, who: Author) -> NewAnnotation {
        NewAnnotation {
            target: Target::object(TargetKind::Pod, "team-a", "web-1"),
            severity: Severity::Warning,
            title: title.into(),
            body: "body".into(),
            related_rule_id: None,
            author: who,
        }
    }

    fn created(a: Annotated) -> String {
        match a {
            Annotated::Created(id) => id,
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn empty_state_without_file() {
        let (_d, s) = store();
        let f = s.load(ID, T0).unwrap();
        assert_eq!(f.schema_version, SCHEMA_VERSION);
        assert!(f.last_check.is_none() && f.ignored.is_empty() && f.annotations.is_empty());
    }

    #[test]
    fn check_is_saved_and_replaced() {
        let (d, s) = store();
        let run = |id: &str| CheckRun {
            run_id: id.into(),
            started_at_ms: T0,
            duration_ms: 10,
            trigger: Trigger::Mcp {
                client: Some("claude-ai".into()),
            },
            environment: Environment::Prod,
            incomplete: false,
            scope: Scope {
                projects: vec!["team-a".into()],
                projects_total: 1,
                pods: 3,
                nodes: None,
                requested: None,
            },
            findings: vec![],
        };
        s.save_check(ID, run("r1"), T0).unwrap();
        s.save_check(ID, run("r2"), T0).unwrap();
        let f = s.load(ID, T0).unwrap();
        assert_eq!(f.last_check.unwrap().run_id, "r2");
        let raw: serde_json::Value =
            serde_json::from_slice(&std::fs::read(d.path().join("health/c1.json")).unwrap())
                .unwrap();
        assert_eq!(raw["schemaVersion"], 1);
        assert_eq!(
            raw["lastCheck"]["trigger"],
            serde_json::json!({ "kind": "mcp", "client": "claude-ai" })
        );
        assert_eq!(
            raw["lastCheck"]["scope"]["projects"],
            serde_json::json!(["team-a"])
        );
    }

    #[test]
    fn ignore_and_restore() {
        let (_d, s) = store();
        s.set_ignored(ID, "k1", true, T0).unwrap();
        s.set_ignored(ID, "k1", true, T0).unwrap();
        assert!(s.load(ID, T0).unwrap().is_ignored("k1"));
        assert_eq!(s.load(ID, T0).unwrap().ignored.len(), 1);
        s.set_ignored(ID, "k1", false, T0).unwrap();
        assert!(!s.load(ID, T0).unwrap().is_ignored("k1"));
    }

    #[test]
    fn same_author_target_and_title_updates() {
        let (_d, s) = store();
        let id = created(
            s.annotate(ID, note("Lag grows", author(Some("claude-ai"), "s1")), T0)
                .unwrap(),
        );
        let mut again = note("  lag GROWS ", author(Some("claude-ai"), "s2"));
        again.body = "new body".into();
        again.severity = Severity::Critical;
        assert_eq!(
            s.annotate(ID, again, T0 + 5).unwrap(),
            Annotated::Updated(id.clone())
        );
        // Другой клиент — новое замечание.
        created(
            s.annotate(ID, note("Lag grows", author(Some("cursor"), "s3")), T0)
                .unwrap(),
        );
        let f = s.load(ID, T0 + 5).unwrap();
        assert_eq!(f.annotations.len(), 2);
        let a = f.annotations.iter().find(|a| a.id == id).unwrap();
        assert_eq!(
            (a.body.as_str(), a.severity, a.updated_at_ms),
            ("new body", Severity::Critical, T0 + 5)
        );
        assert_eq!(a.target.kind, TargetKind::Pod);
    }

    #[test]
    fn open_limit_is_two_hundred() {
        let (d, s) = store();
        // 200 открытых замечаний одним файлом (без 200 записей на диск).
        let id = created(
            s.annotate(ID, note("n0", author(Some("c"), "s")), T0)
                .unwrap(),
        );
        let mut f = s.load(ID, T0).unwrap();
        let first = f.annotations[0].clone();
        f.annotations = (0..MAX_OPEN_ANNOTATIONS)
            .map(|i| Annotation {
                id: if i == 0 { id.clone() } else { format!("a{i}") },
                title: format!("n{i}"),
                ..first.clone()
            })
            .collect();
        write_json(&d.path().join("health/c1.json"), &f).unwrap();
        assert_eq!(
            s.annotate(ID, note("one more", author(Some("c"), "s")), T0)
                .unwrap(),
            Annotated::LimitReached
        );
        // Обновление существующего — не новое замечание, лимит не мешает.
        assert_eq!(
            s.annotate(ID, note("n5", author(Some("c"), "s")), T0)
                .unwrap(),
            Annotated::Updated("a5".into())
        );
        // Закрытые не считаются.
        s.set_status(ID, &id, AnnotationStatus::Resolved, T0)
            .unwrap();
        created(
            s.annotate(ID, note("one more", author(Some("c"), "s")), T0)
                .unwrap(),
        );
    }

    #[test]
    fn annotations_expire_after_seven_days() {
        let (_d, s) = store();
        created(
            s.annotate(ID, note("old", author(Some("c"), "s")), T0)
                .unwrap(),
        );
        assert_eq!(
            s.load(ID, T0 + ANNOTATION_TTL_MS - 1)
                .unwrap()
                .annotations
                .len(),
            1
        );
        assert!(
            s.load(ID, T0 + ANNOTATION_TTL_MS)
                .unwrap()
                .annotations
                .is_empty()
        );
    }

    #[test]
    fn status_and_delete() {
        let (_d, s) = store();
        let id = created(
            s.annotate(ID, note("x", author(Some("c"), "s")), T0)
                .unwrap(),
        );
        s.set_status(ID, &id, AnnotationStatus::Dismissed, T0 + 1)
            .unwrap();
        let a = &s.load(ID, T0).unwrap().annotations[0];
        assert_eq!(
            (a.status, a.closed_at_ms),
            (AnnotationStatus::Dismissed, Some(T0 + 1))
        );
        s.set_status(ID, &id, AnnotationStatus::Open, T0 + 2)
            .unwrap();
        assert_eq!(s.load(ID, T0).unwrap().annotations[0].closed_at_ms, None);
        s.delete_annotation(ID, &id, T0).unwrap();
        let e = s.delete_annotation(ID, &id, T0).unwrap_err();
        assert_eq!(e.key, "errors.health.annotationNotFound");
        created(
            s.annotate(ID, note("y", author(Some("c"), "s")), T0)
                .unwrap(),
        );
        assert_eq!(s.delete_all_annotations(ID, T0).unwrap(), 1);
    }

    #[test]
    fn llm_removes_only_own_notes() {
        let (_d, s) = store();
        let named = created(
            s.annotate(ID, note("a", author(Some("claude-ai"), "s1")), T0)
                .unwrap(),
        );
        let unnamed = created(s.annotate(ID, note("b", author(None, "s2")), T0).unwrap());
        assert_eq!(
            s.remove_own(ID, &named, &author(Some("cursor"), "s1"), T0)
                .unwrap(),
            Removed::NotOwner
        );
        assert_eq!(
            s.remove_own(ID, &unnamed, &author(None, "s3"), T0).unwrap(),
            Removed::NotOwner
        );
        assert_eq!(
            s.remove_own(ID, &unnamed, &author(None, "s2"), T0).unwrap(),
            Removed::Removed
        );
        assert_eq!(
            s.remove_own(ID, &named, &author(Some("claude-ai"), "s9"), T0)
                .unwrap(),
            Removed::Removed
        );
        assert_eq!(
            s.remove_own(ID, &named, &author(Some("claude-ai"), "s9"), T0)
                .unwrap(),
            Removed::NotFound
        );
    }

    #[test]
    fn text_is_cleaned_and_cut() {
        assert_eq!(sanitize("  a\u{0007}b\nc\t ", 10), "ab\nc");
        let long = "x".repeat(130);
        let t = sanitize(&long, MAX_TITLE_CHARS);
        assert_eq!(t.chars().count(), MAX_TITLE_CHARS);
        assert!(t.ends_with('…'));
    }

    #[test]
    fn imported_id_gets_a_hex_file_name_and_is_never_a_path() {
        let (d, s) = store();
        // Импортированный id с «/», «..» и пробелом: файл внутри health/, по hex байтов id.
        for (id, stem) in [
            ("../x", "h-2e2e2f78"),
            ("a/b", "h-612f62"),
            ("a b", "h-612062"),
        ] {
            s.set_ignored(id, "k", true, T0).unwrap();
            assert_eq!(file_stem(id).as_deref(), Some(stem), "{id}");
            let file = d.path().join("health").join(format!("{stem}.json"));
            assert!(file.exists(), "{id}");
            assert!(s.load(id, T0).unwrap().is_ignored("k"), "{id}");
            s.delete_connection(id).unwrap();
            assert!(!file.exists(), "{id}");
        }
        assert!(!d.path().join("x.json").exists());
        assert!(!d.path().join("health").join("b.json").exists());
        // Uuid приложения — без изменений.
        assert_eq!(file_stem("c1").as_deref(), Some("c1"));
        // Id, похожий на hex-имя, кодируется сам: имена однозначны.
        assert_eq!(file_stem("h-612f62").as_deref(), Some("h-682d363132663632"));
        assert_eq!(file_stem("H-612f62").as_deref(), Some("h-482d363132663632"));
        // Пустой и слишком длинный id файла не имеют.
        let long = "й".repeat(MAX_ID_BYTES / 2 + 1);
        for id in ["", long.as_str()] {
            assert_eq!(
                s.load(id, T0).unwrap_err().code,
                ErrorCode::NotFound,
                "{id:?}"
            );
        }
        assert_eq!(
            file_stem(&"a".repeat(MAX_ID_BYTES)).as_deref(),
            Some("a".repeat(MAX_ID_BYTES).as_str())
        );
    }

    #[test]
    fn ids_that_differ_only_in_case_get_different_file_names() {
        // На macOS и Windows `Prod-1.json` и `prod-1.json` — один файл: простое имя только из
        // строчных букв, цифр, «-» и «_», всё с заглавной буквой кодируется.
        let (d, s) = store();
        assert_eq!(file_stem("prod-1").as_deref(), Some("prod-1"));
        assert_eq!(
            file_stem("Prod-1").as_deref(),
            Some("h-50726f642d31"),
            "заглавная буква — hex"
        );
        assert_ne!(file_stem("Prod-1"), file_stem("prod-1"));
        // Имена не совпадают и без учёта регистра: так их различит и регистронезависимая ФС.
        let all = [
            "prod-1", "Prod-1", "PROD-1", "pRod-1", "h-1", "H-1", "A_b", "a_b",
        ];
        let mut seen = std::collections::BTreeSet::new();
        for id in all {
            let stem = file_stem(id).unwrap();
            assert!(seen.insert(stem.to_lowercase()), "{id}: {stem}");
        }
        // Настоящие файлы: настройки одного не видны другому.
        s.set_ignored("prod-1", "k", true, T0).unwrap();
        assert!(!s.load("Prod-1", T0).unwrap().is_ignored("k"));
        assert!(s.load("prod-1", T0).unwrap().is_ignored("k"));
        s.set_ignored("Prod-1", "j", true, T0).unwrap();
        assert!(!s.load("prod-1", T0).unwrap().is_ignored("j"));
        assert!(d.path().join("health/prod-1.json").exists());
        assert!(d.path().join("health/h-50726f642d31.json").exists());
        // Uuid приложения (строчные буквы) — без изменений.
        let uuid = "6cbcdae0-39c3-4187-9ebb-3c0279a56a4a";
        assert_eq!(file_stem(uuid).as_deref(), Some(uuid));
    }

    #[test]
    fn corrupted_file_is_quarantined() {
        let (d, s) = store();
        std::fs::create_dir_all(d.path().join("health")).unwrap();
        std::fs::write(d.path().join("health/c1.json"), b"{oops").unwrap();
        assert!(s.load(ID, T0).unwrap().annotations.is_empty());
        let names: Vec<_> = std::fs::read_dir(d.path().join("health"))
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        assert!(
            names.iter().any(|n| n.starts_with("c1.json.corrupt-")),
            "{names:?}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn files_are_private() {
        use std::os::unix::fs::PermissionsExt;
        let (d, s) = store();
        s.set_ignored(ID, "k", true, T0).unwrap();
        let mode = |p: &std::path::Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(&d.path().join("health")), 0o700);
        assert_eq!(mode(&d.path().join("health/c1.json")), 0o600);
    }

    #[test]
    fn delete_connection_removes_file() {
        let (d, s) = store();
        s.set_ignored(ID, "k", true, T0).unwrap();
        s.delete_connection(ID).unwrap();
        s.delete_connection(ID).unwrap();
        assert!(!d.path().join("health/c1.json").exists());
    }
}
