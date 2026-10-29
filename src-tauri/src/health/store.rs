//! Файл `health/<connectionId>.json` (спец. §8.1, §10.5, §13): последняя проверка, скрытые
//! находки, замечания и предложения LLM. Срок замечания и предложения — 7 дней с создания;
//! открытых замечаний — не больше 200, предложений — 50 на подключение.

use std::path::{Path, PathBuf};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use super::model::{Finding, Severity, Target};
use super::proposals::{
    MAX_OPEN_PROPOSALS, MAX_REJECT_REASON_CHARS, PROPOSAL_TTL_MS, Proposal, ProposalStatus,
};
use crate::change::{Change, applied_as_proposed};
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
    /// Предложения LLM: любой статус до истечения срока.
    #[serde(default)]
    pub proposals: Vec<Proposal>,
}

impl HealthFile {
    pub fn is_ignored(&self, key: &str) -> bool {
        self.ignored.iter().any(|i| i.key == key)
    }

    fn purge(&mut self, now_ms: i64) {
        self.annotations
            .retain(|a| now_ms - a.created_at_ms < ANNOTATION_TTL_MS);
        self.proposals
            .retain(|p| now_ms - p.created_at_ms < PROPOSAL_TTL_MS);
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

    fn owns_proposal(&self, p: &Proposal) -> bool {
        self.owns_record(&p.client, p.session_id.as_deref())
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

/// Новое предложение LLM (спец. §10.5).
#[derive(Debug, Clone)]
pub struct NewProposal {
    pub severity: Severity,
    pub title: String,
    pub rationale: String,
    pub change: Change,
    pub related_rule_id: Option<String>,
    pub related_finding_key: Option<String>,
    pub author: Author,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Proposed {
    Created(String),
    /// Открытое предложение того же автора, вида и объекта обновлено.
    Updated(String),
    LimitReached,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Withdrawn {
    Withdrawn,
    NotFound,
    NotOwner,
    /// Уже не открытое: применено, отклонено или отозвано раньше.
    Closed(ProposalStatus),
}

/// Итог отметки «применено» в команде изменения (спец. §9, шаг 5).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarkApplied {
    Applied {
        as_proposed: bool,
    },
    /// Предложения нет, оно закрыто или относится к другому изменению: операция прошла, отметки нет.
    Skipped(&'static str),
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

    // ---- Предложения LLM ----

    fn proposal_not_found() -> crate::error::AppError {
        app_err!(
            ErrorCode::NotFound,
            "errors.health.proposalNotFound",
            "The proposal was deleted or has expired"
        )
    }

    /// `propose_change` из MCP: повтор того же автора по тому же виду и объекту среди открытых
    /// обновляет запись; открытых — не больше `MAX_OPEN_PROPOSALS`.
    pub fn propose(
        &self,
        connection_id: &str,
        new: NewProposal,
        now_ms: i64,
    ) -> AppResult<Proposed> {
        let title = sanitize(&new.title, MAX_TITLE_CHARS);
        let rationale = sanitize(&new.rationale, MAX_BODY_CHARS);
        let change = new.change.trimmed();
        let target = change.target();
        self.update(connection_id, now_ms, |f| {
            if let Some(p) = f.proposals.iter_mut().find(|p| {
                p.status == ProposalStatus::Open
                    && new.author.owns_proposal(p)
                    && p.change.kind_name() == change.kind_name()
                    && p.target == target
            }) {
                p.severity = new.severity;
                p.title = title;
                p.rationale = rationale;
                p.change = change;
                p.related_rule_id = new.related_rule_id;
                p.related_finding_key = new.related_finding_key;
                p.updated_at_ms = now_ms;
                return Proposed::Updated(p.id.clone());
            }
            let open = f
                .proposals
                .iter()
                .filter(|p| p.status == ProposalStatus::Open)
                .count();
            if open >= MAX_OPEN_PROPOSALS {
                return Proposed::LimitReached;
            }
            let id = uuid::Uuid::new_v4().to_string();
            let session_id = new
                .author
                .client
                .is_none()
                .then(|| new.author.session_id.clone());
            f.proposals.push(Proposal {
                id: id.clone(),
                severity: new.severity,
                title,
                rationale,
                target,
                change,
                related_rule_id: new.related_rule_id,
                related_finding_key: new.related_finding_key,
                client: new.author.client,
                session_id,
                created_at_ms: now_ms,
                updated_at_ms: now_ms,
                status: ProposalStatus::Open,
                closed_at_ms: None,
                applied_as_proposed: None,
                applied_change: None,
                reject_reason: None,
            });
            Proposed::Created(id)
        })
    }

    /// Человек: «Отклонить» (из открытого, с необязательной причиной) или «Открыть снова»
    /// (из отклонённого). Применённое и отозванное не меняются.
    pub fn set_proposal_status(
        &self,
        connection_id: &str,
        proposal_id: &str,
        status: ProposalStatus,
        reason: Option<String>,
        now_ms: i64,
    ) -> AppResult<()> {
        let reason = reason
            .map(|r| sanitize(&r, MAX_REJECT_REASON_CHARS))
            .filter(|r| !r.is_empty());
        self.update(connection_id, now_ms, |f| {
            let Some(p) = f.proposals.iter_mut().find(|p| p.id == proposal_id) else {
                return Err(Self::proposal_not_found());
            };
            match (p.status, status) {
                (ProposalStatus::Open, ProposalStatus::Rejected) => {
                    p.status = ProposalStatus::Rejected;
                    p.closed_at_ms = Some(now_ms);
                    p.reject_reason = reason;
                }
                (ProposalStatus::Rejected, ProposalStatus::Open) => {
                    p.status = ProposalStatus::Open;
                    p.closed_at_ms = None;
                    p.reject_reason = None;
                    p.updated_at_ms = now_ms;
                }
                (from, to) => {
                    return Err(app_err!(
                        ErrorCode::InvalidInput,
                        "errors.health.proposalClosed",
                        "The proposal is {{status}} and cannot be changed to {{to}}",
                        status = from.as_str(),
                        to = to.as_str()
                    ));
                }
            }
            Ok(())
        })?
    }

    /// Команда изменения после успеха в кластере: открытое предложение того же вида и объекта
    /// становится `applied`; иначе отметка пропускается (операция уже прошла) и файл не пишется.
    pub fn mark_applied(
        &self,
        connection_id: &str,
        proposal_id: &str,
        applied: &Change,
        now_ms: i64,
    ) -> AppResult<MarkApplied> {
        self.update_if(connection_id, now_ms, |f| {
            let Some(p) = f.proposals.iter_mut().find(|p| p.id == proposal_id) else {
                return (MarkApplied::Skipped("proposal not found"), false);
            };
            if p.status != ProposalStatus::Open {
                return (MarkApplied::Skipped("proposal is not open"), false);
            }
            if p.change.kind_name() != applied.kind_name() || p.target != applied.target() {
                return (
                    MarkApplied::Skipped("proposal is about another change"),
                    false,
                );
            }
            let as_proposed = applied_as_proposed(&p.change, applied);
            p.status = ProposalStatus::Applied;
            p.closed_at_ms = Some(now_ms);
            p.applied_as_proposed = Some(as_proposed);
            // С правками — что применено фактически; как предложено — оно совпадает с `change`.
            p.applied_change = (!as_proposed).then(|| applied.clone());
            (MarkApplied::Applied { as_proposed }, true)
        })
    }

    /// `withdraw_proposal` из MCP: только своё и только открытое.
    pub fn withdraw_own(
        &self,
        connection_id: &str,
        proposal_id: &str,
        author: &Author,
        now_ms: i64,
    ) -> AppResult<Withdrawn> {
        self.update(connection_id, now_ms, |f| {
            let Some(p) = f.proposals.iter_mut().find(|p| p.id == proposal_id) else {
                return Withdrawn::NotFound;
            };
            if !author.owns_proposal(p) {
                return Withdrawn::NotOwner;
            }
            if p.status != ProposalStatus::Open {
                return Withdrawn::Closed(p.status);
            }
            p.status = ProposalStatus::Withdrawn;
            p.closed_at_ms = Some(now_ms);
            Withdrawn::Withdrawn
        })
    }

    pub fn delete_proposal(
        &self,
        connection_id: &str,
        proposal_id: &str,
        now_ms: i64,
    ) -> AppResult<()> {
        let found = self.update(connection_id, now_ms, |f| {
            let n = f.proposals.len();
            f.proposals.retain(|p| p.id != proposal_id);
            f.proposals.len() < n
        })?;
        found.then_some(()).ok_or_else(Self::proposal_not_found)
    }

    /// «Удалить все предложения LLM» у подключения.
    pub fn delete_all_proposals(&self, connection_id: &str, now_ms: i64) -> AppResult<usize> {
        self.update(connection_id, now_ms, |f| {
            let n = f.proposals.len();
            f.proposals.clear();
            n
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

    fn scale(workload: &str, replicas: i32) -> Change {
        Change::Scale {
            namespace: "team-a".into(),
            workload: crate::k8s::pods::WorkloadRef {
                kind: crate::k8s::workloads::WorkloadKind::Deployment,
                name: workload.into(),
            },
            replicas,
        }
    }

    fn proposal(change: Change, who: Author) -> NewProposal {
        NewProposal {
            severity: Severity::Warning,
            title: "Scale web out".into(),
            rationale: "single replica\nis risky".into(),
            change,
            related_rule_id: Some("workload.singleReplica".into()),
            related_finding_key: None,
            author: who,
        }
    }

    fn proposed_id(p: Proposed) -> String {
        match p {
            Proposed::Created(id) => id,
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn proposal_of_same_author_kind_and_object_is_updated() {
        let (d, s) = store();
        let claude = || author(Some("claude-ai"), "s1");
        let mut first = scale("web", 2);
        if let Change::Scale { workload, .. } = &mut first {
            workload.name = " web ".into();
        }
        let id = proposed_id(s.propose(ID, proposal(first, claude()), T0).unwrap());
        let mut again = proposal(scale("web", 3), author(Some("claude-ai"), "s9"));
        again.title = "  New title ".into();
        again.severity = Severity::Critical;
        assert_eq!(
            s.propose(ID, again, T0 + 5).unwrap(),
            Proposed::Updated(id.clone())
        );
        // Другой вид по тому же workload'у и другой автор — новые записи.
        let restart = Change::RolloutRestart {
            namespace: "team-a".into(),
            workload: crate::k8s::pods::WorkloadRef {
                kind: crate::k8s::workloads::WorkloadKind::Deployment,
                name: "web".into(),
            },
        };
        proposed_id(s.propose(ID, proposal(restart, claude()), T0).unwrap());
        proposed_id(
            s.propose(
                ID,
                proposal(scale("web", 2), author(Some("cursor"), "s2")),
                T0,
            )
            .unwrap(),
        );
        let f = s.load(ID, T0 + 5).unwrap();
        assert_eq!(f.proposals.len(), 3);
        let p = f.proposals.iter().find(|p| p.id == id).unwrap();
        assert_eq!(p.title, "New title");
        assert_eq!(p.severity, Severity::Critical);
        assert_eq!(p.updated_at_ms, T0 + 5);
        assert_eq!(
            p.target,
            Target::workload(
                "team-a",
                crate::k8s::workloads::WorkloadKind::Deployment,
                "web"
            )
        );
        assert_eq!(p.change, scale("web", 3));
        assert_eq!(p.status, ProposalStatus::Open);
        assert_eq!(p.client.as_deref(), Some("claude-ai"));
        assert_eq!(p.session_id, None);
        let raw: serde_json::Value =
            serde_json::from_slice(&std::fs::read(d.path().join("health/c1.json")).unwrap())
                .unwrap();
        assert_eq!(raw["proposals"][0]["change"]["kind"], "scale");
        assert_eq!(raw["proposals"][0]["status"], "open");
        assert!(raw["proposals"][0].get("appliedAsProposed").is_none());
    }

    #[test]
    fn open_proposals_are_limited_to_fifty() {
        let (d, s) = store();
        let id = proposed_id(
            s.propose(ID, proposal(scale("w0", 2), author(Some("c"), "s")), T0)
                .unwrap(),
        );
        let mut f = s.load(ID, T0).unwrap();
        let first = f.proposals[0].clone();
        f.proposals = (0..MAX_OPEN_PROPOSALS)
            .map(|i| {
                let change = scale(&format!("w{i}"), 2);
                Proposal {
                    id: if i == 0 { id.clone() } else { format!("p{i}") },
                    target: change.target(),
                    change,
                    ..first.clone()
                }
            })
            .collect();
        write_json(&d.path().join("health/c1.json"), &f).unwrap();
        assert_eq!(
            s.propose(
                ID,
                proposal(scale("one-more", 2), author(Some("c"), "s")),
                T0
            )
            .unwrap(),
            Proposed::LimitReached
        );
        // Обновление существующего лимит не трогает.
        assert_eq!(
            s.propose(ID, proposal(scale("w5", 3), author(Some("c"), "s")), T0)
                .unwrap(),
            Proposed::Updated("p5".into())
        );
        // Закрытые не считаются.
        s.set_proposal_status(ID, &id, ProposalStatus::Rejected, None, T0)
            .unwrap();
        proposed_id(
            s.propose(
                ID,
                proposal(scale("one-more", 2), author(Some("c"), "s")),
                T0,
            )
            .unwrap(),
        );
    }

    #[test]
    fn proposals_expire_after_seven_days_whatever_the_status() {
        let (_d, s) = store();
        let id = proposed_id(
            s.propose(ID, proposal(scale("web", 2), author(Some("c"), "s")), T0)
                .unwrap(),
        );
        s.mark_applied(ID, &id, &scale("web", 2), T0 + 1).unwrap();
        assert_eq!(
            s.load(ID, T0 + PROPOSAL_TTL_MS - 1)
                .unwrap()
                .proposals
                .len(),
            1
        );
        assert!(
            s.load(ID, T0 + PROPOSAL_TTL_MS)
                .unwrap()
                .proposals
                .is_empty()
        );
    }

    #[test]
    fn status_transitions_reject_reopen_and_refuse_closed() {
        let (_d, s) = store();
        let id = proposed_id(
            s.propose(ID, proposal(scale("web", 2), author(Some("c"), "s")), T0)
                .unwrap(),
        );
        s.set_proposal_status(
            ID,
            &id,
            ProposalStatus::Rejected,
            Some("  too risky\u{7} ".into()),
            T0 + 1,
        )
        .unwrap();
        let p = &s.load(ID, T0).unwrap().proposals[0];
        assert_eq!(
            (p.status, p.closed_at_ms),
            (ProposalStatus::Rejected, Some(T0 + 1))
        );
        assert_eq!(p.reject_reason.as_deref(), Some("too risky"));
        let e = s
            .set_proposal_status(ID, &id, ProposalStatus::Rejected, None, T0 + 2)
            .unwrap_err();
        assert_eq!(e.key, "errors.health.proposalClosed");
        s.set_proposal_status(ID, &id, ProposalStatus::Open, None, T0 + 3)
            .unwrap();
        let p = &s.load(ID, T0).unwrap().proposals[0];
        assert_eq!(
            (p.status, p.closed_at_ms, p.reject_reason.clone()),
            (ProposalStatus::Open, None, None)
        );
        assert_eq!(p.updated_at_ms, T0 + 3);
        // Применённое снова не открыть и не отклонить.
        let id2 = proposed_id(
            s.propose(ID, proposal(scale("api", 2), author(Some("c"), "s")), T0)
                .unwrap(),
        );
        s.mark_applied(ID, &id2, &scale("api", 2), T0).unwrap();
        for to in [ProposalStatus::Open, ProposalStatus::Rejected] {
            let e = s.set_proposal_status(ID, &id2, to, None, T0).unwrap_err();
            assert_eq!(
                (e.code, e.key.as_str()),
                (ErrorCode::InvalidInput, "errors.health.proposalClosed")
            );
            assert_eq!(e.params["status"], "applied");
        }
        let e = s
            .set_proposal_status(ID, "nope", ProposalStatus::Rejected, None, T0)
            .unwrap_err();
        assert_eq!(e.key, "errors.health.proposalNotFound");
    }

    #[test]
    fn mark_applied_records_edits_and_skips_foreign_or_closed() {
        let (d, s) = store();
        assert_eq!(
            s.mark_applied(ID, "nope", &scale("web", 2), T0).unwrap(),
            MarkApplied::Skipped("proposal not found")
        );
        assert!(
            !d.path().join("health").join("c1.json").exists(),
            "пропуск файл не пишет"
        );
        let id = proposed_id(
            s.propose(ID, proposal(scale("web", 2), author(Some("c"), "s")), T0)
                .unwrap(),
        );
        assert_eq!(
            s.mark_applied(ID, &id, &scale("other", 2), T0).unwrap(),
            MarkApplied::Skipped("proposal is about another change")
        );
        assert_eq!(
            s.mark_applied(ID, &id, &scale("web", 4), T0 + 1).unwrap(),
            MarkApplied::Applied { as_proposed: false }
        );
        let p = &s.load(ID, T0).unwrap().proposals[0];
        assert_eq!(
            (p.status, p.closed_at_ms, p.applied_as_proposed),
            (ProposalStatus::Applied, Some(T0 + 1), Some(false))
        );
        assert_eq!(p.applied_change, Some(scale("web", 4)));
        assert_eq!(p.change, scale("web", 2));
        assert_eq!(
            s.mark_applied(ID, &id, &scale("web", 2), T0 + 2).unwrap(),
            MarkApplied::Skipped("proposal is not open")
        );
        let id2 = proposed_id(
            s.propose(ID, proposal(scale("api", 2), author(Some("c"), "s")), T0)
                .unwrap(),
        );
        assert_eq!(
            s.mark_applied(ID, &id2, &scale("api", 2), T0).unwrap(),
            MarkApplied::Applied { as_proposed: true }
        );
        let f = s.load(ID, T0).unwrap();
        assert_eq!(
            f.proposals
                .iter()
                .find(|p| p.id == id2)
                .unwrap()
                .applied_change,
            None
        );
    }

    #[test]
    fn llm_withdraws_only_its_own_open_proposals() {
        let (_d, s) = store();
        let named = proposed_id(
            s.propose(
                ID,
                proposal(scale("a", 2), author(Some("claude-ai"), "s1")),
                T0,
            )
            .unwrap(),
        );
        let unnamed = proposed_id(
            s.propose(ID, proposal(scale("b", 2), author(None, "s2")), T0)
                .unwrap(),
        );
        assert_eq!(
            s.withdraw_own(ID, &named, &author(Some("cursor"), "s1"), T0)
                .unwrap(),
            Withdrawn::NotOwner
        );
        assert_eq!(
            s.withdraw_own(ID, &unnamed, &author(None, "s3"), T0)
                .unwrap(),
            Withdrawn::NotOwner
        );
        assert_eq!(
            s.withdraw_own(ID, &unnamed, &author(None, "s2"), T0)
                .unwrap(),
            Withdrawn::Withdrawn
        );
        assert_eq!(
            s.withdraw_own(ID, &unnamed, &author(None, "s2"), T0)
                .unwrap(),
            Withdrawn::Closed(ProposalStatus::Withdrawn)
        );
        assert_eq!(
            s.withdraw_own(ID, "nope", &author(Some("claude-ai"), "s1"), T0)
                .unwrap(),
            Withdrawn::NotFound
        );
        // Отозванное LLM человек снова не откроет.
        let e = s
            .set_proposal_status(ID, &unnamed, ProposalStatus::Open, None, T0)
            .unwrap_err();
        assert_eq!(e.params["status"], "withdrawn");
    }

    #[test]
    fn proposals_are_deleted_one_by_one_and_all_at_once() {
        let (_d, s) = store();
        let id = proposed_id(
            s.propose(ID, proposal(scale("a", 2), author(Some("c"), "s")), T0)
                .unwrap(),
        );
        proposed_id(
            s.propose(ID, proposal(scale("b", 2), author(Some("c"), "s")), T0)
                .unwrap(),
        );
        s.delete_proposal(ID, &id, T0).unwrap();
        let e = s.delete_proposal(ID, &id, T0).unwrap_err();
        assert_eq!(e.key, "errors.health.proposalNotFound");
        assert_eq!(s.delete_all_proposals(ID, T0).unwrap(), 1);
        assert!(s.load(ID, T0).unwrap().proposals.is_empty());
        // Замечания при этом не трогаются.
        created(
            s.annotate(ID, note("n", author(Some("c"), "s")), T0)
                .unwrap(),
        );
        assert_eq!(s.delete_all_proposals(ID, T0).unwrap(), 0);
        assert_eq!(s.load(ID, T0).unwrap().annotations.len(), 1);
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
