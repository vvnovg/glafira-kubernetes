//! Подтверждение MCP-клиентов (спец. §10.1): решение принимается на сессию; одобренные по
//! имени клиенты запоминаются в настройках. Клиент без имени (протокол 2026-07-28 без
//! `clientInfo`) одобряется только на текущую сессию.

use std::sync::{Arc, Mutex};

use glafira_mcp_core::{ClientIdent, SessionAccess, SessionView};
use serde::Serialize;

use crate::error::AppResult;
use crate::settings::SettingsStore;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Decision {
    Pending,
    Approved,
    Rejected,
}

#[derive(Debug, Clone)]
struct Entry {
    session_id: String,
    /// Клиент сессии; может стать известен позже (для отзыва по имени).
    client: Option<ClientIdent>,
    /// Клиент, каким его показывает диалог: зафиксирован при создании ожидания, его же
    /// запоминает «Разрешить» («Неизвестный MCP-клиент» не запоминается и потом).
    shown: Option<ClientIdent>,
    decision: Decision,
}

/// Решение пользователя по ожидающей сессии.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Answered {
    /// Клиент, которого видел пользователь (`None` — «Неизвестный MCP-клиент»).
    pub client: Option<ClientIdent>,
    /// Другие ожидающие сессии того же клиента, разрешённые этим же ответом (второй терминал).
    pub also_approved: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApprovalRequest {
    pub session_id: String,
    pub client_name: Option<String>,
    pub client_version: Option<String>,
}

pub struct Approvals {
    settings: Arc<SettingsStore>,
    entries: Mutex<Vec<Entry>>,
}

impl Approvals {
    pub fn new(settings: Arc<SettingsStore>) -> Self {
        Self {
            settings,
            entries: Mutex::new(Vec::new()),
        }
    }

    fn entries(&self) -> std::sync::MutexGuard<'_, Vec<Entry>> {
        self.entries.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Право сессии. Второе значение — true, если сессия только что стала ожидающей
    /// (показать диалог и привлечь внимание). Запоминание клиента пишет `settings.json`.
    pub fn access(&self, s: &SessionView, now_ms: u64) -> (SessionAccess, bool) {
        // Пустое имя — как отсутствующее: такой клиент не запоминается (спец. §4).
        let client = s.client.clone().filter(|c| !c.name.trim().is_empty());
        let mut entries = self.entries();
        if let Some(e) = entries.iter_mut().find(|e| e.session_id == s.id) {
            if e.client.is_none() {
                e.client = client.clone();
            }
            return (to_access(e.decision), false);
        }
        // Только обновление записи (не upsert): отзыв клиента в это же время не отменяется.
        // Ошибка возможна лишь при записи файла, то есть клиент найден — сессия разрешена.
        let remembered = client.as_ref().is_some_and(|c| {
            self.settings
                .touch_client(&c.name, &c.version, now_ms)
                .unwrap_or_else(|e| {
                    tracing::warn!("mcp client lastSeen not saved: {}", e.message);
                    true
                })
        });
        if remembered {
            entries.push(Entry {
                session_id: s.id.clone(),
                shown: client.clone(),
                client,
                decision: Decision::Approved,
            });
            return (SessionAccess::Approved, false);
        }
        entries.push(Entry {
            session_id: s.id.clone(),
            shown: client.clone(),
            client,
            decision: Decision::Pending,
        });
        (SessionAccess::Pending, true)
    }

    pub fn pending(&self) -> Vec<ApprovalRequest> {
        self.entries()
            .iter()
            .filter(|e| e.decision == Decision::Pending)
            .map(|e| ApprovalRequest {
                session_id: e.session_id.clone(),
                client_name: e.shown.as_ref().map(|c| c.name.clone()),
                client_version: e.shown.as_ref().map(|c| c.version.clone()),
            })
            .collect()
    }

    /// Имя клиента сессии, каким его видел пользователь (автор замечаний G2); `None` —
    /// безымянный клиент или сессии нет.
    pub fn shown_name(&self, session_id: &str) -> Option<String> {
        self.entries()
            .iter()
            .find(|e| e.session_id == session_id)
            .and_then(|e| e.shown.as_ref().map(|c| c.name.clone()))
    }

    pub fn is_approved(&self, session_id: &str) -> bool {
        self.entries()
            .iter()
            .any(|e| e.session_id == session_id && e.decision == Decision::Approved)
    }

    /// Ответ пользователя. `Ok(None)` — сессия уже не ждёт ответа (закрыта или решена);
    /// `Ok(Some(_))` — решение принято. «Разрешить» запоминает показанного в диалоге клиента и
    /// разрешает другие его ожидающие сессии; «Отклонить» касается только этой сессии.
    /// Блокирующий вызов (запись настроек).
    pub fn answer(
        &self,
        session_id: &str,
        approve: bool,
        now_ms: u64,
    ) -> AppResult<Option<Answered>> {
        let mut entries = self.entries();
        let Some(e) = entries
            .iter_mut()
            .find(|e| e.session_id == session_id && e.decision == Decision::Pending)
        else {
            return Ok(None);
        };
        let shown = e.shown.clone();
        if !approve {
            e.decision = Decision::Rejected;
            return Ok(Some(Answered {
                client: shown,
                also_approved: Vec::new(),
            }));
        }
        if let Some(c) = &shown {
            self.settings.remember_client(&c.name, &c.version, now_ms)?;
        }
        e.decision = Decision::Approved;
        let mut also_approved = Vec::new();
        if let Some(c) = &shown {
            for other in entries.iter_mut().filter(|o| {
                o.decision == Decision::Pending
                    && o.shown.as_ref().is_some_and(|s| s.name == c.name)
            }) {
                other.decision = Decision::Approved;
                also_approved.push(other.session_id.clone());
            }
        }
        Ok(Some(Answered {
            client: shown,
            also_approved,
        }))
    }

    /// Убирает клиента из запомненных; его открытые сессии теряют право сразу.
    pub fn revoke(&self, name: &str) -> AppResult<bool> {
        let removed = self.settings.revoke_client(name)?;
        for e in self.entries().iter_mut() {
            if e.client.as_ref().is_some_and(|c| c.name == name) {
                e.decision = Decision::Rejected;
            }
        }
        Ok(removed)
    }

    pub fn closed(&self, session_id: &str) {
        self.entries().retain(|e| e.session_id != session_id);
    }
}

fn to_access(d: Decision) -> SessionAccess {
    match d {
        Decision::Pending => SessionAccess::Pending,
        Decision::Approved => SessionAccess::Approved,
        Decision::Rejected => SessionAccess::Rejected,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn view(id: &str, client: Option<&str>) -> SessionView {
        SessionView {
            id: id.into(),
            client: client.map(|n| ClientIdent {
                name: n.into(),
                version: "1.0".into(),
            }),
        }
    }

    fn approvals() -> (tempfile::TempDir, Arc<SettingsStore>, Approvals) {
        let dir = tempfile::tempdir().unwrap();
        let settings = Arc::new(SettingsStore::open(dir.path()));
        let a = Approvals::new(settings.clone());
        (dir, settings, a)
    }

    #[test]
    fn new_client_waits_then_is_remembered() {
        let (_d, settings, a) = approvals();
        assert_eq!(
            a.access(&view("s1", Some("claude-ai")), 1),
            (SessionAccess::Pending, true)
        );
        assert_eq!(
            a.access(&view("s1", Some("claude-ai")), 2),
            (SessionAccess::Pending, false)
        );
        assert_eq!(a.pending()[0].client_name.as_deref(), Some("claude-ai"));
        let client = a.answer("s1", true, 3).unwrap().unwrap().client.unwrap();
        assert_eq!(client.name, "claude-ai");
        assert!(a.pending().is_empty());
        assert!(a.is_approved("s1"));
        assert!(settings.is_client_approved("claude-ai"));
        assert_eq!(
            a.answer("s1", true, 4).unwrap(),
            None,
            "повторный ответ не нужен"
        );
        // Новая сессия того же клиента — без вопроса, lastSeen обновлён.
        assert_eq!(
            a.access(&view("s2", Some("claude-ai")), 9),
            (SessionAccess::Approved, false)
        );
        assert_eq!(settings.mcp().approved_clients[0].last_seen_ms, 9);
    }

    #[test]
    fn rejection_is_not_remembered() {
        let (_d, settings, a) = approvals();
        a.access(&view("s1", Some("cursor")), 1);
        a.answer("s1", false, 2).unwrap();
        assert_eq!(
            a.access(&view("s1", Some("cursor")), 3).0,
            SessionAccess::Rejected
        );
        assert!(!settings.is_client_approved("cursor"));
        assert_eq!(
            a.access(&view("s2", Some("cursor")), 4),
            (SessionAccess::Pending, true)
        );
    }

    #[test]
    fn unnamed_client_is_approved_only_for_the_session() {
        let (_d, settings, a) = approvals();
        a.access(&view("s1", None), 1);
        assert_eq!(a.pending()[0].client_name, None);
        assert_eq!(
            a.answer("s1", true, 2).unwrap().map(|r| r.client),
            Some(None)
        );
        assert!(settings.mcp().approved_clients.is_empty());
        assert_eq!(
            a.access(&view("s2", None), 3),
            (SessionAccess::Pending, true)
        );
    }

    #[test]
    fn blank_client_name_is_treated_as_unnamed() {
        let (_d, settings, a) = approvals();
        a.access(&view("s1", Some("  ")), 1);
        assert_eq!(a.pending()[0].client_name, None);
        assert_eq!(
            a.answer("s1", true, 2).unwrap().map(|r| r.client),
            Some(None)
        );
        assert!(
            settings.mcp().approved_clients.is_empty(),
            "пустое имя не запоминается"
        );
    }

    #[test]
    fn revoke_cuts_open_sessions_and_closed_sessions_are_forgotten() {
        let (_d, settings, a) = approvals();
        settings.remember_client("claude-ai", "1", 0).unwrap();
        assert_eq!(
            a.access(&view("s1", Some("claude-ai")), 1).0,
            SessionAccess::Approved
        );
        assert!(a.revoke("claude-ai").unwrap());
        assert_eq!(
            a.access(&view("s1", Some("claude-ai")), 2).0,
            SessionAccess::Rejected
        );
        a.access(&view("s3", Some("x")), 3);
        a.closed("s3");
        assert!(a.pending().is_empty());
    }

    #[test]
    fn approving_one_session_approves_the_other_waiting_sessions_of_the_client() {
        let (_d, settings, a) = approvals();
        a.access(&view("s1", Some("claude-code")), 1);
        a.access(&view("s2", Some("claude-code")), 2);
        a.access(&view("s3", Some("cursor")), 3);
        a.access(&view("s4", None), 4);
        let r = a.answer("s1", true, 5).unwrap().unwrap();
        assert_eq!(r.client.unwrap().name, "claude-code");
        assert_eq!(r.also_approved, ["s2"]);
        assert!(a.is_approved("s2"), "второй терминал того же клиента");
        let waiting: Vec<String> = a.pending().into_iter().map(|p| p.session_id).collect();
        assert_eq!(waiting, ["s3", "s4"]);
        assert!(settings.is_client_approved("claude-code"));
        // «Отклонить» касается только своей сессии.
        a.access(&view("s5", Some("cursor")), 6);
        let r = a.answer("s3", false, 7).unwrap().unwrap();
        assert!(r.also_approved.is_empty());
        assert_eq!(a.pending().len(), 2);
    }

    #[test]
    fn the_name_shown_in_the_dialog_is_what_gets_remembered() {
        let (_d, settings, a) = approvals();
        a.access(&view("s1", None), 1);
        // Имя пришло позже (протокол 2026-07-28: clientInfo в _meta следующего запроса).
        a.access(&view("s1", Some("claude-ai")), 2);
        assert_eq!(
            a.pending()[0].client_name,
            None,
            "диалог показывает то, что было при создании"
        );
        let r = a.answer("s1", true, 3).unwrap().unwrap();
        assert_eq!(r.client, None);
        assert!(
            settings.mcp().approved_clients.is_empty(),
            "«Неизвестный MCP-клиент» не запоминается"
        );
        assert!(a.is_approved("s1"));
        // Сессия при этом знает имя: отзыв клиента её закрывает.
        settings.remember_client("claude-ai", "1", 4).unwrap();
        a.revoke("claude-ai").unwrap();
        assert!(!a.is_approved("s1"));
    }

    #[test]
    fn new_session_after_revoke_does_not_restore_the_client() {
        let (_d, settings, a) = approvals();
        settings.remember_client("claude-ai", "1", 0).unwrap();
        a.access(&view("s1", Some("claude-ai")), 1);
        assert!(a.revoke("claude-ai").unwrap());
        assert_eq!(
            a.access(&view("s2", Some("claude-ai")), 2),
            (SessionAccess::Pending, true)
        );
        assert!(!settings.is_client_approved("claude-ai"));
    }

    /// Отзыв между проверкой «клиент запомнен» и обновлением `lastSeenMs` в `access` не должен
    /// возвращать клиента в `approvedClients` (гонка access ↔ revoke).
    #[test]
    fn revoke_is_not_undone_by_a_concurrent_session() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let (_d, settings, a) = approvals();
        let a = Arc::new(a);
        let stop = Arc::new(AtomicBool::new(false));
        let opener = {
            let (a, stop) = (a.clone(), stop.clone());
            std::thread::spawn(move || {
                let mut n = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    let id = format!("s{n}");
                    a.access(&view(&id, Some("claude-ai")), n);
                    a.closed(&id);
                    n += 1;
                }
            })
        };
        for round in 0..100 {
            settings.remember_client("claude-ai", "1", 0).unwrap();
            a.revoke("claude-ai").unwrap();
            assert!(
                !settings.is_client_approved("claude-ai"),
                "раунд {round}: отозванный клиент вернулся"
            );
        }
        stop.store(true, Ordering::Relaxed);
        opener.join().unwrap();
    }
}
