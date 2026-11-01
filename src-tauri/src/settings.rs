//! Пользовательские настройки (язык интерфейса, MCP) и выбор действующего языка.

use std::path::{Path, PathBuf};
use std::sync::RwLock;

use serde::{Deserialize, Serialize};

use crate::error::{AppResult, ErrorCode};
use crate::storage::fs::{read_json, read_json_or_quarantine, write_json};
use crate::sync::RwLockExt;

/// Языки, для которых есть словарь `src/locales/<код>.json`. Остальные 10 языков Kafka-версии
/// добавляются на этапе O6 (спец. §16).
pub const LANGUAGES: [&str; 2] = ["en", "ru"];

fn is_known(code: &str) -> bool {
    LANGUAGES.contains(&code)
}

pub fn match_language(tag: &str) -> Option<&'static str> {
    match_language_in(tag, &LANGUAGES)
}

/// Язык ОС → код из `langs`. Китайский: явный Hans → zh-CN; Hant/TW/HK/MO → zh-TW; иначе zh-CN.
fn match_language_in(tag: &str, langs: &[&'static str]) -> Option<&'static str> {
    let lower = tag.replace('_', "-").to_ascii_lowercase();
    let mut parts = lower.split('-');
    let primary = parts.next().unwrap_or("");
    let rest: Vec<&str> = parts.collect();
    let pick = |code: &'static str| langs.iter().copied().find(|c| *c == code);
    match primary {
        "pt" => pick("pt-BR"),
        "zh" => {
            let traditional = !rest.contains(&"hans")
                && rest
                    .iter()
                    .any(|p| matches!(*p, "hant" | "tw" | "hk" | "mo"));
            pick(if traditional { "zh-TW" } else { "zh-CN" })
        }
        _ => langs
            .iter()
            .copied()
            .find(|c| c.eq_ignore_ascii_case(primary)),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum LanguageReason {
    Policy,
    User,
    System,
    Default,
}

/// Политика → выбор → ОС → en. Все языки доступны без лицензии.
pub fn resolve(
    system: Option<&str>,
    chosen: Option<&str>,
    policy: Option<&str>,
) -> (String, LanguageReason) {
    let candidate = policy
        .map(|p| (p.to_string(), LanguageReason::Policy))
        .or_else(|| chosen.map(|c| (c.to_string(), LanguageReason::User)))
        .or_else(|| {
            system
                .and_then(match_language)
                .map(|s| (s.to_string(), LanguageReason::System))
        });
    match candidate {
        Some((code, reason)) if is_known(&code) => (code, reason),
        _ => ("en".into(), LanguageReason::Default),
    }
}

/// Клиент MCP, которому пользователь разрешил подключаться (спец. G1 §4).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ApprovedClient {
    pub name: String,
    pub version: String,
    pub first_seen_ms: u64,
    pub last_seen_ms: u64,
    /// Компактный набор инструментов: без заметок и предложений (для локальной модели).
    #[serde(default)]
    pub compact_tools: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct McpSettings {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub approved_clients: Vec<ApprovedClient>,
    /// Предел размера ответа LLM, КБ; `None` — по умолчанию (100 КБ).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_limit_kb: Option<u32>,
}

impl McpSettings {
    /// Значение вне [`RESPONSE_LIMITS_KB`](glafira_mcp_core::fit::RESPONSE_LIMITS_KB) (файл
    /// правили руками: 0, 50) — тоже по умолчанию.
    pub fn response_limit_kb(&self) -> u32 {
        self.response_limit_kb
            .filter(|kb| glafira_mcp_core::fit::RESPONSE_LIMITS_KB.contains(&(*kb as usize)))
            .unwrap_or((glafira_mcp_core::fit::MAX_RESPONSE_BYTES / 1024) as u32)
    }
}

#[derive(Serialize, Deserialize, Default, Clone)]
#[serde(rename_all = "camelCase")]
struct SettingsFile {
    schema_version: u32,
    #[serde(default)]
    language: Option<String>,
    #[serde(default)]
    mcp: McpSettings,
}

pub struct SettingsStore {
    path: PathBuf,
    data: RwLock<SettingsFile>,
}

/// Включён ли MCP в `settings.json` каталога `dir` — для шима: без карантина и без записи,
/// нечитаемый файл означает «выключено».
pub fn mcp_enabled_in(dir: &Path) -> bool {
    read_json::<SettingsFile>(&dir.join("settings.json"))
        .ok()
        .flatten()
        .is_some_and(|f| f.mcp.enabled)
}

impl SettingsStore {
    pub fn open(dir: &Path) -> Self {
        let path = dir.join("settings.json");
        let mut data = match read_json_or_quarantine::<SettingsFile>(&path, 1) {
            Ok(f) => f.unwrap_or_default(),
            Err(e) => {
                tracing::warn!("settings not read: {}", e.message);
                SettingsFile::default()
            }
        };
        data.language = data.language.filter(|l| is_known(l));
        Self {
            path,
            data: RwLock::new(data),
        }
    }

    pub fn language(&self) -> Option<String> {
        self.data.read_safe().language.clone()
    }

    pub fn mcp(&self) -> McpSettings {
        self.data.read_safe().mcp.clone()
    }

    /// Блокирующий вызов: меняет копию, пишет файл и только потом обновляет кэш.
    fn update(&self, f: impl FnOnce(&mut SettingsFile)) -> AppResult<()> {
        let mut data = self.data.write_safe();
        let mut next = data.clone();
        f(&mut next);
        next.schema_version = 1;
        write_json(&self.path, &next)?;
        *data = next;
        Ok(())
    }

    /// Блокирующий вызов (запись файла).
    pub fn set_language(&self, lang: Option<&str>) -> AppResult<()> {
        if let Some(l) = lang
            && !is_known(l)
        {
            return Err(crate::app_err!(
                ErrorCode::InvalidInput,
                "errors.settings.unknownLanguage",
                "Unknown language “{{code}}”",
                code = l
            ));
        }
        self.update(|d| d.language = lang.map(String::from))
    }

    /// Блокирующий вызов (запись файла).
    pub fn set_mcp_enabled(&self, enabled: bool) -> AppResult<()> {
        self.update(|d| d.mcp.enabled = enabled)
    }

    pub fn is_client_approved(&self, name: &str) -> bool {
        self.data
            .read_safe()
            .mcp
            .approved_clients
            .iter()
            .any(|c| c.name == name)
    }

    /// Запоминает клиента или обновляет его версию и время последнего сеанса. Блокирующий вызов.
    pub fn remember_client(&self, name: &str, version: &str, now_ms: u64) -> AppResult<()> {
        self.update(|d| {
            let clients = &mut d.mcp.approved_clients;
            match clients.iter_mut().find(|c| c.name == name) {
                Some(c) => {
                    c.version = version.to_string();
                    c.last_seen_ms = now_ms;
                }
                None => clients.push(ApprovedClient {
                    name: name.to_string(),
                    version: version.to_string(),
                    first_seen_ms: now_ms,
                    last_seen_ms: now_ms,
                    compact_tools: false,
                }),
            }
        })
    }

    /// Только обновляет версию и время последнего сеанса уже запомненного клиента: проверка и
    /// запись — под одной блокировкой, поэтому параллельный отзыв клиента не отменяется.
    /// `Ok(false)` — клиента нет (файл не пишется). Блокирующий вызов.
    pub fn touch_client(&self, name: &str, version: &str, now_ms: u64) -> AppResult<bool> {
        let mut data = self.data.write_safe();
        let Some(i) = data
            .mcp
            .approved_clients
            .iter()
            .position(|c| c.name == name)
        else {
            return Ok(false);
        };
        let mut next = data.clone();
        let c = &mut next.mcp.approved_clients[i];
        c.version = version.to_string();
        c.last_seen_ms = now_ms;
        next.schema_version = 1;
        write_json(&self.path, &next)?;
        *data = next;
        Ok(true)
    }

    /// Блокирующий вызов (запись файла).
    pub fn set_mcp_response_limit(&self, kb: u32) -> AppResult<()> {
        if !glafira_mcp_core::fit::RESPONSE_LIMITS_KB.contains(&(kb as usize)) {
            return Err(crate::app_err!(
                ErrorCode::InvalidInput,
                "errors.settings.unknownResponseLimit",
                "Unsupported response limit {{kb}} KB",
                kb = kb
            ));
        }
        self.update(|d| d.mcp.response_limit_kb = Some(kb))
    }

    /// Блокирующий вызов. `false` — такого клиента нет.
    pub fn set_client_compact(&self, name: &str, compact: bool) -> AppResult<bool> {
        if !self.is_client_approved(name) {
            return Ok(false);
        }
        self.update(|d| {
            for c in d.mcp.approved_clients.iter_mut().filter(|c| c.name == name) {
                c.compact_tools = compact;
            }
        })?;
        Ok(true)
    }

    /// Блокирующий вызов. `false` — такого клиента не было.
    pub fn revoke_client(&self, name: &str) -> AppResult<bool> {
        if !self.is_client_approved(name) {
            return Ok(false);
        }
        self.update(|d| d.mcp.approved_clients.retain(|c| c.name != name))?;
        Ok(true)
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LanguageSettings {
    pub system: Option<String>,
    pub system_match: Option<String>,
    pub chosen: Option<String>,
    pub policy: Option<String>,
    pub effective: String,
    pub reason: LanguageReason,
    pub languages: Vec<String>,
}

pub fn system_locale() -> Option<String> {
    sys_locale::get_locale()
}

pub fn language_settings(
    system: Option<String>,
    chosen: Option<String>,
    policy: Option<String>,
) -> LanguageSettings {
    let (effective, reason) = resolve(system.as_deref(), chosen.as_deref(), policy.as_deref());
    LanguageSettings {
        system_match: system.as_deref().and_then(match_language).map(String::from),
        system,
        chosen,
        policy,
        effective,
        reason,
        languages: LANGUAGES.iter().map(|c| c.to_string()).collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matching_system_tags() {
        assert_eq!(match_language("ru_RU"), Some("ru"));
        assert_eq!(match_language("en-GB"), Some("en"));
        assert_eq!(match_language("de-AT"), None, "словаря de пока нет");
        assert_eq!(match_language("zh-Hans-CN"), None);
        assert_eq!(match_language("uk-UA"), None);
    }

    const ALL: [&str; 12] = [
        "en", "ru", "de", "fr", "es", "it", "pl", "pt-BR", "zh-CN", "zh-TW", "ja", "ko",
    ];

    #[test]
    fn matching_asian_tags() {
        for tag in [
            "zh-Hans-CN",
            "zh-CN",
            "zh-SG",
            "zh",
            "zh-Hans-HK",
            "zh-Hans",
        ] {
            assert_eq!(match_language_in(tag, &ALL), Some("zh-CN"), "{tag}");
        }
        for tag in ["zh-Hant-TW", "zh-TW", "zh-HK", "zh-MO", "zh_TW", "zh-Hant"] {
            assert_eq!(match_language_in(tag, &ALL), Some("zh-TW"), "{tag}");
        }
        assert_eq!(match_language_in("ja-JP", &ALL), Some("ja"));
        assert_eq!(match_language_in("ko-KR", &ALL), Some("ko"));
        assert_eq!(match_language_in("pt-PT", &ALL), Some("pt-BR"));
        assert_eq!(match_language_in("de-AT", &ALL), Some("de"));
        assert_eq!(match_language_in("xx", &ALL), None);
    }

    #[test]
    fn asian_tags_without_asian_languages() {
        const BEFORE_E: [&str; 8] = ["en", "ru", "de", "fr", "es", "it", "pl", "pt-BR"];
        assert_eq!(match_language_in("zh-CN", &BEFORE_E), None);
        assert_eq!(match_language_in("zh-Hant-TW", &BEFORE_E), None);
        assert_eq!(match_language_in("ja-JP", &BEFORE_E), None);
    }

    #[test]
    fn priority_of_sources() {
        use LanguageReason::*;
        assert_eq!(resolve(Some("ru-RU"), None, None), ("ru".into(), System));
        assert_eq!(resolve(Some("ru"), Some("en"), None), ("en".into(), User));
        assert_eq!(resolve(None, Some("ru"), None), ("ru".into(), User));
        assert_eq!(
            resolve(Some("en"), Some("en"), Some("ru")),
            ("ru".into(), Policy)
        );
        assert_eq!(resolve(Some("de-DE"), None, None), ("en".into(), Default));
        assert_eq!(resolve(None, Some("fr"), None), ("en".into(), Default));
        assert_eq!(resolve(None, None, None), ("en".into(), Default));
    }

    #[test]
    fn mcp_settings_persist_next_to_language() {
        let dir = tempfile::tempdir().unwrap();
        assert!(!mcp_enabled_in(dir.path()), "нет файла — выключено");
        let s = SettingsStore::open(dir.path());
        s.set_language(Some("ru")).unwrap();
        s.set_mcp_enabled(true).unwrap();
        s.remember_client("claude-ai", "1.0", 10).unwrap();
        s.remember_client("claude-ai", "1.1", 20).unwrap();
        s.remember_client("cursor", "2", 30).unwrap();
        assert!(mcp_enabled_in(dir.path()));
        let again = SettingsStore::open(dir.path());
        assert_eq!(again.language().as_deref(), Some("ru"));
        let mcp = again.mcp();
        assert!(mcp.enabled);
        assert_eq!(
            mcp.approved_clients[0],
            ApprovedClient {
                name: "claude-ai".into(),
                version: "1.1".into(),
                first_seen_ms: 10,
                last_seen_ms: 20,
                compact_tools: false
            }
        );
        assert_eq!(again.mcp().response_limit_kb(), 100);
        again.set_mcp_response_limit(24).unwrap();
        assert_eq!(
            again.set_mcp_response_limit(50).unwrap_err().code,
            crate::error::ErrorCode::InvalidInput
        );
        assert!(again.set_client_compact("cursor", true).unwrap());
        assert!(!again.set_client_compact("unknown", true).unwrap());
        let mcp = SettingsStore::open(dir.path()).mcp();
        assert_eq!(mcp.response_limit_kb(), 24);
        assert!(mcp.approved_clients[1].compact_tools);
        for hand_edited in [0, 50] {
            let mcp = McpSettings {
                response_limit_kb: Some(hand_edited),
                ..mcp.clone()
            };
            assert_eq!(mcp.response_limit_kb(), 100, "{hand_edited}");
        }
        assert!(again.revoke_client("cursor").unwrap());
        again.remember_client("cursor", "3", 40).unwrap();
        assert!(
            !again.mcp().approved_clients[1].compact_tools,
            "новое подтверждение — полный набор"
        );
        assert!(again.revoke_client("claude-ai").unwrap());
        assert!(!again.revoke_client("claude-ai").unwrap());
        assert!(!again.is_client_approved("claude-ai"));
        assert!(again.is_client_approved("cursor"));
        again.set_language(None).unwrap();
        assert!(
            SettingsStore::open(dir.path()).mcp().enabled,
            "смена языка не сбрасывает MCP"
        );
    }

    #[test]
    fn touching_a_client_never_adds_it() {
        let dir = tempfile::tempdir().unwrap();
        let s = SettingsStore::open(dir.path());
        assert!(!s.touch_client("cursor", "2", 5).unwrap());
        assert!(!s.is_client_approved("cursor"), "только обновление");
        assert!(
            !dir.path().join("settings.json").exists(),
            "без записи файла"
        );
        s.remember_client("cursor", "1", 1).unwrap();
        assert!(s.touch_client("cursor", "2", 5).unwrap());
        let c = &s.mcp().approved_clients[0];
        assert_eq!(
            (c.version.as_str(), c.first_seen_ms, c.last_seen_ms),
            ("2", 1, 5)
        );
    }

    #[test]
    fn unreadable_settings_mean_mcp_off_for_the_shim() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("settings.json"), b"{broken").unwrap();
        assert!(!mcp_enabled_in(dir.path()));
        assert!(
            dir.path().join("settings.json").exists(),
            "шим не трогает файл"
        );
    }

    #[test]
    fn store_persists_and_validates() {
        let dir = tempfile::tempdir().unwrap();
        let s = SettingsStore::open(dir.path());
        assert_eq!(s.language(), None);
        s.set_language(Some("ru")).unwrap();
        assert_eq!(
            SettingsStore::open(dir.path()).language().as_deref(),
            Some("ru")
        );
        assert_eq!(
            s.set_language(Some("xx")).unwrap_err().code,
            crate::error::ErrorCode::InvalidInput
        );
        s.set_language(None).unwrap();
        assert_eq!(SettingsStore::open(dir.path()).language(), None);
    }

    #[test]
    fn corrupt_settings_do_not_block() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("settings.json"), "{broken").unwrap();
        assert_eq!(SettingsStore::open(dir.path()).language(), None);
    }
}
