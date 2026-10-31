//! Логи подов для LLM (спец. §10.4, §10.7; уровень `logs`): операция бэкенда, сырые строки и их
//! обработка в шлюзе — фильтр по немаскированному тексту (поиск по значению из ошибки работает),
//! маскировка, обрезка строки до 4 КБ. Строки, которые модель не получит, бэкенд не хранит дольше
//! вызова и в журнал они не попадают.

use glafira_mcp_core::BackendError;
use regex::{Regex, RegexBuilder};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::mask::Masker;

/// Строка в ответе длиннее обрезается (по границе символа).
pub const MAX_LINE_BYTES: usize = 4096;
/// Сколько байт строки бэкенд читает: запас поверх 4 КБ, чтобы секрет на границе обрезки
/// маскировался по целому значению. Остаток длинной строки пропускается.
pub const READ_LINE_BYTES: usize = 8192;
pub const DEFAULT_TAIL_LINES: u32 = 200;
pub const MAX_TAIL_LINES: u32 = 2000;
/// Наибольший `sinceSeconds` у `get_pod_logs`: 7 дней.
pub const MAX_POD_SINCE_S: u32 = 7 * 86_400;
/// Наибольшая длина подстроки или регулярного выражения фильтра.
pub const MAX_FILTER_CHARS: usize = 1024;
/// Предел размера скомпилированного регулярного выражения.
const REGEX_SIZE_LIMIT: usize = 1 << 20;

pub const OLDER_LINES_HINT: &str = "The response was truncated to 100 KB: the oldest lines were dropped. Narrow the time range, lower tailLines or add a filter";

// Описание типа уходит модели в схеме параметров — поэтому по-английски.
/// Line filter: exactly one of contains and regex.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct LogFilter {
    /// Substring to look for.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contains: Option<String>,
    /// Regular expression (Rust regex syntax) to look for.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub regex: Option<String>,
    /// Default: false — the case is ignored.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub case_sensitive: Option<bool>,
}

/// Проверенный фильтр.
#[derive(Debug)]
pub enum Matcher {
    All,
    Text {
        needle: String,
        case_sensitive: bool,
    },
    Regex(Regex),
}

impl Matcher {
    pub fn new(f: Option<&LogFilter>) -> Result<Self, BackendError> {
        let Some(f) = f else {
            return Ok(Matcher::All);
        };
        let case_sensitive = f.case_sensitive.unwrap_or(false);
        let too_long = |s: &str| s.chars().count() > MAX_FILTER_CHARS;
        match (&f.contains, &f.regex) {
            (Some(_), Some(_)) | (None, None) => Err(BackendError::invalid_argument(
                "filter needs exactly one of contains and regex",
            )),
            (Some(s), None) | (None, Some(s)) if s.is_empty() || too_long(s) => {
                Err(BackendError::invalid_argument(format!(
                    "filter text must be 1..={MAX_FILTER_CHARS} characters"
                )))
            }
            (Some(s), None) => Ok(Matcher::Text {
                needle: if case_sensitive {
                    s.clone()
                } else {
                    s.to_lowercase()
                },
                case_sensitive,
            }),
            (None, Some(r)) => RegexBuilder::new(r)
                .case_insensitive(!case_sensitive)
                .size_limit(REGEX_SIZE_LIMIT)
                .build()
                .map(Matcher::Regex)
                .map_err(|e| BackendError::invalid_argument(format!("invalid regex: {e}"))),
        }
    }

    pub fn matches(&self, line: &str) -> bool {
        match self {
            Matcher::All => true,
            Matcher::Text {
                needle,
                case_sensitive: true,
            } => line.contains(needle.as_str()),
            Matcher::Text { needle, .. } => line.to_lowercase().contains(needle.as_str()),
            Matcher::Regex(r) => r.is_match(line),
        }
    }
}

/// С какого момента читать лог: секунды назад или время RFC 3339.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Since {
    Seconds(u32),
    Time(String),
}

/// Чтение логов у бэкенда (уровень `logs` проверяет и шлюз, и бэкенд).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogsOp {
    /// Лог одного контейнера пода. Без `container` — контейнер по умолчанию (аннотация
    /// `kubectl.kubernetes.io/default-container`, иначе первый).
    Pod {
        connection_id: String,
        namespace: String,
        pod: String,
        container: Option<String>,
        previous: bool,
        since: Option<Since>,
        tail_lines: Option<u32>,
    },
}

impl LogsOp {
    pub fn connection_id(&self) -> &str {
        match self {
            LogsOp::Pod { connection_id, .. } => connection_id,
        }
    }

    pub fn namespace(&self) -> &str {
        match self {
            LogsOp::Pod { namespace, .. } => namespace,
        }
    }
}

/// Строка лога как её прочитал бэкенд: время (RFC 3339) и текст не длиннее
/// [`READ_LINE_BYTES`]; `cut` — от строки осталось только начало.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawLine {
    pub ts: Option<String>,
    pub text: String,
    pub cut: bool,
}

/// Лог одного контейнера.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainerLog {
    pub pod: String,
    pub container: String,
    pub lines: Vec<RawLine>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogsOutput {
    Pod(ContainerLog),
}

/// Строка в ответе: после маскировки и обрезки.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Line {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ts: Option<String>,
    pub text: String,
    /// Строка длиннее 4 КБ показана не целиком.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub cut: bool,
}

/// Строка не длиннее `max` байт по границе символа и признак обрезки.
pub fn cut_to(text: &str, max: usize) -> (String, bool) {
    if text.len() <= max {
        return (text.to_string(), false);
    }
    let mut end = max;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    (text[..end].to_string(), true)
}

/// Строка для модели: маскировка по целому прочитанному тексту (шаблоны и пары с именами
/// подключения), затем обрезка до [`MAX_LINE_BYTES`]. Возвращает строку и число замен.
pub fn masked_line(m: &Masker, raw: &RawLine) -> (Line, usize) {
    let (text, n) = m.text(&raw.text);
    let (text, cut) = cut_to(&text, MAX_LINE_BYTES);
    (
        Line {
            ts: raw.ts.clone(),
            text,
            cut: cut || raw.cut,
        },
        n,
    )
}

/// Ответ `get_pod_logs`.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PodLogs {
    pub pod: String,
    pub container: String,
    pub lines: Vec<Line>,
    /// Строк прочитано из лога (до фильтра).
    pub scanned: u64,
    /// Строк в ответе.
    pub returned: u64,
    /// Замен маскировки в строках ответа.
    pub masked: u64,
    pub truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<&'static str>,
}

fn size(v: &impl Serialize) -> usize {
    serde_json::to_vec(v).map_or(usize::MAX, |b| b.len())
}

/// Лог контейнера для модели: фильтр по немаскированным строкам, маскировка, обрезка строк;
/// не помещается в 100 КБ — отбрасываются самые старые строки.
pub fn pod_logs(log: ContainerLog, matcher: &Matcher, m: &Masker) -> PodLogs {
    let scanned = log.lines.len() as u64;
    let mut lines: Vec<(Line, usize)> = log
        .lines
        .iter()
        .filter(|l| matcher.matches(&l.text))
        .map(|l| masked_line(m, l))
        .collect();
    let mut out = PodLogs {
        pod: log.pod,
        container: log.container,
        lines: Vec::new(),
        scanned,
        returned: 0,
        masked: 0,
        truncated: false,
        hint: None,
    };
    // Размер без строк, затем строки с конца, пока помещаются.
    let mut budget = glafira_mcp_core::fit::MAX_RESPONSE_BYTES
        .saturating_sub(size(&out) + OLDER_LINES_HINT.len() + 16);
    let mut keep = 0;
    for (line, _) in lines.iter().rev() {
        let n = size(line) + 1;
        if n > budget {
            break;
        }
        budget -= n;
        keep += 1;
    }
    if keep < lines.len() {
        lines.drain(..lines.len() - keep);
        out.truncated = true;
        out.hint = Some(OLDER_LINES_HINT);
    }
    out.masked = lines.iter().map(|(_, n)| *n as u64).sum();
    out.lines = lines.into_iter().map(|(l, _)| l).collect();
    out.returned = out.lines.len() as u64;
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw(text: &str) -> RawLine {
        RawLine {
            ts: Some("2026-10-30T10:00:00.000000001Z".into()),
            text: text.into(),
            cut: false,
        }
    }

    fn filter(contains: Option<&str>, regex: Option<&str>, case: Option<bool>) -> LogFilter {
        LogFilter {
            contains: contains.map(String::from),
            regex: regex.map(String::from),
            case_sensitive: case,
        }
    }

    #[test]
    fn filter_needs_exactly_one_kind_of_text() {
        assert!(matches!(Matcher::new(None), Ok(Matcher::All)));
        for f in [
            filter(None, None, None),
            filter(Some("a"), Some("b"), None),
            filter(Some(""), None, None),
            filter(None, Some("("), None),
            filter(Some(&"x".repeat(MAX_FILTER_CHARS + 1)), None, None),
        ] {
            assert_eq!(
                Matcher::new(Some(&f)).unwrap_err().code,
                "invalid_argument",
                "{f:?}"
            );
        }
    }

    #[test]
    fn filter_ignores_the_case_unless_asked() {
        let m = Matcher::new(Some(&filter(Some("ERROR"), None, None))).unwrap();
        assert!(m.matches("an error here"));
        let m = Matcher::new(Some(&filter(Some("ERROR"), None, Some(true)))).unwrap();
        assert!(!m.matches("an error here") && m.matches("an ERROR here"));
        let m = Matcher::new(Some(&filter(None, Some(r"timeout \d+ms"), None))).unwrap();
        assert!(m.matches("TIMEOUT 300ms"));
    }

    #[test]
    fn filter_sees_values_the_model_gets_masked() {
        let log = ContainerLog {
            pod: "web-1".into(),
            container: "web".into(),
            lines: vec![
                raw("login ok"),
                raw("connect failed password=hunter2 user=bob"),
                raw("Bearer abc.def-123 rejected"),
            ],
        };
        let m = Matcher::new(Some(&filter(Some("hunter2"), None, None))).unwrap();
        let out = pod_logs(log.clone(), &m, &Masker::default());
        assert_eq!((out.scanned, out.returned, out.masked), (3, 1, 1));
        assert_eq!(out.lines[0].text, "connect failed password=*** user=bob");
        let out = pod_logs(log, &Matcher::All, &Masker::default());
        assert_eq!((out.returned, out.masked), (3, 2));
        assert_eq!(out.lines[2].text, "Bearer *** rejected");
        assert!(!out.truncated);
    }

    #[test]
    fn long_lines_are_masked_whole_then_cut() {
        // Пароль на границе 4 КБ маскируется по целому значению.
        let text = format!("{}password=hunter2hunter2", "a".repeat(MAX_LINE_BYTES - 6));
        let (line, n) = masked_line(&Masker::default(), &raw(&text));
        assert_eq!(n, 1);
        assert!(line.cut && line.text.len() <= MAX_LINE_BYTES);
        assert!(
            !line.text.contains("hunter"),
            "{}",
            &line.text[MAX_LINE_BYTES - 40..]
        );
        // Обрезанная бэкендом строка остаётся помеченной; многобайтные символы не рвутся.
        let mut r = raw(&"я".repeat(3000));
        r.cut = true;
        let (line, _) = masked_line(&Masker::default(), &r);
        assert!(line.cut && line.text.len() <= MAX_LINE_BYTES);
        assert!(line.text.chars().all(|c| c == 'я'));
    }

    #[test]
    fn oldest_lines_are_dropped_to_fit_100_kb() {
        let lines: Vec<RawLine> = (0..400)
            .map(|i| raw(&format!("{i:04} {}", "x".repeat(1000))))
            .collect();
        let out = pod_logs(
            ContainerLog {
                pod: "p".into(),
                container: "c".into(),
                lines,
            },
            &Matcher::All,
            &Masker::default(),
        );
        assert!(out.truncated && out.hint.is_some());
        assert!(
            size(&out) <= glafira_mcp_core::fit::MAX_RESPONSE_BYTES,
            "{}",
            size(&out)
        );
        assert_eq!(out.scanned, 400);
        assert_eq!(out.returned as usize, out.lines.len());
        assert!(
            out.lines.last().unwrap().text.starts_with("0399 "),
            "новые строки остаются"
        );
    }
}
