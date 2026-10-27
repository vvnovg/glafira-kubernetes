//! Журнал MCP (спец. §10.7): `mcp-audit.jsonl`, строка — событие, ротация в `mcp-audit.1.jsonl`.
//! Содержимое ответов не пишется. Сбой записи журнала не ломает вызов (best effort).

use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use serde_json::Value;

pub const FILE_NAME: &str = "mcp-audit.jsonl";
pub const ROTATED_NAME: &str = "mcp-audit.1.jsonl";
pub const MAX_BYTES: u64 = 5 * 1024 * 1024;
pub const MAX_ARGS_BYTES: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Outcome {
    Ok,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AuditConnection {
    pub id: String,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "event", rename_all = "camelCase")]
pub enum AuditEvent {
    #[serde(rename_all = "camelCase")]
    Call {
        session: String,
        client: Option<String>,
        tool: String,
        connection: Option<AuditConnection>,
        args: Value,
        outcome: Outcome,
        code: Option<String>,
        bytes: usize,
        ms: u64,
    },
    #[serde(rename_all = "camelCase")]
    SessionStarted {
        session: String,
    },
    #[serde(rename_all = "camelCase")]
    SessionEnded {
        session: String,
        client: Option<String>,
        calls: u64,
    },
    #[serde(rename_all = "camelCase")]
    Approved {
        session: String,
        client: Option<String>,
    },
    #[serde(rename_all = "camelCase")]
    Rejected {
        session: String,
        client: Option<String>,
    },
    #[serde(rename_all = "camelCase")]
    Revoked {
        client: String,
    },
    #[serde(rename_all = "camelCase")]
    ServerStarted {
        endpoint: String,
    },
    ServerStopped,
}

#[derive(Serialize)]
struct Line<'a> {
    ts: u64,
    #[serde(flatten)]
    event: &'a AuditEvent,
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

/// Аргументы вызова для журнала — не длиннее [`MAX_ARGS_BYTES`] байт JSON.
pub fn truncate_args(v: Value) -> Value {
    let s = v.to_string();
    if s.len() <= MAX_ARGS_BYTES {
        return v;
    }
    let mut end = MAX_ARGS_BYTES;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    Value::String(format!("{}…", &s[..end]))
}

pub struct AuditLog {
    dir: PathBuf,
    file: Mutex<Option<File>>,
}

impl AuditLog {
    pub fn new(dir: &Path) -> Self {
        Self {
            dir: dir.to_path_buf(),
            file: Mutex::new(None),
        }
    }

    pub fn path(&self) -> PathBuf {
        self.dir.join(FILE_NAME)
    }

    pub fn record(&self, event: AuditEvent) {
        let line = Line {
            ts: now_ms(),
            event: &event,
        };
        let mut bytes = match serde_json::to_vec(&line) {
            Ok(b) => b,
            Err(e) => return tracing::warn!("mcp audit: {e}"),
        };
        bytes.push(b'\n');
        if let Err(e) = self.write(&bytes) {
            tracing::warn!("mcp audit not written: {e}");
        }
    }

    fn open(&self) -> io::Result<File> {
        let mut opts = OpenOptions::new();
        opts.create(true).append(true);
        #[cfg(unix)]
        std::os::unix::fs::OpenOptionsExt::mode(&mut opts, 0o600);
        opts.open(self.path())
    }

    fn write(&self, line: &[u8]) -> io::Result<()> {
        let mut guard = self.file.lock().unwrap_or_else(|e| e.into_inner());
        if guard.is_none() {
            *guard = Some(self.open()?);
        }
        let len = guard
            .as_ref()
            .map_or(0, |f| f.metadata().map_or(0, |m| m.len()));
        if len + line.len() as u64 > MAX_BYTES {
            *guard = None;
            std::fs::rename(self.path(), self.dir.join(ROTATED_NAME))?;
            *guard = Some(self.open()?);
        }
        let f = guard.as_mut().expect("file opened above");
        f.write_all(line)?;
        f.flush()
    }

    /// Последние `limit` записей, новые первыми.
    pub fn recent(&self, limit: usize) -> Vec<Value> {
        let mut lines: Vec<Value> = Vec::new();
        for name in [ROTATED_NAME, FILE_NAME] {
            let Ok(f) = File::open(self.dir.join(name)) else {
                continue;
            };
            // По байтам, не `lines()`: строка с битым UTF-8 (оборванная запись) пропускается,
            // а не обрывает чтение остального файла.
            lines.extend(
                BufReader::new(f)
                    .split(b'\n')
                    .map_while(Result::ok)
                    .filter_map(|l| serde_json::from_slice(&l).ok()),
            );
        }
        let start = lines.len().saturating_sub(limit);
        lines.split_off(start).into_iter().rev().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_lines_newest_first() {
        let dir = tempfile::tempdir().unwrap();
        let log = AuditLog::new(dir.path());
        log.record(AuditEvent::SessionStarted {
            session: "s1".into(),
        });
        log.record(AuditEvent::Call {
            session: "s1".into(),
            client: Some("claude-ai 1.0".into()),
            tool: "list_topics".into(),
            connection: Some(AuditConnection {
                id: "c1".into(),
                name: "dev".into(),
            }),
            args: serde_json::json!({"connectionId": "c1"}),
            outcome: Outcome::Ok,
            code: None,
            bytes: 10,
            ms: 3,
        });
        let r = log.recent(10);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0]["event"], "call");
        assert_eq!(r[0]["connection"]["name"], "dev");
        assert_eq!(r[0]["outcome"], "ok");
        assert!(r[0]["ts"].as_u64().unwrap() > 0);
        assert_eq!(r[1]["event"], "sessionStarted");
        assert_eq!(log.recent(1).len(), 1);
    }

    #[cfg(unix)]
    #[test]
    fn file_is_private() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let log = AuditLog::new(dir.path());
        log.record(AuditEvent::ServerStopped);
        let mode = std::fs::metadata(log.path()).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }

    #[test]
    fn rotates_at_limit() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(FILE_NAME), vec![b' '; MAX_BYTES as usize]).unwrap();
        let log = AuditLog::new(dir.path());
        log.record(AuditEvent::ServerStopped);
        assert!(dir.path().join(ROTATED_NAME).exists());
        let current = std::fs::read_to_string(dir.path().join(FILE_NAME)).unwrap();
        assert_eq!(current.lines().count(), 1);
        assert_eq!(
            log.recent(5).len(),
            1,
            "старый файл из пробелов не разбирается"
        );
    }

    #[test]
    fn broken_line_is_skipped_not_the_rest() {
        let dir = tempfile::tempdir().unwrap();
        let log = AuditLog::new(dir.path());
        log.record(AuditEvent::ServerStopped);
        // Строка, оборванная посреди многобайтного символа (сбой записи), и строка не JSON.
        let mut f = OpenOptions::new().append(true).open(log.path()).unwrap();
        f.write_all(b"{\"event\":\"call\",\"tool\":\"\xd0\n{not json\n")
            .unwrap();
        log.record(AuditEvent::Revoked {
            client: "cursor".into(),
        });
        let r = log.recent(10);
        assert_eq!(r.len(), 2, "{r:?}");
        assert_eq!(r[0]["event"], "revoked");
        assert_eq!(r[1]["event"], "serverStopped");
    }

    #[test]
    fn long_args_are_truncated() {
        let v = truncate_args(serde_json::json!({"topics": vec!["t".repeat(100); 50]}));
        let s = v.as_str().unwrap();
        assert!(s.ends_with('…') && s.len() <= MAX_ARGS_BYTES + 3);
        let small = serde_json::json!({"a": 1});
        assert_eq!(truncate_args(small.clone()), small);
    }
}
