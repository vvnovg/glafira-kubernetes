//! Маскировка (спец. §10.3): чистые функции, которые применяются к ответу до его сборки.
//! Замаскированное значение — `***`, число замен — счётчик `masked` в ответе.
//!
//! - **имена полей** (по умолчанию [`DEFAULT_NAMES`], без учёта регистра, вхождением): значение
//!   аннотации с таким ключом, флага `--name=value` и `--name value` в `command`/`args`, пары
//!   `name=value`, `name: value` и `"name": value` в тексте;
//! - **встроенные шаблоны:** JWT, `Bearer <токен>`, `user:pass@` в URL, email, номера карт
//!   с проверкой Луна, ключи `AKIA…`/`ASIA…`, PEM-блоки приватных ключей, токены `sha256~…`.
//!
//! Аннотации с копией объекта (`last-applied-configuration` и подобные) в ответ не попадают.

use std::collections::BTreeMap;
use std::sync::LazyLock;

use regex::{Captures, Regex};

pub const MASK: &str = "***";

/// Имена полей для маскировки по умолчанию (спец. §10.3).
pub const DEFAULT_NAMES: [&str; 10] = [
    "password",
    "passwd",
    "secret",
    "token",
    "apikey",
    "api_key",
    "api-key",
    "credentials",
    "private",
    "authorization",
];

/// Аннотации с копией всего объекта: в них лежат и значения env, и всё остальное (как в YAML
/// интерфейса, спец. §6.4).
pub const OBJECT_COPIES: [&str; 4] = [
    "kubectl.kubernetes.io/last-applied-configuration",
    "kapp.k14s.io/original",
    "objectset.rio.cattle.io/applied",
    "openshift.io/encoded-deployment-config",
];

fn re(pattern: &str) -> Regex {
    Regex::new(pattern).expect("шаблон маскировки")
}

static PEM_KEY: LazyLock<Regex> = LazyLock::new(|| {
    re(r"(?s)-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----.*?(?:-----END [A-Z0-9 ]*PRIVATE KEY-----|$)")
});
static JWT: LazyLock<Regex> =
    LazyLock::new(|| re(r"\beyJ[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]{5,}"));
static BEARER: LazyLock<Regex> = LazyLock::new(|| re(r"(?i)\b(bearer)\s+[A-Za-z0-9\-._~+/]+=*"));
static URL_CREDENTIALS: LazyLock<Regex> =
    LazyLock::new(|| re(r"(?i)\b([a-z][a-z0-9+.\-]*://)([^/\s:@]+):([^/\s@]+)@"));
static AWS_KEY: LazyLock<Regex> = LazyLock::new(|| re(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b"));
static OPENSHIFT_TOKEN: LazyLock<Regex> = LazyLock::new(|| re(r"sha256~[A-Za-z0-9_\-]{10,}"));
static EMAIL: LazyLock<Regex> =
    LazyLock::new(|| re(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9\-]+(?:\.[A-Za-z0-9\-]+)*\.[A-Za-z]{2,}\b"));
static CARD: LazyLock<Regex> = LazyLock::new(|| re(r"\b\d(?:[ \-]?\d){12,18}\b"));

/// Номер проходит проверку Луна.
fn luhn(digits: &str) -> bool {
    let mut sum = 0;
    for (i, d) in digits
        .chars()
        .rev()
        .filter_map(|c| c.to_digit(10))
        .enumerate()
    {
        let d = if i % 2 == 1 {
            let x = d * 2;
            if x > 9 { x - 9 } else { x }
        } else {
            d
        };
        sum += d;
    }
    sum % 10 == 0
}

/// Маскировщик с именами полей подключения.
pub struct Masker {
    names: Vec<String>,
    pairs: Option<Regex>,
}

impl Default for Masker {
    fn default() -> Self {
        Self::new(DEFAULT_NAMES)
    }
}

impl Masker {
    pub fn new<S: AsRef<str>>(names: impl IntoIterator<Item = S>) -> Self {
        let names: Vec<String> = names
            .into_iter()
            .map(|n| n.as_ref().trim().to_lowercase())
            .filter(|n| !n.is_empty())
            .collect();
        let pairs = (!names.is_empty()).then(|| {
            let alt = names
                .iter()
                .map(|n| regex::escape(n))
                .collect::<Vec<_>>()
                .join("|");
            re(&format!(
                r#"(?i)([A-Za-z0-9_.\-]*(?:{alt})[A-Za-z0-9_.\-]*"?\s*[=:]\s*)("[^"]*"|'[^']*'|[^\s,;&|"']+)"#
            ))
        });
        Self { names, pairs }
    }

    /// Имя поля из списка маскировки (вхождением, без учёта регистра).
    pub fn is_secret_name(&self, name: &str) -> bool {
        let n = name.to_lowercase();
        self.names.iter().any(|m| n.contains(m.as_str()))
    }

    /// Встроенные шаблоны.
    pub fn patterns(&self, text: &str) -> (String, usize) {
        let mut n = 0;
        let mut s = PEM_KEY
            .replace_all(text, |_: &Captures| {
                n += 1;
                MASK.to_string()
            })
            .into_owned();
        s = JWT
            .replace_all(&s, |_: &Captures| {
                n += 1;
                MASK.to_string()
            })
            .into_owned();
        s = BEARER
            .replace_all(&s, |c: &Captures| {
                n += 1;
                format!("{} {MASK}", &c[1])
            })
            .into_owned();
        s = URL_CREDENTIALS
            .replace_all(&s, |c: &Captures| {
                n += 1;
                format!("{}{}:{MASK}@", &c[1], &c[2])
            })
            .into_owned();
        s = AWS_KEY
            .replace_all(&s, |_: &Captures| {
                n += 1;
                MASK.to_string()
            })
            .into_owned();
        s = OPENSHIFT_TOKEN
            .replace_all(&s, |_: &Captures| {
                n += 1;
                format!("sha256~{MASK}")
            })
            .into_owned();
        s = EMAIL
            .replace_all(&s, |_: &Captures| {
                n += 1;
                MASK.to_string()
            })
            .into_owned();
        s = CARD
            .replace_all(&s, |c: &Captures| {
                let digits: String = c[0].chars().filter(char::is_ascii_digit).collect();
                if luhn(&digits) {
                    n += 1;
                    MASK.to_string()
                } else {
                    c[0].to_string()
                }
            })
            .into_owned();
        (s, n)
    }

    /// Пары `name=value`, `name: value` и `"name": value` с именем из списка.
    pub fn pairs(&self, text: &str) -> (String, usize) {
        let Some(re) = &self.pairs else {
            return (text.to_string(), 0);
        };
        let mut n = 0;
        let s = re
            .replace_all(text, |c: &Captures| {
                let value = &c[2];
                if value == MASK || value == format!("\"{MASK}\"") {
                    return c[0].to_string();
                }
                n += 1;
                match value.chars().next() {
                    Some(q @ ('"' | '\'')) => format!("{}{q}{MASK}{q}", &c[1]),
                    _ => format!("{}{MASK}", &c[1]),
                }
            })
            .into_owned();
        (s, n)
    }

    /// Текст из кластера: сначала встроенные шаблоны (`Authorization: Bearer …` теряет токен
    /// целиком), затем пары с именами из списка.
    pub fn text(&self, text: &str) -> (String, usize) {
        let (s, a) = self.patterns(text);
        let (s, b) = self.pairs(&s);
        (s, a + b)
    }

    /// `command` или `args` контейнера: каждый аргумент — как текст; значение после флага с
    /// именем из списка (`--password`, `-token`) — целиком.
    pub fn args(&self, args: &[String]) -> (Vec<String>, usize) {
        let mut n = 0;
        let mut out = Vec::with_capacity(args.len());
        let mut hide_next = false;
        for a in args {
            if hide_next && !a.starts_with('-') {
                out.push(MASK.to_string());
                n += 1;
                hide_next = false;
                continue;
            }
            let (s, k) = self.text(a);
            n += k;
            hide_next = a.starts_with('-')
                && !a.contains('=')
                && self.is_secret_name(a.trim_start_matches('-'));
            out.push(s);
        }
        (out, n)
    }

    /// Аннотации: без копий объекта; ключ с именем из списка — `***`, остальные значения —
    /// как текст.
    pub fn annotations(&self, a: &BTreeMap<String, String>) -> (BTreeMap<String, String>, usize) {
        let mut n = 0;
        let out = a
            .iter()
            .filter(|(k, _)| !OBJECT_COPIES.contains(&k.as_str()))
            .map(|(k, v)| {
                if self.is_secret_name(k) {
                    n += 1;
                    (k.clone(), MASK.to_string())
                } else {
                    let (s, c) = self.text(v);
                    n += c;
                    (k.clone(), s)
                }
            })
            .collect();
        (out, n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m() -> Masker {
        Masker::default()
    }

    #[test]
    fn builtin_patterns() {
        let jwt = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozjgNryP4J3jVmNHl0w5N";
        let cases = [
            (format!("token {jwt} end"), "token *** end".to_string(), 1),
            (
                "Authorization: Bearer abcdef.123-xyz=".into(),
                "Authorization: Bearer ***".into(),
                1,
            ),
            (
                "postgres://app:s3cr3t@db.svc:5432/app".into(),
                "postgres://app:***@db.svc:5432/app".into(),
                1,
            ),
            ("mail ops@example.com now".into(), "mail *** now".into(), 1),
            ("card 4111 1111 1111 1111".into(), "card ***".into(), 1),
            (
                "not a card 1234 5678 9012 3456".into(),
                "not a card 1234 5678 9012 3456".into(),
                0,
            ),
            ("key AKIAIOSFODNN7EXAMPLE".into(), "key ***".into(), 1),
            (
                "-----BEGIN RSA PRIVATE KEY-----\nMIIE\n-----END RSA PRIVATE KEY-----".into(),
                "***".into(),
                1,
            ),
            (
                "oc login --token=sha256~AbCdEfGhIjKlMnOp".into(),
                "oc login --token=sha256~***".into(),
                1,
            ),
            (
                "image web@sha256:abcdef".into(),
                "image web@sha256:abcdef".into(),
                0,
            ),
        ];
        for (input, want, count) in cases {
            assert_eq!(m().patterns(&input), (want, count), "{input}");
        }
    }

    #[test]
    fn pairs_with_secret_names() {
        let cases = [
            ("--db-password=hunter2", "--db-password=***", 1),
            ("PASSWORD=x ./run", "PASSWORD=*** ./run", 1),
            (
                r#"{"apiKey": "k-1", "user": "bob"}"#,
                r#"{"apiKey": "***", "user": "bob"}"#,
                1,
            ),
            ("token: 'abc'", "token: '***'", 1),
            ("timeout=5s", "timeout=5s", 0),
            ("password=***", "password=***", 0),
        ];
        for (input, want, count) in cases {
            assert_eq!(m().pairs(input), (want.to_string(), count), "{input}");
        }
    }

    #[test]
    fn args_hide_values_of_secret_flags() {
        let args: Vec<String> = [
            "--port=8080",
            "--client-secret",
            "s3cr3t",
            "--token=abc",
            "--verbose",
            "postgres://u:p@db/x",
        ]
        .map(String::from)
        .to_vec();
        let (out, n) = m().args(&args);
        assert_eq!(
            out,
            [
                "--port=8080",
                "--client-secret",
                "***",
                "--token=***",
                "--verbose",
                "postgres://u:***@db/x"
            ]
        );
        assert_eq!(n, 3);
        let (out, n) = m().args(&["--password".into(), "--next".into()]);
        assert_eq!((out.len(), n), (2, 0), "за флагом нет значения");
    }

    #[test]
    fn annotations_without_object_copies_and_secret_keys() {
        let a = BTreeMap::from([
            (
                "kubectl.kubernetes.io/last-applied-configuration".to_string(),
                "{...}".to_string(),
            ),
            ("example.com/api-token".into(), "abc".into()),
            (
                "kubernetes.io/change-cause".into(),
                "kubectl set env DB_PASSWORD=x".into(),
            ),
            ("team".into(), "payments".into()),
        ]);
        let (out, n) = m().annotations(&a);
        assert_eq!(n, 2);
        assert_eq!(
            out,
            BTreeMap::from([
                ("example.com/api-token".to_string(), "***".to_string()),
                (
                    "kubernetes.io/change-cause".into(),
                    "kubectl set env DB_PASSWORD=***".into()
                ),
                ("team".into(), "payments".into()),
            ])
        );
    }

    #[test]
    fn custom_names_replace_the_default_list() {
        let m = Masker::new(["pin"]);
        assert!(m.is_secret_name("CARD_PIN"));
        assert!(!m.is_secret_name("PASSWORD"));
        assert_eq!(m.pairs("pin=1234 password=x").1, 1);
        assert_eq!(Masker::new(Vec::<String>::new()).pairs("password=x").1, 0);
    }
}
