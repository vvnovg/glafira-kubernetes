//! Сторожок «через MCP кластер не изменить» (спец. §4.1, §17): код `mcp/` не берёт клиент `kube`
//! мимо `ReadClient`, не знает изменяющих операций (`ops`, команды изменений) и не ходит в сеть
//! сам. Проверка — по разбору кода, а не по подстрокам: комментарии и содержимое литералов
//! вырезаются, `use`-деревья раскрываются (`use kube::{Api, Client}` — это пути `kube::Api` и
//! `kube::Client`), псевдонимы (`as`), `self` и `super` разрешаются, пробелы вокруг `::` не мешают.
//! Строки примеров в тестах этого модуля — литералы, поэтому сам модуль под проверку не попадает.

use std::collections::HashMap;
use std::path::Path;

/// Слова кода, которых в `mcp/` нет ни в каком виде: клиент и `Api` из `kube`, запись в кластер
/// (`ClusterWriter`, `Connected::writer`, `Connected::kube_client`, параметры запросов записи),
/// команды изменений.
const FORBIDDEN_WORDS: &[&str] = &[
    "Api",
    "Client",
    "ClusterWriter",
    "KubeWriter",
    "writer",
    "kube_client",
    "Patch",
    "PostParams",
    "PatchParams",
    "DeleteParams",
    "apply_change",
    "preview_change",
];

/// Пути, начало которых запрещено: изменяющие операции, команды изменений, клиенты HTTP.
const FORBIDDEN_PATHS: &[&str] = &[
    "crate::ops",
    "crate::commands::changes",
    "reqwest",
    "hyper",
    "http",
];

/// Из `kube` разрешено только это (и то, что внутри): параметры списка для `ReadClient` и тело
/// ответа об ошибке API-сервера (моки в тестах).
const KUBE_ALLOWED: &[&str] = &["kube::api::ListParams", "kube::core::Status"];

#[derive(Debug, Clone, PartialEq, Eq)]
enum Tok {
    Ident(String),
    /// `::`
    Path,
    Punct(char),
}

/// Токены кода без комментариев, строк, символов и времён жизни.
fn tokens(src: &str) -> Vec<Tok> {
    let c: Vec<char> = src.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    let ident_start = |ch: char| ch == '_' || ch.is_alphabetic();
    let ident_char = |ch: char| ch == '_' || ch.is_alphanumeric();
    while i < c.len() {
        let ch = c[i];
        let next = c.get(i + 1).copied();
        if ch == '/' && next == Some('/') {
            while i < c.len() && c[i] != '\n' {
                i += 1;
            }
        } else if ch == '/' && next == Some('*') {
            let mut depth = 0;
            while i < c.len() {
                if c[i] == '/' && c.get(i + 1) == Some(&'*') {
                    depth += 1;
                    i += 2;
                } else if c[i] == '*' && c.get(i + 1) == Some(&'/') {
                    depth -= 1;
                    i += 2;
                    if depth == 0 {
                        break;
                    }
                } else {
                    i += 1;
                }
            }
        } else if let Some(end) = raw_string_end(&c, i) {
            i = end;
        } else if ch == '"' {
            i += 1;
            while i < c.len() && c[i] != '"' {
                i += if c[i] == '\\' { 2 } else { 1 };
            }
            i += 1;
        } else if ch == '\'' {
            if next == Some('\\') {
                // Символ с экранированием: '\n', '\'', '\u{1F600}'.
                i += 2;
                while i < c.len() && c[i] != '\'' {
                    i += 1;
                }
                i += 1;
            } else if c.get(i + 2) == Some(&'\'') {
                i += 3;
            } else {
                // Время жизни: 'a, 'static.
                i += 1;
                while i < c.len() && ident_char(c[i]) {
                    i += 1;
                }
            }
        } else if ident_start(ch) {
            // Сырой идентификатор r#type.
            if ch == 'r' && next == Some('#') && c.get(i + 2).is_some_and(|x| ident_start(*x)) {
                i += 2;
            }
            let from = i;
            while i < c.len() && ident_char(c[i]) {
                i += 1;
            }
            out.push(Tok::Ident(c[from..i].iter().collect()));
        } else if ch == ':' && next == Some(':') {
            out.push(Tok::Path);
            i += 2;
        } else {
            if !ch.is_whitespace() {
                out.push(Tok::Punct(ch));
            }
            i += 1;
        }
    }
    out
}

/// Конец сырой или байтовой строки (`r"…"`, `r#"…"#`, `b"…"`, `br#"…"#`, `c"…"`), начатой в `i`.
fn raw_string_end(c: &[char], i: usize) -> Option<usize> {
    if i > 0 && (c[i - 1] == '_' || c[i - 1].is_alphanumeric()) {
        return None;
    }
    let mut j = i;
    if matches!(c.get(j), Some('b' | 'c')) {
        j += 1;
    }
    let raw = c.get(j) == Some(&'r');
    if raw {
        j += 1;
    }
    if j == i {
        return None;
    }
    let mut hashes = 0;
    while raw && c.get(j) == Some(&'#') {
        hashes += 1;
        j += 1;
    }
    if c.get(j) != Some(&'"') {
        return None;
    }
    j += 1;
    if !raw {
        while j < c.len() && c[j] != '"' {
            j += if c[j] == '\\' { 2 } else { 1 };
        }
        return Some(j + 1);
    }
    while j < c.len() {
        if c[j] == '"' && (1..=hashes).all(|k| c.get(j + k) == Some(&'#')) {
            return Some(j + 1 + hashes);
        }
        j += 1;
    }
    Some(j)
}

/// Путь из `use`: сегменты, `*` в конце и имя, под которым он виден в файле.
#[derive(Debug)]
struct UsePath {
    segments: Vec<String>,
    glob: bool,
    alias: Option<String>,
}

/// Разбор дерева `use` с позиции `i`: `a::b::{c, d::e as f, g::*}`.
fn use_tree(t: &[Tok], i: &mut usize, prefix: Vec<String>, out: &mut Vec<UsePath>) {
    let mut segs = prefix;
    loop {
        match t.get(*i) {
            Some(Tok::Path) => *i += 1,
            Some(Tok::Ident(s)) if s != "as" => {
                segs.push(s.clone());
                *i += 1;
            }
            Some(Tok::Punct('*')) => {
                *i += 1;
                out.push(UsePath {
                    segments: segs,
                    glob: true,
                    alias: None,
                });
                return;
            }
            Some(Tok::Punct('{')) => {
                *i += 1;
                while !matches!(t.get(*i), Some(Tok::Punct('}')) | None) {
                    use_tree(t, i, segs.clone(), out);
                    if t.get(*i) == Some(&Tok::Punct(',')) {
                        *i += 1;
                    }
                }
                *i += 1;
                return;
            }
            _ => break,
        }
    }
    let mut alias = None;
    if t.get(*i) == Some(&Tok::Ident("as".into())) {
        if let Some(Tok::Ident(a)) = t.get(*i + 1) {
            alias = Some(a.clone());
        }
        *i += 2;
    }
    // `use a::{self}` — сам `a`.
    if segs.last().is_some_and(|s| s == "self") {
        segs.pop();
    }
    out.push(UsePath {
        segments: segs,
        glob: false,
        alias,
    });
}

/// Путь модуля по пути файла внутри `src/`: `mcp/backend.rs` → `crate::mcp::backend`.
fn module_of(rel: &Path) -> Vec<String> {
    let mut m = vec!["crate".to_string()];
    let parts: Vec<String> = rel
        .iter()
        .map(|p| p.to_string_lossy().into_owned())
        .collect();
    for (k, p) in parts.iter().enumerate() {
        let last = k + 1 == parts.len();
        if !last {
            m.push(p.clone());
        } else if p != "mod.rs" && p != "lib.rs" && p != "main.rs" {
            m.push(p.trim_end_matches(".rs").to_string());
        }
    }
    m
}

/// Полный путь: `self`, `super` и `crate` — от модуля, псевдоним `use` — его путём.
fn resolve(
    path: &[String],
    module: &[String],
    aliases: &HashMap<String, Vec<String>>,
) -> Vec<String> {
    let Some(first) = path.first() else {
        return Vec::new();
    };
    let mut base: Vec<String> = match first.as_str() {
        "crate" => vec!["crate".into()],
        "self" => module.to_vec(),
        "super" => {
            let mut m = module.to_vec();
            let mut k = 0;
            while path.get(k).is_some_and(|s| s == "super") {
                if m.len() > 1 {
                    m.pop();
                }
                k += 1;
            }
            m.extend(path[k..].iter().cloned());
            return m;
        }
        other => match aliases.get(other) {
            Some(full) => full.clone(),
            None => vec![other.to_string()],
        },
    };
    base.extend(path[1..].iter().cloned());
    base
}

fn starts_with(path: &[String], prefix: &str) -> bool {
    let p: Vec<&str> = prefix.split("::").collect();
    path.len() >= p.len() && path.iter().zip(&p).all(|(a, b)| a == b)
}

/// Нарушения в одном файле: слово, путь или `use`, которых в `mcp/` быть не должно.
fn violations(module: &[String], src: &str) -> Vec<String> {
    let t = tokens(src);
    let mut out = Vec::new();
    for tok in &t {
        if let Tok::Ident(s) = tok
            && FORBIDDEN_WORDS.contains(&s.as_str())
        {
            out.push(format!("word {s}"));
        }
        if tok == &Tok::Ident("extern".into()) {
            out.push("extern crate".into());
        }
    }
    // Модули внутри файла (`mod tests { … }`): `super` в них — от их собственного пути.
    let mut uses = Vec::new();
    let mut i = 0;
    let mut depth = 0usize;
    let mut stack: Vec<(String, usize)> = Vec::new();
    let mut paths: Vec<(Vec<String>, Vec<String>)> = Vec::new();
    while i < t.len() {
        let here: Vec<String> = module
            .iter()
            .cloned()
            .chain(stack.iter().map(|(m, _)| m.clone()))
            .collect();
        match &t[i] {
            Tok::Ident(s) if s == "mod" => {
                if let (Some(Tok::Ident(name)), Some(Tok::Punct('{'))) =
                    (t.get(i + 1), t.get(i + 2))
                {
                    depth += 1;
                    stack.push((name.clone(), depth));
                    i += 3;
                    continue;
                }
                i += 1;
            }
            Tok::Ident(s) if s == "use" && t.get(i + 1) != Some(&Tok::Punct('<')) => {
                i += 1;
                let mut found = Vec::new();
                use_tree(&t, &mut i, Vec::new(), &mut found);
                for u in found {
                    uses.push((here.clone(), u));
                }
            }
            Tok::Ident(s) => {
                // Путь в коде: a::b::c (и абсолютный ::a::b).
                let mut segs = vec![s.clone()];
                let mut j = i + 1;
                while t.get(j) == Some(&Tok::Path) {
                    match t.get(j + 1) {
                        Some(Tok::Ident(n)) => {
                            segs.push(n.clone());
                            j += 2;
                        }
                        _ => break,
                    }
                }
                if segs.len() > 1 {
                    paths.push((here.clone(), segs));
                }
                i = j;
            }
            Tok::Punct('{') => {
                depth += 1;
                i += 1;
            }
            Tok::Punct('}') => {
                if stack.last().is_some_and(|(_, d)| *d == depth) {
                    stack.pop();
                }
                depth = depth.saturating_sub(1);
                i += 1;
            }
            _ => i += 1,
        }
    }
    let mut aliases: HashMap<String, Vec<String>> = HashMap::new();
    let mut resolved = Vec::new();
    for (here, u) in &uses {
        let full = resolve(&u.segments, here, &aliases);
        if let Some(name) = u.alias.clone().or_else(|| u.segments.last().cloned())
            && !u.glob
            && name != "_"
        {
            aliases.insert(name, full.clone());
        }
        resolved.push((full, u.glob));
    }
    for (here, p) in &paths {
        resolved.push((resolve(p, here, &aliases), false));
    }
    for (path, glob) in resolved {
        let text = path.join("::") + if glob { "::*" } else { "" };
        if FORBIDDEN_PATHS.iter().any(|f| starts_with(&path, f)) {
            out.push(format!("path {text}"));
        }
        if path.first().is_some_and(|s| s == "kube")
            && (glob || !KUBE_ALLOWED.iter().any(|a| starts_with(&path, a)))
        {
            out.push(format!("path {text}"));
        }
        // `use crate::*` открыл бы и `ops`.
        if glob && path == ["crate"] {
            out.push(format!("path {text}"));
        }
    }
    out
}

/// Все `.rs` каталога и подкаталогов.
fn rust_files(dir: &Path, out: &mut Vec<std::path::PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

#[test]
fn mcp_never_reaches_writes_or_the_raw_client() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    rust_files(&src.join("mcp"), &mut files);
    for path in &files {
        let text = std::fs::read_to_string(path).unwrap();
        let module = module_of(path.strip_prefix(&src).unwrap());
        let found = violations(&module, &text);
        assert!(found.is_empty(), "{}: {found:?}", path.display());
    }
    assert!(files.len() >= 8, "mcp/ files: {}", files.len());
}

#[test]
fn use_trees_aliases_and_spacing_do_not_hide_the_raw_client() {
    let m = module_of(Path::new("mcp/backend.rs"));
    assert_eq!(m, ["crate", "mcp", "backend"]);
    for bad in [
        "use kube::{Api, Client};",
        "use kube::{api::{Api as A}, self};",
        "use kube as k;\nfn f() { let _ = k::Config::infer(); }",
        "fn f() { let _ = kube :: Config :: infer(); }",
        "fn f() { let _ = ::kube::Config::infer(); }",
        "use kube::api::*;",
        "use crate::ops;",
        "use crate::{health, ops::preview};",
        "use crate as c;\nfn f() { c::ops::apply(); }",
        "fn f() { super::super::ops::apply(); }",
        "use crate::*;",
        "use crate::commands::changes::apply;",
        "use reqwest::Client as R;",
        "fn f(c: &Connected) { c.writer(); }",
        "fn f(c: &Connected) { let _ = c.kube_client(); }",
        "extern crate kube;",
        "fn f() -> http::Request<()> { todo!() }",
    ] {
        assert!(!violations(&m, bad).is_empty(), "not found: {bad}");
    }
}

#[test]
fn comments_literals_and_allowed_paths_pass() {
    let m = module_of(Path::new("mcp/mod.rs"));
    assert_eq!(m, ["crate", "mcp"]);
    for good in [
        "use kube::api::ListParams;",
        "use kube::api::{ListParams};\nfn f() { let _ = ListParams::default(); }",
        "// use kube::Client; crate::ops::apply()\nfn f() {}",
        "/* kube::Api /* nested */ ops:: */ fn f() {}",
        "fn f() { let _ = \"kube::Client ops:: writer\"; }",
        "fn f() { let _ = r#\"use crate::ops;\"#; let c = 'a'; let q = '\\''; }",
        "fn f<'a>(x: &'a str) -> impl Iterator + use<'a> { x.chars() }",
        "use crate::k8s::read::ReadClient;\nfn f() { let _ = super::files::load(); }",
        "mod tests {\n use super::*;\n}",
        "fn f() { let stdin_writer = 1; let _ = stdin_writer; }",
    ] {
        assert!(
            violations(&m, good).is_empty(),
            "{good}: {:?}",
            violations(&m, good)
        );
    }
}
