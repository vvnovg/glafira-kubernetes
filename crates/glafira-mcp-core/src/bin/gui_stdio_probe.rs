//! Проба для CI: GUI-процесс Windows (`windows_subsystem = "windows"`, как release-сборка
//! Glafira) читает stdin и пишет в stdout через унаследованные pipe — так работает шим.
#![cfg_attr(windows, windows_subsystem = "windows")]

use std::io::{BufRead, Write};

fn main() {
    let mut out = std::io::stdout().lock();
    for line in std::io::stdin().lock().lines() {
        let Ok(line) = line else { break };
        let _ = writeln!(out, "echo:{line}");
        let _ = out.flush();
    }
}
