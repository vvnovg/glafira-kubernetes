//! Файл обмена подключениями (`glafira-kubernetes-connections`): экспорт без секретов, предпросмотр и
//! применение импорта.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::error::{AppResult, ErrorCode};
use crate::i18n::LocalizedText;
use crate::secrets::{CLIENT_KEY, OIDC_CLIENT_SECRET, SecretStore, TOKEN, secret_key};
use crate::storage::connections::{
    ConnectionFolder, ConnectionProfile, ConnectionStore, StoreData, check_tree, folder_not_found,
    unique_name, validate_profile,
};
use crate::storage::profile::{AuthConfig, CaConfig, normalize_profile, strip_private_keys};
use crate::storage::tree::TreeItemKind;

pub const EXCHANGE_FORMAT: &str = "glafira-kubernetes-connections";
pub const EXCHANGE_VERSION: u32 = 1;

/// Какие секреты были сохранены у подключения при экспорте: получатель вводит свои.
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NeedsSecret {
    pub token: bool,
    pub client_key: bool,
    /// Секрет клиента OIDC; в файлах до O5 его нет.
    #[serde(default)]
    pub oidc_client_secret: bool,
}

impl NeedsSecret {
    /// Нужен ли получателю хоть один секрет.
    pub fn any(&self) -> bool {
        self.token || self.client_key || self.oidc_client_secret
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExchangeFileOut<'a> {
    pub format: &'a str,
    pub version: u32,
    pub exported_at: String,
    pub app_version: &'a str,
    pub folders: Vec<ConnectionFolder>,
    pub connections: Vec<ExportedConnection>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ExportedConnection {
    #[serde(flatten)]
    pub profile: ConnectionProfile,
    #[serde(default)]
    pub needs_secret: NeedsSecret,
}

/// Итог экспорта для фронтенда.
#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ExportSummary {
    pub connections: usize,
    pub folders: usize,
}

/// Флаг поднимается, только если секрет сохранён и профиль его действительно использует:
/// устаревшие записи keychain не требуют ввода у получателя.
fn needs_secret(secrets: &dyn SecretStore, p: &ConnectionProfile) -> AppResult<NeedsSecret> {
    let stored = |field: &str| -> AppResult<bool> {
        Ok(secrets
            .get(&secret_key(&p.id, field))?
            .is_some_and(|v| !v.is_empty()))
    };
    Ok(NeedsSecret {
        token: p.auth.uses_token() && stored(TOKEN)?,
        client_key: matches!(p.auth, AuthConfig::ClientCert { .. }) && stored(CLIENT_KEY)?,
        oidc_client_secret: matches!(p.auth, AuthConfig::Oidc(_)) && stored(OIDC_CLIENT_SECRET)?,
    })
}

/// Exec-плагин в файле обмена: строка запуска переносится, но её подтверждение — нет
/// (получатель подтверждает её заново), значения переменных и аргументов, похожих на секреты,
/// не пишутся.
fn strip_exec_for_export(p: &mut ConnectionProfile) {
    if let AuthConfig::Exec(e) = &mut p.auth {
        e.approved = None;
        for v in &mut e.env {
            if crate::auth::exec::is_secret_name(&v.name) {
                v.value.clear();
            }
        }
        e.args = crate::auth::exec::blank_secret_args(&e.args);
    }
}

/// Приватный ключ из PEM-полей в файл обмена не попадает, сертификаты остаются (спец. §5.1:
/// секреты — только в keychain). Профили читаются с диска без повторной проверки (файл могли
/// править руками), поэтому ключ вырезается и здесь.
fn strip_pem_keys_for_export(p: &mut ConnectionProfile) {
    if let AuthConfig::ClientCert { certificate_pem } = &mut p.auth {
        *certificate_pem = strip_private_keys(certificate_pem);
    }
    if let CaConfig::Pem { pem } = &mut p.ca {
        *pem = strip_private_keys(pem);
    }
}

/// Собирает файл экспорта: всё дерево или поддерево `root` (сама папка становится корнем).
/// Секреты keychain не попадают в файл; неизвестные локальные поля (`extra`) не выносятся.
pub fn build_export(
    store: &ConnectionStore,
    secrets: &dyn SecretStore,
    root: Option<&str>,
    app_version: &'static str,
    now: &str,
) -> AppResult<ExchangeFileOut<'static>> {
    let all_folders = store.list_folders();
    let included: Option<HashSet<String>> = match root {
        None => None,
        Some(root) => {
            if !all_folders.iter().any(|f| f.id == root) {
                return Err(folder_not_found(root));
            }
            let mut set = HashSet::from([root.to_string()]);
            // Дерево без циклов (инвариант хранилища), поэтому достаточно расширять до неподвижной точки.
            loop {
                let before = set.len();
                for f in &all_folders {
                    if f.parent_id.as_deref().is_some_and(|p| set.contains(p)) {
                        set.insert(f.id.clone());
                    }
                }
                if set.len() == before {
                    break;
                }
            }
            Some(set)
        }
    };
    let in_scope = |folder: Option<&str>| match &included {
        None => true,
        Some(set) => folder.is_some_and(|id| set.contains(id)),
    };

    let folders = all_folders
        .into_iter()
        .filter(|f| in_scope(Some(&f.id)))
        .map(|mut f| {
            if Some(f.id.as_str()) == root {
                f.parent_id = None;
            }
            f.extra.clear();
            f
        })
        .collect();

    let connections = store
        .list()
        .into_iter()
        .filter(|p| in_scope(p.folder_id.as_deref()))
        .map(|mut profile| {
            profile.extra.clear();
            // Доступ LLM и источник метрик — локальные решения получателя (спец. §5.1): в файл
            // не пишутся.
            profile.llm_access = Default::default();
            profile.llm_namespaces = None;
            profile.llm_mask_fields = None;
            profile.metrics = Default::default();
            strip_exec_for_export(&mut profile);
            strip_pem_keys_for_export(&mut profile);
            let needs_secret = needs_secret(secrets, &profile)?;
            Ok(ExportedConnection {
                profile,
                needs_secret,
            })
        })
        .collect::<AppResult<Vec<_>>>()?;

    Ok(ExchangeFileOut {
        format: EXCHANGE_FORMAT,
        version: EXCHANGE_VERSION,
        exported_at: now.to_string(),
        app_version,
        folders,
        connections,
    })
}

/// Подключение файла, которое не будет импортировано, и причина.
#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Skipped {
    pub name: String,
    pub reason: LocalizedText,
}

/// Прочитанный файл обмена: папки, подключения (с флагами секретов) и пропущенные подключения.
#[derive(Debug, Clone, Default)]
pub struct ParsedExchange {
    pub folders: Vec<ConnectionFolder>,
    pub connections: Vec<(ConnectionProfile, NeedsSecret)>,
    pub skipped: Vec<Skipped>,
}

fn not_glafira_file() -> crate::error::AppError {
    app_err!(
        ErrorCode::InvalidInput,
        "errors.import.notGlafiraFile",
        "This is not a Glafira connections file"
    )
}

fn invalid_json(error: impl std::fmt::Display) -> crate::error::AppError {
    app_err!(
        ErrorCode::InvalidInput,
        "errors.import.invalidJson",
        "The file is damaged or is not JSON: {{error}}",
        error = error
    )
}

/// Имя подключения для списка пропущенных: из файла, иначе id, иначе номер в списке.
fn display_name(v: &serde_json::Value, index: usize) -> String {
    ["name", "id"]
        .iter()
        .filter_map(|k| v.get(*k).and_then(|x| x.as_str()))
        .map(str::trim)
        .find(|s| !s.is_empty())
        .map(String::from)
        .unwrap_or_else(|| format!("#{}", index + 1))
}

/// Читает файл обмена. Ошибка — только для чужого или повреждённого файла и более новой версии;
/// неизвестные поля игнорируются, неподдерживаемое или невалидное подключение попадает в `skipped`.
pub fn parse_exchange(bytes: &[u8]) -> AppResult<ParsedExchange> {
    let root: serde_json::Value = serde_json::from_slice(bytes).map_err(invalid_json)?;
    if root.get("format").and_then(|f| f.as_str()) != Some(EXCHANGE_FORMAT) {
        return Err(not_glafira_file());
    }
    let version = root
        .get("version")
        .and_then(|v| v.as_u64())
        .ok_or_else(not_glafira_file)?;
    if version > u64::from(EXCHANGE_VERSION) {
        return Err(app_err!(
            ErrorCode::InvalidInput,
            "errors.import.incompatibleVersion",
            "The file was created by an incompatible version of Glafira — update the app"
        ));
    }
    let list = |key: &str| -> AppResult<Vec<serde_json::Value>> {
        match root.get(key) {
            None | Some(serde_json::Value::Null) => Ok(Vec::new()),
            Some(serde_json::Value::Array(items)) => Ok(items.clone()),
            Some(_) => Err(invalid_json(format!("“{key}” is not a list"))),
        }
    };

    let mut seen = HashSet::new();
    let mut folders = Vec::new();
    for v in list("folders")? {
        let mut f: ConnectionFolder = serde_json::from_value(v).map_err(invalid_json)?;
        // Папка без id пропускается (её подключения попадут в цель); из повторов id берётся первая.
        if f.id.trim().is_empty() || !seen.insert(f.id.clone()) {
            continue;
        }
        f.extra.clear();
        let name = f.name.trim();
        f.name = if name.is_empty() { f.id.trim() } else { name }.to_string();
        folders.push(f);
    }

    let mut seen = HashSet::new();
    let mut connections = Vec::new();
    let mut skipped = Vec::new();
    for (i, v) in list("connections")?.into_iter().enumerate() {
        let name = display_name(&v, i);
        let mut c: ExportedConnection = match serde_json::from_value(v) {
            Ok(c) => c,
            Err(e) => {
                skipped.push(Skipped {
                    name,
                    reason: ltext!(
                        "errors.import.unsupportedConnection",
                        "Not supported by this version of Glafira (update the app): {{error}}",
                        error = e
                    ),
                });
                continue;
            }
        };
        // Неизвестные поля файла не переносятся в хранилище; строку запуска exec-плагина
        // из чужого файла человек подтверждает заново (спец. §5.2).
        c.profile.extra.clear();
        if let AuthConfig::Exec(e) = &mut c.profile.auth {
            e.approved = None;
        }
        // Как при сохранении в форме: адрес без «/» на конце, проекты без пробелов и повторов.
        normalize_profile(&mut c.profile);
        if let Err(e) = validate_profile(&c.profile) {
            skipped.push(Skipped {
                name,
                reason: e.text(),
            });
            continue;
        }
        if !seen.insert(c.profile.id.clone()) {
            skipped.push(Skipped {
                name,
                reason: ltext!(
                    "errors.import.duplicateId",
                    "Duplicate connection id in the file"
                ),
            });
            continue;
        }
        connections.push((c.profile, c.needs_secret));
    }
    Ok(ParsedExchange {
        folders,
        connections,
        skipped,
    })
}

pub struct ImportOptions<'a> {
    /// Папка, под которую попадают корневые элементы файла; `None` — корень.
    pub target: Option<&'a str>,
    pub update_existing: bool,
    /// `false` — политика запрещает подключения без проверки сертификата (спец. §12).
    pub allow_insecure_tls: bool,
    /// `false` — политика запрещает exec-плагины (спец. §5.2).
    pub allow_exec_auth: bool,
}

#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PreviewItem {
    pub kind: TreeItemKind,
    pub id: String,
    pub name: String,
    /// Имена папок от корня до места элемента (сам элемент не входит).
    pub path: Vec<String>,
}

#[derive(Serialize, Debug, Clone, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct ImportPreview {
    pub added: Vec<PreviewItem>,
    pub updated: Vec<PreviewItem>,
    pub unchanged: Vec<PreviewItem>,
    pub skipped: Vec<Skipped>,
    pub warnings: Vec<LocalizedText>,
    pub needs_secrets: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct ImportPlan {
    pub next: StoreData,
    pub preview: ImportPreview,
}

/// Имена папок от корня до `folder` включительно.
fn path_of(folders: &[ConnectionFolder], folder: Option<&str>) -> Vec<String> {
    let mut path = Vec::new();
    let mut seen = HashSet::new();
    let mut cur = folder;
    while let Some(id) = cur {
        if !seen.insert(id) {
            break;
        }
        let Some(f) = folders.iter().find(|f| f.id == id) else {
            break;
        };
        path.push(f.name.clone());
        cur = f.parent_id.as_deref();
    }
    path.reverse();
    path
}

/// Папки файла в порядке «родитель раньше потомка» (внутри уровня — по `order`).
/// Папка с родителем не из файла — корневая в файле; цикл разрывается на первой оставшейся папке.
fn parents_first(folders: &[ConnectionFolder]) -> Vec<(&ConnectionFolder, bool)> {
    let ids: HashSet<&str> = folders.iter().map(|f| f.id.as_str()).collect();
    let mut rest: Vec<&ConnectionFolder> = folders.iter().collect();
    rest.sort_by_key(|f| f.order);
    let mut done: HashSet<&str> = HashSet::new();
    let mut out = Vec::new();
    while !rest.is_empty() {
        let ready = |f: &&ConnectionFolder| match f.parent_id.as_deref() {
            None => true,
            Some(p) => !ids.contains(p) || done.contains(p),
        };
        let (mut now, later): (Vec<_>, Vec<_>) = rest.into_iter().partition(ready);
        let mut forced_root = None;
        if now.is_empty() {
            // Цикл в файле: первая оставшаяся папка становится корневой.
            forced_root = Some(later[0].id.as_str());
            now.push(later[0]);
        }
        rest = later
            .into_iter()
            .filter(|f| Some(f.id.as_str()) != forced_root)
            .collect();
        for f in now {
            let root = Some(f.id.as_str()) == forced_root
                || f.parent_id.as_deref().is_none_or(|p| !ids.contains(p));
            out.push((f, root));
            done.insert(f.id.as_str());
        }
    }
    out
}

/// Настройки подключения без места в дереве и локальных неизвестных полей. У exec-плагина не
/// учитывается подтверждение (файл его не несёт), а пустое значение секрета во входящем файле
/// считается совпадающим с локальным: файл секретов не несёт.
fn same_settings(local: &ConnectionProfile, incoming: &ConnectionProfile) -> bool {
    let strip = |p: &ConnectionProfile| {
        let mut p = ConnectionProfile {
            id: String::new(),
            folder_id: None,
            order: 0,
            llm_access: Default::default(),
            llm_namespaces: None,
            llm_mask_fields: None,
            metrics: Default::default(),
            extra: Default::default(),
            ..p.clone()
        };
        if let AuthConfig::Exec(e) = &mut p.auth {
            e.approved = None;
        }
        p
    };
    let mut incoming = incoming.clone();
    if let (AuthConfig::Exec(l), AuthConfig::Exec(i)) = (&local.auth, &mut incoming.auth) {
        crate::auth::exec::inherit_secrets(i, l);
    }
    strip(local) == strip(&incoming)
}

/// Меняется ли то, куда уходят учётные данные подключения: адрес, прокси, CA или имя для
/// проверки сертификата.
fn destination_changes(local: &ConnectionProfile, new: &ConnectionProfile) -> bool {
    local.api_url != new.api_url
        || local.proxy != new.proxy
        || local.ca != new.ca
        || local.tls_server_name != new.tls_server_name
}

/// Сохранены ли в keychain токен или ключ подключений, которые импорт обновит с новым
/// направлением (`destination_changes`). Читается до `ConnectionStore::update`: запрос доступа
/// к keychain (на macOS — окно с паролем) не должен ждать под блокировкой записи хранилища, иначе
/// ждали бы и `list_connections` на главном потоке, и `connections.get`.
#[derive(Debug, Default)]
pub(crate) struct StoredSecrets {
    /// id подключения → есть ли сохранённый секрет, который использует профиль из файла.
    by_id: HashMap<String, bool>,
}

impl StoredSecrets {
    /// Читает только нужное: подключения, которые есть и в `data`, и в файле, у которых
    /// направление в файле другое, и только тот секрет, который использует профиль из файла.
    /// Сбой keychain считается «есть»: лишнее предупреждение лучше пропущенного.
    pub(crate) fn read(
        secrets: &dyn SecretStore,
        data: &StoreData,
        parsed: &ParsedExchange,
    ) -> Self {
        let by_id = parsed
            .connections
            .iter()
            .filter(|(incoming, _)| !matches!(incoming.auth, AuthConfig::Exec(_)))
            .filter(|(incoming, _)| {
                data.connections
                    .iter()
                    .any(|local| local.id == incoming.id && destination_changes(local, incoming))
            })
            .map(|(incoming, _)| {
                let stored = needs_secret(secrets, incoming).map_or(true, |n| n.any());
                (incoming.id.clone(), stored)
            })
            .collect();
        Self { by_id }
    }

    /// Сохранён ли секрет подключения `id`. Не прочитанное заранее (хранилище изменилось между
    /// чтением и записью) считается сохранённым.
    fn has(&self, id: &str) -> bool {
        self.by_id.get(id).copied().unwrap_or(true)
    }
}

/// Уходят ли учётные данные подключения на сервер: токен или ключ в keychain (`StoredSecrets`)
/// либо подтверждённая строка запуска exec-плагина.
fn has_credentials(stored: &StoredSecrets, p: &ConnectionProfile) -> bool {
    match &p.auth {
        AuthConfig::Exec(e) => crate::auth::exec::is_approved(e),
        _ => stored.has(&p.id),
    }
}

/// Предупреждения об обновлении подключения, у которого меняется то, куда уходят сохранённые
/// учётные данные: адрес, прокси, CA или имя для проверки сертификата. `new` — профиль после
/// обновления (подтверждение exec-плагина в нём уже сброшено, если изменилась строка запуска).
fn destination_warnings(
    stored: &StoredSecrets,
    local: &ConnectionProfile,
    new: &ConnectionProfile,
) -> Vec<LocalizedText> {
    let address = local.api_url != new.api_url;
    let proxy = local.proxy != new.proxy;
    let tls = local.ca != new.ca || local.tls_server_name != new.tls_server_name;
    if !destination_changes(local, new) || !has_credentials(stored, new) {
        return Vec::new();
    }
    let name = &new.name;
    let mut out = Vec::new();
    if address {
        out.push(ltext!(
            "messages.import.addressChanged",
            "“{{name}}”: the API address changes from {{from}} to {{to}}. The saved credentials (token, key or confirmed exec plugin) will be sent to the new address.",
            name = name,
            from = local.api_url,
            to = new.api_url
        ));
    }
    if proxy {
        out.push(ltext!(
            "messages.import.proxyChanged",
            "“{{name}}”: the proxy settings change. The saved credentials (token, key or confirmed exec plugin) will go through the new proxy.",
            name = name
        ));
    }
    if tls {
        out.push(ltext!(
            "messages.import.tlsChanged",
            "“{{name}}”: the certificate verification settings (CA or TLS server name) change. The saved credentials (token, key or confirmed exec plugin) will be used with the new settings.",
            name = name
        ));
    }
    out
}

/// Строит новое состояние хранилища и предпросмотр импорта. Функция чистая: разрешение
/// политики приходит в `opts`, а сохранённые секреты — в `stored` (`StoredSecrets::read`, до
/// блокировки хранилища), для предупреждений об обновлении подключения с учётными данными.
pub(crate) fn plan_import(
    data: &StoreData,
    parsed: &ParsedExchange,
    opts: &ImportOptions,
    stored: &StoredSecrets,
) -> AppResult<ImportPlan> {
    let target = opts.target.map(str::to_string);
    if let Some(t) = &target
        && !data.folders.iter().any(|f| &f.id == t)
    {
        return Err(folder_not_found(t));
    }
    let mut next = data.clone();
    let mut preview = ImportPreview {
        skipped: parsed.skipped.clone(),
        ..Default::default()
    };
    // id папок файла совпадают с id в хранилище: существующие берутся как есть, новые создаются с тем же id.
    let file_folders: HashSet<&str> = parsed.folders.iter().map(|f| f.id.as_str()).collect();
    let local_folders: HashSet<&str> = data.folders.iter().map(|f| f.id.as_str()).collect();
    // (вид, id) в порядке появления, чтобы пути в предпросмотре считались по итоговому дереву.
    let mut added = Vec::new();
    let mut updated = Vec::new();
    let mut unchanged = Vec::new();

    for (f, is_root) in parents_first(&parsed.folders) {
        if local_folders.contains(f.id.as_str()) {
            unchanged.push((TreeItemKind::Folder, f.id.clone()));
            continue;
        }
        let parent = if is_root {
            target.clone()
        } else {
            f.parent_id.clone()
        };
        let siblings: Vec<&str> = next
            .folders
            .iter()
            .filter(|s| s.parent_id == parent)
            .map(|s| s.name.as_str())
            .collect();
        let name = unique_name(&siblings, &f.name);
        let order = next
            .folders
            .iter()
            .filter(|s| s.parent_id == parent)
            .map(|s| s.order + 1)
            .max()
            .unwrap_or(0);
        next.folders.push(ConnectionFolder {
            id: f.id.clone(),
            name,
            parent_id: parent,
            order,
            extra: Default::default(),
        });
        added.push((TreeItemKind::Folder, f.id.clone()));
    }

    let mut connections: Vec<&(ConnectionProfile, NeedsSecret)> =
        parsed.connections.iter().collect();
    connections.sort_by_key(|(p, _)| p.order);
    for (incoming, needs) in connections {
        if !opts.allow_insecure_tls && incoming.ca == CaConfig::Insecure {
            preview.skipped.push(Skipped {
                name: incoming.name.clone(),
                reason: ltext!(
                    "errors.import.insecureForbidden",
                    "Connections without TLS certificate verification are forbidden by your organization"
                ),
            });
            continue;
        }
        if !opts.allow_exec_auth && matches!(incoming.auth, AuthConfig::Exec(_)) {
            preview.skipped.push(Skipped {
                name: incoming.name.clone(),
                reason: ltext!(
                    "errors.import.execForbidden",
                    "Connections with exec plugins are forbidden by your organization"
                ),
            });
            continue;
        }
        let mut profile = incoming.clone();
        if let Some(slot) = next.connections.iter_mut().find(|c| c.id == profile.id) {
            if same_settings(slot, incoming) || !opts.update_existing {
                unchanged.push((TreeItemKind::Connection, profile.id.clone()));
                continue;
            }
            // Место в дереве, доступ LLM, источник метрик и неизвестные поля — локальные; пароли
            // получателя не трогаются.
            profile.folder_id = slot.folder_id.clone();
            profile.order = slot.order;
            profile.llm_access = slot.llm_access;
            profile.llm_namespaces = slot.llm_namespaces.take();
            profile.llm_mask_fields = slot.llm_mask_fields.take();
            profile.metrics = std::mem::take(&mut slot.metrics);
            profile.extra = std::mem::take(&mut slot.extra);
            // Секреты exec-плагина тоже локальные: файл их не несёт. Подтверждение остаётся,
            // только если строка запуска с этими секретами не изменилась (спец. §5.2).
            if let (AuthConfig::Exec(new), AuthConfig::Exec(old)) = (&mut profile.auth, &slot.auth)
            {
                crate::auth::exec::inherit_secrets(new, old);
                new.approved = old
                    .approved
                    .clone()
                    .filter(|a| *a == crate::auth::exec::fingerprint(new));
            }
            // Адрес, прокси и CA берутся из файла, а токен, ключ и подтверждение остаются: файл
            // из чужих рук отправил бы их на другой сервер, и человек должен это увидеть.
            preview
                .warnings
                .extend(destination_warnings(stored, slot, &profile));
            updated.push((TreeItemKind::Connection, profile.id.clone()));
            *slot = profile;
            continue;
        }
        let folder = profile
            .folder_id
            .as_deref()
            .filter(|f| file_folders.contains(f))
            .map(str::to_string)
            .or_else(|| target.clone());
        let siblings: Vec<&str> = next
            .connections
            .iter()
            .filter(|c| c.folder_id == folder)
            .map(|c| c.name.as_str())
            .collect();
        let wanted = profile.name.trim();
        let taken = siblings
            .iter()
            .any(|s| s.trim().to_lowercase() == wanted.to_lowercase());
        let name = if taken {
            unique_name(&siblings, wanted)
        } else {
            profile.name.clone()
        };
        profile.order = next
            .connections
            .iter()
            .filter(|c| c.folder_id == folder)
            .map(|c| c.order + 1)
            .max()
            .unwrap_or(0);
        profile.folder_id = folder;
        profile.name = name;
        // Файл не открывает кластер модели и не задаёт, куда ходить за метриками: доступ LLM
        // нового подключения выключен, источник метрик — автоматический.
        profile.llm_access = Default::default();
        profile.llm_namespaces = None;
        profile.llm_mask_fields = None;
        profile.metrics = Default::default();
        profile.extra = Default::default();
        if needs.any() {
            preview.needs_secrets += 1;
        }
        added.push((TreeItemKind::Connection, profile.id.clone()));
        next.connections.push(profile);
    }

    check_tree(&next)?;

    let folders_by_id: HashMap<&str, &ConnectionFolder> =
        next.folders.iter().map(|f| (f.id.as_str(), f)).collect();
    let item = |(kind, id): (TreeItemKind, String)| -> PreviewItem {
        let (name, location) = match kind {
            TreeItemKind::Folder => {
                let f = folders_by_id[id.as_str()];
                (f.name.clone(), f.parent_id.as_deref())
            }
            TreeItemKind::Connection => {
                let c = next
                    .connections
                    .iter()
                    .find(|c| c.id == id)
                    .expect("planned connection exists");
                (c.name.clone(), c.folder_id.as_deref())
            }
        };
        PreviewItem {
            kind,
            path: path_of(&next.folders, location),
            id,
            name,
        }
    };
    preview.added = added.into_iter().map(item).collect();
    preview.updated = updated.into_iter().map(item).collect();
    preview.unchanged = unchanged.into_iter().map(item).collect();
    Ok(ImportPlan { next, preview })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::secrets::MemoryStore;
    use crate::storage::connections::sample_profile;
    use crate::storage::profile::{TEST_CERT_PEM, pem_certificates};

    fn store() -> (tempfile::TempDir, ConnectionStore) {
        let d = tempfile::tempdir().unwrap();
        let s = ConnectionStore::open(d.path()).unwrap();
        (d, s)
    }

    fn in_folder(id: &str, folder: Option<&str>) -> ConnectionProfile {
        let mut p = sample_profile(id);
        p.folder_id = folder.map(String::from);
        p
    }

    /// ИФТ/{kafka-1}, ПСИ; подключения: a в ИФТ, b в kafka-1, c в ПСИ, d в корне.
    fn tree(s: &ConnectionStore) -> (String, String, String) {
        let ift = s.create_folder("ИФТ", None).unwrap();
        let k1 = s.create_folder("kafka-1", Some(&ift.id)).unwrap();
        let psi = s.create_folder("ПСИ", None).unwrap();
        s.upsert(in_folder("a", Some(&ift.id))).unwrap();
        s.upsert(in_folder("b", Some(&k1.id))).unwrap();
        s.upsert(in_folder("c", Some(&psi.id))).unwrap();
        s.upsert(in_folder("d", None)).unwrap();
        (ift.id, k1.id, psi.id)
    }

    fn ids<T>(items: &[T], f: impl Fn(&T) -> &str) -> Vec<String> {
        let mut v: Vec<String> = items.iter().map(|i| f(i).to_string()).collect();
        v.sort();
        v
    }

    #[test]
    fn exports_whole_tree() {
        let (_d, s) = store();
        let (ift, k1, psi) = tree(&s);
        let out = build_export(
            &s,
            &MemoryStore::default(),
            None,
            "9.9.9",
            "2026-10-17T12:00:00Z",
        )
        .unwrap();
        assert_eq!(out.format, "glafira-kubernetes-connections");
        assert_eq!(out.version, 1);
        assert_eq!(out.app_version, "9.9.9");
        assert_eq!(out.exported_at, "2026-10-17T12:00:00Z");
        let mut want = vec![ift, k1, psi];
        want.sort();
        assert_eq!(ids(&out.folders, |f| &f.id), want);
        assert_eq!(
            ids(&out.connections, |c| &c.profile.id),
            ["a", "b", "c", "d"]
        );

        let v = serde_json::to_value(&out).unwrap();
        assert_eq!(v["format"], "glafira-kubernetes-connections");
        assert_eq!(v["version"], 1);
        assert!(v["exportedAt"].is_string());
        assert!(v["appVersion"].is_string());
        assert!(v["connections"][0]["needsSecret"]["token"].is_boolean());
        assert!(v["connections"][0]["apiUrl"].is_string());
    }

    #[test]
    fn exports_subtree_with_root_detached() {
        let (_d, s) = store();
        let (ift, k1, _psi) = tree(&s);
        let out = build_export(&s, &MemoryStore::default(), Some(&ift), "1", "now").unwrap();
        let mut want = vec![ift.clone(), k1.clone()];
        want.sort();
        assert_eq!(ids(&out.folders, |f| &f.id), want);
        let root = out.folders.iter().find(|f| f.id == ift).unwrap();
        assert_eq!(root.parent_id, None);
        let child = out.folders.iter().find(|f| f.id == k1).unwrap();
        assert_eq!(child.parent_id.as_deref(), Some(ift.as_str()));
        assert_eq!(ids(&out.connections, |c| &c.profile.id), ["a", "b"]);
        let v = serde_json::to_value(&out).unwrap();
        let root_json = v["folders"]
            .as_array()
            .unwrap()
            .iter()
            .find(|f| f["id"] == ift.as_str())
            .unwrap();
        assert!(root_json["parentId"].is_null());
    }

    #[test]
    fn secrets_are_never_exported() {
        let (_d, s) = store();
        s.upsert(sample_profile("tok")).unwrap();
        let mut cert = sample_profile("cert");
        cert.auth = AuthConfig::ClientCert {
            certificate_pem: TEST_CERT_PEM.into(),
        };
        s.upsert(cert).unwrap();
        s.upsert(sample_profile("empty")).unwrap();
        let secrets = MemoryStore::default();
        secrets
            .set(&secret_key("tok", TOKEN), "sha256~s3cr3t")
            .unwrap();
        secrets
            .set(&secret_key("cert", CLIENT_KEY), "KEY-s3cr3t")
            .unwrap();
        // Устаревший токен у подключения с сертификатом флаг не поднимает.
        secrets
            .set(&secret_key("cert", TOKEN), "sha256~old")
            .unwrap();
        let out = build_export(&s, &secrets, None, "1", "now").unwrap();
        let json = serde_json::to_string(&out).unwrap();
        assert!(!json.contains("s3cr3t") && !json.contains("sha256~old"));
        assert_eq!(out.format, "glafira-kubernetes-connections");
        let flags = |id: &str| {
            out.connections
                .iter()
                .find(|c| c.profile.id == id)
                .unwrap()
                .needs_secret
                .clone()
        };
        assert_eq!(
            flags("tok"),
            NeedsSecret {
                token: true,
                ..Default::default()
            }
        );
        assert_eq!(
            flags("cert"),
            NeedsSecret {
                client_key: true,
                ..Default::default()
            }
        );
        assert_eq!(flags("empty"), NeedsSecret::default());
    }

    /// Секрет клиента OIDC отмечается, как токен и ключ: получатель вводит свой. Токены входа
    /// флага не поднимают — их даёт вход.
    #[test]
    fn oidc_client_secret_is_flagged_and_never_exported() {
        use crate::secrets::{OIDC_CLIENT_SECRET, OIDC_ID_TOKEN, OIDC_REFRESH_TOKEN};
        let (_d, s) = store();
        let mut oidc = sample_profile("oidc");
        oidc.auth = AuthConfig::Oidc(crate::storage::profile::sample_oidc());
        s.upsert(oidc).unwrap();
        let mut public = sample_profile("public");
        public.auth = AuthConfig::Oidc(crate::storage::profile::sample_oidc());
        s.upsert(public).unwrap();
        let secrets = MemoryStore::default();
        for (id, field, value) in [
            ("oidc", OIDC_CLIENT_SECRET, "cs-s3cr3t"),
            ("oidc", OIDC_ID_TOKEN, "idt-s3cr3t"),
            ("oidc", OIDC_REFRESH_TOKEN, "rt-s3cr3t"),
            ("public", OIDC_ID_TOKEN, "idt-s3cr3t"),
        ] {
            secrets.set(&secret_key(id, field), value).unwrap();
        }
        let out = build_export(&s, &secrets, None, "1", "now").unwrap();
        let json = serde_json::to_string(&out).unwrap();
        assert!(!json.contains("s3cr3t"), "{json}");
        let flags = |id: &str| {
            out.connections
                .iter()
                .find(|c| c.profile.id == id)
                .unwrap()
                .needs_secret
                .clone()
        };
        assert_eq!(
            flags("oidc"),
            NeedsSecret {
                oidc_client_secret: true,
                ..Default::default()
            }
        );
        assert_eq!(flags("public"), NeedsSecret::default());
        let v = serde_json::to_value(&out).unwrap();
        let exported = v["connections"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["id"] == "oidc")
            .unwrap()
            .clone();
        assert_eq!(exported["needsSecret"]["oidcClientSecret"], true);

        // Новое подключение из файла считается среди тех, кому нужен секрет.
        let p = plan(
            &StoreData::default(),
            &file(vec![], vec![exported]),
            &opts(None),
        );
        assert_eq!(p.preview.needs_secrets, 1);
    }

    #[test]
    fn unknown_local_fields_are_not_exported() {
        let (_d, s) = store();
        s.upsert(sample_profile("a")).unwrap();
        s.update(|data| {
            data.connections[0]
                .extra
                .insert("futureField".into(), serde_json::json!("local"));
            Ok(())
        })
        .unwrap();
        let out = build_export(&s, &MemoryStore::default(), None, "1", "now").unwrap();
        assert!(out.connections[0].profile.extra.is_empty());
        assert!(!serde_json::to_string(&out).unwrap().contains("futureField"));
    }

    /// Источник метрик — локальная настройка: экспорт её не пишет, импорт не переносит (чужой
    /// файл не направит токен подключения на свой адрес).
    #[test]
    fn metrics_source_is_never_exported_or_imported() {
        use crate::storage::profile::{MetricsAuth, MetricsCa, MetricsConfig, MetricsEndpoint};
        let (_d, s) = store();
        let mut own = sample_profile("a");
        own.metrics = MetricsConfig::Thanos(MetricsEndpoint {
            url: "https://thanos.example.com".into(),
            auth: MetricsAuth::Connection,
            ca: MetricsCa::Connection,
        });
        s.upsert(own.clone()).unwrap();
        let out = build_export(&s, &MemoryStore::default(), None, "1", "now").unwrap();
        let json = serde_json::to_value(&out).unwrap();
        assert!(json["connections"][0].get("metrics").is_none(), "{json}");

        let data = StoreData {
            connections: vec![own.clone()],
            ..Default::default()
        };
        let evil = serde_json::json!({
            "mode": "prometheus", "url": "https://evil.example.com", "auth": "connection"
        });
        let mut incoming_a = conn_json("a", "conn-a", None);
        incoming_a["apiUrl"] = "https://api.other.example.com:6443".into();
        incoming_a["metrics"] = evil.clone();
        let mut incoming_b = conn_json("b", "conn-b", None);
        incoming_b["metrics"] = evil;
        let p = plan(
            &data,
            &file(vec![], vec![incoming_a, incoming_b]),
            &opts(None),
        );
        assert_eq!(
            conn(&p.next, "a").metrics,
            own.metrics,
            "своя настройка сохраняется"
        );
        assert!(
            conn(&p.next, "b").metrics.is_auto(),
            "новое подключение — автоматический источник"
        );
    }

    /// Доступ LLM — локальное решение (спец. §5.1): экспорт его не пишет, импорт не переносит.
    #[test]
    fn llm_access_is_never_exported_or_imported() {
        use crate::storage::profile::LlmAccess;
        let (_d, s) = store();
        let mut open = sample_profile("a");
        open.llm_access = LlmAccess::Metadata;
        open.llm_namespaces = Some(vec!["team-a".into()]);
        open.llm_mask_fields = Some(vec!["inn".into()]);
        s.upsert(open.clone()).unwrap();
        let out = build_export(&s, &MemoryStore::default(), None, "1", "now").unwrap();
        let json = serde_json::to_value(&out).unwrap();
        assert!(json["connections"][0].get("llmAccess").is_none(), "{json}");
        assert!(
            json["connections"][0].get("llmNamespaces").is_none(),
            "{json}"
        );
        assert!(
            json["connections"][0].get("llmMaskFields").is_none(),
            "{json}"
        );

        let data = StoreData {
            connections: vec![open],
            ..Default::default()
        };
        let mut incoming_a = conn_json("a", "conn-a", None);
        incoming_a["apiUrl"] = "https://api.other.example.com:6443".into();
        let mut incoming_b = conn_json("b", "conn-b", None);
        incoming_b["llmAccess"] = "metadata".into();
        incoming_b["llmNamespaces"] = serde_json::json!(["prod"]);
        incoming_a["llmMaskFields"] = serde_json::json!([]);
        incoming_b["llmMaskFields"] = serde_json::json!([]);
        let p = plan(
            &data,
            &file(vec![], vec![incoming_a, incoming_b]),
            &opts(None),
        );
        let a = conn(&p.next, "a");
        assert_eq!(
            (
                a.llm_access,
                a.llm_namespaces.clone(),
                a.llm_mask_fields.clone()
            ),
            (
                LlmAccess::Metadata,
                Some(vec!["team-a".to_string()]),
                Some(vec!["inn".to_string()])
            ),
            "своё решение сохраняется"
        );
        let b = conn(&p.next, "b");
        assert_eq!(
            (
                b.llm_access,
                b.llm_namespaces.clone(),
                b.llm_mask_fields.clone()
            ),
            (LlmAccess::Off, None, None),
            "файл не открывает доступ и не убирает маскировку"
        );

        // Отличие только в доступе LLM — «без изменений».
        let p = plan(
            &data,
            &file(vec![], vec![conn_json("a", "conn-a", None)]),
            &opts(None),
        );
        assert_eq!(preview_ids(&p.preview.unchanged), ["a"]);
    }

    #[test]
    fn unknown_root_is_an_error() {
        let (_d, s) = store();
        tree(&s);
        let err = build_export(&s, &MemoryStore::default(), Some("nope"), "1", "now")
            .err()
            .unwrap();
        assert_eq!(err.key, "errors.folders.notFound");
    }

    // ---- импорт ----

    fn conn_json(id: &str, name: &str, folder: Option<&str>) -> serde_json::Value {
        let mut p = sample_profile(id);
        p.name = name.into();
        p.folder_id = folder.map(String::from);
        let mut v = serde_json::to_value(&p).unwrap();
        v["needsSecret"] = serde_json::json!({"token": false, "clientKey": false});
        v
    }

    fn folder_json(id: &str, name: &str, parent: Option<&str>, order: i64) -> serde_json::Value {
        serde_json::json!({"id": id, "name": name, "parentId": parent, "order": order})
    }

    fn file(folders: Vec<serde_json::Value>, conns: Vec<serde_json::Value>) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "format": "glafira-kubernetes-connections",
            "version": 1,
            "exportedAt": "now",
            "appVersion": "1",
            "folders": folders,
            "connections": conns,
        }))
        .unwrap()
    }

    fn local_folder(id: &str, name: &str, parent: Option<&str>) -> ConnectionFolder {
        ConnectionFolder {
            id: id.into(),
            name: name.into(),
            parent_id: parent.map(String::from),
            order: 0,
            extra: Default::default(),
        }
    }

    fn opts(target: Option<&str>) -> ImportOptions<'_> {
        ImportOptions {
            target,
            update_existing: true,
            allow_insecure_tls: true,
            allow_exec_auth: true,
        }
    }

    fn plan(data: &StoreData, bytes: &[u8], o: &ImportOptions) -> ImportPlan {
        let parsed = parse_exchange(bytes).unwrap();
        plan_with(data, &parsed, o, &MemoryStore::default()).unwrap()
    }

    /// План импорта, как в приложении: сохранённые секреты читаются до плана.
    fn plan_with(
        data: &StoreData,
        parsed: &ParsedExchange,
        o: &ImportOptions,
        secrets: &dyn SecretStore,
    ) -> AppResult<ImportPlan> {
        plan_import(data, parsed, o, &StoredSecrets::read(secrets, data, parsed))
    }

    fn conn<'a>(data: &'a StoreData, id: &str) -> &'a ConnectionProfile {
        data.connections.iter().find(|c| c.id == id).unwrap()
    }

    fn folder<'a>(data: &'a StoreData, id: &str) -> &'a ConnectionFolder {
        data.folders.iter().find(|f| f.id == id).unwrap()
    }

    fn preview_ids(items: &[PreviewItem]) -> Vec<&str> {
        items.iter().map(|i| i.id.as_str()).collect()
    }

    #[test]
    fn new_folders_and_connections_go_under_target_keeping_structure() {
        let data = StoreData {
            folders: vec![local_folder("t", "Target", None)],
            ..Default::default()
        };
        let bytes = file(
            vec![
                folder_json("k1", "kafka-1", Some("ift"), 0),
                folder_json("ift", "ИФТ", None, 0),
            ],
            vec![
                conn_json("a", "A", Some("ift")),
                conn_json("b", "B", Some("k1")),
                conn_json("c", "C", None),
            ],
        );
        let p = plan(&data, &bytes, &opts(Some("t")));
        let next = &p.next;
        assert_eq!(folder(next, "ift").parent_id.as_deref(), Some("t"));
        assert_eq!(folder(next, "ift").name, "ИФТ");
        assert_eq!(folder(next, "k1").parent_id.as_deref(), Some("ift"));
        assert_eq!(conn(next, "a").folder_id.as_deref(), Some("ift"));
        assert_eq!(conn(next, "b").folder_id.as_deref(), Some("k1"));
        assert_eq!(conn(next, "c").folder_id.as_deref(), Some("t"));
        assert_eq!(preview_ids(&p.preview.added), ["ift", "k1", "a", "b", "c"]);
        let b = p.preview.added.iter().find(|i| i.id == "b").unwrap();
        assert_eq!(b.kind, TreeItemKind::Connection);
        assert_eq!(b.path, ["Target", "ИФТ", "kafka-1"]);
        let k1 = p.preview.added.iter().find(|i| i.id == "k1").unwrap();
        assert_eq!(k1.kind, TreeItemKind::Folder);
        assert_eq!(k1.path, ["Target", "ИФТ"]);
        assert!(p.preview.updated.is_empty() && p.preview.unchanged.is_empty());
        check_tree(next).unwrap();
    }

    #[test]
    fn unknown_target_is_an_error() {
        let parsed = parse_exchange(&file(vec![], vec![])).unwrap();
        let err = plan_with(
            &StoreData::default(),
            &parsed,
            &opts(Some("nope")),
            &MemoryStore::default(),
        )
        .err()
        .unwrap();
        assert_eq!(err.key, "errors.folders.notFound");
    }

    #[test]
    fn matched_connection_is_updated_only_with_update_existing() {
        let mut local = sample_profile("a");
        local.folder_id = Some("f".into());
        local.order = 7;
        local.extra.insert("pinned".into(), serde_json::json!(true));
        let data = StoreData {
            folders: vec![local_folder("f", "F", None)],
            connections: vec![local.clone()],
            ..Default::default()
        };
        let mut changed = conn_json("a", "conn-a", None);
        changed["apiUrl"] = "https://api.other:6443".into();
        let bytes = file(vec![], vec![changed]);

        let p = plan(&data, &bytes, &opts(None));
        let a = conn(&p.next, "a");
        assert_eq!(a.api_url, "https://api.other:6443");
        assert_eq!(a.folder_id.as_deref(), Some("f"));
        assert_eq!(a.order, 7);
        assert_eq!(a.extra.get("pinned"), Some(&serde_json::json!(true)));
        assert_eq!(preview_ids(&p.preview.updated), ["a"]);
        assert_eq!(p.preview.updated[0].path, ["F"]);

        let p = plan(
            &data,
            &bytes,
            &ImportOptions {
                update_existing: false,
                ..opts(None)
            },
        );
        assert_eq!(conn(&p.next, "a"), &local);
        assert_eq!(preview_ids(&p.preview.unchanged), ["a"]);
        assert!(p.preview.updated.is_empty());

        // Те же настройки — «без изменений» и при update_existing.
        let same = file(vec![], vec![conn_json("a", "conn-a", None)]);
        let p = plan(&data, &same, &opts(None));
        assert_eq!(preview_ids(&p.preview.unchanged), ["a"]);
        assert_eq!(conn(&p.next, "a"), &local);
    }

    /// Локальное хранилище с подключением `a` и план импорта файла, где `a` изменено `edit`.
    fn plan_update(
        local: ConnectionProfile,
        secrets: &dyn SecretStore,
        edit: impl FnOnce(&mut serde_json::Value),
    ) -> ImportPreview {
        let data = StoreData {
            connections: vec![local],
            ..Default::default()
        };
        let mut changed = conn_json("a", "conn-a", None);
        edit(&mut changed);
        let parsed = parse_exchange(&file(vec![], vec![changed])).unwrap();
        let p = plan_with(&data, &parsed, &opts(None), secrets).unwrap();
        assert_eq!(preview_ids(&p.preview.updated), ["a"]);
        p.preview
    }

    fn with_token() -> MemoryStore {
        let secrets = MemoryStore::default();
        secrets.set(&secret_key("a", TOKEN), "sha256~t").unwrap();
        secrets
    }

    fn warning_keys(preview: &ImportPreview) -> Vec<&str> {
        preview.warnings.iter().map(|w| w.key.as_str()).collect()
    }

    #[test]
    fn address_change_with_a_stored_token_warns() {
        let p = plan_update(sample_profile("a"), &with_token(), |c| {
            c["apiUrl"] = "https://api.other:6443".into();
        });
        assert_eq!(warning_keys(&p), ["messages.import.addressChanged"]);
        let w = &p.warnings[0];
        assert_eq!(w.params["name"], "conn-a");
        assert_eq!(w.params["from"], "https://api.dev.example.com:6443");
        assert_eq!(w.params["to"], "https://api.other:6443");
        assert!(w.text.contains("conn-a") && w.text.contains("https://api.other:6443"));
    }

    /// Хранилище секретов, которое не читается (keychain заблокирован или отказал) и
    /// записывает, какие ключи у него спрашивали.
    #[derive(Default)]
    struct FailingStore {
        asked: std::sync::Mutex<Vec<String>>,
    }

    impl SecretStore for FailingStore {
        fn get(&self, key: &str) -> AppResult<Option<String>> {
            self.asked.lock().unwrap().push(key.to_string());
            Err(crate::error::AppError::internal_with("keychain is locked"))
        }
        fn set(&self, _: &str, _: &str) -> AppResult<()> {
            Err(crate::error::AppError::internal_with("keychain is locked"))
        }
        fn delete(&self, _: &str) -> AppResult<()> {
            Err(crate::error::AppError::internal_with("keychain is locked"))
        }
    }

    /// Сбой keychain считается «учётные данные есть»: лишнее предупреждение лучше пропущенного.
    #[test]
    fn keychain_error_counts_as_stored_credentials() {
        let secrets = FailingStore::default();
        let p = plan_update(sample_profile("a"), &secrets, |c| {
            c["apiUrl"] = "https://api.other:6443".into();
        });
        assert_eq!(warning_keys(&p), ["messages.import.addressChanged"]);
        assert_eq!(*secrets.asked.lock().unwrap(), [secret_key("a", TOKEN)]);
    }

    /// Секреты читаются только у подключений, которые обновятся с новым направлением, и только
    /// тот, который использует профиль из файла; то, чего не прочитали заранее, считается
    /// сохранённым.
    #[test]
    fn stored_secrets_are_read_only_where_a_warning_is_possible() {
        let mut exec = sample_profile("e");
        exec.auth = AuthConfig::Exec(crate::storage::profile::sample_exec());
        let data = StoreData {
            connections: vec![sample_profile("a"), sample_profile("same"), exec.clone()],
            ..Default::default()
        };
        let moved = |id: &str| {
            let mut c = conn_json(id, &format!("conn-{id}"), None);
            c["apiUrl"] = "https://api.other:6443".into();
            c
        };
        let mut e = moved("e");
        e["auth"] = serde_json::to_value(&exec.auth).unwrap();
        let mut same = conn_json("same", "conn-same", None);
        same["readOnly"] = true.into();
        let parsed =
            parse_exchange(&file(vec![], vec![moved("a"), same, e, moved("new")])).unwrap();
        let secrets = FailingStore::default();
        let stored = StoredSecrets::read(&secrets, &data, &parsed);
        assert_eq!(*secrets.asked.lock().unwrap(), [secret_key("a", TOKEN)]);
        assert!(stored.has("a"), "сбой keychain — «есть»");
        assert!(stored.has("not-read"), "не прочитанное заранее — «есть»");
        let p = plan_import(&data, &parsed, &opts(None), &stored).unwrap();
        assert_eq!(preview_ids(&p.preview.updated), ["a", "same", "e"]);
        assert_eq!(warning_keys(&p.preview), ["messages.import.addressChanged"]);
    }

    #[test]
    fn address_change_without_stored_credentials_does_not_warn() {
        // Токена в keychain нет: уходить на новый адрес нечему.
        let p = plan_update(sample_profile("a"), &MemoryStore::default(), |c| {
            c["apiUrl"] = "https://api.other:6443".into();
        });
        assert!(p.warnings.is_empty());
        // Ключ от другого способа входа не в счёт: профиль использует токен.
        let secrets = MemoryStore::default();
        secrets
            .set(&secret_key("a", CLIENT_KEY), "KEY-s3cr3t")
            .unwrap();
        let p = plan_update(sample_profile("a"), &secrets, |c| {
            c["apiUrl"] = "https://api.other:6443".into();
        });
        assert!(p.warnings.is_empty());
    }

    #[test]
    fn unchanged_address_does_not_warn() {
        // Обновляется другое поле, адрес, прокси и CA прежние.
        let p = plan_update(sample_profile("a"), &with_token(), |c| {
            c["environment"] = "prod".into();
            c["readOnly"] = true.into();
        });
        assert!(p.warnings.is_empty());
        // Адрес с «/» на конце тот же адрес.
        let p = plan_update(sample_profile("a"), &with_token(), |c| {
            c["apiUrl"] = "https://api.dev.example.com:6443/".into();
            c["environment"] = "prod".into();
        });
        assert!(p.warnings.is_empty());
    }

    #[test]
    fn proxy_and_certificate_changes_warn_separately() {
        let p = plan_update(sample_profile("a"), &with_token(), |c| {
            c["proxy"] = serde_json::json!({"mode": "url", "url": "http://proxy.other:3128"});
        });
        assert_eq!(warning_keys(&p), ["messages.import.proxyChanged"]);
        assert_eq!(p.warnings[0].params["name"], "conn-a");
        assert!(
            !p.warnings[0].text.contains("proxy.other"),
            "адрес прокси не печатается"
        );
        let p = plan_update(sample_profile("a"), &with_token(), |c| {
            c["tlsServerName"] = "api.internal".into();
        });
        assert_eq!(warning_keys(&p), ["messages.import.tlsChanged"]);
        let p = plan_update(sample_profile("a"), &with_token(), |c| {
            c["ca"] = serde_json::json!({"mode": "pem", "pem": TEST_CERT_PEM});
        });
        assert_eq!(warning_keys(&p), ["messages.import.tlsChanged"]);
        // Всё сразу: по предупреждению на каждое.
        let p = plan_update(sample_profile("a"), &with_token(), |c| {
            c["apiUrl"] = "https://api.other:6443".into();
            c["proxy"] = serde_json::json!({"mode": "none"});
            c["ca"] = serde_json::json!({"mode": "insecure"});
        });
        assert_eq!(
            warning_keys(&p),
            [
                "messages.import.addressChanged",
                "messages.import.proxyChanged",
                "messages.import.tlsChanged"
            ]
        );
    }

    #[test]
    fn client_key_counts_as_stored_credentials() {
        let mut local = sample_profile("a");
        local.auth = AuthConfig::ClientCert {
            certificate_pem: TEST_CERT_PEM.into(),
        };
        let edit = |c: &mut serde_json::Value| {
            c["auth"] =
                serde_json::json!({"method": "clientCert", "certificatePem": TEST_CERT_PEM});
            c["apiUrl"] = "https://api.other:6443".into();
        };
        let secrets = MemoryStore::default();
        secrets
            .set(&secret_key("a", CLIENT_KEY), "KEY-s3cr3t")
            .unwrap();
        let p = plan_update(local.clone(), &secrets, edit);
        assert_eq!(warning_keys(&p), ["messages.import.addressChanged"]);
        let p = plan_update(local, &MemoryStore::default(), edit);
        assert!(p.warnings.is_empty());
    }

    #[test]
    fn confirmed_exec_plugin_counts_as_stored_credentials() {
        // Подтверждённая строка запуска остаётся при смене адреса: плагин получит новый адрес.
        let (data, mut v) = exec_store_and_export();
        v["connections"][0]["apiUrl"] = "https://api.other:6443".into();
        let bytes = serde_json::to_vec(&v).unwrap();
        let p = plan(&data, &bytes, &opts(None));
        assert_eq!(warning_keys(&p.preview), ["messages.import.addressChanged"]);
        assert_eq!(p.preview.warnings[0].params["name"], "conn-e");
        // Строка запуска изменилась вместе с адресом: подтверждение сброшено, плагин не запустится.
        v["connections"][0]["auth"]["args"]
            .as_array_mut()
            .unwrap()
            .push("--v=1".into());
        let bytes = serde_json::to_vec(&v).unwrap();
        let p = plan(&data, &bytes, &opts(None));
        assert_eq!(preview_ids(&p.preview.updated), ["e"]);
        assert!(p.preview.warnings.is_empty());
        // Неподтверждённый плагин ничего не запускает.
        let (mut data, mut v) = exec_store_and_export();
        let AuthConfig::Exec(e) = &mut data.connections[0].auth else {
            panic!("exec")
        };
        e.approved = None;
        v["connections"][0]["apiUrl"] = "https://api.other:6443".into();
        let bytes = serde_json::to_vec(&v).unwrap();
        let p = plan(&data, &bytes, &opts(None));
        assert_eq!(preview_ids(&p.preview.updated), ["e"]);
        assert!(p.preview.warnings.is_empty());
    }

    #[test]
    fn added_connections_and_disabled_updates_do_not_warn() {
        // Новое подключение: сохранённых учётных данных у него нет.
        let data = StoreData::default();
        let mut c = conn_json("a", "conn-a", None);
        c["apiUrl"] = "https://api.other:6443".into();
        let bytes = file(vec![], vec![c.clone()]);
        let parsed = parse_exchange(&bytes).unwrap();
        let p = plan_with(&data, &parsed, &opts(None), &with_token()).unwrap();
        assert_eq!(preview_ids(&p.preview.added), ["a"]);
        assert!(p.preview.warnings.is_empty());
        // Без «Обновлять существующие» подключение остаётся как было.
        let data = StoreData {
            connections: vec![sample_profile("a")],
            ..Default::default()
        };
        let off = ImportOptions {
            update_existing: false,
            ..opts(None)
        };
        let p = plan_with(&data, &parsed, &off, &with_token()).unwrap();
        assert_eq!(preview_ids(&p.preview.unchanged), ["a"]);
        assert!(p.preview.warnings.is_empty());
    }

    #[test]
    fn matched_folder_is_not_renamed_or_moved_and_receives_new_connections() {
        let data = StoreData {
            folders: vec![
                local_folder("p", "Parent", None),
                local_folder("f", "Local name", Some("p")),
            ],
            ..Default::default()
        };
        let bytes = file(
            vec![folder_json("f", "Renamed", None, 0)],
            vec![conn_json("n", "New", Some("f"))],
        );
        let p = plan(&data, &bytes, &opts(None));
        let f = folder(&p.next, "f");
        assert_eq!(f.name, "Local name");
        assert_eq!(f.parent_id.as_deref(), Some("p"));
        assert_eq!(conn(&p.next, "n").folder_id.as_deref(), Some("f"));
        assert_eq!(preview_ids(&p.preview.unchanged), ["f"]);
        assert_eq!(preview_ids(&p.preview.added), ["n"]);
        assert_eq!(p.preview.added[0].path, ["Parent", "Local name"]);
    }

    #[test]
    fn name_conflicts_get_suffixes() {
        let mut x = sample_profile("x");
        x.name = "Kafka".into();
        let mut y = sample_profile("y");
        y.name = "kafka (2)".into();
        let data = StoreData {
            folders: vec![local_folder("l1", "ИФТ", None)],
            connections: vec![x, y],
            ..Default::default()
        };
        let bytes = file(
            vec![folder_json("n1", "ифт", None, 0)],
            vec![conn_json("n", "Kafka", None)],
        );
        let p = plan(&data, &bytes, &opts(None));
        assert_eq!(folder(&p.next, "n1").name, "ифт (2)");
        assert_eq!(conn(&p.next, "n").name, "Kafka (3)");
        assert_eq!(p.preview.added[1].name, "Kafka (3)");
    }

    #[test]
    fn foreign_local_connections_are_untouched() {
        let mut mine = sample_profile("mine");
        mine.order = 3;
        let data = StoreData {
            connections: vec![mine.clone()],
            ..Default::default()
        };
        let p = plan(
            &data,
            &file(vec![], vec![conn_json("a", "A", None)]),
            &opts(None),
        );
        assert_eq!(conn(&p.next, "mine"), &mine);
        assert_eq!(conn(&p.next, "a").order, 4);
    }

    #[test]
    fn prod_read_only_comes_from_the_file() {
        // readOnly prod-подключения берётся из файла как есть: без принуждения и предупреждения.
        let mut prod = conn_json("p", "P", None);
        prod["environment"] = "prod".into();
        let bytes = file(vec![], vec![prod, conn_json("d", "D", None)]);
        let p = plan(&StoreData::default(), &bytes, &opts(None));
        assert!(!conn(&p.next, "p").read_only);
        assert!(!conn(&p.next, "d").read_only);
        assert!(p.preview.warnings.is_empty());

        let mut ro = conn_json("r", "R", None);
        ro["environment"] = "prod".into();
        ro["readOnly"] = true.into();
        let p = plan(&StoreData::default(), &file(vec![], vec![ro]), &opts(None));
        assert!(conn(&p.next, "r").read_only);
        assert!(p.preview.warnings.is_empty());
    }

    #[test]
    fn duplicate_connection_ids_are_skipped_with_reason() {
        let bytes = file(
            vec![],
            vec![
                conn_json("a", "First", None),
                conn_json("a", "Second", None),
            ],
        );
        let parsed = parse_exchange(&bytes).unwrap();
        assert_eq!(parsed.connections.len(), 1);
        assert_eq!(parsed.connections[0].0.name, "First");
        assert_eq!(parsed.skipped.len(), 1);
        assert_eq!(parsed.skipped[0].name, "Second");
        assert_eq!(parsed.skipped[0].reason.key, "errors.import.duplicateId");
    }

    #[test]
    fn exec_launch_line_is_exported_without_approval_and_secret_values() {
        let (_d, s) = store();
        let mut p = sample_profile("e");
        let mut exec = crate::storage::profile::sample_exec();
        exec.env.push(crate::storage::profile::ExecEnv {
            name: "AWS_SECRET_ACCESS_KEY".into(),
            value: "s3cr3t".into(),
        });
        exec.approved = Some(crate::auth::exec::fingerprint(&exec));
        p.auth = AuthConfig::Exec(exec);
        s.upsert(p).unwrap();
        let out = build_export(&s, &MemoryStore::default(), None, "1", "now").unwrap();
        let v = serde_json::to_value(&out).unwrap();
        let auth = &v["connections"][0]["auth"];
        assert_eq!(auth["method"], "exec");
        assert_eq!(auth["args"][1], "get-token");
        assert_eq!(auth["approved"], serde_json::Value::Null);
        assert_eq!(auth["env"][0]["value"], "http://proxy:3128");
        assert_eq!(auth["env"][1]["value"], "", "секрет не пишется");
        assert!(!serde_json::to_string(&v).unwrap().contains("s3cr3t"));
    }

    #[test]
    fn exec_import_needs_new_confirmation_and_respects_policy() {
        let mut exec = crate::storage::profile::sample_exec();
        exec.approved = Some(crate::auth::exec::fingerprint(&exec));
        let mut e = conn_json("e", "Exec", None);
        e["auth"] = serde_json::to_value(AuthConfig::Exec(exec)).unwrap();
        let bytes = file(vec![], vec![e, conn_json("ok", "OK", None)]);
        let parsed = parse_exchange(&bytes).unwrap();
        let AuthConfig::Exec(imported) = &parsed.connections[0].0.auth else {
            panic!("exec")
        };
        assert_eq!(
            imported.approved, None,
            "подтверждение из файла не принимается"
        );
        let p = plan_with(
            &StoreData::default(),
            &parsed,
            &opts(None),
            &MemoryStore::default(),
        )
        .unwrap();
        assert_eq!(preview_ids(&p.preview.added), ["e", "ok"]);
        let forbid = ImportOptions {
            allow_exec_auth: false,
            ..opts(None)
        };
        let p = plan_with(
            &StoreData::default(),
            &parsed,
            &forbid,
            &MemoryStore::default(),
        )
        .unwrap();
        assert_eq!(preview_ids(&p.preview.added), ["ok"]);
        assert_eq!(
            p.preview.skipped[0].reason.key,
            "errors.import.execForbidden"
        );
    }

    /// Строка запуска из файла с переменной загрузчика или с ключом-флагом, который прячет
    /// подкоманду, не импортируется: проверка та же, что при сохранении.
    #[test]
    fn exec_with_forbidden_environment_or_arguments_is_skipped_on_import() {
        let mut preload = crate::storage::profile::sample_exec();
        preload.env.push(crate::storage::profile::ExecEnv {
            name: "LD_PRELOAD".into(),
            value: "/tmp/evil.so".into(),
        });
        let mut hidden = crate::storage::profile::sample_exec();
        hidden.args = [
            "--insecure-skip-tls-verify",
            "exec",
            "oidc-login",
            "get-token",
        ]
        .map(String::from)
        .to_vec();
        let mut a = conn_json("a", "Preload", None);
        a["auth"] = serde_json::to_value(AuthConfig::Exec(preload)).unwrap();
        let mut b = conn_json("b", "Hidden", None);
        b["auth"] = serde_json::to_value(AuthConfig::Exec(hidden)).unwrap();
        let bytes = file(vec![], vec![a, b, conn_json("ok", "OK", None)]);
        let parsed = parse_exchange(&bytes).unwrap();
        let ids: Vec<&str> = parsed
            .connections
            .iter()
            .map(|(p, _)| p.id.as_str())
            .collect();
        assert_eq!(ids, ["ok"]);
        let skipped: Vec<(&str, &str)> = parsed
            .skipped
            .iter()
            .map(|s| (s.name.as_str(), s.reason.key.as_str()))
            .collect();
        assert_eq!(
            skipped,
            [
                ("Preload", "errors.exec.envForbidden"),
                ("Hidden", "errors.exec.optionBeforePlugin"),
            ]
        );
        assert_eq!(parsed.skipped[0].reason.params["name"], "LD_PRELOAD");
    }

    /// Exec-плагин с секретами в переменной окружения и в аргументах (оба вида записи), подтверждённый.
    fn approved_exec_with_secrets() -> crate::storage::profile::ExecAuth {
        let mut exec = crate::storage::profile::sample_exec();
        exec.env.push(crate::storage::profile::ExecEnv {
            name: "AWS_SECRET_ACCESS_KEY".into(),
            value: "s3cr3t".into(),
        });
        exec.args.extend([
            "--oidc-client-secret=top-secret".to_string(),
            "--client-secret".to_string(),
            "pair-secret".to_string(),
        ]);
        exec.approved = Some(crate::auth::exec::fingerprint(&exec));
        exec
    }

    /// Локальное хранилище с одним подтверждённым exec-подключением и его экспорт в виде JSON.
    fn exec_store_and_export() -> (StoreData, serde_json::Value) {
        let (_d, s) = store();
        let mut p = sample_profile("e");
        p.auth = AuthConfig::Exec(approved_exec_with_secrets());
        s.upsert(p).unwrap();
        let out = build_export(&s, &MemoryStore::default(), None, "1", "now").unwrap();
        (s.snapshot(), serde_json::to_value(&out).unwrap())
    }

    fn exec_of(p: &ConnectionProfile) -> &crate::storage::profile::ExecAuth {
        let AuthConfig::Exec(e) = &p.auth else {
            panic!("exec")
        };
        e
    }

    #[test]
    fn exec_secret_arguments_are_not_exported() {
        let (_data, v) = exec_store_and_export();
        let text = serde_json::to_string(&v).unwrap();
        for secret in ["s3cr3t", "top-secret", "pair-secret"] {
            assert!(!text.contains(secret), "{secret} в файле экспорта");
        }
        let args = &v["connections"][0]["auth"]["args"];
        assert_eq!(args[2], "--oidc-issuer-url=https://sso.corp/realms/k8s");
        assert_eq!(args[3], "--oidc-client-secret=");
        assert_eq!(args[4], "--client-secret");
        assert_eq!(args[5], "");
    }

    /// Повторный импорт собственного экспорта ничего не меняет: подтверждение и секреты остаются.
    #[test]
    fn reimporting_own_export_leaves_exec_connection_unchanged() {
        let (data, v) = exec_store_and_export();
        let bytes = serde_json::to_vec(&v).unwrap();
        let p = plan(&data, &bytes, &opts(None));
        assert_eq!(preview_ids(&p.preview.unchanged), ["e"]);
        assert!(p.preview.updated.is_empty() && p.preview.added.is_empty());
        assert_eq!(conn(&p.next, "e"), conn(&data, "e"));
        let e = exec_of(conn(&p.next, "e"));
        assert!(crate::auth::exec::is_approved(e));
        assert_eq!(e.env[1].value, "s3cr3t");
        assert_eq!(e.args[3], "--oidc-client-secret=top-secret");
        assert_eq!(e.args[5], "pair-secret");
    }

    /// Изменённый аргумент: секреты остаются локальными, но строка запуска другая — нужно новое подтверждение.
    #[test]
    fn import_changing_an_argument_keeps_secrets_and_drops_approval() {
        let (data, mut v) = exec_store_and_export();
        v["connections"][0]["auth"]["args"]
            .as_array_mut()
            .unwrap()
            .push("--v=1".into());
        let bytes = serde_json::to_vec(&v).unwrap();
        let p = plan(&data, &bytes, &opts(None));
        assert_eq!(preview_ids(&p.preview.updated), ["e"]);
        let e = exec_of(conn(&p.next, "e"));
        assert_eq!(e.args.last().map(String::as_str), Some("--v=1"));
        assert_eq!(e.env[1].value, "s3cr3t");
        assert_eq!(e.args[3], "--oidc-client-secret=top-secret");
        assert_eq!(e.args[5], "pair-secret");
        assert_eq!(e.approved, None);
        assert!(!crate::auth::exec::is_approved(e));
    }

    /// Изменились только настройки вне строки запуска: подтверждение остаётся.
    #[test]
    fn import_keeping_the_launch_line_keeps_approval() {
        let (data, mut v) = exec_store_and_export();
        v["connections"][0]["apiUrl"] = "https://api.other:6443".into();
        let bytes = serde_json::to_vec(&v).unwrap();
        let p = plan(&data, &bytes, &opts(None));
        assert_eq!(preview_ids(&p.preview.updated), ["e"]);
        let c = conn(&p.next, "e");
        assert_eq!(c.api_url, "https://api.other:6443");
        let e = exec_of(c);
        assert!(crate::auth::exec::is_approved(e));
        assert_eq!(e.env[1].value, "s3cr3t");
        assert_eq!(e.args[5], "pair-secret");
    }

    /// Непустое значение секрета из файла заменяет локальное и тоже требует нового подтверждения.
    #[test]
    fn import_with_a_new_secret_value_replaces_it_and_drops_approval() {
        let (data, mut v) = exec_store_and_export();
        v["connections"][0]["auth"]["env"][1]["value"] = "rotated".into();
        let bytes = serde_json::to_vec(&v).unwrap();
        let p = plan(&data, &bytes, &opts(None));
        assert_eq!(preview_ids(&p.preview.updated), ["e"]);
        let e = exec_of(conn(&p.next, "e"));
        assert_eq!(e.env[1].value, "rotated");
        assert_eq!(e.args[3], "--oidc-client-secret=top-secret");
        assert_eq!(e.approved, None);
    }

    /// Подключение, которого нет локально, приходит с пустыми секретами и без подтверждения.
    #[test]
    fn new_exec_connection_from_a_file_has_no_secrets_and_no_approval() {
        let (_data, v) = exec_store_and_export();
        let bytes = serde_json::to_vec(&v).unwrap();
        let p = plan(&StoreData::default(), &bytes, &opts(None));
        assert_eq!(preview_ids(&p.preview.added), ["e"]);
        let e = exec_of(conn(&p.next, "e"));
        assert_eq!(e.env[1].value, "");
        assert_eq!(e.args[3], "--oidc-client-secret=");
        assert_eq!(e.args[5], "");
        assert_eq!(e.approved, None);
    }

    #[test]
    fn insecure_connections_are_skipped_when_policy_forbids() {
        let mut insecure = conn_json("i", "Insecure", None);
        insecure["ca"] = serde_json::json!({"mode": "insecure"});
        let bytes = file(vec![], vec![insecure, conn_json("ok", "OK", None)]);
        let parsed = parse_exchange(&bytes).unwrap();
        let p = plan_with(
            &StoreData::default(),
            &parsed,
            &opts(None),
            &MemoryStore::default(),
        )
        .unwrap();
        assert_eq!(preview_ids(&p.preview.added), ["i", "ok"]);
        let forbid = ImportOptions {
            allow_insecure_tls: false,
            ..opts(None)
        };
        let p = plan_with(
            &StoreData::default(),
            &parsed,
            &forbid,
            &MemoryStore::default(),
        )
        .unwrap();
        assert_eq!(preview_ids(&p.preview.added), ["ok"]);
        assert_eq!(p.preview.skipped.len(), 1);
        assert_eq!(p.preview.skipped[0].name, "Insecure");
        assert_eq!(
            p.preview.skipped[0].reason.key,
            "errors.import.insecureForbidden"
        );
        assert!(p.next.connections.iter().all(|c| c.id != "i"));
    }

    #[test]
    fn needs_secrets_counts_new_connections_only() {
        let mut a = conn_json("a", "A", None);
        a["needsSecret"]["token"] = true.into();
        let mut b = conn_json("b", "B", None);
        b["needsSecret"]["clientKey"] = true.into();
        b["apiUrl"] = "https://api.changed:6443".into();
        let data = StoreData {
            connections: vec![sample_profile("b")],
            ..Default::default()
        };
        let p = plan(&data, &file(vec![], vec![a, b]), &opts(None));
        assert_eq!(preview_ids(&p.preview.updated), ["b"]);
        assert_eq!(p.preview.needs_secrets, 1);
    }

    #[test]
    fn parse_exchange_tolerates_unknown_fields_and_skips_unsupported() {
        let mut ok = conn_json("ok", "OK", None);
        ok["futureField"] = serde_json::json!({"x": 1});
        let mut oauth = conn_json("oauth", "OAuth", None);
        oauth["auth"] = serde_json::json!({"method": "oidcExec", "command": "oc"});
        let mut invalid = conn_json("bad", "Bad", None);
        invalid["apiUrl"] = "http://api.c:6443".into();
        let mut v: serde_json::Value =
            serde_json::from_slice(&file(vec![], vec![ok, oauth, invalid])).unwrap();
        v["somethingNew"] = serde_json::json!([1, 2]);
        let parsed = parse_exchange(&serde_json::to_vec(&v).unwrap()).unwrap();
        assert_eq!(parsed.connections.len(), 1);
        assert_eq!(parsed.connections[0].0.id, "ok");
        assert!(parsed.connections[0].0.extra.is_empty());
        assert_eq!(parsed.skipped.len(), 2);
        assert_eq!(parsed.skipped[0].name, "OAuth");
        assert_eq!(
            parsed.skipped[0].reason.key,
            "errors.import.unsupportedConnection"
        );
        assert_eq!(parsed.skipped[1].name, "Bad");
        assert_eq!(
            parsed.skipped[1].reason.key,
            "errors.connections.httpsRequired"
        );

        let p = plan_with(
            &StoreData::default(),
            &parsed,
            &opts(None),
            &MemoryStore::default(),
        )
        .unwrap();
        assert_eq!(preview_ids(&p.preview.added), ["ok"]);
        assert_eq!(p.preview.skipped.len(), 2);

        v["version"] = 2.into();
        let err = parse_exchange(&serde_json::to_vec(&v).unwrap())
            .err()
            .unwrap();
        assert_eq!(err.key, "errors.import.incompatibleVersion");
        v["version"] = 1.into();
        v["format"] = "other".into();
        let err = parse_exchange(&serde_json::to_vec(&v).unwrap())
            .err()
            .unwrap();
        assert_eq!(err.key, "errors.import.notGlafiraFile");
        let err = parse_exchange(b"{not json").err().unwrap();
        assert_eq!(err.key, "errors.import.invalidJson");
        let err = parse_exchange(b"[1, 2]").err().unwrap();
        assert_eq!(err.key, "errors.import.notGlafiraFile");
    }

    /// Импортируемый профиль приводится к каноническому виду до проверки, как при сохранении
    /// в форме: адрес без «/» на конце, проекты без пробелов и повторов.
    #[test]
    fn imported_profiles_are_normalized_before_validation() {
        let mut c = conn_json("n", "N", None);
        c["apiUrl"] = " https://api.c:6443/ ".into();
        c["favoriteNamespaces"] = serde_json::json!([" team-a ", "", "team-a", "b"]);
        c["manualNamespaces"] = serde_json::json!(["  team-c"]);
        let parsed = parse_exchange(&file(vec![], vec![c.clone()])).unwrap();
        assert!(parsed.skipped.is_empty(), "{:?}", parsed.skipped);
        let p = &parsed.connections[0].0;
        assert_eq!(p.api_url, "https://api.c:6443");
        assert_eq!(p.favorite_namespaces, ["team-a", "b"]);
        assert_eq!(p.manual_namespaces, ["team-c"]);
        let plan = plan_with(
            &StoreData::default(),
            &parsed,
            &opts(None),
            &MemoryStore::default(),
        )
        .unwrap();
        let stored = conn(&plan.next, "n");
        assert_eq!(stored.api_url, "https://api.c:6443");
        assert_eq!(stored.favorite_namespaces, ["team-a", "b"]);

        // Нормализация не прячет настоящие ошибки.
        c["favoriteNamespaces"] = serde_json::json!(["Bad Name"]);
        let parsed = parse_exchange(&file(vec![], vec![c])).unwrap();
        assert_eq!(parsed.connections.len(), 0);
        assert_eq!(
            parsed.skipped[0].reason.key,
            "errors.connections.badNamespace"
        );
    }

    #[test]
    fn export_then_import_into_empty_store_round_trips() {
        let (_d, s) = store();
        let (ift, k1, psi) = tree(&s);
        let mut x = sample_profile("x");
        x.folder_id = Some(psi.clone());
        s.upsert(x).unwrap();
        let secrets = MemoryStore::default();
        secrets
            .set(&secret_key("x", TOKEN), "sha256~s3cr3t")
            .unwrap();
        let out = build_export(&s, &secrets, None, "1", "now").unwrap();
        let bytes = serde_json::to_vec(&out).unwrap();

        let parsed = parse_exchange(&bytes).unwrap();
        assert!(parsed.skipped.is_empty());
        let p = plan_with(
            &StoreData::default(),
            &parsed,
            &opts(None),
            &MemoryStore::default(),
        )
        .unwrap();
        assert_eq!(p.preview.needs_secrets, 1);
        let sort_f = |mut v: Vec<ConnectionFolder>| {
            v.sort_by(|a, b| a.id.cmp(&b.id));
            v
        };
        let sort_c = |mut v: Vec<ConnectionProfile>| {
            v.sort_by(|a, b| a.id.cmp(&b.id));
            v
        };
        assert_eq!(sort_f(p.next.folders.clone()), sort_f(s.list_folders()));
        assert_eq!(sort_c(p.next.connections.clone()), sort_c(s.list()));
        let _ = (ift, k1);
    }

    #[test]
    fn private_keys_in_stored_pem_fields_never_reach_the_export() {
        let key = include_str!("testdata/test-key.pem");
        let mut p = sample_profile("k");
        p.auth = AuthConfig::ClientCert {
            certificate_pem: format!("{TEST_CERT_PEM}\n{key}"),
        };
        p.ca = CaConfig::Pem {
            pem: format!("{key}\n{TEST_CERT_PEM}"),
        };
        // Профиль, записанный в обход проверки (файл правили руками или писала старая версия).
        let dir = tempfile::tempdir().unwrap();
        let stored = serde_json::json!({
            "schemaVersion": 1,
            "folders": [],
            "connections": [serde_json::to_value(&p).unwrap()],
        });
        std::fs::write(
            dir.path().join("connections.json"),
            serde_json::to_vec(&stored).unwrap(),
        )
        .unwrap();
        let s = ConnectionStore::open(dir.path()).unwrap();
        assert_eq!(s.get("k").unwrap().auth, p.auth, "в хранилище ключ остался");

        let out = build_export(&s, &MemoryStore::default(), None, "1", "now").unwrap();
        let text = serde_json::to_string(&out).unwrap();
        assert!(!text.contains("PRIVATE KEY"), "{text}");
        assert!(!text.contains(key.lines().nth(1).unwrap()), "{text}");
        let exported = &out.connections[0].profile;
        let AuthConfig::ClientCert { certificate_pem } = &exported.auth else {
            panic!("clientCert")
        };
        let CaConfig::Pem { pem } = &exported.ca else {
            panic!("pem")
        };
        let want = pem_certificates(TEST_CERT_PEM).unwrap();
        assert_eq!(pem_certificates(certificate_pem).unwrap(), want);
        assert_eq!(pem_certificates(pem).unwrap(), want);
        // Результат экспорта проходит проверку при импорте.
        let parsed = parse_exchange(&serde_json::to_vec(&out).unwrap()).unwrap();
        assert!(parsed.skipped.is_empty());
        assert_eq!(parsed.connections.len(), 1);
    }
}
