use std::collections::HashMap;
use std::sync::Arc;

use serde::Deserialize;
use tauri::State;

use super::blocking;
use crate::error::{AppError, AppResult};
use crate::k8s::cluster::ConnectionInfo;
use crate::k8s::connectivity::{TestReport, ensure_allowed};
use crate::secrets::{
    CLIENT_KEY, METRICS_TOKEN, OIDC_CLIENT_SECRET, OIDC_ID_TOKEN, OIDC_REFRESH_TOKEN, OverlayStore,
    SecretStore, TOKEN, secret_key,
};
use crate::state::AppState;
use crate::storage::profile::{AuthConfig, ConnectionProfile, MetricsAuth, REDACTED};

/// Секреты из формы. Для каждого: нет или `null` — оставить сохранённый, `""` — удалить,
/// иное — сохранить. `Debug` печатает вместо значений `***`.
#[derive(Default, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecretsInput {
    #[serde(default)]
    pub token: Option<String>,
    #[serde(default)]
    pub client_key: Option<String>,
    /// Bearer-токен своего источника метрик.
    #[serde(default)]
    pub metrics_token: Option<String>,
    /// Секрет клиента OIDC.
    #[serde(default)]
    pub oidc_client_secret: Option<String>,
}

/// Токен и приватный ключ не печатаются: структура может попасть в лог или сообщение об ошибке.
impl std::fmt::Debug for SecretsInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let hide = |secret: &Option<String>| secret.as_ref().map(|_| REDACTED);
        f.debug_struct("SecretsInput")
            .field("token", &hide(&self.token))
            .field("client_key", &hide(&self.client_key))
            .field("metrics_token", &hide(&self.metrics_token))
            .field("oidc_client_secret", &hide(&self.oidc_client_secret))
            .finish()
    }
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionInput {
    pub profile: ConnectionProfile,
    #[serde(default)]
    pub secrets: SecretsInput,
    /// Человек подтвердил строку запуска exec-плагина в форме (спец. §5.2). Подтверждение
    /// из самого профиля не принимается.
    #[serde(default)]
    pub confirm_exec: bool,
}

/// Секреты формы печатаются как `***` (`SecretsInput`); профиль секретов не содержит.
impl std::fmt::Debug for ConnectionInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionInput")
            .field("profile", &self.profile)
            .field("secrets", &self.secrets)
            .field("confirm_exec", &self.confirm_exec)
            .finish()
    }
}

/// Подтверждение строки запуска: новое — по согласию в форме, иначе прежнее сохранённое, но
/// только если оно подтверждает именно сохраняемую строку. Устаревшее не оставляется: форма
/// считала бы строку подтверждённой и не предложила бы подтвердить заново.
fn set_exec_approval(
    profile: &mut ConnectionProfile,
    previous: Option<&ConnectionProfile>,
    confirm: bool,
) {
    if let AuthConfig::Exec(e) = &mut profile.auth {
        e.approved = if confirm {
            Some(crate::auth::exec::fingerprint(e))
        } else {
            previous.and_then(|p| match &p.auth {
                AuthConfig::Exec(old) => old
                    .approved
                    .clone()
                    .filter(|a| *a == crate::auth::exec::fingerprint(e)),
                _ => None,
            })
        };
    }
}

/// Что сделать с секретом при сохранении.
#[derive(Debug, PartialEq, Eq)]
enum SecretOp {
    Keep,
    Set(String),
    Delete,
}

/// Используемые профилем секреты — по данным формы, неиспользуемые — удаляются. Токены входа
/// через OIDC форма не передаёт: они остаются, пока способ входа — OIDC.
fn secret_ops(profile: &ConnectionProfile, s: &SecretsInput) -> [(&'static str, SecretOp); 6] {
    let op = |used: bool, v: Option<&String>| match (used, v.map(|x| x.trim())) {
        (false, _) => SecretOp::Delete,
        (true, None) => SecretOp::Keep,
        (true, Some("")) => SecretOp::Delete,
        (true, Some(v)) => SecretOp::Set(v.to_string()),
    };
    let oidc = matches!(profile.auth, AuthConfig::Oidc(_));
    [
        (TOKEN, op(profile.auth.uses_token(), s.token.as_ref())),
        (
            CLIENT_KEY,
            op(
                matches!(profile.auth, AuthConfig::ClientCert { .. }),
                s.client_key.as_ref(),
            ),
        ),
        (
            METRICS_TOKEN,
            op(
                profile
                    .metrics
                    .endpoint()
                    .is_some_and(|e| e.auth == MetricsAuth::Bearer),
                s.metrics_token.as_ref(),
            ),
        ),
        (OIDC_CLIENT_SECRET, op(oidc, s.oidc_client_secret.as_ref())),
        (OIDC_ID_TOKEN, op(oidc, None)),
        (OIDC_REFRESH_TOKEN, op(oidc, None)),
    ]
}

/// Прежний способ входа — OIDC, а издатель или client id другие (адреса сравниваются без
/// завершающего «/»).
fn oidc_identity_changed(before: &AuthConfig, after: &AuthConfig) -> bool {
    match (before, after) {
        (AuthConfig::Oidc(a), AuthConfig::Oidc(b)) => {
            let issuer = |o: &crate::storage::profile::OidcAuth| {
                o.issuer_url.trim().trim_end_matches('/').to_string()
            };
            issuer(a) != issuer(b) || a.client_id.trim() != b.client_id.trim()
        }
        _ => false,
    }
}

#[tauri::command]
pub fn list_connections(state: State<'_, AppState>) -> AppResult<Vec<ConnectionProfile>> {
    Ok(state.connections.list())
}

#[tauri::command]
pub async fn save_connection(
    state: State<'_, AppState>,
    input: ConnectionInput,
) -> AppResult<ConnectionProfile> {
    let st = state.inner().clone();
    blocking(move || save_connection_blocking(&st, input)).await
}

/// Сохраняет профиль и секреты согласованно: если keychain не принял изменение, профиль
/// возвращается к прежнему состоянию (или удаляется, если он новый).
pub fn save_connection_blocking(
    st: &AppState,
    input: ConnectionInput,
) -> AppResult<ConnectionProfile> {
    let ConnectionInput {
        mut profile,
        secrets,
        confirm_exec,
    } = input;
    ensure_allowed(&profile, &st.policy.status)?;
    if profile.id.is_empty() {
        profile.id = uuid::Uuid::new_v4().to_string();
    }
    let previous = st.connections.get(&profile.id).ok();
    set_exec_approval(&mut profile, previous.as_ref(), confirm_exec);
    // upsert проверяет профиль — секреты трогаем только после успешного сохранения.
    let saved = st.connections.upsert(profile)?;
    // Сначала `Set`, потом `Delete`: если keychain не примет новый секрет после смены способа
    // входа, прежний ещё на месте, и откат профиля не оставляет его без секрета.
    let mut ops = secret_ops(&saved, &secrets);
    // Токены выданы прежним издателем и клиенту: при смене любого из них они не годятся.
    if previous
        .as_ref()
        .is_some_and(|p| oidc_identity_changed(&p.auth, &saved.auth))
    {
        for (field, op) in &mut ops {
            if matches!(*field, OIDC_ID_TOKEN | OIDC_REFRESH_TOKEN) {
                *op = SecretOp::Delete;
            }
        }
    }
    ops.sort_by_key(|(_, op)| *op == SecretOp::Delete);
    let result = ops.into_iter().try_for_each(|(field, op)| {
        let key = secret_key(&saved.id, field);
        match op {
            SecretOp::Keep => Ok(()),
            SecretOp::Set(v) => st.secrets.set(&key, &v),
            SecretOp::Delete => st.secrets.delete(&key),
        }
    });
    if result.is_err() {
        let rollback = match previous {
            Some(p) => st.connections.upsert(p).map(|_| ()),
            None => st.connections.delete(&saved.id),
        };
        if let Err(r) = rollback {
            tracing::error!("failed to roll back profile {}: {r}", saved.id);
        }
    }
    // Клиент работает со старыми адресом и учётными данными: следующий вызов подключится заново,
    // а сессии на нём останавливаются. Сбрасывается и после отката: клиент, открытый на
    // промежуточном профиле, не нужен.
    st.sessions.stop_for(&saved.id);
    st.evict_connection(&saved.id);
    result.map(|()| saved)
}

#[tauri::command]
pub async fn delete_connection(state: State<'_, AppState>, id: String) -> AppResult<()> {
    let st = state.inner().clone();
    st.logins.cancel(&id);
    st.logins.cancel(&crate::auth::test_key(&id));
    blocking(move || st.delete_connection_full(&id)).await
}

/// Проверяет и несохранённый профиль: секреты из формы накладываются поверх keychain
/// (с той же семантикой, что и при сохранении). Ожидание exec-плагина отменяет
/// `cancel_connection_test`: у проверки свой ключ в `Logins`, и она не отменяет подключение
/// вкладки того же профиля, а та — её.
#[tauri::command]
pub async fn test_connection(
    state: State<'_, AppState>,
    input: ConnectionInput,
) -> AppResult<TestReport> {
    let mut input = input;
    let previous = state.connections.get(&input.profile.id).ok();
    set_exec_approval(&mut input.profile, previous.as_ref(), input.confirm_exec);
    let id = input.profile.id.clone();
    let mut overrides = HashMap::new();
    for (field, v) in [
        (TOKEN, &input.secrets.token),
        (CLIENT_KEY, &input.secrets.client_key),
        (METRICS_TOKEN, &input.secrets.metrics_token),
        (OIDC_CLIENT_SECRET, &input.secrets.oidc_client_secret),
    ] {
        if let Some(v) = v {
            overrides.insert(secret_key(&id, field), v.trim().to_string());
        }
    }
    let secrets: Arc<dyn SecretStore> = Arc::new(OverlayStore {
        base: state.secrets.clone(),
        overrides,
    });
    let exec = matches!(input.profile.auth, AuthConfig::Exec(_));
    let key = crate::auth::test_key(&id);
    let (n, cancel) = match exec.then(|| state.logins.begin(&key)) {
        Some((n, rx)) => (Some(n), rx),
        None => (None, tokio::sync::oneshot::channel().1),
    };
    let report = crate::k8s::connectivity::test_connection(
        &input.profile,
        secrets,
        &state.policy.status,
        cancel,
    )
    .await;
    if let Some(n) = n {
        state.logins.finish(&key, n);
    }
    Ok(report)
}

/// Подключается (или берёт клиент из пула) и возвращает сведения о подключении.
/// `fresh` — подключиться заново (после входа или смены токена).
#[tauri::command]
pub async fn connect(
    state: State<'_, AppState>,
    connection_id: String,
    fresh: bool,
) -> AppResult<ConnectionInfo> {
    connection_info(state.inner(), &connection_id, fresh).await
}

/// Сведения о подключении для `connect`; политика проверяется и для клиента из пула. Ошибка
/// самого подключения клиент не сбрасывает (`evict_if_broken_client` — для ошибок уже
/// полученного клиента): неудачная попытка в пул не попадает, а сброс сменил бы поколение, и
/// ожидавшие очереди не получили бы её итог — каждый снова запустил бы exec-плагин или вход.
pub async fn connection_info(st: &AppState, id: &str, fresh: bool) -> AppResult<ConnectionInfo> {
    ensure_allowed(&st.connections.get(id)?, &st.policy.status)?;
    let c = st.connected(id, fresh).await?;
    Ok(c.info.clone())
}

/// Отключение: клиент сбрасывается, сессии watch и логов подключения останавливаются.
#[tauri::command]
pub fn disconnect(state: State<'_, AppState>, connection_id: String) -> AppResult<()> {
    state.sessions.stop_for(&connection_id);
    state.evict_connection(&connection_id);
    Ok(())
}

/// Сохраняет вставленный токен (кнопка «Вставить токен» при истёкшей сессии).
#[tauri::command]
pub async fn set_connection_token(
    state: State<'_, AppState>,
    connection_id: String,
    token: String,
) -> AppResult<()> {
    let st = state.inner().clone();
    blocking(move || set_token_blocking(&st, &connection_id, &token)).await
}

/// Способ входа подключения не хранит токен (сертификат, exec-плагин): вставить токен
/// и войти через браузер нельзя.
pub(super) fn token_not_used() -> AppError {
    app_err!(
        crate::error::ErrorCode::InvalidInput,
        "errors.connections.tokenNotUsed",
        "This connection does not sign in with a token"
    )
}

pub fn set_token_blocking(st: &AppState, id: &str, token: &str) -> AppResult<()> {
    let p = st.connections.get(id)?;
    if !p.auth.uses_token() {
        return Err(token_not_used());
    }
    st.secrets.set(&secret_key(id, TOKEN), token.trim())?;
    st.sessions.stop_for(id);
    st.evict_connection(id);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;
    use crate::secrets::MemoryStore;
    use crate::storage::profile::{
        CaConfig, MetricsCa, MetricsConfig, MetricsEndpoint, TEST_CERT_PEM, sample_profile,
    };

    fn state(dir: &std::path::Path) -> AppState {
        AppState::new(dir, Arc::new(MemoryStore::default())).unwrap()
    }

    fn input(
        profile: ConnectionProfile,
        token: Option<&str>,
        key: Option<&str>,
    ) -> ConnectionInput {
        ConnectionInput {
            profile,
            secrets: SecretsInput {
                token: token.map(String::from),
                client_key: key.map(String::from),
                metrics_token: None,
                oidc_client_secret: None,
            },
            confirm_exec: false,
        }
    }

    fn secret(st: &AppState, id: &str, field: &str) -> Option<String> {
        st.secrets.get(&secret_key(id, field)).unwrap()
    }

    #[test]
    fn secret_ops_matrix() {
        let token = sample_profile("a");
        let s = |t: Option<&str>, k: Option<&str>| SecretsInput {
            token: t.map(String::from),
            client_key: k.map(String::from),
            metrics_token: Some("m".into()),
            oidc_client_secret: Some("cs".into()),
        };
        assert_eq!(
            secret_ops(&token, &s(None, Some("k"))),
            [
                (TOKEN, SecretOp::Keep),
                (CLIENT_KEY, SecretOp::Delete),
                (METRICS_TOKEN, SecretOp::Delete),
                (OIDC_CLIENT_SECRET, SecretOp::Delete),
                (OIDC_ID_TOKEN, SecretOp::Delete),
                (OIDC_REFRESH_TOKEN, SecretOp::Delete)
            ]
        );
        assert_eq!(
            secret_ops(&token, &s(Some(" t "), None))[0],
            (TOKEN, SecretOp::Set("t".into()))
        );
        assert_eq!(
            secret_ops(&token, &s(Some(""), None))[0],
            (TOKEN, SecretOp::Delete)
        );
        let mut cert = sample_profile("a");
        cert.auth = AuthConfig::ClientCert {
            certificate_pem: TEST_CERT_PEM.into(),
        };
        assert_eq!(
            secret_ops(&cert, &s(Some("t"), Some("KEY"))),
            [
                (TOKEN, SecretOp::Delete),
                (CLIENT_KEY, SecretOp::Set("KEY".into())),
                (METRICS_TOKEN, SecretOp::Delete),
                (OIDC_CLIENT_SECRET, SecretOp::Delete),
                (OIDC_ID_TOKEN, SecretOp::Delete),
                (OIDC_REFRESH_TOKEN, SecretOp::Delete)
            ]
        );
        // Вход через OIDC: секрет клиента — из формы, токены входа форма не трогает.
        let mut oidc = sample_profile("a");
        oidc.auth = AuthConfig::Oidc(crate::storage::profile::sample_oidc());
        assert_eq!(
            secret_ops(&oidc, &s(Some("t"), None)),
            [
                (TOKEN, SecretOp::Delete),
                (CLIENT_KEY, SecretOp::Delete),
                (METRICS_TOKEN, SecretOp::Delete),
                (OIDC_CLIENT_SECRET, SecretOp::Set("cs".into())),
                (OIDC_ID_TOKEN, SecretOp::Keep),
                (OIDC_REFRESH_TOKEN, SecretOp::Keep)
            ]
        );
        assert_eq!(
            secret_ops(&oidc, &SecretsInput::default())[3],
            (OIDC_CLIENT_SECRET, SecretOp::Keep)
        );
        // Токен метрик хранится, только пока у профиля свой источник с авторизацией bearer.
        let mut bearer = sample_profile("a");
        bearer.metrics = MetricsConfig::Prometheus(MetricsEndpoint {
            url: "https://prometheus.example.com".into(),
            auth: MetricsAuth::Bearer,
            ca: MetricsCa::Connection,
        });
        assert_eq!(
            secret_ops(&bearer, &s(None, None))[2],
            (METRICS_TOKEN, SecretOp::Set("m".into()))
        );
        assert_eq!(
            secret_ops(&bearer, &SecretsInput::default())[2],
            (METRICS_TOKEN, SecretOp::Keep)
        );
    }

    /// Сохраняет OIDC-подключение с токенами входа, затем сохраняет его же с правкой `edit`.
    fn oidc_tokens_after_edit(
        edit: impl FnOnce(&mut crate::storage::profile::OidcAuth),
    ) -> [Option<String>; 2] {
        let dir = tempfile::tempdir().unwrap();
        let st = state(dir.path());
        let mut p = sample_profile("");
        p.id = String::new();
        p.auth = AuthConfig::Oidc(crate::storage::profile::sample_oidc());
        let saved = save_connection_blocking(&st, input(p, None, None)).unwrap();
        st.secrets
            .set(&secret_key(&saved.id, OIDC_ID_TOKEN), "idt")
            .unwrap();
        st.secrets
            .set(&secret_key(&saved.id, OIDC_REFRESH_TOKEN), "rt")
            .unwrap();
        let mut next = saved.clone();
        if let AuthConfig::Oidc(o) = &mut next.auth {
            edit(o);
        }
        save_connection_blocking(&st, input(next, None, None)).unwrap();
        [
            secret(&st, &saved.id, OIDC_ID_TOKEN),
            secret(&st, &saved.id, OIDC_REFRESH_TOKEN),
        ]
    }

    #[test]
    fn oidc_tokens_are_dropped_when_issuer_or_client_changes() {
        // Другой издатель: токены прежнего нельзя отправлять новому.
        assert_eq!(
            oidc_tokens_after_edit(|o| o.issuer_url = "https://other.example.com".into()),
            [None, None]
        );
        // Другой client id: refresh_token выдан прежнему клиенту.
        assert_eq!(
            oidc_tokens_after_edit(|o| o.client_id = "other-client".into()),
            [None, None]
        );
        // Ничего не изменилось (завершающий «/» адреса — та же нормализация): токены остаются.
        assert_eq!(
            oidc_tokens_after_edit(|o| o.issuer_url.push('/')),
            [Some("idt".into()), Some("rt".into())]
        );
        assert_eq!(
            oidc_tokens_after_edit(|_| ()),
            [Some("idt".into()), Some("rt".into())]
        );
    }

    #[test]
    fn save_assigns_id_stores_secrets_and_switches_method() {
        let dir = tempfile::tempdir().unwrap();
        let st = state(dir.path());
        let mut p = sample_profile("");
        p.id = String::new();
        let saved = save_connection_blocking(&st, input(p, Some("sha256~t"), None)).unwrap();
        assert!(!saved.id.is_empty());
        assert_eq!(secret(&st, &saved.id, TOKEN).as_deref(), Some("sha256~t"));
        // Повторное сохранение без токена оставляет сохранённый.
        save_connection_blocking(&st, input(saved.clone(), None, None)).unwrap();
        assert_eq!(secret(&st, &saved.id, TOKEN).as_deref(), Some("sha256~t"));
        // Переход на сертификат удаляет токен.
        let mut cert = saved.clone();
        cert.auth = AuthConfig::ClientCert {
            certificate_pem: TEST_CERT_PEM.into(),
        };
        save_connection_blocking(&st, input(cert, None, Some("KEY"))).unwrap();
        assert_eq!(secret(&st, &saved.id, TOKEN), None);
        assert_eq!(secret(&st, &saved.id, CLIENT_KEY).as_deref(), Some("KEY"));
    }

    #[test]
    fn invalid_profile_touches_no_secret_and_policy_blocks_insecure() {
        let dir = tempfile::tempdir().unwrap();
        let mut policy = crate::policy::Policy::default();
        policy.status.allow_insecure_tls = false;
        let st = state(dir.path()).with_policy(policy);
        let mut p = sample_profile("a");
        p.ca = CaConfig::Insecure;
        let e = save_connection_blocking(&st, input(p, Some("t"), None)).unwrap_err();
        assert_eq!(e.key, "errors.connections.insecureForbidden");
        let mut p = sample_profile("a");
        p.name = " ".into();
        assert_eq!(
            save_connection_blocking(&st, input(p, Some("t"), None))
                .unwrap_err()
                .code,
            ErrorCode::InvalidInput
        );
        assert_eq!(secret(&st, "a", TOKEN), None);
        assert!(st.connections.list().is_empty());
    }

    #[test]
    fn exec_approval_comes_only_from_the_form() {
        let dir = tempfile::tempdir().unwrap();
        let st = state(dir.path());
        let mut p = sample_profile("e");
        let mut forged = crate::storage::profile::sample_exec();
        forged.approved = Some(crate::auth::exec::fingerprint(&forged));
        p.auth = AuthConfig::Exec(forged);
        let approved = |p: &ConnectionProfile| match &p.auth {
            AuthConfig::Exec(e) => crate::auth::exec::is_approved(e),
            _ => false,
        };
        // Подтверждение, пришедшее в самом профиле, не принимается.
        let saved = save_connection_blocking(&st, input(p.clone(), None, None)).unwrap();
        assert!(!approved(&saved));
        let mut i = input(saved.clone(), None, None);
        i.confirm_exec = true;
        let saved = save_connection_blocking(&st, i).unwrap();
        assert!(approved(&saved));
        // Повторное сохранение той же строки подтверждение сохраняет, изменённой — нет.
        assert!(approved(
            &save_connection_blocking(&st, input(saved.clone(), None, None)).unwrap()
        ));
        let mut changed = saved;
        if let AuthConfig::Exec(e) = &mut changed.auth {
            e.args.push("--v=8".into());
        }
        assert!(!approved(
            &save_connection_blocking(&st, input(changed, None, None)).unwrap()
        ));
        // Политика запрещает exec-плагины.
        let mut policy = crate::policy::Policy::default();
        policy.status.allow_exec_auth = false;
        let st = state(dir.path()).with_policy(policy);
        assert_eq!(
            save_connection_blocking(&st, input(p, None, None))
                .unwrap_err()
                .key,
            "errors.connections.execForbidden"
        );
    }

    #[test]
    fn stale_exec_approval_is_dropped_when_the_line_changes() {
        let dir = tempfile::tempdir().unwrap();
        let st = state(dir.path());
        let approval = |p: &ConnectionProfile| match &p.auth {
            AuthConfig::Exec(e) => e.approved.clone(),
            _ => panic!("exec-плагин"),
        };
        let mut p = sample_profile("e");
        p.auth = AuthConfig::Exec(crate::storage::profile::sample_exec());
        let mut confirm = input(p, None, None);
        confirm.confirm_exec = true;
        let saved = save_connection_blocking(&st, confirm).unwrap();
        let fp = approval(&saved).expect("подтверждение поставлено");
        // Та же строка без галочки: подтверждение остаётся.
        let again = save_connection_blocking(&st, input(saved.clone(), None, None)).unwrap();
        assert_eq!(approval(&again), Some(fp.clone()));
        // Изменённая строка без галочки: в профиле не остаётся отпечатка прежней строки.
        let mut changed = saved;
        if let AuthConfig::Exec(e) = &mut changed.auth {
            e.args.push("--v=8".into());
        }
        let saved = save_connection_blocking(&st, input(changed, None, None)).unwrap();
        assert_eq!(approval(&saved), None);
        assert_eq!(approval(&st.connections.get("e").unwrap()), None);
        // Возврат прежней строки старого подтверждения не воскрешает.
        let mut back = saved;
        if let AuthConfig::Exec(e) = &mut back.auth {
            e.args.pop();
        }
        let saved = save_connection_blocking(&st, input(back, None, None)).unwrap();
        assert_eq!(approval(&saved), None);
    }

    struct FailingStore;
    impl SecretStore for FailingStore {
        fn get(&self, _: &str) -> AppResult<Option<String>> {
            Ok(None)
        }
        fn set(&self, _: &str, _: &str) -> AppResult<()> {
            Err(AppError::internal_with("keychain locked"))
        }
        fn delete(&self, _: &str) -> AppResult<()> {
            Ok(())
        }
    }

    #[test]
    fn keychain_failure_rolls_back_new_profile() {
        let dir = tempfile::tempdir().unwrap();
        let st = AppState::new(dir.path(), Arc::new(FailingStore)).unwrap();
        let e = save_connection_blocking(&st, input(sample_profile("a"), Some("t"), None));
        assert!(e.is_err());
        assert!(
            st.connections.list().is_empty(),
            "профиль без секрета не остался"
        );
    }

    #[test]
    fn token_can_be_replaced_only_for_token_methods() {
        let dir = tempfile::tempdir().unwrap();
        let st = state(dir.path());
        st.connections.upsert(sample_profile("a")).unwrap();
        set_token_blocking(&st, "a", " sha256~new ").unwrap();
        assert_eq!(secret(&st, "a", TOKEN).as_deref(), Some("sha256~new"));
        let mut cert = sample_profile("c");
        cert.auth = AuthConfig::ClientCert {
            certificate_pem: TEST_CERT_PEM.into(),
        };
        st.connections.upsert(cert).unwrap();
        assert_eq!(
            set_token_blocking(&st, "c", "t").unwrap_err().key,
            "errors.connections.tokenNotUsed"
        );
    }

    /// Сохранение профиля и новый токен сбрасывают клиент — и сессии, которые шли на нём.
    #[test]
    fn saving_or_replacing_the_token_stops_the_sessions() {
        let dir = tempfile::tempdir().unwrap();
        let st = state(dir.path());
        let saved =
            save_connection_blocking(&st, input(sample_profile("a"), Some("t"), None)).unwrap();
        let (_, other) = st.sessions.register("b");
        let (_, watch) = st.sessions.register("a");
        save_connection_blocking(&st, input(saved, None, None)).unwrap();
        assert!(watch.is_cancelled(), "профиль сохранён");
        let (_, logs) = st.sessions.register("a");
        set_token_blocking(&st, "a", "sha256~new").unwrap();
        assert!(logs.is_cancelled(), "токен заменён");
        assert!(!other.is_cancelled(), "сессии другого подключения идут");
    }
    /// Хранилище, которое не принимает одну запись (`Set` по этому ключу).
    struct RejectsKey {
        inner: MemoryStore,
        key: String,
    }
    impl SecretStore for RejectsKey {
        fn get(&self, key: &str) -> AppResult<Option<String>> {
            self.inner.get(key)
        }
        fn set(&self, key: &str, value: &str) -> AppResult<()> {
            if key == self.key {
                return Err(AppError::internal_with("keychain locked"));
            }
            self.inner.set(key, value)
        }
        fn delete(&self, key: &str) -> AppResult<()> {
            self.inner.delete(key)
        }
    }

    #[test]
    fn failed_set_after_method_switch_keeps_the_old_secret_and_drops_the_client() {
        let dir = tempfile::tempdir().unwrap();
        let store = RejectsKey {
            inner: MemoryStore::default(),
            key: secret_key("a", CLIENT_KEY),
        };
        let st = AppState::new(dir.path(), Arc::new(store)).unwrap();
        let saved =
            save_connection_blocking(&st, input(sample_profile("a"), Some("old"), None)).unwrap();
        let generation = st.pool.generation("a");
        // Токен -> сертификат: ключ keychain не принял, токен удалять нельзя.
        let mut cert = saved.clone();
        cert.auth = AuthConfig::ClientCert {
            certificate_pem: TEST_CERT_PEM.into(),
        };
        let e = save_connection_blocking(&st, input(cert, None, Some("KEY"))).unwrap_err();
        assert_eq!(e.key, "errors.internal");
        assert_eq!(secret(&st, "a", TOKEN).as_deref(), Some("old"));
        assert_eq!(st.connections.get("a").unwrap().auth, AuthConfig::Token);
        assert!(
            st.pool.generation("a") > generation,
            "клиент сброшен и при ошибке"
        );
    }

    #[test]
    fn debug_output_hides_secrets() {
        let secrets = SecretsInput {
            token: Some("sha256~tok-123".into()),
            client_key: Some("KEY-456".into()),
            metrics_token: Some("METRICS-789".into()),
            oidc_client_secret: Some("OIDC-012".into()),
        };
        let connection = ConnectionInput {
            profile: sample_profile("a"),
            secrets: secrets.clone(),
            confirm_exec: true,
        };
        for text in [
            format!("{secrets:?}"),
            format!("{secrets:#?}"),
            format!("{connection:?}"),
            format!("{connection:#?}"),
        ] {
            assert!(
                !text.contains("tok-123")
                    && !text.contains("KEY-456")
                    && !text.contains("METRICS-789")
                    && !text.contains("OIDC-012"),
                "{text}"
            );
            assert!(text.contains("***"), "{text}");
        }
        let text = format!("{:?}", SecretsInput::default());
        assert!(
            !text.contains("***"),
            "нет секрета — нечего скрывать: {text}"
        );
        assert!(format!("{connection:?}").contains("conn-a"));
    }
}
