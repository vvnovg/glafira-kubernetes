//! Вход через OIDC (спека O5 §5): метаданные издателя, Authorization Code + PKCE с возвратом на
//! фиксированный порт, обмен кода на `id_token`, обновление по `refresh_token`. Подпись
//! `id_token` проверяет API-сервер; здесь читаются только `exp` и `nonce`.

use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::Deserialize;
use tokio::sync::oneshot;

use crate::auth::callback::{CallbackServer, cancelled};
use crate::auth::oauth::{from_reqwest, http_client_with, is_http_url};
use crate::auth::pkce;
use crate::error::{AppError, AppResult, ErrorCode};
use crate::k8s::errors::chain_text;
use crate::secrets::{
    OIDC_CLIENT_SECRET, OIDC_ID_TOKEN, OIDC_REFRESH_TOKEN, SecretStore, secret_key,
};
use crate::storage::profile::{AuthConfig, ConnectionProfile, OidcAuth};

/// Порты возврата по порядку — те же, что у kubelogin: они есть у клиента `dapp` DropApp.
pub const OIDC_PORTS: [u16; 2] = [8000, 18000];
/// За сколько до `exp` токен обновляется.
const REFRESH_AHEAD_SECS: i64 = 60;

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct OidcMetadata {
    pub issuer: String,
    pub authorization_endpoint: String,
    pub token_endpoint: String,
}

/// Токены входа: `id_token` уходит в API-сервер, `refresh_token` обновляет его, `expires_at` —
/// `exp` из `id_token` (секунды Unix).
#[derive(Clone)]
pub struct OidcTokens {
    pub id_token: String,
    pub refresh_token: Option<String>,
    pub expires_at: Option<i64>,
}

/// Токены не печатаются: структура может попасть в лог или сообщение об ошибке.
impl std::fmt::Debug for OidcTokens {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let redacted = crate::storage::profile::REDACTED;
        f.debug_struct("OidcTokens")
            .field("id_token", &redacted)
            .field(
                "refresh_token",
                &self.refresh_token.as_ref().map(|_| redacted),
            )
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

/// HTTP-клиент к издателю — как к OAuth-серверу OpenShift (CA профиля добавляется к системным
/// корням, прокси и TLS профиля); переадресации не выполняются: форма с кодом и секретом
/// клиента уходит только на `token_endpoint`.
pub fn http_client(p: &ConnectionProfile) -> AppResult<reqwest::Client> {
    http_client_with(p, |b| b.redirect(reqwest::redirect::Policy::none()))
}

fn no_metadata(url: &str) -> AppError {
    app_err!(
        ErrorCode::AuthFailed,
        "errors.oidc.noMetadata",
        "The OIDC issuer {{url}} returned no OpenID configuration — check the issuer address",
        url = url
    )
}

/// Адрес авторизации открывается в браузере, а на адрес токенов уходят код, verifier и секрет
/// клиента: только https. http допустим лишь для локального издателя (127.0.0.1 и localhost,
/// как в `validate_oidc`: тесты и имитация).
fn check_endpoints(issuer: &str, meta: &OidcMetadata) -> AppResult<()> {
    let local_http = url::Url::parse(issuer).is_ok_and(|u| {
        u.scheme() == "http" && matches!(u.host_str(), Some("127.0.0.1" | "localhost"))
    });
    for (name, value) in [
        ("authorization_endpoint", &meta.authorization_endpoint),
        ("token_endpoint", &meta.token_endpoint),
    ] {
        if !is_http_url(value) {
            return Err(no_metadata(issuer).with_details(format!("{name}: {value}")));
        }
        if !local_http && !value.trim().to_ascii_lowercase().starts_with("https://") {
            return Err(app_err!(
                ErrorCode::AuthFailed,
                "errors.oidc.insecureEndpoint",
                "The OIDC provider {{url}} advertises an insecure (http) endpoint",
                url = issuer
            )
            .with_details(format!("{name}: {value}")));
        }
    }
    Ok(())
}

/// `GET <issuer>/.well-known/openid-configuration`; `issuer` ответа должен совпасть с адресом
/// издателя (без завершающего «/»).
pub async fn fetch_metadata(http: &reqwest::Client, issuer_url: &str) -> AppResult<OidcMetadata> {
    let issuer = issuer_url.trim().trim_end_matches('/');
    let url = format!("{issuer}/.well-known/openid-configuration");
    let r = http.get(&url).send().await.map_err(|e| from_reqwest(&e))?;
    if !r.status().is_success() {
        return Err(no_metadata(issuer).with_details(format!("{url}: HTTP {}", r.status())));
    }
    let meta = r
        .json::<OidcMetadata>()
        .await
        .map_err(|e| no_metadata(issuer).with_details(chain_text(&e)))?;
    check_endpoints(issuer, &meta)?;
    if meta.issuer.trim_end_matches('/') != issuer {
        return Err(app_err!(
            ErrorCode::AuthFailed,
            "errors.oidc.issuerMismatch",
            "The issuer address does not match: the provider reports “{{actual}}”, the connection has “{{expected}}”",
            actual = meta.issuer,
            expected = issuer
        ));
    }
    Ok(meta)
}

/// Адрес страницы входа: Authorization Code + PKCE (S256) с `state` и `nonce`.
fn authorize_url(
    meta: &OidcMetadata,
    auth: &OidcAuth,
    redirect_uri: &str,
    challenge: &str,
    state: &str,
    nonce: &str,
) -> AppResult<String> {
    let mut u =
        url::Url::parse(&meta.authorization_endpoint).map_err(|_| no_metadata(&auth.issuer_url))?;
    u.query_pairs_mut().extend_pairs([
        ("client_id", auth.client_id.as_str()),
        ("response_type", "code"),
        ("redirect_uri", redirect_uri),
        ("scope", &auth.scopes.join(" ")),
        ("code_challenge", challenge),
        ("code_challenge_method", "S256"),
        ("state", state),
        ("nonce", nonce),
    ]);
    Ok(u.into())
}

/// Данные `id_token` (вторая часть JWT, base64url JSON) без проверки подписи.
fn jwt_claims(token: &str) -> Option<serde_json::Value> {
    let payload = token.split('.').nth(1)?;
    let bytes = URL_SAFE_NO_PAD.decode(payload.trim_end_matches('=')).ok()?;
    serde_json::from_slice(&bytes).ok()
}

/// Срок `id_token` — поле `exp` (секунды Unix).
pub fn jwt_exp(id_token: &str) -> Option<i64> {
    let exp = jwt_claims(id_token)?.get("exp").cloned()?;
    exp.as_i64().or_else(|| exp.as_f64().map(|v| v as i64))
}

#[derive(Deserialize)]
struct TokenResponse {
    #[serde(default)]
    id_token: Option<String>,
    #[serde(default)]
    refresh_token: Option<String>,
}

#[derive(Deserialize, Default)]
struct TokenError {
    #[serde(default)]
    error: String,
    #[serde(default)]
    error_description: String,
}

/// Запрос к `token_endpoint` формой; секрет клиента — `client_secret_post`, если он есть.
/// Отказ провайдера — его `error` и `error_description`; секрет, код и токены в ошибку не
/// попадают.
async fn token_request(
    http: &reqwest::Client,
    meta: &OidcMetadata,
    auth: &OidcAuth,
    client_secret: Option<&str>,
    grant: &[(&str, &str)],
) -> AppResult<OidcTokens> {
    let mut form = grant.to_vec();
    form.push(("client_id", &auth.client_id));
    if let Some(secret) = client_secret {
        form.push(("client_secret", secret));
    }
    let r = http
        .post(&meta.token_endpoint)
        .form(&form)
        .send()
        .await
        .map_err(|e| from_reqwest(&e))?;
    let status = r.status();
    let text = r.text().await.map_err(|e| from_reqwest(&e))?;
    if !status.is_success() {
        let err: TokenError = serde_json::from_str(&text).unwrap_or_default();
        let reason = match (err.error.as_str(), err.error_description.as_str()) {
            ("", "") => format!("HTTP {status}"),
            (e, "") | ("", e) => e.to_string(),
            (e, d) => format!("{e}: {d}"),
        };
        return Err(app_err!(
            ErrorCode::AuthFailed,
            "errors.oidc.providerError",
            "The OIDC provider rejected the sign-in: {{reason}}",
            reason = reason
        ));
    }
    let r: TokenResponse = serde_json::from_str(&text).map_err(|e| {
        app_err!(
            ErrorCode::AuthFailed,
            "errors.oidc.badTokenResponse",
            "The OIDC provider returned an unexpected response"
        )
        .with_details(e.to_string())
    })?;
    let id_token = r.id_token.filter(|t| !t.trim().is_empty()).ok_or_else(|| {
        app_err!(
            ErrorCode::AuthFailed,
            "errors.oidc.noIdToken",
            "The OIDC provider returned no ID token — check that the scopes include openid"
        )
    })?;
    Ok(OidcTokens {
        expires_at: jwt_exp(&id_token),
        id_token,
        refresh_token: r.refresh_token.filter(|t| !t.is_empty()),
    })
}

/// Полный вход: метаданные → сервер возврата на фиксированном порте → браузер → код → токены.
/// Токены не сохраняются здесь: это делает команда (keychain). `cancel` действует, как в
/// `oauth::login`: и пока запрашиваются метаданные, и пока ждём redirect.
pub async fn login(
    p: &ConnectionProfile,
    auth: &OidcAuth,
    client_secret: Option<&str>,
    ports: &[u16],
    open_browser: impl FnOnce(&str) -> AppResult<()>,
    mut cancel: oneshot::Receiver<()>,
    limit: Duration,
) -> AppResult<OidcTokens> {
    let http = http_client(p)?;
    let prepare = async {
        let meta = fetch_metadata(&http, &auth.issuer_url).await?;
        let server = CallbackServer::bind_fixed(ports).await?;
        Ok::<_, AppError>((meta, server))
    };
    let (meta, server) = tokio::select! {
        biased;
        _ = &mut cancel => return Err(cancelled()),
        prepared = prepare => prepared?,
    };
    let redirect_uri = server.redirect_uri.clone();
    let pkce = pkce::generate()?;
    let state = pkce::random_token()?;
    let nonce = pkce::random_token()?;
    open_browser(&authorize_url(
        &meta,
        auth,
        &redirect_uri,
        &pkce.challenge,
        &state,
        &nonce,
    )?)?;
    let code = server.wait(&state, limit, cancel).await?;
    let tokens = token_request(
        &http,
        &meta,
        auth,
        client_secret,
        &[
            ("grant_type", "authorization_code"),
            ("code", &code),
            ("redirect_uri", &redirect_uri),
            ("code_verifier", &pkce.verifier),
        ],
    )
    .await?;
    let got = jwt_claims(&tokens.id_token).and_then(|c| c.get("nonce")?.as_str().map(String::from));
    if got.as_deref() != Some(nonce.as_str()) {
        return Err(app_err!(
            ErrorCode::AuthFailed,
            "errors.oidc.nonceMismatch",
            "The ID token from the OIDC provider does not belong to this sign-in — sign in again"
        ));
    }
    Ok(tokens)
}

/// Обновление по `refresh_token`. Новый `refresh_token` заменяет прежний, если провайдер его
/// выдал, иначе остаётся прежний.
pub async fn refresh(
    http: &reqwest::Client,
    meta: &OidcMetadata,
    auth: &OidcAuth,
    client_secret: Option<&str>,
    refresh_token: &str,
) -> AppResult<OidcTokens> {
    let mut tokens = token_request(
        http,
        meta,
        auth,
        client_secret,
        &[
            ("grant_type", "refresh_token"),
            ("refresh_token", refresh_token),
        ],
    )
    .await?;
    tokens
        .refresh_token
        .get_or_insert_with(|| refresh_token.to_string());
    Ok(tokens)
}

/// Нужен вход через браузер: токена нет, он истёк без `refresh_token` или провайдер отказал в
/// обновлении.
fn login_required() -> AppError {
    app_err!(
        ErrorCode::SessionExpired,
        "errors.connections.oidcLoginRequired",
        "Sign in to the cluster: use “Sign in via OIDC”"
    )
}

/// Сохраняет токены входа в keychain; без `refresh_token` прежний удаляется — он от другого
/// входа. Блокирующий вызов.
pub fn store_tokens(secrets: &dyn SecretStore, id: &str, tokens: &OidcTokens) -> AppResult<()> {
    secrets.set(&secret_key(id, OIDC_ID_TOKEN), &tokens.id_token)?;
    match &tokens.refresh_token {
        Some(r) => secrets.set(&secret_key(id, OIDC_REFRESH_TOKEN), r),
        None => secrets.delete(&secret_key(id, OIDC_REFRESH_TOKEN)),
    }
}

/// Непустой секрет из keychain на blocking-потоке.
async fn read_secret(secrets: &Arc<dyn SecretStore>, key: String) -> AppResult<Option<String>> {
    let secrets = secrets.clone();
    let value = tokio::task::spawn_blocking(move || secrets.get(&key))
        .await
        .map_err(|e| AppError::internal_with(e.to_string()))??;
    Ok(value
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty()))
}

/// Секрет клиента подключения из keychain; `None` — публичный клиент.
pub async fn client_secret(secrets: &Arc<dyn SecretStore>, id: &str) -> AppResult<Option<String>> {
    read_secret(secrets, secret_key(id, OIDC_CLIENT_SECRET)).await
}

/// `id_token` подключения из keychain, если до `exp` больше минуты; ничего не обновляет и не
/// записывает.
async fn fresh_id_token(
    p: &ConnectionProfile,
    secrets: &Arc<dyn SecretStore>,
) -> AppResult<Option<OidcTokens>> {
    let id_token = read_secret(secrets, secret_key(&p.id, OIDC_ID_TOKEN)).await?;
    let now = time::OffsetDateTime::now_utc().unix_timestamp();
    Ok(id_token.and_then(|id_token| {
        let expires_at = jwt_exp(&id_token);
        expires_at
            .is_none_or(|exp| exp > now + REFRESH_AHEAD_SECS)
            .then_some(OidcTokens {
                id_token,
                refresh_token: None,
                expires_at,
            })
    }))
}

/// Токен для проверки подключения: только действующий сохранённый `id_token` — без обновления
/// (хранилище проверки только на чтение) и без входа; его нет — нужен вход.
pub async fn stored_credential(
    p: &ConnectionProfile,
    secrets: &Arc<dyn SecretStore>,
) -> AppResult<OidcTokens> {
    fresh_id_token(p, secrets).await?.ok_or_else(login_required)
}

/// Токен для подключения: `id_token` из keychain, если до `exp` больше минуты; иначе — обновление
/// по `refresh_token` (оба новых токена записываются в keychain). Одновременные подключения
/// профиля сериализует `ClientPool::connect_lock`: второе видит уже обновлённый токен.
pub async fn credential_for(
    p: &ConnectionProfile,
    secrets: Arc<dyn SecretStore>,
) -> AppResult<OidcTokens> {
    let AuthConfig::Oidc(auth) = &p.auth else {
        return Err(AppError::internal_with(
            "credential_for: not an OIDC profile",
        ));
    };
    if let Some(tokens) = fresh_id_token(p, &secrets).await? {
        return Ok(tokens);
    }
    let Some(refresh_token) = read_secret(&secrets, secret_key(&p.id, OIDC_REFRESH_TOKEN)).await?
    else {
        return Err(login_required());
    };
    let client_secret = client_secret(&secrets, &p.id).await?;
    let http = http_client(p)?;
    let meta = fetch_metadata(&http, &auth.issuer_url).await?;
    let tokens = refresh(&http, &meta, auth, client_secret.as_deref(), &refresh_token)
        .await
        // Отказ провайдера (истёкший или отозванный `refresh_token`) — войти заново; сеть и TLS
        // остаются своими ошибками.
        .map_err(|e| match e.code {
            ErrorCode::AuthFailed => login_required().with_details(e.message),
            _ => e,
        })?;
    let (store, id, saved) = (secrets.clone(), p.id.clone(), tokens.clone());
    tokio::task::spawn_blocking(move || store_tokens(store.as_ref(), &id, &saved))
        .await
        .map_err(|e| AppError::internal_with(e.to_string()))??;
    Ok(tokens)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex, OnceLock};

    use base64::Engine;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;

    use super::*;
    use crate::auth::oauth::tests::{fake_server, plain_client, test_profile};
    use crate::secrets::MemoryStore;
    use crate::storage::profile::sample_oidc;

    /// `id_token` без подписи: заголовок, данные (`claims`) и подпись-заглушка.
    fn jwt(claims: serde_json::Value) -> String {
        format!(
            "{}.{}.sig",
            URL_SAFE_NO_PAD.encode(br#"{"alg":"RS256"}"#),
            URL_SAFE_NO_PAD.encode(claims.to_string())
        )
    }

    fn now() -> i64 {
        time::OffsetDateTime::now_utc().unix_timestamp()
    }

    /// Свободный порт: занимаем 127.0.0.1:0 и отпускаем.
    async fn free_port() -> u16 {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        l.local_addr().unwrap().port()
    }

    /// Поля формы из записи запроса фейкового сервера («POST /path тело»).
    fn form_of(request: &str) -> HashMap<String, String> {
        let body = request.splitn(3, ' ').nth(2).unwrap_or("");
        url::form_urlencoded::parse(body.as_bytes())
            .into_owned()
            .collect()
    }

    /// Издатель на фейковом сервере: метаданные указывают на него самого, токены выдаёт `token`
    /// по полям формы. Возвращает адрес издателя и журнал запросов.
    async fn issuer(
        token: impl Fn(&HashMap<String, String>) -> (u16, String) + Send + Sync + 'static,
    ) -> (String, Arc<Mutex<Vec<String>>>) {
        let base = Arc::new(OnceLock::<String>::new());
        let b = base.clone();
        let (url, log) = fake_server(move |method, path, body| match (method, path) {
            ("GET", "/dex/.well-known/openid-configuration") => {
                let b = b.get().unwrap();
                (
                    200,
                    serde_json::json!({
                        "issuer": format!("{b}/dex"),
                        "authorization_endpoint": format!("{b}/dex/auth"),
                        "token_endpoint": format!("{b}/dex/token"),
                    })
                    .to_string(),
                )
            }
            ("POST", "/dex/token") => token(
                &url::form_urlencoded::parse(body.as_bytes())
                    .into_owned()
                    .collect(),
            ),
            _ => (404, "{}".into()),
        })
        .await;
        base.set(url.clone()).unwrap();
        (format!("{url}/dex"), log)
    }

    fn auth(issuer_url: &str) -> OidcAuth {
        OidcAuth {
            issuer_url: issuer_url.into(),
            ..sample_oidc()
        }
    }

    #[tokio::test]
    async fn metadata_issuer_must_match() {
        let (issuer_url, log) = issuer(|_| (404, "{}".into())).await;
        let http = http_client(&test_profile()).unwrap();
        // Завершающий «/» адреса издателя не мешает ни запросу, ни сравнению.
        let m = fetch_metadata(&http, &format!("{issuer_url}/"))
            .await
            .unwrap();
        assert_eq!(m.token_endpoint, format!("{issuer_url}/token"));
        assert_eq!(
            log.lock().unwrap()[0],
            "GET /dex/.well-known/openid-configuration "
        );
        // Ответ издателя со «/» на конце тоже совпадает.
        let (base, _) = fake_server(|_, _, _| {
            (
                200,
                serde_json::json!({"issuer": "https://auth.c1/", "authorization_endpoint": "https://auth.c1/auth",
                    "token_endpoint": "https://auth.c1/token"})
                .to_string(),
            )
        })
        .await;
        let e = fetch_metadata(&http, &base).await.unwrap_err();
        assert_eq!(
            (e.code, e.key.as_str()),
            (ErrorCode::AuthFailed, "errors.oidc.issuerMismatch")
        );
        assert_eq!(e.params["actual"], "https://auth.c1/");
        let (other, _) = fake_server(|_, _, _| (404, "{}".into())).await;
        let e = fetch_metadata(&http, &other).await.unwrap_err();
        assert_eq!(
            (e.code, e.key.as_str()),
            (ErrorCode::AuthFailed, "errors.oidc.noMetadata")
        );
    }

    #[test]
    fn https_issuer_must_not_advertise_http_endpoints() {
        let meta = |auth: &str, token: &str| OidcMetadata {
            issuer: "https://auth.c1".into(),
            authorization_endpoint: auth.into(),
            token_endpoint: token.into(),
        };
        let ok = meta("https://auth.c1/auth", "https://auth.c1/token");
        assert!(check_endpoints("https://auth.c1", &ok).is_ok());
        for bad in [
            meta("http://auth.c1/auth", "https://auth.c1/token"),
            meta("https://auth.c1/auth", "http://auth.c1/token"),
        ] {
            let e = check_endpoints("https://auth.c1", &bad).unwrap_err();
            assert_eq!(
                (e.code, e.key.as_str()),
                (ErrorCode::AuthFailed, "errors.oidc.insecureEndpoint")
            );
        }
        // Локальный http-издатель (тесты и имитация) может отдавать http-адреса.
        let local = meta("http://127.0.0.1:1/auth", "http://127.0.0.1:1/token");
        assert!(check_endpoints("http://127.0.0.1:1", &local).is_ok());
        // Адрес не http(s) отклоняется и для локального издателя.
        let junk = meta("javascript:alert(1)", "http://127.0.0.1:1/token");
        assert_eq!(
            check_endpoints("http://127.0.0.1:1", &junk)
                .unwrap_err()
                .key,
            "errors.oidc.noMetadata"
        );
    }

    #[test]
    fn exp_and_nonce_are_read_without_the_signature() {
        let t = jwt(serde_json::json!({"exp": 1_700_000_000, "nonce": "n1"}));
        assert_eq!(jwt_exp(&t), Some(1_700_000_000));
        assert_eq!(jwt_exp(&jwt(serde_json::json!({"sub": "u"}))), None);
        for bad in ["", "a.b.c", "x", "a.!!!.c"] {
            assert_eq!(jwt_exp(bad), None, "{bad}");
        }
    }

    /// «Браузер»: проверяет адрес авторизации, запоминает его параметры и проходит redirect с
    /// кодом `code` или отказом `error` и тем же `state`.
    fn browser(
        seen: Arc<Mutex<HashMap<String, String>>>,
        answer: &'static str,
    ) -> impl FnOnce(&str) -> AppResult<()> {
        move |url: &str| {
            let u = url::Url::parse(url).unwrap();
            assert_eq!(u.path(), "/dex/auth");
            let q: HashMap<_, _> = u.query_pairs().into_owned().collect();
            *seen.lock().unwrap() = q.clone();
            let callback = format!("{}?{answer}&state={}", q["redirect_uri"], q["state"]);
            tokio::spawn(async move {
                plain_client().get(callback).send().await.unwrap();
            });
            Ok(())
        }
    }

    #[tokio::test]
    async fn full_login_through_a_fake_issuer() {
        let seen = Arc::new(Mutex::new(HashMap::<String, String>::new()));
        let nonce = seen.clone();
        let exp = now() + 3600;
        let (issuer_url, log) = issuer(move |form| {
            let nonce = nonce.lock().unwrap()["nonce"].clone();
            match form.get("code").map(String::as_str) {
                Some("the-code") => (
                    200,
                    serde_json::json!({
                        "access_token": "a",
                        "id_token": jwt(serde_json::json!({"exp": exp, "nonce": nonce})),
                        "refresh_token": "r1",
                    })
                    .to_string(),
                ),
                _ => (400, r#"{"error":"invalid_grant"}"#.into()),
            }
        })
        .await;
        let (busy, port) = (free_port().await, free_port().await);
        let _holder = tokio::net::TcpListener::bind(("127.0.0.1", busy))
            .await
            .unwrap();
        let (_tx, rx) = tokio::sync::oneshot::channel();
        let tokens = login(
            &test_profile(),
            &auth(&issuer_url),
            Some("s3cr3t"),
            &[busy, port],
            browser(seen.clone(), "code=the-code"),
            rx,
            Duration::from_secs(10),
        )
        .await
        .unwrap();
        assert_eq!(tokens.refresh_token.as_deref(), Some("r1"));
        assert_eq!(tokens.expires_at, Some(exp));
        assert_eq!(jwt_exp(&tokens.id_token), Some(exp));
        assert!(!format!("{tokens:?}").contains("r1"), "Debug без токенов");
        let q = seen.lock().unwrap().clone();
        assert_eq!(q["client_id"], "dapp");
        assert_eq!(q["response_type"], "code");
        assert_eq!(q["scope"], "openid email groups profile offline_access");
        assert_eq!(q["code_challenge_method"], "S256");
        assert_eq!(q["redirect_uri"], format!("http://localhost:{port}"));
        assert!(!q["nonce"].is_empty() && q["nonce"] != q["state"]);
        let requests = log.lock().unwrap().clone();
        let exchange = requests
            .iter()
            .find(|r| r.starts_with("POST /dex/token "))
            .unwrap();
        let form = form_of(exchange);
        assert_eq!(form["grant_type"], "authorization_code");
        assert_eq!(form["client_id"], "dapp");
        assert_eq!(form["client_secret"], "s3cr3t");
        assert_eq!(form["redirect_uri"], format!("http://localhost:{port}"));
        assert_eq!(
            crate::auth::pkce::challenge_for(&form["code_verifier"]),
            q["code_challenge"]
        );
    }

    #[tokio::test]
    async fn wrong_nonce_and_denial_fail_the_login() {
        let (issuer_url, log) = issuer(|_| {
            (
                200,
                serde_json::json!({"id_token": jwt(serde_json::json!({"exp": 1, "nonce": "forged"}))})
                    .to_string(),
            )
        })
        .await;
        let seen = Arc::new(Mutex::new(HashMap::new()));
        let (_tx, rx) = tokio::sync::oneshot::channel();
        let e = login(
            &test_profile(),
            &auth(&issuer_url),
            None,
            &[free_port().await],
            browser(seen.clone(), "code=c"),
            rx,
            Duration::from_secs(10),
        )
        .await
        .unwrap_err();
        assert_eq!(
            (e.code, e.key.as_str()),
            (ErrorCode::AuthFailed, "errors.oidc.nonceMismatch")
        );
        // Публичный клиент: без секрета форма обмена его не содержит.
        let form = form_of(log.lock().unwrap().last().unwrap());
        assert!(!form.contains_key("client_secret"), "{form:?}");

        let (_tx, rx) = tokio::sync::oneshot::channel();
        let e = login(
            &test_profile(),
            &auth(&issuer_url),
            None,
            &[free_port().await],
            browser(seen, "error=access_denied"),
            rx,
            Duration::from_secs(10),
        )
        .await
        .unwrap_err();
        assert_eq!(
            (e.code, e.key.as_str()),
            (ErrorCode::AuthFailed, "errors.oauth.denied")
        );
        assert_eq!(e.params["error"], "access_denied");
    }

    #[tokio::test]
    async fn refresh_replaces_or_keeps_the_refresh_token() {
        let exp = now() + 3600;
        let (issuer_url, log) = issuer(move |form| {
            match form.get("refresh_token").map(String::as_str) {
                Some("r1") => (
                    200,
                    serde_json::json!({"id_token": jwt(serde_json::json!({"exp": exp})), "refresh_token": "r2"})
                        .to_string(),
                ),
                Some("r2") => (
                    200,
                    serde_json::json!({"id_token": jwt(serde_json::json!({"exp": exp}))}).to_string(),
                ),
                _ => (
                    400,
                    r#"{"error":"invalid_client","error_description":"Invalid client credentials."}"#
                        .into(),
                ),
            }
        })
        .await;
        let http = http_client(&test_profile()).unwrap();
        let a = auth(&issuer_url);
        let meta = fetch_metadata(&http, &issuer_url).await.unwrap();
        let t = refresh(&http, &meta, &a, Some("s3cr3t"), "r1")
            .await
            .unwrap();
        assert_eq!(
            (t.refresh_token.as_deref(), t.expires_at),
            (Some("r2"), Some(exp))
        );
        let form = form_of(log.lock().unwrap().last().unwrap());
        assert_eq!(form["grant_type"], "refresh_token");
        assert_eq!(form["client_id"], "dapp");
        assert_eq!(form["client_secret"], "s3cr3t");
        let t = refresh(&http, &meta, &a, None, "r2").await.unwrap();
        assert_eq!(t.refresh_token.as_deref(), Some("r2"), "прежний остаётся");
        let e = refresh(&http, &meta, &a, Some("s3cr3t"), "r0")
            .await
            .unwrap_err();
        assert_eq!(
            (e.code, e.key.as_str()),
            (ErrorCode::AuthFailed, "errors.oidc.providerError")
        );
        assert!(e.params["reason"].contains("Invalid client credentials."));
        let json = serde_json::to_string(&e).unwrap();
        assert!(!json.contains("s3cr3t") && !json.contains("r0"), "{json}");
    }

    fn oidc_profile(issuer_url: &str) -> ConnectionProfile {
        let mut p = test_profile();
        p.auth = AuthConfig::Oidc(auth(issuer_url));
        p
    }

    fn get(s: &MemoryStore, field: &str) -> Option<String> {
        s.get(&secret_key("a", field)).unwrap()
    }

    #[tokio::test]
    async fn credential_uses_a_fresh_token_or_refreshes_it() {
        let new = jwt(serde_json::json!({"exp": now() + 3600, "n": 2}));
        let issued = new.clone();
        let (issuer_url, log) = issuer(move |form| match form["refresh_token"].as_str() {
            "r1" => (
                200,
                serde_json::json!({"id_token": issued, "refresh_token": "r2"}).to_string(),
            ),
            _ => (400, r#"{"error":"invalid_grant"}"#.into()),
        })
        .await;
        let p = oidc_profile(&issuer_url);
        let store = Arc::new(MemoryStore::default());
        let set = |field: &str, v: &str| store.set(&secret_key("a", field), v).unwrap();

        // Свежий токен — без запросов к издателю.
        let fresh = jwt(serde_json::json!({"exp": now() + 600}));
        set(OIDC_ID_TOKEN, &fresh);
        set(OIDC_REFRESH_TOKEN, "r1");
        let t = credential_for(&p, store.clone()).await.unwrap();
        assert_eq!(t.id_token, fresh);
        assert!(log.lock().unwrap().is_empty());

        // Истекает в ближайшую минуту — обновление, оба токена записаны в keychain.
        set(OIDC_ID_TOKEN, &jwt(serde_json::json!({"exp": now() + 30})));
        let t = credential_for(&p, store.clone()).await.unwrap();
        assert_eq!(t.id_token, new);
        assert_eq!(get(&store, OIDC_ID_TOKEN).as_deref(), Some(new.as_str()));
        assert_eq!(get(&store, OIDC_REFRESH_TOKEN).as_deref(), Some("r2"));

        // Издатель отказал в обновлении — нужен вход заново.
        set(OIDC_ID_TOKEN, &jwt(serde_json::json!({"exp": now() - 10})));
        let e = credential_for(&p, store.clone()).await.unwrap_err();
        assert_eq!(
            (e.code, e.key.as_str()),
            (
                ErrorCode::SessionExpired,
                "errors.connections.oidcLoginRequired"
            )
        );

        // Без refresh_token — тоже вход заново, без запросов.
        let requests = log.lock().unwrap().len();
        store.delete(&secret_key("a", OIDC_REFRESH_TOKEN)).unwrap();
        let e = credential_for(&p, store.clone()).await.unwrap_err();
        assert_eq!(
            (e.code, e.key.as_str()),
            (
                ErrorCode::SessionExpired,
                "errors.connections.oidcLoginRequired"
            )
        );
        assert_eq!(e.message, "Sign in to the cluster: use “Sign in via OIDC”");
        store.delete(&secret_key("a", OIDC_ID_TOKEN)).unwrap();
        let e = credential_for(&p, store).await.unwrap_err();
        assert_eq!(e.key, "errors.connections.oidcLoginRequired");
        assert_eq!(log.lock().unwrap().len(), requests);
    }
}
