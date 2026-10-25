//! Ошибки API-сервера и сети → `AppError` (спец. §15). Классификация сетевых ошибок — по тексту
//! цепочки `source()`: так её можно применить и к ошибкам `kube`, и к ошибкам HTTP-клиента OAuth.

use std::sync::LazyLock;

use kube::core::Status;
use regex::Regex;

use crate::error::{AppError, ErrorCode};

/// Текст ошибки вместе со всеми причинами: «a: b: c».
pub fn chain_text(e: &dyn std::error::Error) -> String {
    let mut out = e.to_string();
    let mut cur = e.source();
    while let Some(s) = cur {
        let t = s.to_string();
        if !out.contains(&t) {
            out.push_str(": ");
            out.push_str(&t);
        }
        cur = s.source();
    }
    out
}

pub fn from_kube(e: &kube::Error) -> AppError {
    match e {
        kube::Error::Api(status) => from_status(status),
        kube::Error::Auth(_) => app_err!(
            ErrorCode::AuthFailed,
            "errors.k8s.credentials",
            "The credentials of this connection cannot be used: {{message}}",
            message = chain_text(e)
        ),
        kube::Error::SerdeError(_) => app_err!(
            ErrorCode::Internal,
            "errors.k8s.unexpectedResponse",
            "Unexpected response — is this the address of a Kubernetes API server?"
        )
        .with_details(chain_text(e)),
        _ => from_transport_text(&chain_text(e)),
    }
}

static FORBIDDEN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"cannot (?P<verb>[a-z]+) resource "(?P<resource>[^"]+)"(?: in API group "[^"]*")?(?: in the namespace "(?P<ns>[^"]+)")?"#,
    )
    .expect("valid regex")
});

/// Нет права `verb` на `resource` в проекте `namespace` или на уровне кластера (`None`). Так же
/// выглядит ответ 403, и так же — часть экрана, которую Glafira не читает, зная, что права нет.
pub fn not_allowed(verb: &str, resource: &str, namespace: Option<&str>) -> AppError {
    match namespace {
        Some(ns) => app_err!(
            ErrorCode::Forbidden,
            "errors.k8s.forbiddenIn",
            "Not allowed: {{verb}} {{resource}} in project “{{namespace}}”",
            verb = verb,
            resource = resource,
            namespace = ns
        ),
        None => app_err!(
            ErrorCode::Forbidden,
            "errors.k8s.forbiddenCluster",
            "Not allowed: {{verb}} {{resource}} at the cluster level",
            verb = verb,
            resource = resource
        ),
    }
}

/// 403: какого права не хватает — из текста ответа API-сервера.
pub fn forbidden(message: &str) -> AppError {
    let e = match FORBIDDEN.captures(message) {
        Some(c) => not_allowed(&c["verb"], &c["resource"], c.name("ns").map(|m| m.as_str())),
        None => app_err!(
            ErrorCode::Forbidden,
            "errors.k8s.forbidden",
            "Not allowed: {{message}}",
            message = message
        ),
    };
    e.with_details(message)
}

pub fn from_status(s: &Status) -> AppError {
    let e = match s.code {
        401 => app_err!(
            ErrorCode::SessionExpired,
            "errors.k8s.unauthorized",
            "The cluster rejected the credentials: the token has expired or is invalid"
        ),
        403 => return forbidden(&s.message),
        404 => app_err!(
            ErrorCode::NotFound,
            "errors.k8s.notFound",
            "Not found: {{message}}",
            message = s.message
        ),
        409 => app_err!(
            ErrorCode::Conflict,
            "errors.k8s.conflict",
            "The object has been changed by someone else: {{message}}",
            message = s.message
        ),
        400 | 422 => app_err!(
            ErrorCode::InvalidInput,
            "errors.k8s.invalid",
            "The cluster rejected the request: {{message}}",
            message = s.message
        ),
        429 => app_err!(
            ErrorCode::Timeout,
            "errors.k8s.tooManyRequests",
            "The API server is overloaded — try again later"
        ),
        500..=599 => app_err!(
            ErrorCode::ConnectionFailed,
            "errors.k8s.serverError",
            "The API server returned error {{code}}: {{message}}",
            code = s.code,
            message = s.message
        ),
        _ => app_err!(
            ErrorCode::Internal,
            "errors.k8s.apiError",
            "API server error {{code}}: {{message}}",
            code = s.code,
            message = s.message
        ),
    };
    e.with_details(format!("{} {}: {}", s.code, s.reason, s.message))
}

/// Сетевая ошибка по тексту цепочки причин (hyper, rustls, DNS).
pub fn from_transport_text(text: &str) -> AppError {
    let t = text.to_lowercase();
    let e = if t.contains("invalid peer certificate") || t.contains("certificate") {
        if t.contains("notvalidforname") || t.contains("not valid for name") {
            app_err!(
                ErrorCode::TlsError,
                "errors.k8s.hostnameMismatch",
                "The server certificate does not match its address — check the address or “TLS server name”"
            )
        } else if t.contains("expired") || t.contains("notvalidyet") {
            app_err!(
                ErrorCode::TlsError,
                "errors.k8s.certExpired",
                "The server certificate has expired or is not valid yet"
            )
        } else if t.contains("unknownissuer")
            || t.contains("not trusted")
            || t.contains("untrusted")
        {
            app_err!(
                ErrorCode::TlsError,
                "errors.k8s.caUntrusted",
                "The server certificate is not signed by a trusted CA — specify the cluster CA"
            )
        } else {
            app_err!(
                ErrorCode::TlsError,
                "errors.k8s.tlsError",
                "TLS connection error"
            )
        }
    } else if t.contains("tls") || t.contains("handshake") {
        app_err!(
            ErrorCode::TlsError,
            "errors.k8s.tlsError",
            "TLS connection error"
        )
    } else if t.contains("dns error") || t.contains("failed to lookup address") {
        app_err!(
            ErrorCode::ConnectionFailed,
            "errors.k8s.dns",
            "Cannot resolve the server host name"
        )
    } else if t.contains("timed out") || t.contains("elapsed") || t.contains("timeout") {
        app_err!(
            ErrorCode::Timeout,
            "errors.k8s.timeout",
            "The server did not respond in time"
        )
    } else if t.contains("connect") || t.contains("refused") || t.contains("reset") {
        app_err!(
            ErrorCode::ConnectionFailed,
            "errors.k8s.connect",
            "Cannot connect to the server"
        )
    } else {
        app_err!(
            ErrorCode::Internal,
            "errors.k8s.transport",
            "Network error: {{message}}",
            message = text
        )
    };
    e.with_details(text)
}

/// Ошибка, если операция не уложилась в таймаут запроса.
pub fn timeout() -> AppError {
    app_err!(
        ErrorCode::Timeout,
        "errors.k8s.timeout",
        "The server did not respond in time"
    )
}

/// С такой ошибкой клиент подключения дальше не работает: учётные данные (401, exec-плагин,
/// OAuth) или TLS. Следующее чтение должно подключиться заново.
pub fn breaks_client(code: ErrorCode) -> bool {
    matches!(
        code,
        ErrorCode::SessionExpired | ErrorCode::AuthFailed | ErrorCode::TlsError
    )
}

/// Объект `kind` с именем `name` пришёл в виде, который Glafira не разбирает (вид без типа
/// в `k8s-openapi`, например DeploymentConfig). Текст ошибки разбора — в `details`.
pub fn malformed(kind: &str, name: &str, details: impl std::fmt::Display) -> AppError {
    app_err!(
        ErrorCode::Internal,
        "errors.k8s.malformedObject",
        "Cannot read {{kind}} “{{name}}”: unexpected object format",
        kind = kind,
        name = name
    )
    .with_details(details.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_codes() {
        for (code, expected, key) in [
            (401, ErrorCode::SessionExpired, "errors.k8s.unauthorized"),
            (404, ErrorCode::NotFound, "errors.k8s.notFound"),
            (409, ErrorCode::Conflict, "errors.k8s.conflict"),
            (422, ErrorCode::InvalidInput, "errors.k8s.invalid"),
            (429, ErrorCode::Timeout, "errors.k8s.tooManyRequests"),
            (503, ErrorCode::ConnectionFailed, "errors.k8s.serverError"),
            (418, ErrorCode::Internal, "errors.k8s.apiError"),
        ] {
            let e = from_status(&Status::failure("boom", "Reason").with_code(code));
            assert_eq!((e.code, e.key.as_str()), (expected, key), "{code}");
            assert!(e.details.unwrap().contains("boom"));
        }
    }

    #[test]
    fn forbidden_names_the_missing_right() {
        let e = forbidden(
            r#"pods is forbidden: User "dev1" cannot list resource "pods" in API group "" in the namespace "team-a""#,
        );
        assert_eq!(e.code, ErrorCode::Forbidden);
        assert_eq!(e.key, "errors.k8s.forbiddenIn");
        assert_eq!(
            (
                e.params["verb"].as_str(),
                e.params["resource"].as_str(),
                e.params["namespace"].as_str()
            ),
            ("list", "pods", "team-a")
        );
        assert_eq!(e.message, "Not allowed: list pods in project “team-a”");

        let e = forbidden(
            r#"nodes is forbidden: User "system:serviceaccount:a:b" cannot list resource "nodes" in API group "" at the cluster scope"#,
        );
        assert_eq!(e.key, "errors.k8s.forbiddenCluster");
        assert_eq!(e.message, "Not allowed: list nodes at the cluster level");

        let e = forbidden(
            r#"deployments.apps "web" is forbidden: User "x" cannot patch resource "deployments" in API group "apps" in the namespace "a""#,
        );
        assert_eq!(e.params["verb"], "patch");
        assert_eq!(e.params["resource"], "deployments");

        assert_eq!(
            not_allowed("list", "nodes", None).message,
            "Not allowed: list nodes at the cluster level"
        );
        assert_eq!(
            not_allowed("get", "pods/log", Some("a")).params["namespace"],
            "a"
        );

        let e = forbidden("access denied by webhook");
        assert_eq!(e.key, "errors.k8s.forbidden");
        assert_eq!(e.params["message"], "access denied by webhook");
        let e = from_status(&Status::failure("access denied", "Forbidden").with_code(403));
        assert_eq!(e.code, ErrorCode::Forbidden);
    }

    #[test]
    fn transport_texts() {
        for (text, code, key) in [
            (
                "ServiceError: client error (Connect): invalid peer certificate: NotValidForName",
                ErrorCode::TlsError,
                "errors.k8s.hostnameMismatch",
            ),
            (
                "client error (Connect): invalid peer certificate: UnknownIssuer",
                ErrorCode::TlsError,
                "errors.k8s.caUntrusted",
            ),
            (
                "invalid peer certificate: Other(OtherError(\"“*.badssl.com” certificate is not trusted: -67843\"))",
                ErrorCode::TlsError,
                "errors.k8s.caUntrusted",
            ),
            (
                "invalid peer certificate: Other(OtherError(\"“*.badssl.com” certificate is expired: -67818\"))",
                ErrorCode::TlsError,
                "errors.k8s.certExpired",
            ),
            (
                "invalid peer certificate: BadSignature",
                ErrorCode::TlsError,
                "errors.k8s.tlsError",
            ),
            (
                "ServiceError: client error (Connect): dns error: failed to lookup address information",
                ErrorCode::ConnectionFailed,
                "errors.k8s.dns",
            ),
            (
                "ServiceError: client error (Connect): tcp connect error: Connection refused (os error 61)",
                ErrorCode::ConnectionFailed,
                "errors.k8s.connect",
            ),
            (
                "ServiceError: deadline has elapsed",
                ErrorCode::Timeout,
                "errors.k8s.timeout",
            ),
            (
                "something else",
                ErrorCode::Internal,
                "errors.k8s.transport",
            ),
        ] {
            let e = from_transport_text(text);
            assert_eq!((e.code, e.key.as_str()), (code, key), "{text}");
            assert_eq!(e.details.as_deref(), Some(text));
        }
        assert!(from_transport_text("tcp connect error").retryable);
    }

    #[tokio::test]
    async fn real_connection_refused_is_connection_failed() {
        // Порт 1 на localhost закрыт: ошибка приходит от настоящего клиента kube.
        let mut c = kube::Config::new("https://127.0.0.1:1".parse().unwrap());
        c.connect_timeout = Some(std::time::Duration::from_secs(2));
        let client = kube::Client::try_from(c).unwrap();
        let e = client.apiserver_version().await.unwrap_err();
        let app = from_kube(&e);
        assert_eq!(app.code, ErrorCode::ConnectionFailed, "{:?}", app.details);
    }
}
