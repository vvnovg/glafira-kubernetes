//! PKCE (RFC 7636, метод S256) и `state` запроса авторизации.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use sha2::{Digest, Sha256};

use crate::error::{AppError, AppResult};

#[derive(Debug, Clone)]
pub struct Pkce {
    pub verifier: String,
    pub challenge: String,
}

/// `code_challenge` для `verifier`: BASE64URL(SHA256(verifier)) без выравнивания.
pub fn challenge_for(verifier: &str) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()))
}

/// 32 случайных байта из генератора ОС в BASE64URL: 43 символа.
pub fn random_token() -> AppResult<String> {
    let mut buf = [0u8; 32];
    getrandom::fill(&mut buf).map_err(|e| AppError::internal_with(format!("getrandom: {e}")))?;
    Ok(URL_SAFE_NO_PAD.encode(buf))
}

pub fn generate() -> AppResult<Pkce> {
    let verifier = random_token()?;
    Ok(Pkce {
        challenge: challenge_for(&verifier),
        verifier,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc7636_appendix_b_vector() {
        assert_eq!(
            challenge_for("dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"),
            "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
        );
    }

    #[test]
    fn generated_values_are_unique_and_url_safe() {
        let a = generate().unwrap();
        let b = generate().unwrap();
        assert_ne!(a.verifier, b.verifier);
        assert_eq!(a.verifier.len(), 43);
        assert_eq!(a.challenge, challenge_for(&a.verifier));
        assert!(
            a.verifier
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        );
    }
}
