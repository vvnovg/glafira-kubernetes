//! Защита изменяющих операций (спец. §9, §12): проверяется в Rust, а не только в UI.

use crate::error::{AppResult, ErrorCode};
use crate::storage::profile::{ConnectionProfile, Environment};

/// read-only запрещает любые изменения; prod без read-only требует ввести имя подключения.
pub fn ensure_mutation_allowed(p: &ConnectionProfile, confirm: Option<&str>) -> AppResult<()> {
    if p.read_only {
        return Err(app_err!(
            ErrorCode::ReadOnly,
            "errors.guard.readOnly",
            "Connection “{{name}}” is read-only",
            name = p.name
        ));
    }
    if p.environment == Environment::Prod && confirm.map(str::trim) != Some(p.name.trim()) {
        return Err(app_err!(
            ErrorCode::ConfirmationRequired,
            "errors.guard.prodConfirm",
            "To change a prod connection, type its name “{{name}}”",
            name = p.name
        ));
    }
    Ok(())
}

/// Удаление пода без контроллера требует ввести имя пода.
pub fn ensure_name_confirmed(expected: &str, typed: Option<&str>) -> AppResult<()> {
    if typed.map(str::trim) == Some(expected.trim()) {
        Ok(())
    } else {
        Err(app_err!(
            ErrorCode::ConfirmationRequired,
            "errors.guard.typeToConfirm",
            "Type “{{expected}}” to confirm",
            expected = expected
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;
    use crate::storage::profile::{Environment, sample_profile};

    #[test]
    fn read_only_blocks_even_with_confirmation() {
        let mut p = sample_profile("a");
        p.read_only = true;
        let e = ensure_mutation_allowed(&p, Some(&p.name.clone())).unwrap_err();
        assert_eq!(e.code, ErrorCode::ReadOnly);
        assert_eq!(e.key, "errors.guard.readOnly");
        assert_eq!(e.params["name"], p.name);
        assert_eq!(e.message, format!("Connection “{}” is read-only", p.name));
    }

    #[test]
    fn prod_requires_connection_name() {
        let mut p = sample_profile("a");
        p.environment = Environment::Prod;
        p.name = "ocp-prod".into();
        for confirm in [None, Some(""), Some("ocp-pro"), Some("OCP-PROD")] {
            let e = ensure_mutation_allowed(&p, confirm).unwrap_err();
            assert_eq!(e.code, ErrorCode::ConfirmationRequired, "{confirm:?}");
            assert_eq!(e.key, "errors.guard.prodConfirm");
            assert_eq!(
                e.message,
                "To change a prod connection, type its name “ocp-prod”"
            );
        }
        assert!(ensure_mutation_allowed(&p, Some("ocp-prod")).is_ok());
        assert!(ensure_mutation_allowed(&p, Some(" ocp-prod ")).is_ok());
    }

    #[test]
    fn other_environments_need_no_confirmation() {
        for env in [
            Environment::Local,
            Environment::Dev,
            Environment::Test,
            Environment::Stage,
        ] {
            let mut p = sample_profile("a");
            p.environment = env;
            assert!(ensure_mutation_allowed(&p, None).is_ok(), "{env:?}");
        }
    }

    #[test]
    fn object_name_must_match_exactly() {
        assert!(ensure_name_confirmed("web-1", Some("web-1")).is_ok());
        assert!(ensure_name_confirmed("web-1", Some(" web-1 ")).is_ok());
        for typed in [None, Some("web"), Some("WEB-1")] {
            let e = ensure_name_confirmed("web-1", typed).unwrap_err();
            assert_eq!(e.code, ErrorCode::ConfirmationRequired, "{typed:?}");
            assert_eq!(e.key, "errors.guard.typeToConfirm");
            assert_eq!(e.message, "Type “web-1” to confirm");
        }
    }
}
