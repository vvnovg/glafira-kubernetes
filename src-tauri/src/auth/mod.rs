//! Вход через браузер в OAuth OpenShift (спец. §5.2): Authorization Code + PKCE, как `oc login --web`.

pub mod callback;
pub mod exec;
pub mod oauth;
pub mod pkce;
