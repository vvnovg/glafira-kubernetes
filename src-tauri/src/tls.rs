//! Криптопровайдер rustls один на процесс: `kube` и HTTP-клиент входа используют ring.

/// Ставит ring провайдером по умолчанию, если он ещё не выбран. Повторный вызов безвреден.
pub fn ensure_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}
