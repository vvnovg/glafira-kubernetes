import type { AppError, ConnectionProfile } from "../api/types";

/** Ошибки, которые лечатся входом заново или новым токеном. */
export const SIGN_IN_KEYS = new Set([
  "errors.connections.tokenMissing",
  "errors.connections.loginRequired",
  "errors.connections.oidcLoginRequired",
  "errors.connections.clientKeyMissing",
]);

/**
 * Помогает ли войти заново: истёкшая сессия, нет токена или ключа, ошибка exec-плагина (если
 * профиль известен и вход у него — exec-плагином).
 */
export function needsSignIn(e: AppError, profile?: ConnectionProfile): boolean {
  if (e.code === "SESSION_EXPIRED" || SIGN_IN_KEYS.has(e.key ?? "")) return true;
  const exec = profile ? profile.auth.method === "exec" : e.key?.startsWith("errors.exec.");
  return Boolean(exec) && (e.key?.startsWith("errors.exec.") || e.key === "errors.oauth.cancelled");
}
