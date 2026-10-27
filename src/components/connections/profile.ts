import type { AuthConfig, AuthMethod, CaConfig, CaMode, ConnectionProfile, ExecAuth, ProxyConfig, ProxyMode } from "../../api/types";
import { newExec } from "./exec";

export function newProfile(): ConnectionProfile {
  return {
    id: "",
    name: "",
    color: "#3b82f6",
    environment: "dev",
    readOnly: false,
    apiUrl: "",
    auth: { method: "browser" },
    ca: { mode: "system" },
    tlsServerName: null,
    proxy: { mode: "system" },
    favoriteNamespaces: [],
    manualNamespaces: [],
    requestTimeoutMs: 30000,
    connectTimeoutMs: 10000,
    llmAccess: "off",
    llmNamespaces: null,
  };
}

/** Способ входа; сертификат и exec-плагин сохраняются при переключении туда и обратно. */
export function withAuthMethod(auth: AuthConfig, method: AuthMethod, lastCertificate = "", lastExec: ExecAuth = newExec()): AuthConfig {
  if (method === "clientCert") return { method, certificatePem: auth.method === "clientCert" ? auth.certificatePem : lastCertificate };
  if (method === "exec") return auth.method === "exec" ? auth : { method, ...lastExec };
  return { method };
}

export function withCaMode(ca: CaConfig, mode: CaMode, lastPem = ""): CaConfig {
  if (mode === "pem") return { mode, pem: ca.mode === "pem" ? ca.pem : lastPem };
  return { mode };
}

export function withProxyMode(proxy: ProxyConfig, mode: ProxyMode, lastUrl = ""): ProxyConfig {
  if (mode === "url") return { mode, url: proxy.mode === "url" ? proxy.url : lastUrl };
  return { mode };
}

/** Проекты: через запятую, пробел или с новой строки; пустые и повторы убираются. */
export function parseNamespaces(text: string): string[] {
  const seen = new Set<string>();
  return text
    .split(/[\s,]+/)
    .map((s) => s.trim())
    .filter((s) => s && !seen.has(s) && seen.add(s));
}

export function formatNamespaces(list: string[]): string {
  return list.join(", ");
}
