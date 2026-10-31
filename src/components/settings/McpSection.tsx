import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { api } from "../../api/client";
import type { McpAuditEntry, McpServerState, McpStatus } from "../../api/types";
import { tr } from "../../i18n/text";
import { formatCount, formatTimestamp } from "../../lib/format";
import { MCP_STATUS_KEY, useMcpStatus } from "../../lib/useMcpStatus";
import { ErrorBanner } from "../common/ErrorBanner";
import { Hint } from "../common/Hint";

const STATE_KEYS: Record<McpServerState, string> = {
  off: "settings.mcp.state.off",
  running: "settings.mcp.state.running",
  otherInstance: "settings.mcp.state.otherInstance",
  error: "settings.mcp.state.error",
  disabledByPolicy: "settings.mcp.state.disabledByPolicy",
};

const EVENT_KEYS: Record<string, string> = {
  sessionStarted: "settings.mcp.audit.sessionStarted",
  sessionEnded: "settings.mcp.audit.sessionEnded",
  approved: "settings.mcp.audit.approved",
  rejected: "settings.mcp.audit.rejected",
  revoked: "settings.mcp.audit.revoked",
  serverStarted: "settings.mcp.audit.serverStarted",
  serverStopped: "settings.mcp.audit.serverStopped",
};

const AUDIT_LIMIT = 200;

function CopyBlock({ id, label, text }: { id: string; label: string; text: string }) {
  const { t } = useTranslation();
  const [copied, setCopied] = useState(false);
  const copy = () => {
    try {
      void navigator.clipboard
        .writeText(text)
        .then(() => {
          setCopied(true);
          setTimeout(() => setCopied(false), 2000);
        })
        .catch(() => {});
    } catch {
      /* буфер обмена недоступен */
    }
  };
  return (
    <div className="mcp-config">
      <div className="toolbar">
        <strong id={id}>{label}</strong>
        <button type="button" aria-describedby={id} onClick={copy}>
          {copied ? t("settings.mcp.copied") : t("settings.mcp.copy")}
        </button>
      </div>
      <pre aria-labelledby={id}>{text}</pre>
    </div>
  );
}

function auditWhat(e: McpAuditEntry, t: (k: string, o?: Record<string, string>) => string): string {
  if (e.event === "call" && e.scanned !== undefined) {
    const counts = { returned: formatCount(e.returned ?? 0), scanned: formatCount(e.scanned), masked: formatCount(e.masked ?? 0) };
    return `${e.tool ?? ""} — ${t("settings.mcp.audit.lines", counts)}`;
  }
  if (e.event === "call") return e.tool ?? "";
  const key = EVENT_KEYS[e.event];
  return key ? t(key) : e.event;
}

/** Что меняется вместе с журналом: состояние сервера, сессии, их записи в журнале и клиенты. */
function activity(s: McpStatus): string {
  const sessions = s.sessions.map((x) => `${x.id}:${x.logged}`).join(",");
  const clients = s.approvedClients.map((c) => c.name).join(",");
  return `${s.state}|${s.enabled}|${sessions}|${clients}|${s.pending.length}`;
}

export function McpSection() {
  const { t } = useTranslation();
  const qc = useQueryClient();
  const status = useMcpStatus();
  const configs = useQuery({ queryKey: ["mcp-configs"], queryFn: api.getMcpClientConfigs, staleTime: Infinity });
  const audit = useQuery({ queryKey: ["mcp-audit"], queryFn: () => api.getMcpAudit(AUDIT_LIMIT) });
  const apply = (s: McpStatus) => qc.setQueryData(MCP_STATUS_KEY, s);
  const toggle = useMutation({ mutationFn: (enabled: boolean) => api.setMcpEnabled(enabled), onSuccess: apply });
  const refreshAudit = () => void qc.invalidateQueries({ queryKey: ["mcp-audit"] });
  const revoke = useMutation({
    mutationFn: (name: string) => api.revokeMcpClient(name),
    onSuccess: (s) => {
      apply(s);
      refreshAudit();
    },
  });
  const disconnect = useMutation({
    mutationFn: (sessionId: string | null) => api.disconnectMcpSessions(sessionId),
    onSuccess: (s) => {
      apply(s);
      refreshAudit();
    },
  });
  const reveal = useMutation({ mutationFn: () => api.revealMcpAudit() });
  // Журнал перечитывается сам, пока раздел открыт: статус приходит событиями из Rust, и его
  // изменение (записанный вызов или отказ — счётчик `logged`, сессия, клиент) значит новые записи журнала.
  const seen = useRef<string | undefined>(undefined);
  const now = status.data ? activity(status.data) : undefined;
  useEffect(() => {
    if (now === undefined) return;
    if (seen.current !== undefined && seen.current !== now) void qc.invalidateQueries({ queryKey: ["mcp-audit"] });
    seen.current = now;
  }, [now, qc]);

  if (status.error) return <ErrorBanner error={status.error} onRetry={() => status.refetch()} />;
  const s = status.data;
  if (!s) return <p className="empty">{t("common.loading")}</p>;
  const error = toggle.error ?? revoke.error ?? disconnect.error ?? reveal.error;

  return (
    <section aria-labelledby="mcp-title">
      <h3 id="mcp-title">{t("settings.mcp.section")}</h3>
      <p className="hint">{t("settings.mcp.intro")}</p>
      <div className="form">
        <span className="field-label">
          <label htmlFor="mcp-enabled">{t("settings.mcp.enable")}</label>
          <Hint k="hints.settings.mcpEnable" label={t("settings.mcp.enable")} />
        </span>
        <span>
          <input
            id="mcp-enabled"
            type="checkbox"
            checked={s.enabled && s.policyAllows}
            disabled={!s.policyAllows || toggle.isPending}
            onChange={(e) => toggle.mutate(e.target.checked)}
          />
        </span>
      </div>
      {!s.policyAllows && <p className="hint">{t("settings.mcp.policy")}</p>}
      <p role="status">
        {t(STATE_KEYS[s.state])}
        {s.error && <> — {tr(s.error)}</>}
      </p>
      {error && <ErrorBanner error={error} />}

      {s.policyAllows && configs.error && <ErrorBanner error={configs.error} onRetry={() => configs.refetch()} />}
      {s.policyAllows && configs.data && (
        <>
          <h4>{t("settings.mcp.connectTitle")}</h4>
          <p className="hint">{t("settings.mcp.connectHint")}</p>
          <CopyBlock id="mcp-cfg-desktop" label={t("settings.mcp.claudeDesktop")} text={configs.data.claudeDesktop} />
          <CopyBlock id="mcp-cfg-code" label={t("settings.mcp.claudeCode")} text={configs.data.claudeCode} />
          <CopyBlock id="mcp-cfg-other" label={t("settings.mcp.otherClient")} text={configs.data.otherClient} />
        </>
      )}

      <h4>{t("settings.mcp.sessions")}</h4>
      {s.sessions.length === 0 ? (
        <p className="muted">{t("settings.mcp.noSessions")}</p>
      ) : (
        <>
          <table>
            <thead>
              <tr>
                <th>{t("settings.mcp.colClient")}</th>
                <th>{t("settings.mcp.colSince")}</th>
                <th>{t("settings.mcp.colCalls")}</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {s.sessions.map((x) => (
                <tr key={x.id}>
                  <td>
                    {x.clientName ? `${x.clientName} ${x.clientVersion ?? ""}` : t("mcp.unknownClient")}
                    {!x.approved && <span className="muted"> · {t("settings.mcp.waiting")}</span>}
                  </td>
                  <td>{formatTimestamp(x.startedMs)}</td>
                  <td>{x.calls}</td>
                  <td>
                    <button type="button" disabled={disconnect.isPending} onClick={() => disconnect.mutate(x.id)}>
                      {t("settings.mcp.disconnect")}
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          <button type="button" disabled={disconnect.isPending} onClick={() => disconnect.mutate(null)}>
            {t("settings.mcp.disconnectAll")}
          </button>
        </>
      )}

      <h4>{t("settings.mcp.clients")}</h4>
      {s.approvedClients.length === 0 ? (
        <p className="muted">{t("settings.mcp.noClients")}</p>
      ) : (
        <table>
          <thead>
            <tr>
              <th>{t("settings.mcp.colClient")}</th>
              <th>{t("settings.mcp.colFirstSeen")}</th>
              <th>{t("settings.mcp.colLastSeen")}</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {s.approvedClients.map((c) => (
              <tr key={c.name}>
                <td>
                  {c.name} {c.version}
                </td>
                <td>{formatTimestamp(c.firstSeenMs)}</td>
                <td>{formatTimestamp(c.lastSeenMs)}</td>
                <td>
                  <button type="button" disabled={revoke.isPending} onClick={() => revoke.mutate(c.name)}>
                    {t("settings.mcp.revoke")}
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      <div className="toolbar">
        <h4>{t("settings.mcp.audit.title")}</h4>
        <button type="button" onClick={() => audit.refetch()}>
          {t("settings.mcp.audit.refresh")}
        </button>
        <button type="button" onClick={() => reveal.mutate()}>
          {t("settings.mcp.audit.reveal")}
        </button>
      </div>
      {audit.error && <ErrorBanner error={audit.error} onRetry={() => audit.refetch()} />}
      {audit.data && audit.data.length === 0 && <p className="muted">{t("settings.mcp.audit.empty")}</p>}
      {audit.data && audit.data.length > 0 && (
        <table>
          <thead>
            <tr>
              <th>{t("settings.mcp.audit.colTime")}</th>
              <th>{t("settings.mcp.colClient")}</th>
              <th>{t("settings.mcp.audit.colWhat")}</th>
              <th>{t("settings.mcp.audit.colConnection")}</th>
              <th>{t("settings.mcp.audit.colResult")}</th>
              <th>{t("settings.mcp.audit.colMs")}</th>
            </tr>
          </thead>
          <tbody>
            {audit.data.map((e, i) => (
              <tr key={`${e.ts}-${i}`}>
                <td>{formatTimestamp(e.ts)}</td>
                <td>{e.client ?? ""}</td>
                <td>{auditWhat(e, t)}</td>
                <td>{e.connection?.name ?? ""}</td>
                <td>{e.outcome === "error" ? e.code ?? t("settings.mcp.audit.error") : e.outcome === "ok" ? t("settings.mcp.audit.ok") : ""}</td>
                <td>{e.ms ?? ""}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </section>
  );
}
