import { useIsMutating, useQuery } from "@tanstack/react-query";
import { useTranslation } from "react-i18next";
import { api } from "../../api/client";
import type { CheckRun } from "../../api/types";
import { currentLocale } from "../../i18n";
import { ENV_LABELS } from "../../lib/env";
import { formatAgo } from "../../lib/formatAgo";
import { healthCheckKey, useHealth, useHealthActions } from "../../lib/useHealth";
import { useWorkspace } from "../../state/workspace";
import { ErrorBanner } from "../common/ErrorBanner";
import { Hint } from "../common/Hint";
import { SeverityMark } from "./parts";
import { findingsOf, SEVERITIES } from "./findings";

function useWho() {
  const { t } = useTranslation();
  return (run: CheckRun) =>
    run.trigger.kind === "ui" ? t("health.byUi") : (run.trigger.client ?? t("mcp.unknownClient"));
}

/**
 * Кнопка «Проверить кластер» и итог последней проверки: время, кто, число находок по строгости, что проверено.
 * `allLink` — ссылка «Все замечания» на вкладку замечаний (на обзоре кластера; на самой вкладке не нужна).
 * «Проверка…» общая для всех CheckBar подключения (обзор и вкладка «Замечания»): идущую мутацию
 * видно по её ключу, а не по локальному `isPending`. Проверка, сделанная под другим окружением,
 * чем у подключения сейчас, помечается: её строгость считалась иначе.
 */
export function CheckBar({ connectionId, allLink = false }: { connectionId: string; allLink?: boolean }) {
  const { t } = useTranslation();
  const open = useWorkspace((s) => s.open);
  const health = useHealth(connectionId);
  const connections = useQuery({ queryKey: ["connections"], queryFn: api.listConnections });
  const connection = connections.data?.find((c) => c.id === connectionId);
  const { run } = useHealthActions(connectionId);
  const checking = useIsMutating({ mutationKey: healthCheckKey(connectionId) }) > 0;
  const who = useWho();
  const last = health.data?.lastCheck ?? null;
  const visible = findingsOf(health.data, false);
  const otherEnv = last !== null && connection !== undefined && last.environment !== connection.environment;
  return (
    <div className="check-bar">
      <div className="check-bar-row">
        <button type="button" disabled={checking} onClick={() => run.mutate()}>
          {checking ? t("health.checking") : t("health.check")}
        </button>
        <Hint k="hints.health.check" label={t("health.check")} />
        {last ? (
          <span>
            {t("health.last", { when: formatAgo(last.startedAtMs, Date.now(), currentLocale()), who: who(last) })}
            {SEVERITIES.map((s) => {
              const n = visible.filter((f) => f.severity === s).length;
              return n > 0 ? (
                <span key={s} className="check-bar-count">
                  <SeverityMark severity={s} count={n} />
                </span>
              ) : null;
            })}
            {allLink && (
              <>
                {" · "}
                <button type="button" className="link" onClick={() => open({ kind: "findings", connectionId })}>
                  {t("health.allFindings")}
                </button>
              </>
            )}
            {last.incomplete && <span className="warn"> · {t("health.partial")}</span>}
            {otherEnv && <span className="warn"> · {t("health.checkedAs", { env: ENV_LABELS[last.environment] })}</span>}
          </span>
        ) : (
          !health.isPending && <span className="muted">{t("health.never")}</span>
        )}
      </div>
      {last && (
        <p className="muted">
          {t("health.scope", {
            checked: last.scope.projects.length,
            total: last.scope.projectsTotal,
            pods: last.scope.pods,
            nodes: last.scope.nodes ?? t("health.nodesUnknown"),
          })}
          {last.scope.requested && <> · {t("health.scopeRequested", { projects: last.scope.requested.join(", ") })}</>}
        </p>
      )}
      {run.error && <ErrorBanner error={run.error} onRetry={() => run.mutate()} />}
      {health.error && <ErrorBanner error={health.error} onRetry={() => health.refetch()} />}
    </div>
  );
}
