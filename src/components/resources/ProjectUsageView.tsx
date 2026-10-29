import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { api } from "../../api/client";
import type { AppError, ProjectUsage } from "../../api/types";
import { currentLocale } from "../../i18n";
import { formatAgo } from "../../lib/formatAgo";
import { cpuText, memoryText } from "../../lib/quantity";
import { useWorkspace } from "../../state/workspace";
import { KindErrors } from "../project/KindErrors";
import { quotaAmount } from "../change/changes";
import { MAX_ROWS, type Sort, excessText, projectRows, riskText } from "./projectRows";
import { DEFAULT_WINDOW, sourceLabel } from "./recommend";
import { ResourcesError, SourceReasons } from "./SourceReasons";
import { WindowSelect } from "./WorkloadResources";

type State = { status: "loading" } | { status: "done"; usage: ProjectUsage } | { status: "failed"; error: AppError; at: number };

/**
 * Сбор по проекту — фоновая сессия `get_project_usage` (дедлайн 45 с, кэш 5 минут в Rust); закрытие вкладки или
 * новый запуск останавливает прежнюю. `nonce` — «Обновить»: собрать заново, мимо кэша.
 */
function useProjectUsage(connectionId: string, namespace: string, days: number, nonce: number): State {
  const [state, setState] = useState<State>({ status: "loading" });
  useEffect(() => {
    let stopped = false;
    let sessionId: string | null = null;
    setState({ status: "loading" });
    api
      .startProjectUsage(connectionId, namespace, days, nonce > 0, (e) => {
        if (stopped) return;
        setState(e.kind === "done" ? { status: "done", usage: e.usage } : { status: "failed", error: e.error, at: Date.now() });
      })
      .then((id) => {
        if (stopped) api.stopSession(id).catch(() => undefined);
        else sessionId = id;
      })
      .catch((error: AppError) => !stopped && setState({ status: "failed", error, at: Date.now() }));
    return () => {
      stopped = true;
      if (sessionId) api.stopSession(sessionId).catch(() => undefined);
    };
  }, [connectionId, namespace, days, nonce]);
  return state;
}

/**
 * «Ресурсы» проекта (спец. §7.4): суммарные requests и limits работающих подов против потребления за окно, резерв,
 * риски и избыток по workload'ам, использование квот; сортировка по риску или резерву, фильтр «только с
 * рекомендациями» (риск или избыток), не больше 100 строк.
 */
export function ProjectUsageView({ connectionId, namespace }: { connectionId: string; namespace: string }) {
  const { t } = useTranslation();
  const open = useWorkspace((s) => s.open);
  const [days, setDays] = useState(DEFAULT_WINDOW);
  const [nonce, setNonce] = useState(0);
  const [sort, setSort] = useState<Sort>("risk");
  const [onlyActionable, setOnlyActionable] = useState(false);
  const [query, setQuery] = useState("");
  const state = useProjectUsage(connectionId, namespace, days, nonce);
  const u = state.status === "done" ? state.usage : null;
  const rows = u ? projectRows(u.workloads, sort, onlyActionable, query) : [];
  return (
    <div className="section">
      <div className="toolbar">
        <WindowSelect days={days} onChange={setDays} disabledReason={state.status === "done" && !state.usage.source.history ? t("resources.currentOnly") : undefined} />
        <select aria-label={t("resources.sort")} value={sort} onChange={(e) => setSort(e.target.value as Sort)}>
          <option value="risk">{t("resources.sortRisk")}</option>
          <option value="reserve">{t("resources.sortReserve")}</option>
        </select>
        <label>
          <input type="checkbox" checked={onlyActionable} onChange={(e) => setOnlyActionable(e.target.checked)} />
          {t("resources.onlyActionable")}
        </label>
        <input aria-label={t("resources.filter")} placeholder={t("resources.filter")} value={query} onChange={(e) => setQuery(e.target.value)} />
        <button type="button" disabled={state.status === "loading"} onClick={() => setNonce((n) => n + 1)}>
          {t("common.refresh")}
        </button>
      </div>
      {state.status === "loading" && <p className="muted">{t("resources.collecting")}</p>}
      {state.status === "failed" && <ResourcesError error={state.error} connectionId={connectionId} at={state.at} onRetry={() => setNonce((n) => n + 1)} />}
      {u && (
        <>
          <p className="muted">
            {t("resources.source", { source: sourceLabel(u.source) })} · {u.source.history ? t("resources.days", { count: u.windowDays }) : t("resources.currentOnly")} ·{" "}
            {t("resources.collected", { when: formatAgo(u.collectedAtMs, Date.now(), currentLocale()) })}
          </p>
          <SourceReasons rejected={u.sourceStatus.rejected} />
          {u.truncated && <p className="muted">{t("resources.truncated")}</p>}
          <KindErrors errors={u.errors} />
          {u.quotas.length > 0 && (
            <p>
              {t("resources.quotas")}{" "}
              {u.quotas
                .map((q) => `${q.quota} ${q.resource}: ${quotaAmount(q.resource, q.used)} / ${quotaAmount(q.resource, q.hard)} (${q.hard > 0 ? Math.round((q.used / q.hard) * 100) : 0} %)`)
                .join("; ")}
            </p>
          )}
          {rows.length === 0 ? (
            <p className="empty">{t("resources.noWorkloads")}</p>
          ) : (
            <table className="resource-table">
              <thead>
                <tr>
                  <th>{t("resources.col.workload")}</th>
                  <th className="num">{t("resources.col.pods")}</th>
                  <th className="num">{t("resources.col.requests")}</th>
                  <th className="num">{t("resources.col.limits")}</th>
                  <th className="num">{t("resources.col.usage")}</th>
                  <th className="num">{t("resources.col.reserve")}</th>
                  <th>{t("resources.col.risks")}</th>
                </tr>
              </thead>
              <tbody>
                {rows.slice(0, MAX_ROWS).map((w) => (
                  <tr key={`${w.kind}/${w.name}`}>
                    <td>
                      <span className="muted">{w.kind}</span>{" "}
                      <button type="button" className="link" onClick={() => open({ kind: "workload", connectionId, namespace, workloadKind: w.kind, name: w.name })}>
                        {w.name}
                      </button>
                    </td>
                    <td className="num">{w.pods}</td>
                    <td className="num">
                      {cpuText(w.requests.cpu)} / {memoryText(w.requests.memory)}
                    </td>
                    <td className="num">
                      {cpuText(w.limits.cpu)} / {memoryText(w.limits.memory)}
                    </td>
                    <td className="num">
                      {cpuText(w.usage.cpu)} / {memoryText(w.usage.memory)}
                    </td>
                    <td className="num">
                      {cpuText(w.reserve.cpu)} / {memoryText(w.reserve.memory)}
                    </td>
                    <td>
                      {w.risks.map((r) => (
                        <div key={JSON.stringify(r)} className="risk">
                          {riskText(r, t)}
                        </div>
                      ))}
                      {w.excess.map((e) => (
                        <div key={JSON.stringify(e)}>{excessText(e, t)}</div>
                      ))}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
          {rows.length > MAX_ROWS && <p className="muted">{t("resources.moreRows", { shown: MAX_ROWS, total: rows.length })}</p>}
        </>
      )}
    </div>
  );
}
