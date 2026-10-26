import { useQuery } from "@tanstack/react-query";
import { useTranslation } from "react-i18next";
import { api } from "../../api/client";
import { useRefetchInterval } from "../../lib/tabActive";
import { PartView } from "../common/PartView";
import { ReadError } from "../common/ReadError";
import { RefreshControls, useRefreshInterval } from "../common/RefreshControls";
import { isNearLimit } from "./quota";

/** Квоты проекта (спец. §6.3): ResourceQuota — использовано и лимит (строки от порога `quota.nearLimit` выделены), LimitRange — границы и значения по умолчанию. */
export function QuotasView({ connectionId, namespace }: { connectionId: string; namespace: string }) {
  const { t } = useTranslation();
  const [interval, setInterval] = useRefreshInterval();
  const refetchInterval = useRefetchInterval(interval);
  const q = useQuery({ queryKey: ["quotas", connectionId, namespace], queryFn: () => api.getQuotas(connectionId, namespace), refetchInterval });
  return (
    <div className="section">
      <RefreshControls interval={interval} onInterval={setInterval} onRefresh={() => void q.refetch()} busy={q.isFetching} />
      {q.isPending && <p className="muted">{t("common.loading")}</p>}
      {q.error && <ReadError error={q.error} connectionId={connectionId} onRetry={() => void q.refetch()} />}
      {q.data && (
        <>
          <h3>ResourceQuota</h3>
          <PartView part={q.data.quotas}>
            {(quotas) =>
              quotas.length === 0 ? (
                <p className="muted">—</p>
              ) : (
                quotas.map((quota) => (
                  <div key={quota.name} className="section">
                    <h4>
                      {quota.name}
                      {quota.scopes.length > 0 && <span className="muted"> ({quota.scopes.join(", ")})</span>}
                    </h4>
                    <table>
                      <thead>
                        <tr>
                          <th>{t("project.quotas.resource")}</th>
                          <th>{t("project.quotas.used")}</th>
                          <th>{t("project.quotas.hard")}</th>
                          <th>%</th>
                        </tr>
                      </thead>
                      <tbody>
                        {quota.entries.map((e) => (
                          <tr key={e.resource} className={isNearLimit(e.ratio) ? "warn" : undefined}>
                            <td>
                              <code>{e.resource}</code>
                            </td>
                            <td>{e.used ?? "—"}</td>
                            <td>{e.hard}</td>
                            <td>{e.ratio === null ? "—" : Math.round(e.ratio * 100)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                ))
              )
            }
          </PartView>
          <h3>LimitRange</h3>
          <PartView part={q.data.limitRanges}>
            {(ranges) =>
              ranges.length === 0 ? (
                <p className="muted">—</p>
              ) : (
                <table>
                  <thead>
                    <tr>
                      <th>{t("project.network.name")}</th>
                      <th>{t("project.network.type")}</th>
                      <th>{t("project.quotas.resource")}</th>
                      <th>min</th>
                      <th>max</th>
                      <th>default</th>
                      <th>defaultRequest</th>
                      <th>maxLimitRequestRatio</th>
                    </tr>
                  </thead>
                  <tbody>
                    {ranges.flatMap((r) =>
                      r.limits.map((l) => (
                        <tr key={`${r.name}/${l.type}/${l.resource}`}>
                          <td>{r.name}</td>
                          <td>{l.type}</td>
                          <td>{l.resource}</td>
                          <td>{l.min ?? "—"}</td>
                          <td>{l.max ?? "—"}</td>
                          <td>{l.default ?? "—"}</td>
                          <td>{l.defaultRequest ?? "—"}</td>
                          <td>{l.maxLimitRequestRatio ?? "—"}</td>
                        </tr>
                      )),
                    )}
                  </tbody>
                </table>
              )
            }
          </PartView>
        </>
      )}
    </div>
  );
}
