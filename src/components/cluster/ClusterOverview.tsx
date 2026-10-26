import { useQuery } from "@tanstack/react-query";
import { openUrl } from "@tauri-apps/plugin-opener";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { api } from "../../api/client";
import type { ClusterOverview, OpenShiftOverview, OperatorRow, Part } from "../../api/types";
import { tr } from "../../i18n/text";
import { Hint } from "../common/Hint";
import { PartView } from "../common/PartView";
import { ReadError } from "../common/ReadError";
import { RefreshControls, useRefreshInterval } from "../common/RefreshControls";

/** Какие части обзора скрыты правами (у них ошибка FORBIDDEN). */
function hiddenByRights(o: ClusterOverview): boolean {
  const parts: (Part<unknown> | null | undefined)[] = [o.nodes, o.ingressClasses, o.gatewayClasses, o.projects];
  const os = o.platformDetails;
  if (os) parts.push(os.clusterVersion, os.infrastructure, os.consoleUrl, os.operators, os.machineConfigPools);
  return parts.some((p) => p?.error?.code === "FORBIDDEN");
}

const flag = (v: boolean | null) => (v === true ? "✅" : v === false ? "⚠" : "—");

/** Обзор кластера (спец. §6.2): API-сервер, ноды, проекты, классы Ingress и Gateway, у OpenShift — обновления, операторы, пулы машин. */
export function ClusterOverviewView({ connectionId }: { connectionId: string }) {
  const { t } = useTranslation();
  const [interval, setInterval] = useRefreshInterval();
  const q = useQuery({
    queryKey: ["overview", connectionId],
    queryFn: () => api.getClusterOverview(connectionId),
    refetchInterval: interval || false,
  });
  const o = q.data;
  return (
    <section className="section">
      <div className="toolbar">
        <h3>{t("cluster.overview.title")}</h3>
        <RefreshControls interval={interval} onInterval={setInterval} onRefresh={() => void q.refetch()} busy={q.isFetching} />
      </div>
      {q.isPending && <p className="muted">{t("common.loading")}</p>}
      {q.error && <ReadError error={q.error} connectionId={connectionId} onRetry={() => void q.refetch()} />}
      {o && (
        <>
          <dl className="stats">
            <div>
              <dt>
                <span className="field-label">
                  {t("cluster.overview.apiServer")}
                  <Hint k="hints.cluster.readyz" label={t("cluster.overview.apiServer")} />
                </span>
              </dt>
              <dd>{o.readyz.ok ? `✅ ${t("cluster.overview.ready")}` : `⚠ ${t("cluster.overview.notReady")}`}</dd>
            </div>
            <div>
              <dt>{t("cluster.overview.nodes")}</dt>
              <dd>
                <PartView part={o.nodes}>{(n) => `${n.ready} / ${n.total}`}</PartView>
              </dd>
            </div>
            <div>
              <dt>{t("cluster.overview.projects")}</dt>
              <dd>
                <PartView part={o.projects}>{(p) => `${p.count}${p.more ? "+" : ""}`}</PartView>
              </dd>
            </div>
          </dl>
          {!o.readyz.ok && o.readyz.failed.length > 0 && (
            <p className="warn">
              {t("cluster.overview.failedChecks")}: <code>{o.readyz.failed.join(", ")}</code>
            </p>
          )}
          {o.readyz.error && <p className="muted">— {tr(o.readyz.error)}</p>}
          {o.nodes.data && o.nodes.data.roles.length > 0 && (
            <p>
              {t("cluster.overview.byRole")}: {o.nodes.data.roles.map((r) => `${r.role} ${r.ready}/${r.total}`).join(", ")}
            </p>
          )}
          <h4>{t("cluster.overview.ingressClasses")}</h4>
          <PartView part={o.ingressClasses}>
            {(list) =>
              list.length === 0 ? (
                <p className="muted">—</p>
              ) : (
                <ul>
                  {list.map((c) => (
                    <li key={c.name}>
                      <code>{c.name}</code> {c.default && <strong>{t("cluster.overview.defaultClass")}</strong>}{" "}
                      <span className="muted">{c.controller ?? ""}</span>
                    </li>
                  ))}
                </ul>
              )
            }
          </PartView>
          {o.gatewayClasses && (
            <>
              <h4>{t("cluster.overview.gatewayClasses")}</h4>
              <PartView part={o.gatewayClasses}>
                {(list) =>
                  list.length === 0 ? (
                    <p className="muted">—</p>
                  ) : (
                    <ul>
                      {list.map((c) => (
                        <li key={c.name}>
                          {flag(c.accepted)} <code>{c.name}</code> <span className="muted">{c.controller ?? ""}</span>
                        </li>
                      ))}
                    </ul>
                  )
                }
              </PartView>
            </>
          )}
          {o.platformDetails && <OpenShiftSection connectionId={connectionId} os={o.platformDetails} />}
          {hiddenByRights(o) && <p className="muted">{t("cluster.overview.hiddenByRights")}</p>}
        </>
      )}
    </section>
  );
}

function OperatorList({ rows }: { rows: OperatorRow[] }) {
  const { t } = useTranslation();
  return (
    <table>
      <thead>
        <tr>
          <th>{t("cluster.operators.name")}</th>
          <th>{t("cluster.operators.available")}</th>
          <th>{t("cluster.operators.progressing")}</th>
          <th>{t("cluster.operators.degraded")}</th>
          <th>{t("cluster.operators.message")}</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((r) => (
          <tr key={r.name}>
            <td>
              <code>{r.name}</code> <span className="muted">{r.version ?? ""}</span>
            </td>
            <td>{flag(r.available)}</td>
            <td>{r.progressing ? "⏳" : "—"}</td>
            <td className={r.degraded ? "problem" : undefined}>{r.degraded ? "⚠" : "—"}</td>
            <td>{r.message ?? ""}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

/** Часть обзора OpenShift: версия и обновления, инфраструктура, консоль, операторы и пулы машин не в норме. */
function OpenShiftSection({ connectionId, os }: { connectionId: string; os: OpenShiftOverview }) {
  const { t } = useTranslation();
  const [all, setAll] = useState(false);
  const operators = useQuery({ queryKey: ["clusterOperators", connectionId], queryFn: () => api.listClusterOperators(connectionId), enabled: all });
  return (
    <>
      <h4>OpenShift</h4>
      <PartView part={os.clusterVersion}>
        {(cv) => (
          <dl className="facts">
            <dt>{t("cluster.overview.version")}</dt>
            <dd>{cv.version ?? "—"}</dd>
            <dt>{t("cluster.overview.channel")}</dt>
            <dd>{cv.channel ?? "—"}</dd>
            <dt>{t("cluster.overview.updates")}</dt>
            <dd>{cv.availableUpdates}</dd>
            {cv.failing?.status === "True" && (
              <>
                <dt className="problem">{t("cluster.overview.failing")}</dt>
                <dd>{cv.failing.message ?? cv.failing.reason ?? ""}</dd>
              </>
            )}
            {cv.progressing?.status === "True" && (
              <>
                <dt>{t("cluster.overview.progressing")}</dt>
                <dd>{cv.progressing.message ?? ""}</dd>
              </>
            )}
          </dl>
        )}
      </PartView>
      <dl className="facts">
        <dt>{t("cluster.overview.infrastructure")}</dt>
        <dd>
          <PartView part={os.infrastructure}>{(infrastructure) => infrastructure ?? "—"}</PartView>
        </dd>
        <dt>{t("cluster.overview.console")}</dt>
        <dd>
          <PartView part={os.consoleUrl}>
            {(url) =>
              url ? (
                <button type="button" className="link" onClick={() => void openUrl(url)}>
                  {url}
                </button>
              ) : (
                "—"
              )
            }
          </PartView>
        </dd>
      </dl>
      <h4>{t("cluster.overview.operators")}</h4>
      <PartView part={os.operators}>
        {(ops) => (
          <>
            <p>
              {ops.problems.length === 0
                ? t("cluster.overview.operatorsOk", { total: ops.total })
                : t("cluster.overview.operatorsProblems", { bad: ops.problems.length, total: ops.total })}{" "}
              <button type="button" className="link" onClick={() => setAll((v) => !v)}>
                {all ? t("cluster.overview.operatorsProblemsOnly") : t("cluster.overview.operatorsAll")}
              </button>
            </p>
            {all ? (
              operators.data ? <OperatorList rows={operators.data} /> : operators.error ? <ReadError error={operators.error} connectionId={connectionId} /> : null
            ) : (
              ops.problems.length > 0 && <OperatorList rows={ops.problems} />
            )}
          </>
        )}
      </PartView>
      <h4>{t("cluster.overview.machineConfigPools")}</h4>
      <PartView part={os.machineConfigPools}>
        {(pools) => (
          <ul>
            {pools.map((p) => (
              <li key={p.name} className={p.degraded ? "problem" : undefined}>
                <code>{p.name}</code> {p.updated}/{p.machines} {t("cluster.overview.machinesUpdated")}
                {p.updating && ` · ${t("cluster.overview.updating")}`}
                {p.degraded && ` · ${t("cluster.overview.degraded", { count: p.degradedMachines })}`}
              </li>
            ))}
          </ul>
        )}
      </PartView>
    </>
  );
}
