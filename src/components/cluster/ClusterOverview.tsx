import { useQuery } from "@tanstack/react-query";
import { openUrl } from "@tauri-apps/plugin-opener";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { api } from "../../api/client";
import type { ClusterOverview, ComponentRow, DropAppOverview, OpenShiftOverview, OperatorRow, Part } from "../../api/types";
import { useRefetchInterval } from "../../lib/tabActive";
import { Hint } from "../common/Hint";
import { CheckBar } from "../health/CheckBar";
import { matchCluster } from "../health/findings";
import { HealthBlock } from "../health/HealthBlock";
import { PartView, Unavailable } from "../common/PartView";
import { ReadError } from "../common/ReadError";
import { RefreshControls, useRefreshInterval } from "../common/RefreshControls";

/** Какие части обзора скрыты правами (у них ошибка FORBIDDEN). */
function hiddenByRights(o: ClusterOverview): boolean {
  const parts: (Part<unknown> | null | undefined)[] = [o.readyz, o.nodes, o.ingressClasses, o.gatewayClasses, o.projects];
  const d = o.platformDetails;
  if (d?.kind === "openshift") parts.push(d.clusterVersion, d.infrastructure, d.consoleUrl, d.operators, d.machineConfigPools);
  if (d?.kind === "dropapp") parts.push(d.metadata, d.components, d.nodes, d.ingressTls);
  return parts.some((p) => p?.error?.code === "FORBIDDEN");
}

/**
 * Адрес консоли из ответа кластера нельзя отдавать системе как есть: `mailto:` и `tel:` открывают чужие программы.
 * `link` — только http(s); `text` — разобранный адрес другой схемы показывается текстом; `none` — адрес не разобрать.
 */
function consoleTarget(raw: string): "link" | "text" | "none" {
  try {
    const { protocol } = new URL(raw);
    return protocol === "http:" || protocol === "https:" ? "link" : "text";
  } catch {
    return "none";
  }
}

/** Открывает адрес в браузере; отказ системы (запрет возможностей приложения) не должен остаться необработанным отказом промиса. */
async function openConsole(url: string): Promise<void> {
  try {
    await openUrl(url);
  } catch {
    // Адрес остаётся на экране текстом кнопки: его можно скопировать.
  }
}

const flag = (v: boolean | null) => (v === true ? "✅" : v === false ? "⚠" : "—");

/**
 * Обзор кластера (спец. §6.2): API-сервер, ноды, проекты, классы Ingress и Gateway, у OpenShift — обновления, операторы,
 * пулы машин, у DropApp — версия, компоненты, узлы DropApp и сертификаты ingress.
 */
export function ClusterOverviewView({ connectionId }: { connectionId: string }) {
  const { t } = useTranslation();
  const [interval, setRefreshInterval] = useRefreshInterval();
  const refetchInterval = useRefetchInterval(interval);
  const q = useQuery({
    queryKey: ["overview", connectionId],
    queryFn: () => api.getClusterOverview(connectionId),
    refetchInterval,
  });
  const o = q.data;
  return (
    <section className="section">
      <div className="toolbar">
        <h3>{t("cluster.overview.title")}</h3>
        <RefreshControls interval={interval} onInterval={setRefreshInterval} onRefresh={() => void q.refetch()} busy={q.isFetching} />
      </div>
      <CheckBar connectionId={connectionId} allLink />
      <HealthBlock connectionId={connectionId} match={matchCluster} showTarget />
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
              {/* Нет ответа от /readyz — не «не готов»: состояние неизвестно, причина ниже. */}
              <dd>{o.readyz.error ? "—" : o.readyz.ok ? `✅ ${t("cluster.overview.ready")}` : `⚠ ${t("cluster.overview.notReady")}`}</dd>
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
          {!o.readyz.error && !o.readyz.ok && o.readyz.failed.length > 0 && (
            <p className="warn">
              {t("cluster.overview.failedChecks")}: <code>{o.readyz.failed.join(", ")}</code>
            </p>
          )}
          {o.readyz.error && <Unavailable error={o.readyz.error} />}
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
          {o.platformDetails?.kind === "openshift" && <OpenShiftSection connectionId={connectionId} os={o.platformDetails} />}
          {o.platformDetails?.kind === "dropapp" && <DropAppSection d={o.platformDetails} />}
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
              !url || consoleTarget(url) === "none" ? (
                "—"
              ) : consoleTarget(url) === "link" ? (
                <button type="button" className="link" onClick={() => void openConsole(url)}>
                  {url}
                </button>
              ) : (
                url
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
              operators.data ? (
                <OperatorList rows={operators.data} />
              ) : operators.error ? (
                <ReadError error={operators.error} connectionId={connectionId} onRetry={() => void operators.refetch()} />
              ) : (
                <p className="muted">{t("common.loading")}</p>
              )
            ) : (
              ops.problems.length > 0 && <OperatorList rows={ops.problems} />
            )}
          </>
        )}
      </PartView>
      <h4>{t("cluster.overview.machineConfigPools")}</h4>
      {/* Пулов нет (ROSA HCP: машинами управляет внешняя плоскость управления) — «—». */}
      <PartView part={os.machineConfigPools}>
        {(pools) =>
          pools.length === 0 ? (
            <p className="muted">—</p>
          ) : (
            <ul>
              {pools.map((p) => (
                <li key={p.name} className={p.degraded ? "problem" : undefined}>
                  <code>{p.name}</code> {p.updated}/{p.machines} {t("cluster.overview.machinesUpdated")}
                  {p.updating && ` · ${t("cluster.overview.updating")}`}
                  {p.degraded && ` · ${t("cluster.overview.degraded", { count: p.degradedMachines })}`}
                </li>
              ))}
            </ul>
          )
        }
      </PartView>
    </>
  );
}

function ComponentList({ rows }: { rows: ComponentRow[] }) {
  const { t } = useTranslation();
  return (
    <table>
      <thead>
        <tr>
          <th>{t("cluster.dropapp.kind")}</th>
          <th>{t("cluster.dropapp.name")}</th>
          <th>{t("cluster.dropapp.status")}</th>
          <th>{t("cluster.dropapp.retry")}</th>
          <th>{t("cluster.dropapp.event")}</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((r) => (
          <tr key={`${r.kind}/${r.namespace ?? ""}/${r.name}`}>
            <td>{r.kind}</td>
            <td>
              <code>{r.name}</code> <span className="muted">{r.namespace ?? ""}</span>
            </td>
            <td className="problem">{r.status ?? "—"}</td>
            <td>{r.retry ?? "—"}</td>
            <td>{r.event ?? ""}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

/** Часть обзора DropApp: версия и имя кластера, компоненты и инструменты, узлы DropApp и сертификаты ingress не в норме. Нет CRD — «—». */
function DropAppSection({ d }: { d: DropAppOverview }) {
  const { t } = useTranslation();
  return (
    <>
      <h4>DropApp</h4>
      <PartView part={d.metadata}>
        {(m) => (
          <dl className="facts">
            <dt>{t("cluster.overview.version")}</dt>
            <dd>{m.version ?? "—"}</dd>
            <dt>{t("cluster.dropapp.clusterName")}</dt>
            <dd>{m.clusterName ?? "—"}</dd>
          </dl>
        )}
      </PartView>
      <h4>{t("cluster.dropapp.components")}</h4>
      <PartView part={d.components}>
        {(c) =>
          c.total === 0 ? (
            <p className="muted">—</p>
          ) : (
            <>
              <p>
                {c.problems.length === 0
                  ? t("cluster.dropapp.componentsOk", { total: c.total })
                  : t("cluster.dropapp.problems", { bad: c.problems.length, total: c.total })}
              </p>
              {c.problems.length > 0 && <ComponentList rows={c.problems} />}
            </>
          )
        }
      </PartView>
      <h4>{t("cluster.dropapp.nodes")}</h4>
      <PartView part={d.nodes}>
        {(n) =>
          n.total === 0 ? (
            <p className="muted">—</p>
          ) : (
            <>
              <p>{n.problems.length === 0 ? t("cluster.dropapp.nodesOk", { total: n.total }) : t("cluster.dropapp.notReady", { bad: n.problems.length, total: n.total })}</p>
              {n.problems.length > 0 && (
                <ul>
                  {n.problems.map((p) => (
                    <li key={p.name} className="problem">
                      <code>{p.name}</code> {p.status ?? ""}
                    </li>
                  ))}
                </ul>
              )}
            </>
          )
        }
      </PartView>
      <h4>{t("cluster.dropapp.ingressTls")}</h4>
      <PartView part={d.ingressTls}>
        {(tls) =>
          tls.total === 0 ? (
            <p className="muted">—</p>
          ) : (
            <>
              <p>
                {tls.problems.length === 0
                  ? t("cluster.dropapp.ingressTlsOk", { total: tls.total })
                  : t("cluster.dropapp.notReady", { bad: tls.problems.length, total: tls.total })}
              </p>
              {tls.problems.length > 0 && (
                <ul>
                  {tls.problems.map((p) => (
                    <li key={`${p.namespace ?? ""}/${p.name}`} className="problem">
                      <code>{p.namespace ? `${p.namespace}/${p.name}` : p.name}</code> {[p.reason, p.message].filter(Boolean).join(": ")}
                    </li>
                  ))}
                </ul>
              )}
            </>
          )
        }
      </PartView>
    </>
  );
}
