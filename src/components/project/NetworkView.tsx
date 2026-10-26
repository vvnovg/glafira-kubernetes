import { useQuery } from "@tanstack/react-query";
import { useTranslation } from "react-i18next";
import { api } from "../../api/client";
import type { ConditionView, GatewayRouteRow, Part } from "../../api/types";
import { tr } from "../../i18n/text";
import { formatDateTime } from "../../lib/age";
import { useNow } from "../../lib/useNow";
import { PartView } from "../common/PartView";
import { ReadError } from "../common/ReadError";
import { RefreshControls, useRefreshInterval } from "../common/RefreshControls";

/** Сертификат Route, который истекает раньше, — с предупреждением (спец. §8.2, `route.certExpiring`). */
const CERT_WARN_SEC = 30 * 86_400;

/** Срок сертификата: истёк (строже всего) или меньше 30 дней; иначе — без выделения. */
function certState(expires: number | null, now: number): "expired" | "soon" | null {
  if (expires === null) return null;
  return expires <= now ? "expired" : expires - now < CERT_WARN_SEC ? "soon" : null;
}

/** Значок состояния: сам глиф скринридер читает как картинку, поэтому у него есть текст. */
function Glyph({ ok }: { ok: boolean }) {
  const { t } = useTranslation();
  return (
    <span role="img" aria-label={ok ? t("project.network.flagOk") : t("project.network.flagProblem")}>
      {ok ? "✅" : "⚠"}
    </span>
  );
}

function Flag({ v }: { v: boolean | null }) {
  return v === null ? <>—</> : <Glyph ok={v} />;
}

function Cond({ c }: { c: ConditionView | null }) {
  if (!c) return <>—</>;
  return (
    <span className={c.status === "True" ? undefined : "problem"} title={c.message}>
      <Glyph ok={c.status === "True"} /> {c.reason ?? c.status}
    </span>
  );
}

function GatewayRoutes({ title, part }: { title: string; part: Part<GatewayRouteRow[]> | null }) {
  const { t } = useTranslation();
  if (!part) return null;
  return (
    <>
      <h3>{title}</h3>
      <PartView part={part}>
        {(rows) =>
          rows.length === 0 ? (
            <p className="muted">—</p>
          ) : (
            <table>
              <thead>
                <tr>
                  <th>{t("project.network.name")}</th>
                  <th>{t("project.network.hosts")}</th>
                  <th>{t("project.network.parents")}</th>
                  <th>{t("project.network.backends")}</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((r) => (
                  <tr key={r.name}>
                    <td>{r.name}</td>
                    <td>{r.hostnames.join(", ") || "*"}</td>
                    <td>
                      {r.parents.map((p) => (
                        <div key={`${p.namespace ?? ""}/${p.name}/${p.section ?? ""}`}>
                          <code>
                            {p.name}
                            {p.section ? `/${p.section}` : ""}
                          </code>{" "}
                          {t("project.network.accepted")}: <Cond c={p.accepted} />, {t("project.network.resolvedRefs")}: <Cond c={p.resolvedRefs} />
                        </div>
                      ))}
                    </td>
                    <td>{r.backends.join(", ") || "—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )
        }
      </PartView>
    </>
  );
}

/** Сеть проекта (спец. §6.3): Services, Routes, Ingress, Gateway API — каждый вид, только если есть его группа API. */
export function NetworkView({ connectionId, namespace }: { connectionId: string; namespace: string }) {
  const { t } = useTranslation();
  const [interval, setInterval] = useRefreshInterval();
  const q = useQuery({ queryKey: ["network", connectionId, namespace], queryFn: () => api.getNetwork(connectionId, namespace), refetchInterval: interval || false });
  const n = q.data;
  // Срок сертификата считается от часов, которые идут, пока вкладка открыта: выделение не должно застыть на моменте перерисовки.
  const now = useNow(60_000) / 1000;
  return (
    <div className="section">
      <RefreshControls interval={interval} onInterval={setInterval} onRefresh={() => void q.refetch()} busy={q.isFetching} />
      {q.isPending && <p className="muted">{t("common.loading")}</p>}
      {q.error && <ReadError error={q.error} connectionId={connectionId} onRetry={() => void q.refetch()} />}
      {n && (
        <>
          <h3>Services</h3>
          {n.endpointsError && <p className="muted">{t("project.network.endpointsUnknown", { reason: tr(n.endpointsError) })}</p>}
          <PartView part={n.services}>
            {(rows) =>
              rows.length === 0 ? (
                <p className="muted">—</p>
              ) : (
                <table>
                  <thead>
                    <tr>
                      <th>{t("project.network.name")}</th>
                      <th>{t("project.network.type")}</th>
                      <th>Cluster IP</th>
                      <th>{t("project.network.ports")}</th>
                      <th>{t("project.network.endpoints")}</th>
                      <th>{t("project.network.external")}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {rows.map((s) => (
                      <tr key={s.name}>
                        <td>{s.name}</td>
                        <td>{s.type}</td>
                        <td>{s.clusterIp ?? "—"}</td>
                        <td>{s.ports.join(", ") || "—"}</td>
                        <td className={s.endpoints && s.endpoints.ready === 0 ? "problem" : undefined}>
                          {s.endpoints ? `${s.endpoints.ready}/${s.endpoints.total}` : "—"}
                        </td>
                        <td>{s.external.join(", ") || "—"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )
            }
          </PartView>
          {n.routes && (
            <>
              <h3>Routes</h3>
              <PartView part={n.routes}>
                {(rows) =>
                  rows.length === 0 ? (
                    <p className="muted">—</p>
                  ) : (
                    <table>
                      <thead>
                        <tr>
                          <th>{t("project.network.name")}</th>
                          <th>{t("project.network.host")}</th>
                          <th>{t("project.network.service")}</th>
                          <th>TLS</th>
                          <th>{t("project.network.admitted")}</th>
                          <th>{t("project.network.certExpires")}</th>
                        </tr>
                      </thead>
                      <tbody>
                        {rows.map((r) => {
                          const cert = certState(r.certExpires, now);
                          return (
                            <tr key={r.name}>
                              <td>
                                {r.name}
                                {r.ingress && <span className="muted"> ({t("project.network.fromIngress", { name: r.ingress })})</span>}
                              </td>
                              <td>
                                {r.host ?? "—"}
                                {r.path ?? ""}
                              </td>
                              <td>
                                {r.service ?? "—"}
                                {r.port ? `:${r.port}` : ""}
                              </td>
                              <td>{r.tls ?? "—"}</td>
                              <td title={r.admittedMessage ?? undefined} className={r.admitted === false ? "problem" : undefined}>
                                <Flag v={r.admitted} /> {r.admittedMessage ?? ""}
                              </td>
                              {/* Истёкший — «problem» и словом, а не только цветом; скоро истекающий — «warn». */}
                              <td className={cert === "expired" ? "problem" : cert === "soon" ? "warn" : undefined}>
                                {formatDateTime(r.certExpires)}
                                {cert === "expired" && ` — ${t("project.network.certExpired")}`}
                              </td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  )
                }
              </PartView>
            </>
          )}
          <h3>Ingress</h3>
          <PartView part={n.ingresses}>
            {(rows) =>
              rows.length === 0 ? (
                <p className="muted">—</p>
              ) : (
                <table>
                  <thead>
                    <tr>
                      <th>{t("project.network.name")}</th>
                      <th>{t("project.network.class")}</th>
                      <th>{t("project.network.rules")}</th>
                      <th>TLS</th>
                      <th>{t("project.network.address")}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {rows.map((i) => (
                      <tr key={i.name}>
                        <td>
                          {i.name}
                          {i.routes.length > 0 && <div className="muted">{t("project.network.routesFromIngress", { names: i.routes.join(", ") })}</div>}
                        </td>
                        <td>{i.class ?? "—"}</td>
                        <td>
                          {i.rules.map((r) => (
                            <div key={`${r.host ?? "*"}${r.path ?? ""}`}>
                              {r.host ?? "*"}
                              {r.path ?? ""} → {r.service ?? "—"}
                              {r.port ? `:${r.port}` : ""}
                            </div>
                          ))}
                          {i.defaultBackend && <div className="muted">{t("project.network.defaultBackend", { backend: i.defaultBackend })}</div>}
                        </td>
                        <td>
                          {i.tls.map((x) => (
                            <div key={x.secret ?? x.hosts.join(",")}>
                              {x.hosts.join(", ")} {x.secret && <code>{x.secret}</code>}
                            </div>
                          ))}
                        </td>
                        <td>{i.addresses.join(", ") || "—"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )
            }
          </PartView>
          {n.gateways && (
            <>
              <h3>Gateway</h3>
              <PartView part={n.gateways}>
                {(rows) =>
                  rows.length === 0 ? (
                    <p className="muted">—</p>
                  ) : (
                    <table>
                      <thead>
                        <tr>
                          <th>{t("project.network.name")}</th>
                          <th>{t("project.network.class")}</th>
                          <th>Listeners</th>
                          <th>{t("project.network.address")}</th>
                          <th className="cap">{t("project.network.accepted")}</th>
                          <th className="cap">{t("project.network.programmed")}</th>
                        </tr>
                      </thead>
                      <tbody>
                        {rows.map((g) => (
                          <tr key={g.name}>
                            <td>{g.name}</td>
                            <td>{g.class ?? "—"}</td>
                            <td>{g.listeners.map((l) => `${l.name} ${l.protocol ?? ""}:${l.port ?? ""}${l.hostname ? ` ${l.hostname}` : ""}`).join(", ")}</td>
                            <td>{g.addresses.join(", ") || "—"}</td>
                            <td>
                              <Flag v={g.accepted} />
                            </td>
                            <td>
                              <Flag v={g.programmed} />
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  )
                }
              </PartView>
            </>
          )}
          <GatewayRoutes title="HTTPRoute" part={n.httpRoutes} />
          <GatewayRoutes title="GRPCRoute" part={n.grpcRoutes} />
        </>
      )}
    </div>
  );
}
