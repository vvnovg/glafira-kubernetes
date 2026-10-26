import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { api } from "../../api/client";
import { formatAge } from "../../lib/age";
import { PartView } from "../common/PartView";
import { ReadError } from "../common/ReadError";
import { RefreshControls, useRefreshInterval } from "../common/RefreshControls";

/** Значения ConfigMap — по щелчку (спец. §6.3). */
function ConfigMapValues({ connectionId, namespace, name }: { connectionId: string; namespace: string; name: string }) {
  const { t } = useTranslation();
  const q = useQuery({ queryKey: ["configMap", connectionId, namespace, name], queryFn: () => api.getConfigMapValues(connectionId, namespace, name) });
  if (q.error) return <ReadError error={q.error} connectionId={connectionId} onRetry={() => void q.refetch()} />;
  if (!q.data) return <p className="muted">{t("common.loading")}</p>;
  return (
    <dl className="facts">
      {Object.entries(q.data).map(([k, v]) => (
        <div key={k} style={{ display: "contents" }}>
          <dt>
            <code>{k}</code>
          </dt>
          <dd>{v.text !== null ? <pre className="yaml">{v.text}</pre> : <span className="muted">{t("project.config.binary", { size: v.binarySize ?? 0 })}</span>}</dd>
        </div>
      ))}
    </dl>
  );
}

/** Конфигурация проекта: ConfigMaps (ключи, значения по щелчку) и Secrets (имя, тип, ключи — значения не показываются). */
export function ConfigView({ connectionId, namespace }: { connectionId: string; namespace: string }) {
  const { t } = useTranslation();
  const [interval, setInterval] = useRefreshInterval();
  const [shown, setShown] = useState<string | null>(null);
  const q = useQuery({ queryKey: ["config", connectionId, namespace], queryFn: () => api.getConfig(connectionId, namespace), refetchInterval: interval || false });
  return (
    <div className="section">
      <RefreshControls interval={interval} onInterval={setInterval} onRefresh={() => void q.refetch()} busy={q.isFetching} />
      {q.isPending && <p className="muted">{t("common.loading")}</p>}
      {q.error && <ReadError error={q.error} connectionId={connectionId} onRetry={() => void q.refetch()} />}
      {q.data && (
        <>
          <h3>ConfigMaps</h3>
          <PartView part={q.data.configMaps}>
            {(rows) =>
              rows.length === 0 ? (
                <p className="muted">—</p>
              ) : (
                <table>
                  <thead>
                    <tr>
                      <th>{t("project.network.name")}</th>
                      <th>{t("project.config.keys")}</th>
                      <th>{t("project.workloads.age")}</th>
                      <th />
                    </tr>
                  </thead>
                  <tbody>
                    {rows.map((c) => (
                      <tr key={c.name}>
                        <td>{c.name}</td>
                        <td>
                          {[...c.keys, ...c.binaryKeys].join(", ") || "—"}
                          {shown === c.name && <ConfigMapValues connectionId={connectionId} namespace={namespace} name={c.name} />}
                        </td>
                        <td>{formatAge(c.created)}</td>
                        <td>
                          <button type="button" className="link" onClick={() => setShown((s) => (s === c.name ? null : c.name))}>
                            {shown === c.name ? t("project.config.hideValues") : t("project.config.showValues")}
                          </button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )
            }
          </PartView>
          <h3>Secrets</h3>
          <p className="muted">{t("project.config.secretValuesHidden")}</p>
          <PartView part={q.data.secrets}>
            {(rows) =>
              rows.length === 0 ? (
                <p className="muted">—</p>
              ) : (
                <table>
                  <thead>
                    <tr>
                      <th>{t("project.network.name")}</th>
                      <th>{t("project.network.type")}</th>
                      <th>{t("project.config.keys")}</th>
                      <th>{t("project.workloads.age")}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {rows.map((s) => (
                      <tr key={s.name}>
                        <td>{s.name}</td>
                        <td>{s.type}</td>
                        <td>{s.keys.join(", ") || "—"}</td>
                        <td>{formatAge(s.created)}</td>
                      </tr>
                    ))}
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
