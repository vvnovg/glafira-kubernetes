import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { api } from "../../api/client";
import type { NodeRow } from "../../api/types";
import { formatAge } from "../../lib/age";
import { useRefetchInterval } from "../../lib/tabActive";
import { Conditions } from "../common/Conditions";
import { KeyValues } from "../common/KeyValues";
import { ReadError } from "../common/ReadError";
import { RefreshControls, useRefreshInterval } from "../common/RefreshControls";
import { TruncatedNote } from "../common/TruncatedNote";
import { type Column, VirtualTable } from "../common/VirtualTable";

/** Вкладка «Ноды» (спец. §6.1): список с готовностью и ролями, сведения о выбранной ноде. */
export function NodesPanel({ connectionId }: { connectionId: string }) {
  const { t } = useTranslation();
  const [interval, setRefreshInterval] = useRefreshInterval();
  const [selected, setSelected] = useState<string | null>(null);
  const refetchInterval = useRefetchInterval(interval);
  const q = useQuery({ queryKey: ["nodes", connectionId], queryFn: () => api.listNodes(connectionId), refetchInterval });
  const columns: Column<NodeRow>[] = [
    { key: "name", title: t("cluster.nodes.name"), width: "minmax(180px, 2fr)", render: (n) => n.name },
    {
      key: "status",
      title: t("cluster.nodes.status"),
      width: "minmax(120px, 1fr)",
      render: (n) => (
        <span className={n.ready && n.problems.length === 0 ? undefined : "problem"}>
          {n.ready ? "Ready" : "NotReady"}
          {n.problems.filter((p) => p !== "NotReady").map((p) => `, ${p}`)}
          {n.unschedulable && ", SchedulingDisabled"}
        </span>
      ),
    },
    { key: "roles", title: t("cluster.nodes.roles"), width: "minmax(120px, 1fr)", render: (n) => n.roles.join(", ") || "—" },
    { key: "version", title: t("cluster.nodes.version"), width: "110px", render: (n) => n.kubeletVersion ?? "—" },
    { key: "cpu", title: "CPU", width: "70px", render: (n) => n.cpu ?? "—" },
    { key: "memory", title: t("cluster.nodes.memory"), width: "90px", render: (n) => n.memory ?? "—" },
    { key: "age", title: t("cluster.nodes.age"), width: "70px", render: (n) => formatAge(n.created) },
  ];
  return (
    <div className="page">
      <div className="toolbar">
        <h2>{t("cluster.nodes.title")}</h2>
        <RefreshControls interval={interval} onInterval={setRefreshInterval} onRefresh={() => void q.refetch()} busy={q.isFetching} />
      </div>
      {q.isPending && <p className="muted">{t("common.loading")}</p>}
      {q.error && <ReadError error={q.error} connectionId={connectionId} onRetry={() => void q.refetch()} />}
      <TruncatedNote truncated={q.data?.truncated} />
      {q.data && (
        <div className="split">
          <VirtualTable label={t("cluster.nodes.title")} rows={q.data.items} columns={columns} rowKey={(n) => n.name} onRowClick={(n) => setSelected(n.name)} selectedKey={selected} />
          {selected && <NodeDetailsPane connectionId={connectionId} name={selected} interval={interval} />}
        </div>
      )}
    </div>
  );
}

/** Сведения о ноде обновляются с тем же автообновлением, что и список. */
function NodeDetailsPane({ connectionId, name, interval }: { connectionId: string; name: string; interval: number }) {
  const { t } = useTranslation();
  const refetchInterval = useRefetchInterval(interval);
  const q = useQuery({ queryKey: ["node", connectionId, name], queryFn: () => api.getNode(connectionId, name), refetchInterval });
  const d = q.data;
  return (
    <div className="details-pane section" aria-label={name}>
      <h3>{name}</h3>
      {q.isPending && <p className="muted">{t("common.loading")}</p>}
      {q.error && <ReadError error={q.error} connectionId={connectionId} onRetry={() => void q.refetch()} />}
      {d && (
        <>
          <dl className="facts">
            <dt>{t("cluster.nodes.addresses")}</dt>
            <dd>{d.addresses.join(", ") || "—"}</dd>
            <dt>{t("cluster.nodes.os")}</dt>
            <dd>
              {d.osImage ?? "—"} · {d.architecture ?? "—"} · {d.kernelVersion ?? "—"}
            </dd>
            <dt>{t("cluster.nodes.runtime")}</dt>
            <dd>{d.containerRuntime ?? "—"}</dd>
          </dl>
          <h4>{t("cluster.nodes.conditions")}</h4>
          <Conditions conditions={d.conditions} />
          <h4>{t("cluster.nodes.taints")}</h4>
          {d.taints.length === 0 ? (
            <p className="muted">—</p>
          ) : (
            <ul className="kv">
              {d.taints.map((x) => (
                <li key={x}>
                  <code>{x}</code>
                </li>
              ))}
            </ul>
          )}
          <h4>{t("cluster.nodes.resources")}</h4>
          <table>
            <thead>
              <tr>
                <th />
                <th>{t("cluster.nodes.capacity")}</th>
                <th>{t("cluster.nodes.allocatable")}</th>
              </tr>
            </thead>
            <tbody>
              {Object.keys(d.capacity).map((r) => (
                <tr key={r}>
                  <td>{r}</td>
                  <td>{d.capacity[r]}</td>
                  <td>{d.allocatable[r] ?? "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <h4>{t("objects.labels")}</h4>
          <KeyValues values={d.labels} />
        </>
      )}
    </div>
  );
}
