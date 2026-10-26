import { useQuery } from "@tanstack/react-query";
import { useTranslation } from "react-i18next";
import { api } from "../../api/client";
import { formatAge } from "../../lib/age";
import { ReadError } from "../common/ReadError";
import { RefreshControls, useRefreshInterval } from "../common/RefreshControls";

/** Хранилище проекта (спец. §6.3): PVC — статус, класс, ёмкость, режим доступа, кем смонтирован. */
export function StorageView({ connectionId, namespace }: { connectionId: string; namespace: string }) {
  const { t } = useTranslation();
  const [interval, setInterval] = useRefreshInterval();
  const q = useQuery({ queryKey: ["pvcs", connectionId, namespace], queryFn: () => api.listPvcs(connectionId, namespace), refetchInterval: interval || false });
  return (
    <div className="section">
      <RefreshControls interval={interval} onInterval={setInterval} onRefresh={() => void q.refetch()} busy={q.isFetching} />
      {q.isPending && <p className="muted">{t("common.loading")}</p>}
      {q.error && <ReadError error={q.error} connectionId={connectionId} onRetry={() => void q.refetch()} />}
      {q.data && q.data.items.length === 0 && <p className="muted">—</p>}
      {q.data && q.data.items.length > 0 && (
        <table>
          <thead>
            <tr>
              <th>{t("project.network.name")}</th>
              <th>{t("objects.pods.status")}</th>
              <th>{t("project.storage.class")}</th>
              <th>{t("project.storage.capacity")}</th>
              <th>{t("project.storage.accessModes")}</th>
              <th>{t("project.storage.mountedBy")}</th>
              <th>{t("project.workloads.age")}</th>
            </tr>
          </thead>
          <tbody>
            {q.data.items.map((p) => (
              <tr key={p.name}>
                <td>{p.name}</td>
                <td className={p.phase === "Bound" ? undefined : "problem"}>{p.phase ?? "—"}</td>
                <td>{p.class ?? "—"}</td>
                <td>{p.capacity ?? t("project.storage.requested", { size: p.requested ?? "—" })}</td>
                <td>{p.accessModes.join(", ") || "—"}</td>
                <td>{p.mountedBy === null ? <span className="muted">{t("project.storage.unknown")}</span> : p.mountedBy.join(", ") || "—"}</td>
                <td>{formatAge(p.created)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
