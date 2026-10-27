import { useTranslation } from "react-i18next";
import type { PodRow } from "../../api/types";
import { formatAge } from "../../lib/age";
import { podKey } from "../../lib/useWatchList";
import { useWorkspace } from "../../state/workspace";
import { type Column, VirtualTable } from "../common/VirtualTable";
import { matchPod } from "../health/findings";
import { HealthBadge } from "../health/HealthBlock";

/** Таблица подов (спец. §6.3): статус с причиной, готовность, рестарты, нода, возраст, QoS, владелец; щелчок открывает под. */
export function PodsTable({ connectionId, namespace, rows }: { connectionId: string; namespace: string; rows: PodRow[] }) {
  const { t } = useTranslation();
  const open = useWorkspace((s) => s.open);
  const columns: Column<PodRow>[] = [
    {
      key: "name",
      title: t("objects.pods.name"),
      width: "minmax(200px, 2fr)",
      render: (p) => (
        <>
          {p.name}
          <HealthBadge connectionId={connectionId} match={matchPod(namespace, p.name)} />
        </>
      ),
    },
    {
      key: "status",
      title: t("objects.pods.status"),
      width: "minmax(130px, 1fr)",
      render: (p) => <span className={p.problem ? "problem" : undefined}>{p.status}</span>,
    },
    { key: "ready", title: t("objects.pods.ready"), width: "70px", render: (p) => `${p.ready}/${p.containers}` },
    {
      key: "restarts",
      title: t("objects.pods.restarts"),
      width: "110px",
      render: (p) => (p.restarts > 0 && p.lastRestart ? `${p.restarts} (${formatAge(p.lastRestart)})` : String(p.restarts)),
    },
    { key: "node", title: t("objects.pods.node"), width: "minmax(100px, 1fr)", render: (p) => p.node ?? "—" },
    { key: "age", title: t("objects.pods.age"), width: "70px", render: (p) => formatAge(p.created) },
    { key: "qos", title: "QoS", width: "90px", render: (p) => p.qos ?? "—" },
    { key: "owner", title: t("objects.pods.owner"), width: "minmax(120px, 1fr)", render: (p) => (p.owner ? `${p.owner.kind}/${p.owner.name}` : "—") },
  ];
  return (
    <VirtualTable
      label={t("objects.pods.title")}
      rows={rows}
      columns={columns}
      rowKey={podKey}
      onRowClick={(p) => open({ kind: "pod", connectionId, namespace, name: p.name })}
    />
  );
}
