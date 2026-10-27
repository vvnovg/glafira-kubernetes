import { useQuery } from "@tanstack/react-query";
import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { api } from "../../api/client";
import type { WorkloadKind, WorkloadRow } from "../../api/types";
import { formatAge } from "../../lib/age";
import { workloadKey } from "../../lib/useWatchList";
import { useWorkspace } from "../../state/workspace";
import { type Column, VirtualTable } from "../common/VirtualTable";
import { matchWorkload } from "../health/findings";
import { HealthBadge } from "../health/HealthBlock";

const KINDS: WorkloadKind[] = ["Deployment", "StatefulSet", "DaemonSet", "DeploymentConfig", "Job", "CronJob"];

/** Готово / желаемо; у CronJob — расписание, у Job — успешно / нужно. */
function readyText(w: WorkloadRow): string {
  if (w.kind === "CronJob") return w.schedule ?? "—";
  return `${w.ready ?? "—"}/${w.desired ?? "—"}`;
}

/** Таблица workloads проекта (спец. §6.3) с фильтрами по виду и имени; щелчок открывает workload. */
export function WorkloadsTable({ connectionId, namespace, rows }: { connectionId: string; namespace: string; rows: WorkloadRow[] }) {
  const { t } = useTranslation();
  const open = useWorkspace((s) => s.open);
  const [kind, setKind] = useState<WorkloadKind | "">("");
  const [filter, setFilter] = useState("");
  // Сведения о подключении — из вкладки подключения; сама таблица не подключается.
  const info = useQuery({ queryKey: ["connection", connectionId], queryFn: () => api.connect(connectionId, false), enabled: false });
  // DeploymentConfig есть только там, где есть группа API `apps.openshift.io`. Сведений о подключении может ещё не быть
  // (проект открыт из дерева) — тогда вид предлагается, только если такие строки уже пришли.
  const withDeploymentConfigs = info.data?.apiGroups.deploymentConfigs === true || kind === "DeploymentConfig" || rows.some((w) => w.kind === "DeploymentConfig");
  const kinds = KINDS.filter((k) => k !== "DeploymentConfig" || withDeploymentConfigs);
  const shown = useMemo(() => {
    const f = filter.trim().toLowerCase();
    return rows.filter((w) => (!kind || w.kind === kind) && (!f || w.name.toLowerCase().includes(f))).sort((a, b) => a.name.localeCompare(b.name) || a.kind.localeCompare(b.kind));
  }, [rows, kind, filter]);
  const columns: Column<WorkloadRow>[] = [
    { key: "kind", title: t("project.workloads.kind"), width: "130px", render: (w) => w.kind },
    {
      key: "name",
      title: t("project.workloads.name"),
      width: "minmax(180px, 2fr)",
      render: (w) => (
        <>
          {w.name}
          <HealthBadge connectionId={connectionId} match={matchWorkload(namespace, w.kind, w.name)} />
        </>
      ),
    },
    { key: "ready", title: t("project.workloads.ready"), width: "110px", render: (w) => <span className={w.problem ? "problem" : undefined}>{readyText(w)}</span> },
    { key: "images", title: t("project.workloads.images"), width: "minmax(200px, 3fr)", render: (w) => <span title={w.images.join(", ")}>{w.images.join(", ")}</span> },
    { key: "age", title: t("project.workloads.age"), width: "70px", render: (w) => formatAge(w.created) },
  ];
  return (
    <>
      <div className="toolbar">
        <label className="inline">
          {t("project.workloads.kind")}
          <select value={kind} onChange={(e) => setKind(e.target.value as WorkloadKind | "")}>
            <option value="">{t("objects.events.all")}</option>
            {kinds.map((k) => (
              <option key={k} value={k}>
                {k}
              </option>
            ))}
          </select>
        </label>
        <input aria-label={t("objects.pods.filter")} placeholder={t("objects.pods.filter")} value={filter} onChange={(e) => setFilter(e.target.value)} />
      </div>
      <VirtualTable
        label={t("project.workloads.title")}
        rows={shown}
        columns={columns}
        rowKey={workloadKey}
        onRowClick={(w) => open({ kind: "workload", connectionId, namespace, workloadKind: w.kind, name: w.name })}
      />
    </>
  );
}
