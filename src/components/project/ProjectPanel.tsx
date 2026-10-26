import { useState } from "react";
import { useTranslation } from "react-i18next";
import type { WorkloadRow } from "../../api/types";
import { useWatchList, workloadKey } from "../../lib/useWatchList";
import { ReadError } from "../common/ReadError";
import { SubTabs } from "../common/SubTabs";
import { ProjectEvents } from "../objects/EventList";
import { KindErrors } from "./KindErrors";
import { useNamespaceAccess } from "./useNamespaceAccess";
import { PodList } from "../objects/PodList";
import { WatchStatusLine } from "../objects/WatchStatusLine";
import { WORKLOAD_RESOURCES, canList } from "./access";
import { ProjectOverview } from "./ProjectOverview";
import { WorkloadsTable } from "./WorkloadsTable";

type Sub = "overview" | "workloads" | "pods" | "events";

/** Вкладка проекта (спец. §6.3). Подвкладки без прав на чтение не показываются (§5.4); показывается одна, её watch идёт, пока она открыта. */
export function ProjectPanel({ connectionId, namespace }: { connectionId: string; namespace: string }) {
  const { t } = useTranslation();
  const [sub, setSub] = useState<Sub>("overview");
  // Права перечитываются по таймеру и по «Обновить» в обзоре, иначе подвкладка, скрытая по устаревшим правам, не вернулась бы.
  const access = useNamespaceAccess(connectionId, namespace);
  const a = access.data;
  const tabs: { id: Sub; label: string; visible: boolean }[] = [
    { id: "overview", label: t("objects.tabs.overview"), visible: true },
    { id: "workloads", label: t("project.tabs.workloads"), visible: canList(a, ...WORKLOAD_RESOURCES) },
    { id: "pods", label: t("objects.tabs.pods"), visible: canList(a, "pods") },
    { id: "events", label: t("objects.tabs.events"), visible: canList(a, "events") },
  ];
  const visible = tabs.filter((x) => x.visible);
  const active = visible.some((x) => x.id === sub) ? sub : "overview";
  return (
    <div className="page">
      <div className="toolbar">
        <h2>{namespace}</h2>
      </div>
      {access.error && <ReadError error={access.error} connectionId={connectionId} onRetry={() => void access.refetch()} />}
      <SubTabs tabs={visible} active={active} onChange={setSub} />
      {active === "overview" && <ProjectOverview connectionId={connectionId} namespace={namespace} />}
      {active === "workloads" && <ProjectWorkloads connectionId={connectionId} namespace={namespace} />}
      {active === "pods" && <PodList connectionId={connectionId} namespace={namespace} />}
      {active === "events" && <ProjectEvents connectionId={connectionId} namespace={namespace} />}
    </div>
  );
}

/** Workloads проекта через watch; виды без прав называются отдельно. */
function ProjectWorkloads({ connectionId, namespace }: { connectionId: string; namespace: string }) {
  const watch = useWatchList<WorkloadRow>({ connectionId, namespace, kind: "workloads" }, workloadKey);
  return (
    <div className="section">
      <WatchStatusLine state={watch} />
      {watch.status === "error" && watch.error && <ReadError error={watch.error} connectionId={connectionId} />}
      <KindErrors errors={watch.kindErrors} />
      <WorkloadsTable connectionId={connectionId} namespace={namespace} rows={watch.rows} />
    </div>
  );
}
