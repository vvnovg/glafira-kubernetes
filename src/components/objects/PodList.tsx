import { useQuery } from "@tanstack/react-query";
import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { api } from "../../api/client";
import type { PodRow, WorkloadRef } from "../../api/types";
import { useRefetchInterval } from "../../lib/tabActive";
import { podKey, useWatchList } from "../../lib/useWatchList";
import { ReadError } from "../common/ReadError";
import { TruncatedNote } from "../common/TruncatedNote";
import { WatchStatusLine } from "./WatchStatusLine";
import { PodsTable } from "./PodsTable";

/** Поды CronJob не наблюдаются (их Job меняются) — список перечитывается раз в 10 с. */
const CRON_POLL_MS = 10_000;

/** Поды проекта или workload'а с фильтрами «только проблемные» и по имени; обновляются сами (watch или опрос). */
export function PodList({ connectionId, namespace, workload }: { connectionId: string; namespace: string; workload?: WorkloadRef }) {
  const { t } = useTranslation();
  const [problemsOnly, setProblemsOnly] = useState(false);
  const [filter, setFilter] = useState("");
  const cron = workload?.kind === "CronJob";
  const watch = useWatchList<PodRow>(cron ? null : { connectionId, namespace, kind: "pods", workload: workload ?? null }, podKey);
  // Опрос идёт, только пока вкладка показана.
  const refetchInterval = useRefetchInterval(CRON_POLL_MS);
  const listed = useQuery({
    queryKey: ["pods", connectionId, namespace, workload ?? null],
    queryFn: () => api.listPods(connectionId, namespace, workload ?? null),
    enabled: cron,
    refetchInterval,
  });
  const all = cron ? (listed.data?.items ?? []) : watch.rows;
  const rows = useMemo(() => {
    const f = filter.trim().toLowerCase();
    return all.filter((p) => (!problemsOnly || p.problem) && (!f || p.name.toLowerCase().includes(f))).sort((a, b) => a.name.localeCompare(b.name));
  }, [all, problemsOnly, filter]);
  const problems = all.filter((p) => p.problem).length;
  return (
    <div className="section">
      <div className="toolbar">
        <label className="inline">
          <input type="checkbox" checked={problemsOnly} onChange={(e) => setProblemsOnly(e.target.checked)} />
          {t("objects.pods.problemsOnly", { count: problems })}
        </label>
        <input aria-label={t("objects.pods.filter")} placeholder={t("objects.pods.filter")} value={filter} onChange={(e) => setFilter(e.target.value)} />
        {!cron && <WatchStatusLine state={watch} />}
      </div>
      {cron && listed.error && <ReadError error={listed.error} connectionId={connectionId} onRetry={() => void listed.refetch()} />}
      {!cron && watch.status === "error" && watch.error && <ReadError error={watch.error} connectionId={connectionId} />}
      <TruncatedNote truncated={cron ? listed.data?.truncated : watch.truncated} />
      <PodsTable connectionId={connectionId} namespace={namespace} rows={rows} />
    </div>
  );
}
