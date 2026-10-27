import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useTranslation } from "react-i18next";
import { api, toAppError } from "../../api/client";
import type { PodRow } from "../../api/types";
import { formatAge } from "../../lib/age";
import { useRefetchInterval } from "../../lib/tabActive";
import { useWorkspace } from "../../state/workspace";
import { PartView } from "../common/PartView";
import { ReadError } from "../common/ReadError";
import { RefreshControls, useRefreshInterval } from "../common/RefreshControls";
import { TruncatedNote } from "../common/TruncatedNote";
import { matchProject } from "../health/findings";
import { HealthBlock } from "../health/HealthBlock";
import { KindErrors } from "./KindErrors";
import { QUOTA_NEAR_LIMIT_PERCENT, isNearLimit } from "./quota";
import { refreshNamespaceAccess, useNamespaceAccess } from "./useNamespaceAccess";

/** Последние события Warning — за этот срок (спец. §6.3). */
const WARNINGS_FOR_SEC = 3600;
/** Больше стольких строк в каждом списке обзора не показывается. */
const MAX_ITEMS = 20;

const PHASES = ["Running", "Pending", "Succeeded", "Failed", "Unknown"];

/** Одинаковые ошибки (подключение недоступно, сессия истекла) узнаются по этому отпечатку. */
const errorSignature = (error: unknown): string => {
  const e = toAppError(error);
  return JSON.stringify([e.code, e.key, e.params, e.message]);
};

/** Обзор проекта: поды по фазам и проблемам, workloads не в норме, квоты у предела, Warning за час. */
export function ProjectOverview({ connectionId, namespace }: { connectionId: string; namespace: string }) {
  const { t } = useTranslation();
  const open = useWorkspace((s) => s.open);
  const queryClient = useQueryClient();
  const access = useNamespaceAccess(connectionId, namespace);
  const [interval, setRefreshInterval] = useRefreshInterval();
  const refetchInterval = useRefetchInterval(interval);
  const pods = useQuery({ queryKey: ["pods", connectionId, namespace, null], queryFn: () => api.listPods(connectionId, namespace), refetchInterval });
  const workloads = useQuery({ queryKey: ["workloads", connectionId, namespace], queryFn: () => api.listWorkloads(connectionId, namespace), refetchInterval });
  const quotas = useQuery({ queryKey: ["quotas", connectionId, namespace], queryFn: () => api.getQuotas(connectionId, namespace), refetchInterval });
  const events = useQuery({ queryKey: ["events", connectionId, namespace, null], queryFn: () => api.listEvents(connectionId, namespace), refetchInterval });
  const busy = pods.isFetching || workloads.isFetching || quotas.isFetching || events.isFetching;
  // Ручное обновление перечитывает и права проекта (Rust забывает их кэш): подвкладка, скрытая по устаревшим правам, возвращается.
  const refresh = () => Promise.all([pods.refetch(), workloads.refetch(), quotas.refetch(), events.refetch(), refreshNamespaceAccess(queryClient, connectionId, namespace)]);
  // Подключение недоступно — ошибка у всех запросов одна и та же: баннер показывается один раз (если права уже показали её, то в панели), у остальных разделов — «—».
  const shownErrors = new Set<string>(access.error ? [errorSignature(access.error)] : []);
  const sectionError = (error: unknown) => {
    const signature = errorSignature(error);
    if (shownErrors.has(signature)) return <p className="muted">—</p>;
    shownErrors.add(signature);
    return <ReadError error={error} connectionId={connectionId} onRetry={() => void refresh()} />;
  };
  const openPod = (p: PodRow) => open({ kind: "pod", connectionId, namespace, name: p.name });
  const since = Date.now() / 1000 - WARNINGS_FOR_SEC;
  const warnings = (events.data?.items ?? []).filter((e) => e.type === "Warning" && (e.lastSeen ?? 0) >= since).slice(0, MAX_ITEMS);
  const problemPods = (pods.data?.items ?? []).filter((p) => p.problem);
  const problemWorkloads = (workloads.data?.items ?? []).filter((w) => w.problem);
  return (
    <div className="section">
      <RefreshControls interval={interval} onInterval={setRefreshInterval} onRefresh={() => void refresh()} busy={busy} />
      <HealthBlock connectionId={connectionId} match={matchProject(namespace)} showTarget />
      <h3>{t("project.overview.pods")}</h3>
      {pods.error && sectionError(pods.error)}
      {pods.data && (
        <>
          <dl className="stats">
            {PHASES.map((phase) => (
              <div key={phase}>
                <dt>{phase}</dt>
                <dd>{pods.data.items.filter((p) => p.phase === phase).length}</dd>
              </div>
            ))}
          </dl>
          {/* Числа и проблемные поды посчитаны по прочитанному списку: из-за предела в 10 000 объектов они неполные. */}
          <TruncatedNote truncated={pods.data.truncated} />
          {problemPods.length === 0 ? (
            <p className="muted">{t("project.overview.noProblemPods")}</p>
          ) : (
            <ul>
              {problemPods.slice(0, MAX_ITEMS).map((p) => (
                <li key={p.name}>
                  <button type="button" className="link" onClick={() => openPod(p)}>
                    {p.name}
                  </button>{" "}
                  <span className="problem">{p.status}</span> <span className="muted">{t("project.overview.restarts", { count: p.restarts })}</span>
                </li>
              ))}
            </ul>
          )}
        </>
      )}
      <h3>{t("project.overview.workloads")}</h3>
      {workloads.error && sectionError(workloads.error)}
      {workloads.data && (
        <>
          {/* Виды, которые не прочитались, нельзя выдавать за «в норме»: «Все workloads в норме» — только если прочитаны все виды. */}
          <KindErrors errors={workloads.data.errors} />
          <TruncatedNote truncated={workloads.data.truncated} />
          {problemWorkloads.length === 0 ? (
            workloads.data.errors.length === 0 && <p className="muted">{t("project.overview.noProblemWorkloads")}</p>
          ) : (
            <ul>
              {problemWorkloads.slice(0, MAX_ITEMS).map((w) => (
                <li key={`${w.kind}/${w.name}`}>
                  <button type="button" className="link" onClick={() => open({ kind: "workload", connectionId, namespace, workloadKind: w.kind, name: w.name })}>
                    {w.kind}/{w.name}
                  </button>{" "}
                  <span className="problem">{t(`objects.workload.problem.${w.problem}`)}</span>
                </li>
              ))}
            </ul>
          )}
        </>
      )}
      <h3>{t("project.overview.quotas")}</h3>
      {quotas.error && sectionError(quotas.error)}
      {quotas.data && (
        <PartView part={quotas.data.quotas}>
          {(list) => {
            const near = list.flatMap((q) => q.entries.filter((e) => isNearLimit(e.ratio)).map((e) => ({ quota: q.name, ...e })));
            if (list.length === 0) return <p className="muted">{t("project.overview.noQuotas")}</p>;
            if (near.length === 0) return <p className="muted">{t("project.overview.quotasOk", { percent: QUOTA_NEAR_LIMIT_PERCENT })}</p>;
            return (
              <ul>
                {near.map((e) => (
                  <li key={`${e.quota}/${e.resource}`} className="warn">
                    {e.quota}: <code>{e.resource}</code> {e.used} / {e.hard} ({Math.round((e.ratio ?? 0) * 100)} %)
                  </li>
                ))}
              </ul>
            );
          }}
        </PartView>
      )}
      <h3>{t("project.overview.warnings")}</h3>
      {events.error && sectionError(events.error)}
      {events.data && (
        <>
          <TruncatedNote truncated={events.data.truncated} />
          {warnings.length === 0 ? (
            <p className="muted">{t("project.overview.noWarnings")}</p>
          ) : (
            <ul>
              {warnings.map((e) => (
                <li key={e.key}>
                  <span className="muted">{formatAge(e.lastSeen)}</span> <strong>{e.reason}</strong> {e.object.kind}/{e.object.name}: {e.message}
                </li>
              ))}
            </ul>
          )}
        </>
      )}
    </div>
  );
}
