import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { api } from "../../api/client";
import type { WorkloadDetails, WorkloadKind } from "../../api/types";
import { formatAge, formatDateTime } from "../../lib/age";
import { useRefetchInterval } from "../../lib/tabActive";
import { Conditions } from "../common/Conditions";
import { KeyValues } from "../common/KeyValues";
import { PartView } from "../common/PartView";
import { ReadError } from "../common/ReadError";
import { RefreshControls, useRefreshInterval } from "../common/RefreshControls";
import { SubTabs } from "../common/SubTabs";
import { YamlView } from "../common/YamlView";
import { WorkloadActions, hintsOf } from "../change/ChangeActions";
import { ChangeDialog } from "../change/ChangeDialog";
import { useActivePrefill } from "../health/useActivePrefill";
import type { ProposalPrefill } from "../../state/workspace";
import { WorkloadResources } from "../resources/WorkloadResources";
import { matchWorkload } from "../health/findings";
import { HealthBlock } from "../health/HealthBlock";
import { ContainerCard } from "./ContainerCard";
import { ObjectEvents } from "./EventList";
import { PodList } from "./PodList";

type Sub = "overview" | "pods" | "resources" | "events" | "yaml";

/** Вкладка workload'а (спец. §6.4): обзор, его поды, ресурсы, события вместе с подами, YAML. */
export function WorkloadPanel({
  connectionId,
  namespace,
  kind,
  name,
  proposal,
}: {
  connectionId: string;
  namespace: string;
  kind: WorkloadKind;
  name: string;
  /** «Открыть в форме» предложения LLM: форма изменения поверх вкладки. */
  proposal?: ProposalPrefill;
}) {
  const { t } = useTranslation();
  const [sub, setSub] = useState<Sub>("overview");
  const [prefill, clearPrefill] = useActivePrefill(proposal);
  // Подсказки формы ресурсов — из того же запроса, что и обзор.
  const details = useQuery({ queryKey: ["workload", connectionId, namespace, kind, name], queryFn: () => api.getWorkload(connectionId, namespace, kind, name), enabled: prefill !== undefined });
  const tabs: { id: Sub; label: string }[] = [
    { id: "overview", label: t("objects.tabs.overview") },
    { id: "pods", label: t("objects.tabs.pods") },
    { id: "resources", label: t("objects.tabs.resources") },
    { id: "events", label: t("objects.tabs.events") },
    { id: "yaml", label: "YAML" },
  ];
  return (
    <div className="page">
      <div className="toolbar">
        <h2>
          {kind} {name}
        </h2>
        <span className="muted">{namespace}</span>
      </div>
      <SubTabs tabs={tabs} active={sub} onChange={setSub} />
      {sub === "overview" && <WorkloadOverview connectionId={connectionId} namespace={namespace} kind={kind} name={name} />}
      {sub === "pods" && <PodList connectionId={connectionId} namespace={namespace} workload={{ kind, name }} />}
      {sub === "resources" && <WorkloadResources connectionId={connectionId} namespace={namespace} kind={kind} name={name} />}
      {sub === "events" && <ObjectEvents connectionId={connectionId} namespace={namespace} object={{ kind, name }} />}
      {sub === "yaml" && <YamlView connectionId={connectionId} namespace={namespace} kind={kind} name={name} />}
      {prefill && (
        <ChangeDialog
          key={prefill.nonce}
          connectionId={connectionId}
          initial={prefill.proposal.change}
          proposal={prefill.proposal}
          hints={details.data ? hintsOf(details.data) : undefined}
          onClose={clearPrefill}
        />
      )}
    </div>
  );
}

function WorkloadOverview({ connectionId, namespace, kind, name }: { connectionId: string; namespace: string; kind: WorkloadKind; name: string }) {
  const { t } = useTranslation();
  const [interval, setInterval] = useRefreshInterval();
  const refetchInterval = useRefetchInterval(interval);
  const q = useQuery({
    queryKey: ["workload", connectionId, namespace, kind, name],
    queryFn: () => api.getWorkload(connectionId, namespace, kind, name),
    refetchInterval,
  });
  return (
    <div className="section">
      <RefreshControls interval={interval} onInterval={setInterval} onRefresh={() => void q.refetch()} busy={q.isFetching} />
      <HealthBlock connectionId={connectionId} match={matchWorkload(namespace, kind, name)} />
      {q.isPending && <p className="muted">{t("common.loading")}</p>}
      {q.error && <ReadError error={q.error} connectionId={connectionId} onRetry={() => void q.refetch()} />}
      {q.data && <WorkloadActions connectionId={connectionId} namespace={namespace} d={q.data} />}
      {q.data && <WorkloadSummary d={q.data} />}
    </div>
  );
}

const num = (n: number | null | undefined) => (n == null ? "—" : String(n));

function WorkloadSummary({ d }: { d: WorkloadDetails }) {
  const { t } = useTranslation();
  const w = d.workload;
  return (
    <>
      {w.problem && <p className="problem">{t(`objects.workload.problem.${w.problem}`)}</p>}
      <dl className="stats">
        {w.kind === "CronJob" ? (
          <>
            <div>
              <dt>{t("objects.workload.schedule")}</dt>
              <dd>
                <code>{w.schedule ?? "—"}</code>
                {w.suspended && ` · ${t("objects.workload.suspended")}`}
              </dd>
            </div>
            <div>
              <dt>{t("objects.workload.lastSchedule")}</dt>
              <dd>{formatDateTime(w.lastSchedule)}</dd>
            </div>
            <div>
              <dt>{t("objects.workload.activeJobs")}</dt>
              <dd>{d.activeJobs.join(", ") || "—"}</dd>
            </div>
          </>
        ) : (
          <>
            <div>
              <dt>{w.kind === "Job" ? t("objects.workload.completions") : t("objects.workload.desired")}</dt>
              <dd>{num(w.desired)}</dd>
            </div>
            <div>
              <dt>{w.kind === "Job" ? t("objects.workload.succeeded") : t("objects.workload.ready")}</dt>
              <dd>{num(w.ready)}</dd>
            </div>
            {w.kind !== "Job" && (
              <>
                <div>
                  <dt>{t("objects.workload.available")}</dt>
                  <dd>{num(w.available)}</dd>
                </div>
                <div>
                  <dt>{t("objects.workload.updated")}</dt>
                  <dd>{num(w.updated)}</dd>
                </div>
              </>
            )}
          </>
        )}
      </dl>
      <dl className="facts">
        <dt>{t("objects.workload.selector")}</dt>
        <dd>{d.selector ? <code>{d.selector}</code> : "—"}</dd>
        <dt>{t("objects.workload.strategy")}</dt>
        <dd>{d.strategy ?? "—"}</dd>
        <dt>{t("objects.workload.created")}</dt>
        <dd>{formatDateTime(w.created)}</dd>
      </dl>
      <h3>{t("objects.workload.conditions")}</h3>
      <Conditions conditions={d.conditions} />
      <h3>HPA</h3>
      <PartView part={d.hpa}>
        {(h) =>
          h ? (
            <p>
              <code>{h.name}</code>: {num(h.minReplicas)}–{h.maxReplicas}, {t("objects.workload.hpaReplicas", { current: num(h.currentReplicas), desired: h.desiredReplicas })}
              {h.metrics.length > 0 && ` · ${h.metrics.join(", ")}`}
            </p>
          ) : (
            <p className="muted">—</p>
          )
        }
      </PartView>
      <h3>PodDisruptionBudget</h3>
      <PartView part={d.pdb}>
        {(p) =>
          p ? (
            <p>
              <code>{p.name}</code>: {p.minAvailable ? `minAvailable ${p.minAvailable}` : `maxUnavailable ${p.maxUnavailable ?? "—"}`},{" "}
              {t("objects.workload.pdbState", { allowed: p.allowedDisruptions, healthy: p.currentHealthy, desired: p.desiredHealthy })}
            </p>
          ) : (
            <p className="muted">—</p>
          )
        }
      </PartView>
      {d.history && (
        <>
          <h3>{t("objects.workload.history")}</h3>
          <PartView part={d.history}>
            {(revs) => (
              <table>
                <thead>
                  <tr>
                    <th>{t("objects.workload.revision")}</th>
                    <th>{t("objects.workload.revisionName")}</th>
                    <th>{t("objects.workload.images")}</th>
                    <th>{t("objects.workload.replicas")}</th>
                    <th>{t("objects.workload.age")}</th>
                  </tr>
                </thead>
                <tbody>
                  {revs.map((r) => (
                    <tr key={r.name} className={r.current ? "selected" : undefined}>
                      <td>
                        {r.revision}
                        {r.current && ` (${t("objects.workload.current")})`}
                      </td>
                      <td>{r.name}</td>
                      <td>{r.images.join(", ") || "—"}</td>
                      <td>{num(r.replicas)}</td>
                      <td>{formatAge(r.created)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </PartView>
        </>
      )}
      <h3>{t("objects.workload.containers")}</h3>
      <div className="cards">
        {d.containers.map((c) => (
          <ContainerCard key={`${c.init}:${c.name}`} c={c} />
        ))}
      </div>
      <h3>{t("objects.labels")}</h3>
      <KeyValues values={d.labels} />
      <h3>{t("objects.annotations")}</h3>
      <KeyValues values={d.annotations} />
    </>
  );
}
