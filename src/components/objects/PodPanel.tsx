import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { api } from "../../api/client";
import type { PodDetails, WorkloadKind } from "../../api/types";
import { formatDateTime } from "../../lib/age";
import { useRefetchInterval } from "../../lib/tabActive";
import { useWorkspace } from "../../state/workspace";
import { Conditions } from "../common/Conditions";
import { KeyValues } from "../common/KeyValues";
import { ReadError } from "../common/ReadError";
import { RefreshControls, useRefreshInterval } from "../common/RefreshControls";
import { SubTabs } from "../common/SubTabs";
import { YamlView } from "../common/YamlView";
import { matchPod } from "../health/findings";
import { HealthBlock } from "../health/HealthBlock";
import { LogsView } from "../logs/LogsView";
import { ContainerCard } from "./ContainerCard";
import { ObjectEvents } from "./EventList";

type Sub = "overview" | "logs" | "events" | "yaml";

const WORKLOAD_KINDS: string[] = ["Deployment", "StatefulSet", "DaemonSet", "DeploymentConfig", "Job", "CronJob"] satisfies WorkloadKind[];

/** Вкладка пода (спец. §6.5–6.6): обзор с состоянием каждого контейнера, логи, события, YAML. */
export function PodPanel({ connectionId, namespace, name }: { connectionId: string; namespace: string; name: string }) {
  const { t } = useTranslation();
  const [sub, setSub] = useState<Sub>("overview");
  // Контейнеры для логов — из того же запроса, что и обзор.
  const details = useQuery({ queryKey: ["pod", connectionId, namespace, name], queryFn: () => api.getPod(connectionId, namespace, name) });
  const tabs: { id: Sub; label: string }[] = [
    { id: "overview", label: t("objects.tabs.overview") },
    { id: "logs", label: t("logs.title") },
    { id: "events", label: t("objects.tabs.events") },
    { id: "yaml", label: "YAML" },
  ];
  return (
    <div className="page">
      <div className="toolbar">
        <h2>Pod {name}</h2>
        <span className="muted">{namespace}</span>
      </div>
      <SubTabs tabs={tabs} active={sub} onChange={setSub} />
      {sub === "overview" && <PodOverview connectionId={connectionId} namespace={namespace} name={name} />}
      {sub === "logs" && details.isPending && <p className="muted">{t("common.loading")}</p>}
      {sub === "logs" && details.data && <LogsView connectionId={connectionId} namespace={namespace} pod={name} containers={details.data.containers} />}
      {sub === "logs" && details.error && <ReadError error={details.error} connectionId={connectionId} onRetry={() => void details.refetch()} />}
      {sub === "events" && <ObjectEvents connectionId={connectionId} namespace={namespace} object={{ kind: "Pod", name }} />}
      {sub === "yaml" && <YamlView connectionId={connectionId} namespace={namespace} kind="Pod" name={name} />}
    </div>
  );
}

function PodOverview({ connectionId, namespace, name }: { connectionId: string; namespace: string; name: string }) {
  const { t } = useTranslation();
  const [interval, setInterval] = useRefreshInterval();
  const refetchInterval = useRefetchInterval(interval);
  const q = useQuery({
    queryKey: ["pod", connectionId, namespace, name],
    queryFn: () => api.getPod(connectionId, namespace, name),
    refetchInterval,
  });
  return (
    <div className="section">
      <RefreshControls interval={interval} onInterval={setInterval} onRefresh={() => void q.refetch()} busy={q.isFetching} />
      <HealthBlock connectionId={connectionId} match={matchPod(namespace, name)} showTarget />
      {q.isPending && <p className="muted">{t("common.loading")}</p>}
      {q.error && <ReadError error={q.error} connectionId={connectionId} onRetry={() => void q.refetch()} />}
      {q.data && <PodSummary connectionId={connectionId} namespace={namespace} d={q.data} />}
    </div>
  );
}

function PodSummary({ connectionId, namespace, d }: { connectionId: string; namespace: string; d: PodDetails }) {
  const { t } = useTranslation();
  const open = useWorkspace((s) => s.open);
  const p = d.pod;
  const owner = p.owner;
  return (
    <>
      <dl className="stats">
        <div>
          <dt>{t("objects.pods.status")}</dt>
          <dd className={p.problem ? "problem" : undefined}>{p.status}</dd>
        </div>
        <div>
          <dt>{t("objects.pods.ready")}</dt>
          <dd>
            {p.ready}/{p.containers}
          </dd>
        </div>
        <div>
          <dt>{t("objects.pods.restarts")}</dt>
          <dd>{p.restarts}</dd>
        </div>
        <div>
          <dt>QoS</dt>
          <dd>{p.qos ?? "—"}</dd>
        </div>
      </dl>
      <dl className="facts">
        <dt>{t("objects.pod.phase")}</dt>
        <dd>{p.phase}</dd>
        <dt>{t("objects.pods.node")}</dt>
        <dd>{p.node ?? "—"}</dd>
        <dt>{t("objects.pod.ip")}</dt>
        <dd>
          {d.podIp ?? "—"} <span className="muted">({t("objects.pod.hostIp", { ip: d.hostIp ?? "—" })})</span>
        </dd>
        <dt>{t("objects.pods.owner")}</dt>
        <dd>
          {owner && WORKLOAD_KINDS.includes(owner.kind) ? (
            <button
              type="button"
              className="link"
              onClick={() => open({ kind: "workload", connectionId, namespace, workloadKind: owner.kind as WorkloadKind, name: owner.name })}
            >
              {owner.kind}/{owner.name}
            </button>
          ) : owner ? (
            `${owner.kind}/${owner.name}`
          ) : (
            "—"
          )}
        </dd>
        <dt>{t("objects.pod.serviceAccount")}</dt>
        <dd>{d.serviceAccount ?? "—"}</dd>
        <dt>{t("objects.pod.started")}</dt>
        <dd>{formatDateTime(d.started)}</dd>
      </dl>
      <h3>{t("objects.workload.conditions")}</h3>
      <Conditions conditions={d.conditions} />
      <h3>{t("objects.workload.containers")}</h3>
      <div className="cards">
        {d.containers.map((c) => (
          <ContainerCard key={`${c.init}:${c.name}`} c={c} />
        ))}
      </div>
      <h3>{t("objects.pod.volumes")}</h3>
      {d.volumes.length === 0 ? (
        <p className="muted">—</p>
      ) : (
        <ul className="kv">
          {d.volumes.map((v) => (
            <li key={v.name}>
              <code>
                {v.name}: {v.kind}
                {v.source ? ` ${v.source}` : ""}
              </code>
            </li>
          ))}
        </ul>
      )}
      <h3>{t("objects.labels")}</h3>
      <KeyValues values={d.labels} />
      <h3>{t("objects.annotations")}</h3>
      <KeyValues values={d.annotations} />
    </>
  );
}
