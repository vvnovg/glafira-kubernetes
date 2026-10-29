import { useQuery } from "@tanstack/react-query";
import { Suspense, lazy, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { api } from "../../api/client";
import type { Analysis, Change, ContainerRecommendation, ContainerUsage, PodNow, WorkloadKind } from "../../api/types";
import { cpuText, memoryText, percentText, signedText, cpuQuantity, memoryQuantity } from "../../lib/quantity";
import { useConnection } from "../../lib/useConnection";
import { ChangeButton } from "../change/ChangeActions";
import { canSetResources } from "../change/changes";
import { ChangeDialog } from "../change/ChangeDialog";
import { Hint } from "../common/Hint";
import { ChunkBoundary } from "../common/ChunkBoundary";
import { ReadError } from "../common/ReadError";
import { useNamespaceAccess } from "../project/useNamespaceAccess";
import type { ChartLine } from "./chart";
import { DEFAULT_WINDOW, WINDOWS, effective, noteText, recommendationChange, sourceLabel, violationText } from "./recommend";

// uPlot — отдельный чанк: грузится, когда есть что рисовать.
const UsageChart = lazy(() => import("./UsageChart").then((m) => ({ default: m.UsageChart })));

interface Props {
  connectionId: string;
  namespace: string;
  kind: WorkloadKind;
  name: string;
}

/** Выбор окна анализа: 1, 7, 14 или 30 дней. */
export function WindowSelect({ days, onChange }: { days: number; onChange: (d: number) => void }) {
  const { t } = useTranslation();
  return (
    <span className="field-label">
      <label>
        {t("resources.window")}{" "}
        <select value={days} onChange={(e) => onChange(Number(e.target.value))}>
          {WINDOWS.map((d) => (
            <option key={d} value={d}>
              {t("resources.days", { count: d })}
            </option>
          ))}
        </select>
      </label>
      <Hint k="hints.resources.window" label={t("resources.window")} />
    </span>
  );
}

/**
 * «Ресурсы» workload'а (спец. §7.4): окно, источник, покрытие и уверенность; таблица «сейчас → рекомендация» по
 * контейнерам со статистикой худшего пода; итог на весь workload; графики; «Применить рекомендацию» открывает форму
 * «Ресурсы контейнеров» с рекомендованными значениями.
 */
export function WorkloadResources({ connectionId, namespace, kind, name }: Props) {
  const { t } = useTranslation();
  const [days, setDays] = useState(DEFAULT_WINDOW);
  const [form, setForm] = useState<Change | null>(null);
  const access = useNamespaceAccess(connectionId, namespace).data;
  const readOnly = useConnection(connectionId)?.readOnly === true;
  const q = useQuery({
    queryKey: ["resources", connectionId, namespace, kind, name, days],
    queryFn: () => api.recommendResources(connectionId, namespace, kind, name, days),
  });
  const a = q.data;
  const change = a ? recommendationChange(a.recommendation) : null;
  const low = a?.recommendation.confidence === "low";
  return (
    <div className="section">
      <div className="toolbar">
        <WindowSelect days={days} onChange={setDays} />
        <button type="button" disabled={q.isFetching} onClick={() => void q.refetch()}>
          {t("common.refresh")}
        </button>
        {change && change.kind === "setResources" && change.containers.length > 0 && canSetResources(kind) && (
          <ChangeButton change={change} access={access} readOnly={readOnly} label={t("resources.applyRecommendation")} onOpen={setForm} />
        )}
      </div>
      {q.isPending && <p className="muted">{t("common.loading")}</p>}
      {q.error && <ReadError error={q.error} connectionId={connectionId} onRetry={() => void q.refetch()} />}
      {a && <AnalysisView a={a} />}
      {form && (
        <ChangeDialog
          connectionId={connectionId}
          initial={form}
          notice={low ? t("resources.lowConfidence") : undefined}
          onClose={() => setForm(null)}
        />
      )}
    </div>
  );
}

function AnalysisView({ a }: { a: Analysis }) {
  const { t } = useTranslation();
  const { usage: u, recommendation: r } = a;
  const recs = new Map(r.containers.map((c) => [c.name, c]));
  return (
    <>
      <p className="muted">
        {t("resources.source", { source: sourceLabel(u.source) })} · {t("resources.days", { count: u.windowDays })}
        {u.coverage != null && <> · {t("resources.coverage", { value: percentText(u.coverage) })}</>}
        {u.dataSeconds != null && <> · {t("resources.dataDays", { count: Math.round((u.dataSeconds / 86_400) * 10) / 10 })}</>}
        {" · "}
        <span className={`confidence-${r.confidence}`}>{t("resources.confidence", { value: t(`resources.confidenceLevel.${r.confidence}`) })}</span>{" "}
        <Hint k="hints.resources.confidence" label={t("resources.confidenceLabel")} />
      </p>
      {!u.ownerMetrics && u.source.history && <p className="muted">{t("resources.noKubeStateMetrics")}</p>}
      <table className="resource-table">
        <thead>
          <tr>
            <th>{t("change.col.container")}</th>
            <th>{t("change.diff.requests.cpu")}</th>
            <th>{t("change.diff.limits.cpu")}</th>
            <th>{t("change.diff.requests.memory")}</th>
            <th>{t("change.diff.limits.memory")}</th>
            <th className="num">{t("resources.col.cpu")}</th>
            <th className="num">{t("resources.col.memory")}</th>
            <th className="num">{t("resources.col.throttling")}</th>
            <th className="num">{t("resources.col.oom")}</th>
            <th className="num">{t("resources.col.restarts")}</th>
          </tr>
        </thead>
        <tbody>
          {u.containers.map((c) => (
            <ContainerRow key={`${c.init}:${c.name}`} c={c} rec={recs.get(c.name)} />
          ))}
        </tbody>
      </table>
      {r.containers.flatMap((c) => c.notes.map((n) => <p key={`${c.name}:${JSON.stringify(n)}`} className="muted">{`${c.name}: ${noteText(n, t)}`}</p>))}
      <p>
        {t("resources.totals", {
          requestsCpu: signedText(r.totals.requests.cpu, cpuQuantity),
          requestsMemory: signedText(r.totals.requests.memory, memoryQuantity),
          limitsCpu: signedText(r.totals.limits.cpu, cpuQuantity),
          limitsMemory: signedText(r.totals.limits.memory, memoryQuantity),
          replicas: r.replicas ?? "—",
        })}
      </p>
      {r.violations.map((v) => (
        <p key={JSON.stringify(v)} className="problem">
          {violationText(v, t)}
        </p>
      ))}
      {r.hpa.map((h) => (
        <p key={h.hpa + h.resource} className="problem">
          {t("resources.hpaShift", { ...h })}
        </p>
      ))}
      {u.charts && <UsageCharts a={a} />}
      {u.podsNow.length > 0 && <PodsNow pods={u.podsNow} />}
    </>
  );
}

function pair(now: number | null, rec: number | null | undefined, text: (v: number | null) => string) {
  if (rec === undefined || rec === now) return <td>{text(now)}</td>;
  return (
    <td className="changed">
      {text(now)} → {text(rec)}
    </td>
  );
}

function ContainerRow({ c, rec }: { c: ContainerUsage; rec?: ContainerRecommendation }) {
  const { t } = useTranslation();
  const now = effective(c);
  const r = rec?.recommended ?? undefined;
  const s = c.stats;
  return (
    <tr>
      <td>
        {c.name}
        {c.init && !c.sidecar && <span className="muted"> ({t("resources.init")})</span>}
      </td>
      {pair(now.requests.cpu, r?.requests.cpu, cpuText)}
      {pair(now.limits.cpu, r?.limits.cpu, cpuText)}
      {pair(now.requests.memory, r?.requests.memory, memoryText)}
      {pair(now.limits.memory, r?.limits.memory, memoryText)}
      <td className="num">
        {cpuText(s.cpu.p95)} / {cpuText(s.cpu.max)}
      </td>
      <td className="num">{memoryText(s.memory.max)}</td>
      <td className="num">{percentText(s.throttling.mean)}</td>
      <td className="num">{s.oomCount ?? "—"}</td>
      <td className="num">{s.restarts ?? "—"}</td>
    </tr>
  );
}

/** Графики CPU и памяти: худший под по контейнерам, линии текущих request и limit, отметки рестартов и OOM. */
function UsageCharts({ a }: { a: Analysis }) {
  const { t } = useTranslation();
  const charts = a.usage.charts!;
  const lines = useMemo(() => {
    const out: { cpu: ChartLine[]; memory: ChartLine[] } = { cpu: [], memory: [] };
    for (const c of a.usage.containers.filter((x) => !x.init || x.sidecar)) {
      const e = effective(c);
      for (const res of ["cpu", "memory"] as const) {
        if (e.requests[res] != null) out[res].push({ label: `${c.name} request`, value: e.requests[res]!, dash: "request" });
        if (e.limits[res] != null) out[res].push({ label: `${c.name} limit`, value: e.limits[res]!, dash: "limit" });
      }
    }
    return out;
  }, [a]);
  const markers = useMemo(() => [...charts.restarts, ...charts.ooms], [charts]);
  return (
    <ChunkBoundary fallback={<p className="muted">{t("resources.chartFailed")}</p>}>
      <Suspense fallback={<p className="muted">{t("common.loading")}</p>}>
        <UsageChart title={t("resources.chartCpu")} series={charts.cpu} lines={lines.cpu} markers={markers} markerLabel={t("resources.marks")} format={cpuQuantity} />
        <UsageChart title={t("resources.chartMemory")} series={charts.memory} lines={lines.memory} markers={markers} markerLabel={t("resources.marks")} format={memoryQuantity} />
      </Suspense>
    </ChunkBoundary>
  );
}

/** Только metrics.k8s.io: текущее потребление подов. */
export function PodsNow({ pods }: { pods: PodNow[] }) {
  const { t } = useTranslation();
  return (
    <>
      <h3>{t("resources.now")}</h3>
      <table className="resource-table">
        <thead>
          <tr>
            <th>{t("resources.col.pod")}</th>
            <th>{t("change.col.container")}</th>
            <th className="num">CPU</th>
            <th className="num">{t("resources.col.memoryNow")}</th>
          </tr>
        </thead>
        <tbody>
          {pods.flatMap((p) =>
            p.containers.map((c) => (
              <tr key={`${p.pod}:${c.name}`}>
                <td>{p.pod}</td>
                <td>{c.name}</td>
                <td className="num">{cpuText(c.cpu)}</td>
                <td className="num">{memoryText(c.memory)}</td>
              </tr>
            )),
          )}
        </tbody>
      </table>
    </>
  );
}

/** «Ресурсы» пода (спец. §6.5): текущее потребление по контейнерам. */
export function PodResources({ connectionId, namespace, pod }: { connectionId: string; namespace: string; pod: string }) {
  const { t } = useTranslation();
  const q = useQuery({ queryKey: ["resources", connectionId, namespace, "pod", pod], queryFn: () => api.getPodUsage(connectionId, namespace, pod) });
  return (
    <div className="section">
      <div className="toolbar">
        <button type="button" disabled={q.isFetching} onClick={() => void q.refetch()}>
          {t("common.refresh")}
        </button>
      </div>
      {q.isPending && <p className="muted">{t("common.loading")}</p>}
      {q.error && <ReadError error={q.error} connectionId={connectionId} onRetry={() => void q.refetch()} />}
      {q.data && (
        <>
          <p className="muted">{t("resources.source", { source: sourceLabel(q.data.source) })}</p>
          <PodsNow pods={[{ pod, containers: q.data.containers }]} />
        </>
      )}
    </div>
  );
}
