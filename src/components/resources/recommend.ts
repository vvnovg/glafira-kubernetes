import type { TFunction } from "i18next";
import type { Amounts, Change, ContainerCurrent, MetricsSourceInfo, Recommendation, RecommendationNote, ResourceValues, Violation } from "../../api/types";
import { cpuQuantity, memoryQuantity } from "../../lib/quantity";
import { quotaAmount } from "../change/changes";

/** Окна анализа (спец. §7.2); по умолчанию — 7 дней. */
export const WINDOWS = [1, 7, 14, 30] as const;
export const DEFAULT_WINDOW = 7;

/** Источник словами, как `SourceInfo::label` в Rust: «Prometheus monitoring/prometheus-k8s:9090», «metrics.k8s.io». */
export function sourceLabel(s: MetricsSourceInfo): string {
  if (s.kind === "thanos") return `Thanos Querier ${s.location}`;
  if (s.kind === "prometheus") return `Prometheus ${s.location}`;
  return s.location;
}

const values = (a: Amounts): ResourceValues => ({
  ...(a.cpu != null && { cpu: cpuQuantity(a.cpu) }),
  ...(a.memory != null && { memory: memoryQuantity(a.memory) }),
});

/** Форма «Ресурсы контейнеров» из рекомендации: рекомендованные значения; чего нет — не меняется. */
export function recommendationChange(r: Recommendation): Change {
  return {
    kind: "setResources",
    namespace: r.namespace,
    workload: { kind: r.kind, name: r.name },
    containers: r.containers.flatMap((c) => (c.recommended ? [{ name: c.name, requests: values(c.recommended.requests), limits: values(c.recommended.limits) }] : [])),
  };
}

/** Действующие requests и limits: из шаблона, без значения в нём — `LimitRange` по умолчанию. */
export function effective(c: ContainerCurrent): { requests: Amounts; limits: Amounts } {
  const pick = (a: Amounts, d: Amounts): Amounts => ({ cpu: a.cpu ?? d.cpu, memory: a.memory ?? d.memory });
  return { requests: pick(c.requests, c.limitRangeRequests), limits: pick(c.limits, c.limitRangeLimits) };
}

/** Пометка к рекомендации контейнера словами. */
export function noteText(n: RecommendationNote, t: TFunction): string {
  switch (n.code) {
    case "cpuThrottled":
      return t("resources.note.cpuThrottled", { p95: Math.round(n.p95 * 100) });
    case "oomKilled":
      return t("resources.note.oomKilled", { count: n.count });
    case "limitRange":
      return t("resources.note.limitRange", { field: t(`change.fieldName.${n.field}`), bound: t(`resources.bound.${n.bound}`) });
    case "limitRangeDefault":
      return t("resources.note.limitRangeDefault", { field: t(`change.fieldName.${n.field}`) });
    default:
      return t(`resources.note.${n.code}`);
  }
}

/** Нарушение ограничения кластера словами. */
export function violationText(v: Violation, t: TFunction): string {
  if (v.code === "quota") {
    return t("change.warning.quota", { quota: v.quota, resource: v.resource, needed: quotaAmount(v.resource, v.needed), free: quotaAmount(v.resource, v.free) });
  }
  return t("resources.violation.nodeAllocatable", {
    container: v.container,
    resource: v.resource,
    request: quotaAmount(v.resource, v.request),
    allocatable: quotaAmount(v.resource, v.allocatable),
  });
}
