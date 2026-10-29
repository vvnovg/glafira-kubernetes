import type { TFunction } from "i18next";
import type { Excess, Risk, WorkloadResources } from "../../api/types";
import { cpuQuantity, memoryQuantity } from "../../lib/quantity";

/** Строк в таблице проекта (спец. §7.4): дальше — пометка и фильтр по имени. */
export const MAX_ROWS = 100;

export type Sort = "risk" | "reserve";

/** Риск контейнера словами: «app: троттлинг 40 %», «app: OOM 2», «app: память 950Mi из 1Gi». */
export function riskText(r: Risk, t: TFunction): string {
  switch (r.code) {
    case "throttled":
      return t("resources.risk.throttled", { container: r.container, value: Math.round(r.mean * 100) });
    case "oom":
      return t("resources.risk.oom", { container: r.container, count: r.count });
    case "memoryNearLimit":
      return t("resources.risk.memoryNearLimit", { container: r.container, max: memoryQuantity(r.max), limit: memoryQuantity(r.limit) });
  }
}

/** Избыток словами: «app: CPU request 2 при p95 300m». */
export function excessText(e: Excess, t: TFunction): string {
  const f = e.resource === "cpu" ? cpuQuantity : memoryQuantity;
  return t(`resources.excess.${e.resource}`, { container: e.container, request: f(e.request), usage: f(e.usage) });
}

/** Есть что поменять: риск или избыток — как `WorkloadResources::actionable` в Rust. */
export const actionable = (w: WorkloadResources) => w.risks.length > 0 || w.excess.length > 0;

/**
 * Строки таблицы: фильтр «только с рекомендациями» и по имени, сортировка «по риску» (больше рисков — выше, затем
 * избыток) или «по резерву» (больший резерв CPU, затем памяти — выше); имя — при равенстве.
 */
export function projectRows(list: WorkloadResources[], sort: Sort, onlyActionable: boolean, query: string): WorkloadResources[] {
  const q = query.trim().toLowerCase();
  const rows = list.filter((w) => (!onlyActionable || actionable(w)) && (q === "" || w.name.toLowerCase().includes(q)));
  const byName = (a: WorkloadResources, b: WorkloadResources) => a.name.localeCompare(b.name);
  if (sort === "risk") return rows.sort((a, b) => b.risks.length - a.risks.length || b.excess.length - a.excess.length || byName(a, b));
  return rows.sort((a, b) => (b.reserve.cpu ?? 0) - (a.reserve.cpu ?? 0) || (b.reserve.memory ?? 0) - (a.reserve.memory ?? 0) || byName(a, b));
}
