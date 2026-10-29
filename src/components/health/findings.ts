import type { TFunction } from "i18next";
import type { Annotation, Finding, HealthTarget, HealthView, Proposal, Severity, WorkloadKind } from "../../api/types";

export const SEVERITY_RANK: Record<Severity, number> = { info: 0, warning: 1, critical: 2 };
export const SEVERITIES: Severity[] = ["critical", "warning", "info"];

const str = (v: unknown) => (typeof v === "string" ? v : "");
const list = (v: unknown) => (Array.isArray(v) ? v.map(String).join(", ") : "");

/** Сколько пробелов `check.partial` показывает текст; в `params` их список целиком (как `MAX_LISTED` в Rust). */
export const GAPS_SHOWN = 20;

/** Первые `GAPS_SHOWN` пробелов, дальше «…». */
const gapsText = (v: unknown) => (Array.isArray(v) ? `${list(v.slice(0, GAPS_SHOWN))}${v.length > GAPS_SHOWN ? ", …" : ""}` : "");

/** Объект словами: «Deployment team-a/web», «под team-a/web-1», «нода w1», «кластер». */
export function objectText(target: HealthTarget, t: TFunction): string {
  // Не `ns`: так i18next называет пространство имён словаря.
  return t(`health.object.${target.kind}`, {
    project: target.namespace ?? "",
    name: target.name ?? "",
    workloadKind: target.workloadKind ?? "",
  });
}

/**
 * Параметры текста находки (`health.rules.<ruleId>.*`): объект словами (`object`), списки через запятую,
 * затронутые поды workload'а (`affected`: « в 2 из 3 подов (web-1, web-2)», у пода — пусто), причина
 * (`because`: « (reason: message)») и части неполной проверки (`parts`).
 */
export function findingParams(f: Finding, t: TFunction): Record<string, string | number> {
  const p: Record<string, string | number> = { name: f.target.name ?? "", project: f.target.namespace ?? "", object: objectText(f.target, t) };
  for (const [k, v] of Object.entries(f.params)) {
    // Списки объектов (операторы с условиями) в текст не идут: у них есть список имён.
    if (Array.isArray(v)) {
      if (v.every((x) => typeof x !== "object" || x === null)) p[k] = list(v);
    } else if (typeof v === "number" || typeof v === "string") p[k] = v;
    else if (typeof v === "boolean") p[k] = String(v);
  }
  p.affected =
    f.target.kind === "workload" && Array.isArray(f.params.pods)
      ? t("health.podsIn", { count: Number(f.params.count ?? 0), total: Number(f.params.total ?? 0), pods: list(f.params.pods) })
      : "";
  const reason = str(f.params.reason);
  // Точку в конце сообщения Kubernetes ставит сам текст находки, иначе выходит «….).».
  const message = str(f.params.message).replace(/[.\s]+$/, "");
  p.because = reason && message ? ` (${reason}: ${message})` : reason || message ? ` (${reason || message})` : "";
  if (typeof f.params.days === "number") p.daysAgo = -f.params.days;
  if (f.ruleId === "check.partial") {
    const parts: string[] = [];
    if (f.params.projectLimit === true) {
      parts.push(t("health.partialParts.projects", { checked: Number(f.params.projectsChecked ?? 0), total: Number(f.params.projectsTotal ?? 0) }));
    }
    if (f.params.podLimit === true) parts.push(t("health.partialParts.pods"));
    if (f.params.timedOut === true) parts.push(t("health.partialParts.timedOut"));
    if (Array.isArray(f.params.gaps) && f.params.gaps.length > 0) parts.push(t("health.partialParts.gaps", { gaps: gapsText(f.params.gaps) }));
    p.parts = parts.join("; ");
  }
  return p;
}

/** Ключ пояснения: у части правил текст зависит от параметров. */
export function detailsKey(f: Finding): string {
  const p = f.params;
  switch (f.ruleId) {
    case "workload.singleReplica":
      return typeof p.hpa === "string" ? "detailsHpa" : "details";
    case "pvc.pending":
      return p.phase === "Lost" ? "detailsLost" : "details";
    case "route.notAdmitted":
      return p.reason === "noStatus" ? "detailsNoStatus" : "details";
    case "route.certExpiring":
      return p.expired === true ? "detailsExpired" : "details";
    case "ingress.classMissing":
      return typeof p.class === "string" ? "details" : "detailsNoClass";
    case "service.noEndpoints":
      return Array.isArray(p.referencedBy) && p.referencedBy.length > 0 ? "detailsUsedBy" : "details";
    case "check.partial":
      return [p.projectLimit, p.podLimit, p.timedOut].includes(true) || (Array.isArray(p.gaps) && p.gaps.length > 0) ? "details" : "detailsUnknown";
    // Пустой список не называется: «без request CPU: ;» читается как сбой.
    case "container.noRequests":
    case "resources.overProvisioned":
      if (Array.isArray(p.cpu) && p.cpu.length === 0) return "detailsMemory";
      return Array.isArray(p.memory) && p.memory.length === 0 ? "detailsCpu" : "details";
    default:
      return "details";
  }
}

/** Параметра нет (`null` из Rust, правило его не дало) — «—», а не `{{имя}}` в тексте. */
const missing = () => "—";

export function findingTexts(f: Finding, t: TFunction) {
  const p = { ...findingParams(f, t), missingInterpolationHandler: missing };
  const base = `health.rules.${f.ruleId}`;
  return { title: capitalized(t(`${base}.title`, p)), details: t(`${base}.${detailsKey(f)}`, p), fix: t(`${base}.fix`, p) };
}

/** Заголовок начинается с объекта, а «под …», «нода …», «the cluster» пишутся со строчной. */
function capitalized(s: string): string {
  return s.charAt(0).toLocaleUpperCase() + s.slice(1);
}

/** Находки последней проверки: скрытые — только по запросу. */
export function findingsOf(view: HealthView | undefined, showHidden: boolean): Finding[] {
  if (!view?.lastCheck) return [];
  const hidden = new Set(view.ignored);
  return view.lastCheck.findings.filter((f) => showHidden || !hidden.has(f.key));
}

export function openNotes(view: HealthView | undefined): Annotation[] {
  return (view?.annotations ?? []).filter((a) => a.status === "open");
}

export function openProposals(view: HealthView | undefined): Proposal[] {
  return (view?.proposals ?? []).filter((p) => p.status === "open");
}

/** Закрытые замечания и предложения — недавно закрытые сверху (по времени закрытия, иначе обновления). */
export function newestClosedFirst<T extends { closedAtMs?: number; updatedAtMs: number }>(items: T[]): T[] {
  return [...items].sort((a, b) => (b.closedAtMs ?? b.updatedAtMs) - (a.closedAtMs ?? a.updatedAtMs));
}

/**
 * Относится ли находка или замечание к объекту экрана. `params` — у находки: по ним под узнаёт
 * находку своего workload'а (поды одного workload'а дают одну находку со списком `pods`).
 */
export type Match = (target: HealthTarget, params?: Record<string, unknown>) => boolean;

/** Обзор кластера: сам кластер и ноды. */
export const matchCluster: Match = (t) => t.kind === "cluster" || t.kind === "node";

/** Проект и всё в нём. */
export const matchProject =
  (namespace: string): Match =>
  (t) =>
    t.namespace === namespace;

export const matchWorkload =
  (namespace: string, kind: WorkloadKind, name: string): Match =>
  (t) =>
    t.kind === "workload" && t.namespace === namespace && t.workloadKind === kind && t.name === name;

/** Под: его собственные находки и находки его workload'а, в списке подов которых он есть. */
export const matchPod =
  (namespace: string, name: string): Match =>
  (t, params) =>
    t.namespace === namespace &&
    ((t.kind === "pod" && t.name === name) || (t.kind === "workload" && Array.isArray(params?.pods) && params.pods.includes(name)));

/** Что относится к объекту: видимые находки, открытые замечания и открытые предложения. Без `match` — всё подключение. */
export function itemsFor(view: HealthView | undefined, match?: Match) {
  const m = match ?? (() => true);
  return {
    findings: findingsOf(view, false).filter((f) => m(f.target, f.params)),
    notes: openNotes(view).filter((a) => m(a.target)),
    proposals: openProposals(view).filter((p) => m(p.target)),
  };
}

/** Что посчитал значок: находка, открытое замечание или открытое предложение уровня «критично» или «предупреждение». */
export type BadgeItem =
  | { kind: "finding"; severity: Severity; finding: Finding }
  | { kind: "note"; severity: Severity; note: Annotation }
  | { kind: "proposal"; severity: Severity; proposal: Proposal };

export interface Badge {
  count: number;
  severity: Severity;
  /** Самые строгие первыми; внутри строгости — сначала находки, затем замечания, затем предложения; порядок данных сохраняется. */
  items: BadgeItem[];
}

/** Сколько заголовков показывает подсказка значка; остальное сворачивается в «…». */
export const BADGE_TITLES_MAX = 10;

/** Значок: число видимых находок, открытых замечаний и предложений уровня «критично» и «предупреждение»; без `match` — всего подключения. */
export function badgeFor(view: HealthView | undefined, match?: Match): Badge | null {
  const { findings, notes, proposals } = itemsFor(view, match);
  const items: BadgeItem[] = [
    ...findings.map((finding): BadgeItem => ({ kind: "finding", severity: finding.severity, finding })),
    ...notes.map((note): BadgeItem => ({ kind: "note", severity: note.severity, note })),
    ...proposals.map((proposal): BadgeItem => ({ kind: "proposal", severity: proposal.severity, proposal })),
  ]
    .filter((i) => i.severity !== "info")
    // Сортировка стабильна: порядок внутри одной строгости остаётся прежним.
    .sort((a, b) => SEVERITY_RANK[b.severity] - SEVERITY_RANK[a.severity]);
  if (items.length === 0) return null;
  return { count: items.length, severity: items[0].severity, items };
}

const oneLine = (s: string) => s.replace(/\s+/g, " ").trim();

/**
 * Подсказка значка: первая строка — «Требуют внимания: N», далее заголовки учтённого (не больше 10, затем «…»).
 * Строки через перевод строки: нативная подсказка `title` его показывает. Пробелы и переводы строк
 * в заголовке замечания или предложения LLM схлопываются: одна строка подсказки — один заголовок.
 */
export function badgeTooltip(badge: Badge, t: TFunction): string {
  const titles = badge.items
    .slice(0, BADGE_TITLES_MAX)
    .map((i) => (i.kind === "finding" ? findingTexts(i.finding, t).title : i.kind === "note" ? oneLine(i.note.title) : oneLine(i.proposal.title)));
  if (badge.items.length > BADGE_TITLES_MAX) titles.push("…");
  return [t("health.badgeTitle", { count: badge.count }), ...titles].join("\n");
}
