import type { TFunction } from "i18next";
import type { Change, ChangeWarning, NamespaceAccess, ResourceField, WorkloadKind } from "../../api/types";
import { cpuQuantity, memoryQuantity } from "../../lib/quantity";

/** Ресурс прав (`ресурс.группа`) вида workload'а. */
export const WORKLOAD_RESOURCE: Record<WorkloadKind, string> = {
  Deployment: "deployments.apps",
  StatefulSet: "statefulsets.apps",
  DaemonSet: "daemonsets.apps",
  DeploymentConfig: "deploymentconfigs.apps.openshift.io",
  Job: "jobs.batch",
  CronJob: "cronjobs.batch",
};

const SCALE_RESOURCE: Partial<Record<WorkloadKind, string>> = {
  Deployment: "deployments/scale.apps",
  StatefulSet: "statefulsets/scale.apps",
  DeploymentConfig: "deploymentconfigs/scale.apps.openshift.io",
};

/** Что можно сделать с видом workload'а (спец. §9): шаблон Job неизменяем, Job и CronJob не перезапускаются. */
export const canSetResources = (k: WorkloadKind) => k !== "Job";
export const canScale = (k: WorkloadKind) => k in SCALE_RESOURCE;
export const canRestart = (k: WorkloadKind) => k !== "Job" && k !== "CronJob";

type Verb = "patch" | "delete" | "create";

/** Право, которое нужно изменению: ресурс и глагол. */
export function changeRight(c: Change): { resource: string; verb: Verb } {
  switch (c.kind) {
    case "setResources":
      return { resource: WORKLOAD_RESOURCE[c.workload.kind], verb: "patch" };
    case "scale":
      return { resource: SCALE_RESOURCE[c.workload.kind] ?? WORKLOAD_RESOURCE[c.workload.kind], verb: "patch" };
    case "rolloutRestart":
      return c.workload.kind === "DeploymentConfig"
        ? { resource: "deploymentconfigs/instantiate.apps.openshift.io", verb: "create" }
        : { resource: WORKLOAD_RESOURCE[c.workload.kind], verb: "patch" };
    case "setHpaBounds":
      return { resource: "horizontalpodautoscalers.autoscaling", verb: "patch" };
    case "deletePod":
      return { resource: "pods", verb: "delete" };
  }
}

/**
 * Разрешено ли изменение (спец. §5.4): права неизвестны, перечислены не целиком или ресурс не из списка — «да»,
 * окончательно решает API-сервер.
 */
export function allowed(access: NamespaceAccess | undefined, c: Change): boolean {
  if (!access || access.incomplete) return true;
  const { resource, verb } = changeRight(c);
  return access.resources[resource]?.[verb] ?? true;
}

/** Только контейнеры, где что-то меняется: пустые строки формы не отправляются. */
export function withValues(c: Change): Change {
  if (c.kind !== "setResources") return c;
  const set = (v: { cpu?: string | null; memory?: string | null }) => v.cpu !== undefined || v.memory !== undefined;
  return { ...c, containers: c.containers.filter((x) => set(x.requests) || set(x.limits)) };
}

/** Есть что отправить: у ресурсов — хотя бы одно поле. */
export function hasValues(c: Change): boolean {
  const w = withValues(c);
  return w.kind !== "setResources" || w.containers.length > 0;
}

const isCpu = (s: string) => s.toLowerCase().includes("cpu");

/** Значение поля ресурсов: у CPU — ядра, у памяти — байты, у `maxLimitRequestRatio` — само отношение. */
export function fieldAmount(field: ResourceField, v: number, ratio = false): string {
  if (ratio) return String(v);
  return isCpu(field) ? cpuQuantity(v) : memoryQuantity(v);
}

/** Значение ресурса квоты: `requests.cpu` — ядра, `limits.memory` — байты. */
export const quotaAmount = (resource: string, v: number) => (isCpu(resource) ? cpuQuantity(v) : memoryQuantity(v));

/** Предупреждение к изменению словами. */
export function warningText(w: ChangeWarning, t: TFunction): string {
  switch (w.code) {
    case "hpaManaged":
      return t("change.warning.hpaManaged", { hpa: w.hpa });
    case "scaleToZero":
      return t("change.warning.scaleToZero");
    case "limitRange":
      return t("change.warning.limitRange", {
        container: w.container,
        field: t(`change.fieldName.${w.field}`),
        bound: t(`change.bound.${w.bound}`),
        limit: fieldAmount(w.field, w.limit, w.bound === "ratio"),
      });
    case "quota":
      return t("change.warning.quota", { quota: w.quota, resource: w.resource, needed: quotaAmount(w.resource, w.needed), free: quotaAmount(w.resource, w.free) });
    case "unverified":
      return t("change.warning.unverified", { what: t(`change.unverifiedWhat.${w.what}`) });
    case "orphan":
      return t("change.warning.orphan");
  }
}

/** Объект изменения словами: «Deployment web», «HPA web-hpa», «под web-1». */
export function changeObject(c: Change): string {
  switch (c.kind) {
    case "setHpaBounds":
      return `HPA ${c.hpa}`;
    case "deletePod":
      return `Pod ${c.pod}`;
    default:
      return `${c.workload.kind} ${c.workload.name}`;
  }
}
