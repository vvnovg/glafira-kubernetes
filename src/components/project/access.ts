import type { NamespaceAccess } from "../../api/types";

/**
 * Можно ли читать список ресурса в проекте (спец. §5.4): права ещё неизвестны, не перечислены
 * целиком (`incomplete`) или ресурс не из списка — «можно», окончательно решает API-сервер.
 * `key` — `ресурс.группа`: `pods`, `deployments.apps`.
 */
export function canList(access: NamespaceAccess | undefined, ...keys: string[]): boolean {
  if (!access || access.incomplete) return true;
  return keys.some((k) => access.resources[k]?.list ?? true);
}

export const WORKLOAD_RESOURCES = [
  "deployments.apps",
  "statefulsets.apps",
  "daemonsets.apps",
  "jobs.batch",
  "cronjobs.batch",
  "deploymentconfigs.apps.openshift.io",
];
