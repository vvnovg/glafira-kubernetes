import type { TFunction } from "i18next";
import type { Change, Proposal, ResourceValues } from "../../api/types";
import { formatAgo } from "../../lib/formatAgo";
import type { TabSpec } from "../../state/workspace";

/**
 * Вкладка, в которой предложение открывается заполненной формой (спец. §10.5): у изменений workload'а — его вкладка,
 * у границ HPA — вкладка проекта.
 */
export function proposalTab(connectionId: string, proposal: Proposal, nonce = Date.now()): TabSpec {
  const prefill = { proposal, nonce };
  const c = proposal.change;
  if (c.kind === "setHpaBounds" || c.kind === "deletePod") return { kind: "project", connectionId, namespace: c.namespace, proposal: prefill };
  return { kind: "workload", connectionId, namespace: c.namespace, workloadKind: c.workload.kind, name: c.workload.name, proposal: prefill };
}

/** Значения ресурсов словами: «CPU request 250m, memory limit — убрать». */
function valuesText(part: "requests" | "limits", v: ResourceValues, t: TFunction): string[] {
  return (["cpu", "memory"] as const).flatMap((r) =>
    v[r] === undefined ? [] : [`${t(`change.diff.${part}.${r}`)} ${v[r] === null ? t("health.proposals.change.remove") : v[r]}`],
  );
}

/** Описание изменения для карточки предложения: что именно меняется, на языке интерфейса. */
export function changeSummary(change: Change, t: TFunction): string {
  switch (change.kind) {
    case "setResources":
      return `${t("health.proposals.change.setResources")} ${change.containers
        .map((c) => `${c.name} — ${[...valuesText("requests", c.requests, t), ...valuesText("limits", c.limits, t)].join(", ")}`)
        .join("; ")}`;
    case "scale":
      return t("health.proposals.change.scale", { replicas: change.replicas });
    case "rolloutRestart":
      return t("health.proposals.change.rolloutRestart");
    case "setHpaBounds":
      return t("health.proposals.change.setHpaBounds", { hpa: change.hpa, min: change.minReplicas ?? t("health.proposals.change.keep"), max: change.maxReplicas });
    case "deletePod":
      return t("change.title.deletePod");
  }
}

/** Статус закрытого предложения: «применено 5 минут назад», «применено с правками …», «отклонено: …», «отозвано». */
export function proposalStatusText(p: Proposal, t: TFunction, nowMs: number, lang: string): string | null {
  switch (p.status) {
    case "open":
      return null;
    case "applied": {
      const when = formatAgo(p.closedAtMs ?? p.updatedAtMs, nowMs, lang);
      return p.appliedAsProposed === false ? t("health.proposals.status.appliedEdited", { when }) : t("health.proposals.status.applied", { when });
    }
    case "rejected":
      return p.rejectReason ? t("health.proposals.status.rejectedWithReason", { reason: p.rejectReason }) : t("health.proposals.status.rejected");
    case "withdrawn":
      return t("health.proposals.status.withdrawn");
  }
}
