import { describe, expect, it } from "vitest";
import type { Change, Proposal } from "../../api/types";
import { i18n } from "../../i18n";
import { changeSummary, proposalStatusText, proposalTab } from "./proposals";

const t = i18n.getFixedT("ru");
const web = { kind: "Deployment", name: "web" } as const;

function proposal(p: Partial<Proposal> = {}): Proposal {
  return {
    id: "p1",
    severity: "warning",
    title: "Two replicas",
    rationale: "r",
    target: { kind: "workload", namespace: "team-a", name: "web", workloadKind: "Deployment" },
    change: { kind: "scale", namespace: "team-a", workload: web, replicas: 2 },
    client: "claude-ai",
    createdAtMs: 1,
    updatedAtMs: 1,
    status: "open",
    ...p,
  };
}

describe("changeSummary", () => {
  it("ресурсы по контейнерам: значение или «убрать»", () => {
    const c: Change = {
      kind: "setResources",
      namespace: "team-a",
      workload: web,
      containers: [{ name: "app", requests: { cpu: "250m" }, limits: { cpu: null, memory: "1Gi" } }],
    };
    expect(changeSummary(c, t)).toBe("Ресурсы: app — CPU request 250m, CPU limit убрать, Memory limit 1Gi");
  });

  it("реплики, перезапуск и границы HPA", () => {
    expect(changeSummary({ kind: "scale", namespace: "a", workload: web, replicas: 3 }, t)).toBe("Реплик: 3");
    expect(changeSummary({ kind: "rolloutRestart", namespace: "a", workload: web }, t)).toBe("Перезапуск");
    expect(changeSummary({ kind: "setHpaBounds", namespace: "a", hpa: "web", maxReplicas: 8 }, t)).toBe("Границы HPA web: не менять–8");
  });
});

describe("proposalTab", () => {
  it("изменение workload'а — его вкладка, границы HPA — вкладка проекта", () => {
    const p = proposal();
    expect(proposalTab("c1", p, 5)).toEqual({ kind: "workload", connectionId: "c1", namespace: "team-a", workloadKind: "Deployment", name: "web", proposal: { proposal: p, nonce: 5 } });
    const hpa = proposal({ change: { kind: "setHpaBounds", namespace: "team-a", hpa: "web", minReplicas: 2, maxReplicas: 6 } });
    expect(proposalTab("c1", hpa, 5)).toMatchObject({ kind: "project", namespace: "team-a" });
  });
});

describe("proposalStatusText", () => {
  it("статусы закрытого предложения", () => {
    expect(proposalStatusText(proposal(), t, 1, "ru")).toBeNull();
    expect(proposalStatusText(proposal({ status: "rejected", rejectReason: "позже" }), t, 1, "ru")).toBe("отклонено: позже");
    expect(proposalStatusText(proposal({ status: "withdrawn" }), t, 1, "ru")).toBe("отозвано");
  });
});
