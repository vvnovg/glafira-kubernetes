import { describe, expect, it } from "vitest";
import type { Recommendation } from "../../api/types";
import { i18n } from "../../i18n";
import { noteText, recommendationChange, sourceLabel, violationText } from "./recommend";

const t = i18n.t.bind(i18n);
const MI = 1024 * 1024;

describe("рекомендация", () => {
  it("в форму идут рекомендованные значения quantity-строками; контейнер без данных не меняется", () => {
    const r = {
      namespace: "team-a",
      kind: "Deployment",
      name: "web",
      containers: [
        { name: "app", current: {}, recommended: { requests: { cpu: 0.02, memory: 32 * MI }, limits: { cpu: null, memory: 48 * MI } }, notes: [] },
        { name: "proxy", current: {}, recommended: null, notes: [{ code: "noData" }] },
      ],
    } as unknown as Recommendation;
    expect(recommendationChange(r)).toEqual({
      kind: "setResources",
      namespace: "team-a",
      workload: { kind: "Deployment", name: "web" },
      containers: [{ name: "app", requests: { cpu: "20m", memory: "32Mi" }, limits: { memory: "48Mi" } }],
    });
  });

  it("источник, пометки и нарушения словами", () => {
    expect(sourceLabel({ kind: "prometheus", origin: "auto", location: "monitoring/prometheus-k8s:9090", history: true })).toBe("Prometheus monitoring/prometheus-k8s:9090");
    expect(noteText({ code: "cpuThrottled", p95: 0.6 }, t)).toBe("Троттлинг p95 — 60 %: наблюдаемое потребление CPU ограничено лимитом, limit не меньше текущего × 1,5");
    expect(violationText({ code: "nodeAllocatable", container: "app", resource: "memory", request: 64 * 1024 * MI, allocatable: 32 * 1024 * MI }, t)).toBe(
      "app: request memory 64Gi больше allocatable самой крупной ноды 32Gi",
    );
  });
});
