import type { Annotation, Finding, HealthTarget, HealthView } from "../api/types";

/** Находка для тестов: ключ — из правила и объекта. */
export function finding(ruleId: string, severity: Finding["severity"], target: HealthTarget, params: Record<string, unknown> = {}): Finding {
  const object = [target.workloadKind, target.name].filter(Boolean).join("/");
  return { key: `${ruleId}:${target.kind}:${target.namespace ?? ""}:${object}`, ruleId, severity, target, params };
}

/** Замечание LLM для тестов: по умолчанию — открытое, о Deployment team-a/`name`. */
export function note(severity: Annotation["severity"], name: string, status: Annotation["status"] = "open", title = "t"): Annotation {
  return {
    id: name + severity + title + status,
    target: { kind: "workload", namespace: "team-a", name, workloadKind: "Deployment" },
    severity,
    title,
    body: "b",
    client: "claude-ai",
    createdAtMs: 1,
    updatedAtMs: 1,
    status,
  };
}

/** Состояние подключения после проверки `findings` (prod, интерфейс, 2 проекта). */
export function checkedView(findings: Finding[], annotations: Annotation[] = [], ignored: string[] = []): HealthView {
  return {
    lastCheck: {
      runId: "r1",
      startedAtMs: Date.now(),
      durationMs: 1,
      trigger: { kind: "ui" },
      environment: "prod",
      incomplete: false,
      scope: { projects: ["team-a", "team-b"], projectsTotal: 2, pods: 12, nodes: 3 },
      findings,
    },
    ignored,
    annotations,
  };
}
