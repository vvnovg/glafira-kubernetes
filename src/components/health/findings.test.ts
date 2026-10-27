import { describe, expect, it } from "vitest";
import type { Finding, HealthTarget } from "../../api/types";
import en from "../../locales/en.json";
import ru from "../../locales/ru.json";
import { i18n } from "../../i18n";
import { checkedView as view, finding, note } from "../../test/health";
import { BADGE_TITLES_MAX, badgeFor, badgeTooltip, detailsKey, findingsOf, findingTexts, itemsFor, newestClosedFirst } from "./findings";

const t = i18n.getFixedT("ru");
const web: HealthTarget = { kind: "workload", namespace: "team-a", name: "web", workloadKind: "Deployment" };

describe("findingTexts", () => {
  it("names the object and the affected pods of a workload", () => {
    const f = finding("pod.crashLoop", "critical", web, {
      pods: ["web-1", "web-2"],
      count: 2,
      total: 3,
      containers: ["web"],
      reason: "Error",
      exitCode: 1,
      restarts: 7,
    });
    const x = findingTexts(f, t);
    expect(x.title).toBe("Deployment team-a/web: контейнеры перезапускаются по кругу");
    expect(x.details).toBe("Контейнеры web падают и перезапускаются в 2 из 3 подов (web-1, web-2): последнее завершение Error, код 1, перезапусков до 7.");
  });

  it("a pod needs no list of pods; reason and message go in brackets", () => {
    const f = finding("pod.imagePull", "critical", { kind: "pod", namespace: "team-a", name: "pull" }, {
      image: "registry.invalid/x:1",
      reason: "ImagePullBackOff",
      message: "back-off",
    });
    expect(findingTexts(f, t).details).toBe("Образ registry.invalid/x:1 не скачивается (ImagePullBackOff: back-off).");
    expect(findingTexts(f, t).title).toBe("под team-a/pull: образ не скачивается");
  });

  it("a missing parameter reads as a dash, not as a placeholder", () => {
    const f = finding("pod.crashLoop", "critical", { kind: "pod", namespace: "team-a", name: "x" }, { containers: ["x"], reason: null, restarts: 3 });
    expect(findingTexts(f, t).details).toBe("Контейнеры x падают и перезапускаются: последнее завершение —, код —, перезапусков до 3.");
  });

  it("chooses the details by the parameters", () => {
    const cert = (days: number, expired: boolean) =>
      finding("route.certExpiring", "warning", { kind: "route", namespace: "team-a", name: "web" }, { days, expired });
    expect(findingTexts(cert(-3, true), t).details).toBe("Сертификат в spec.tls истёк 3 дн. назад: клиенты отклоняют соединение.");
    expect(findingTexts(cert(12, false), t).details).toBe("Сертификат в spec.tls истекает через 12 дн.");
    const single = (hpa: string | null) => finding("workload.singleReplica", "warning", web, { hpa });
    expect(findingTexts(single("web"), t).details).toContain("HPA web с minReplicas = 1");
    expect(findingTexts(single(null), t).details).toContain("нет HPA");
    const noEndpoints = finding("service.noEndpoints", "warning", { kind: "service", namespace: "team-a", name: "orphan" }, { referencedBy: ["Ingress/broken"] });
    expect(findingTexts(noEndpoints, t).details).toContain("Его используют Ingress/broken.");
  });

  it("lists the reasons of an incomplete check and skips lists of objects", () => {
    const partial = finding("check.partial", "info", { kind: "cluster" }, {
      projectLimit: true,
      projectsChecked: 50,
      projectsTotal: 70,
      podLimit: false,
      timedOut: true,
      gaps: ["nodes: FORBIDDEN"],
      gapCount: 1,
    });
    expect(findingTexts(partial, t).details).toBe(
      "Часть объектов не проверена: проверено только 50 из 70 проектов (предел 50); проверка остановилась по пределу 45 с; не прочитано: nodes: FORBIDDEN. Находок по ним может не хватать.",
    );
    // Список пробелов в params целиком; в тексте — первые 20 и «…».
    const gapsOf = (n: number) => Array.from({ length: n }, (_, i) => `team-${String(i).padStart(2, "0")}/secrets: FORBIDDEN`);
    const many = (n: number) => finding("check.partial", "info", { kind: "cluster" }, { gaps: gapsOf(n), gapCount: n });
    const shown = (n: number) => findingTexts(many(n), t).details;
    expect(shown(20)).toContain("team-19/secrets: FORBIDDEN. Находок");
    expect(shown(20)).not.toContain("…");
    expect(shown(25)).toContain("team-19/secrets: FORBIDDEN, …. Находок");
    expect(shown(25)).not.toContain("team-20");
    const operators = finding("cluster.operatorDegraded", "critical", { kind: "cluster" }, {
      operators: [{ name: "dns", degraded: true }],
      names: ["dns"],
      count: 1,
    });
    expect(findingTexts(operators, t).details).toBe("ClusterOperators с Degraded=True или Available=False: dns (всего 1).");
  });

  it("every rule and every variant has texts in both languages", () => {
    type Dict = { health: { rules: Record<string, Record<string, string>>; ruleNames: Record<string, string> } };
    const samples: Record<string, Record<string, unknown>[]> = {
      "workload.singleReplica": [{}, { hpa: "x" }],
      "pvc.pending": [{}, { phase: "Lost" }],
      "route.notAdmitted": [{}, { reason: "noStatus" }],
      "route.certExpiring": [{}, { expired: true }],
      "ingress.classMissing": [{}, { class: "nginx" }],
      "service.noEndpoints": [{}, { referencedBy: ["Ingress/a"] }],
      "check.partial": [{}, { timedOut: true }],
    };
    for (const dict of [en as Dict, ru as Dict]) {
      const rules = dict.health.rules;
      expect(Object.keys(rules)).toHaveLength(34);
      expect(Object.keys(dict.health.ruleNames).sort()).toEqual(Object.keys(rules).sort());
      for (const [id, texts] of Object.entries(rules)) {
        expect(Object.keys(texts)).toEqual(expect.arrayContaining(["title", "details", "fix"]));
        for (const params of samples[id] ?? [{}]) {
          expect(texts[detailsKey(finding(id, "info", { kind: "cluster" }, params))], `${id} ${JSON.stringify(params)}`).toBeTypeOf("string");
        }
      }
    }
  });
});

describe("selection and badges", () => {
  const a = finding("pod.crashLoop", "critical", web, { pods: ["web-1"], count: 1, total: 2 });
  const b = finding("container.latestTag", "warning", { ...web, name: "api" });
  const info = finding("node.unschedulable", "info", { kind: "node", name: "w1" });
  const isWeb = (x: HealthTarget) => x.kind === "workload" && x.name === "web";

  it("hides ignored findings unless asked", () => {
    const v = view([a, b, info], [], [a.key]);
    expect(findingsOf(v, false).map((f) => f.key)).toEqual([b.key, info.key]);
    expect(findingsOf(v, true)).toHaveLength(3);
    expect(findingsOf(undefined, true)).toEqual([]);
  });

  it("counts severe findings and open notes of an object", () => {
    const v = view([a, b, info], [note("warning", "web"), note("critical", "web", "resolved"), note("info", "web")]);
    expect(badgeFor(v, isWeb)).toMatchObject({ count: 2, severity: "critical" });
    expect(itemsFor(v, isWeb).notes).toHaveLength(2);
    expect(badgeFor(v)).toMatchObject({ count: 3, severity: "critical" });
    expect(badgeFor(view([info]))).toBeNull();
  });

  it("passes the finding params to the match: a pod finds the finding of its workload", () => {
    const v = view([a, b]);
    const pod = (name: string) => (x: HealthTarget, params?: Record<string, unknown>) =>
      x.kind === "workload" && Array.isArray(params?.pods) && params.pods.includes(name);
    expect(itemsFor(v, pod("web-1")).findings).toEqual([a]);
    expect(itemsFor(v, pod("web-9")).findings).toEqual([]);
  });
});

describe("badge tooltip", () => {
  const tag = (name: string, severity: Finding["severity"] = "critical") =>
    finding("container.latestTag", severity, { ...web, name }, { images: ["x:latest"] });
  const isWorkload = (x: HealthTarget) => x.kind === "workload";

  it("lists what the badge counted, most severe first, without info and hidden", () => {
    const v = view(
      [tag("w1", "warning"), tag("c1"), tag("i1", "info"), tag("hidden")],
      [note("critical", "n1", "open", "Critical note"), note("warning", "n2", "open", "Warning note"), note("critical", "n3", "resolved", "Closed note"), note("info", "n4", "open", "Info note")],
      [tag("hidden").key],
    );
    const b = badgeFor(v, isWorkload);
    expect(b).toMatchObject({ count: 4, severity: "critical" });
    // Внутри строгости: сначала находки, затем замечания; порядок как в данных.
    expect(b?.items.map((i) => (i.kind === "finding" ? i.finding.target.name : i.note.title))).toEqual(["c1", "Critical note", "w1", "Warning note"]);
    expect(badgeTooltip(b!, t)).toBe(
      [
        "Требуют внимания: 4",
        "Deployment team-a/c1: образы с тегом latest",
        "Critical note",
        "Deployment team-a/w1: образы с тегом latest",
        "Warning note",
      ].join("\n"),
    );
  });

  it("caps the titles and marks the rest with an ellipsis", () => {
    const names = Array.from({ length: BADGE_TITLES_MAX + 2 }, (_, i) => `t${String(i).padStart(2, "0")}`);
    const b = badgeFor(view(names.map((n) => tag(n))), isWorkload)!;
    expect(b.count).toBe(BADGE_TITLES_MAX + 2);
    const lines = badgeTooltip(b, t).split("\n");
    expect(lines).toHaveLength(1 + BADGE_TITLES_MAX + 1);
    expect(lines[1]).toBe("Deployment team-a/t00: образы с тегом latest");
    expect(lines[lines.length - 1]).toBe("…");
  });

  it("has no ellipsis at exactly the cap", () => {
    const b = badgeFor(view(Array.from({ length: BADGE_TITLES_MAX }, (_, i) => tag(`t${i}`))), isWorkload)!;
    expect(badgeTooltip(b, t).split("\n")).not.toContain("…");
  });

  it("puts a multi-line note title on one line", () => {
    const b = badgeFor(view([], [note("warning", "web", "open", "  Restarts grow\non web\r\n\tfast  ")]), isWorkload)!;
    expect(badgeTooltip(b, t).split("\n")).toEqual(["Требуют внимания: 1", "Restarts grow on web fast"]);
  });
});

describe("newestClosedFirst", () => {
  it("закрытые — по времени закрытия, недавние сверху; без closedAtMs — по updatedAtMs", () => {
    const items = [
      { id: "a", updatedAtMs: 10, closedAtMs: 100 },
      { id: "b", updatedAtMs: 300 },
      { id: "c", updatedAtMs: 20, closedAtMs: 200 },
    ];
    expect(newestClosedFirst(items).map((i) => i.id)).toEqual(["b", "c", "a"]);
    expect(items.map((i) => i.id)).toEqual(["a", "b", "c"]);
  });
});
