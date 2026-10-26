import { describe, expect, it } from "vitest";
import type { TabSpec } from "../../state/workspace";
import { helpChapterFor } from "./helpChapterFor";

const cases: [TabSpec | null, string][] = [
  [{ kind: "connection-form", connectionId: null }, "connections"],
  [{ kind: "cluster", connectionId: "c" }, "connections"],
  [{ kind: "nodes", connectionId: "c" }, "connections"],
  [{ kind: "project", connectionId: "c", namespace: "team-a" }, "projects-workloads"],
  [{ kind: "workload", connectionId: "c", namespace: "team-a", workloadKind: "Deployment", name: "web" }, "projects-workloads"],
  [{ kind: "pod", connectionId: "c", namespace: "team-a", name: "web-1" }, "pods-logs"],
  [{ kind: "about" }, "getting-started"],
  [{ kind: "settings" }, "getting-started"],
  [{ kind: "help", chapter: "connections" }, "getting-started"],
  [null, "getting-started"],
];

describe("helpChapterFor", () => {
  it.each(cases)("%j → %s", (tab, chapter) => {
    expect(helpChapterFor(tab)).toBe(chapter);
  });
});
