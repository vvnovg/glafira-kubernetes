import { describe, expect, it } from "vitest";
import type { TabSpec } from "../../state/workspace";
import { helpChapterFor } from "./helpChapterFor";

const cases: [TabSpec | null, string][] = [
  [{ kind: "connection-form", connectionId: null }, "connections"],
  [{ kind: "cluster", connectionId: "c" }, "connections"],
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
