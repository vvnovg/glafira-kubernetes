import type { ChapterId } from "../../guide/chapters";
import type { TabSpec } from "../../state/workspace";

/** Глава руководства для экрана (F1). */
export function helpChapterFor(tab: TabSpec | null): ChapterId {
  switch (tab?.kind) {
    case "connection-form":
    case "cluster":
    case "nodes":
      return "connections";
    default:
      return "getting-started";
  }
}
