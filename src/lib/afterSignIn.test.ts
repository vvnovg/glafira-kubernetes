import { QueryClient } from "@tanstack/react-query";
import { describe, expect, it } from "vitest";
import { useConnectionEpoch } from "../state/connectionEpoch";
import { afterSignIn } from "./afterSignIn";

describe("afterSignIn", () => {
  it("перечитывает вкладки только этого подключения и перезапускает его сессии", () => {
    const qc = new QueryClient();
    for (const key of [["connection", "c1"], ["pods", "c1", "team-a"], ["pods", "c2", "team-a"]]) qc.setQueryData(key, 1);
    afterSignIn(qc, "c1");
    const stale = (key: string[]) => qc.getQueryState(key)?.isInvalidated;
    expect(stale(["pods", "c1", "team-a"])).toBe(true);
    expect(stale(["pods", "c2", "team-a"])).toBe(false);
    expect(stale(["connection", "c1"])).toBe(false);
    expect(useConnectionEpoch.getState().epochs.c1).toBe(1);
  });
});
