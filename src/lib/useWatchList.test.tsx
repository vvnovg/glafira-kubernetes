import { act, renderHook, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../api/client")>();
  return { ...orig, api: { startWatch: vi.fn(), stopSession: vi.fn() } };
});

import { api } from "../api/client";
import type { PodRow, WatchEvent, WatchParams } from "../api/types";
import { useConnectionEpoch } from "../state/connectionEpoch";
import { podKey, useWatchList } from "./useWatchList";

const pod = (name: string, status = "Running"): PodRow => ({
  name,
  phase: "Running",
  status,
  problem: null,
  ready: 1,
  containers: 1,
  restarts: 0,
  lastRestart: null,
  node: "w1",
  created: 1,
  qos: null,
  owner: null,
});

const PARAMS: WatchParams = { connectionId: "c1", namespace: "team-a", kind: "pods" };

let emit: (e: WatchEvent) => void = () => undefined;

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(api.startWatch).mockImplementation(async (_p, onEvent) => {
    emit = onEvent;
    return `s${vi.mocked(api.startWatch).mock.calls.length}`;
  });
  vi.mocked(api.stopSession).mockResolvedValue(true);
});

describe("useWatchList", () => {
  it("список целиком, затем изменения по порядку, опрос и ошибки", async () => {
    const { result } = renderHook(() => useWatchList(PARAMS, podKey));
    expect(result.current.status).toBe("connecting");
    await waitFor(() => expect(api.startWatch).toHaveBeenCalledWith(PARAMS, expect.any(Function)));
    act(() => emit({ type: "reset", items: [pod("a"), pod("b")], errors: [] }));
    expect(result.current.rows.map((r) => r.name)).toEqual(["a", "b"]);
    expect(result.current.status).toBe("live");
    act(() =>
      emit({
        type: "changes",
        changes: [
          { op: "upsert", row: pod("c") },
          { op: "delete", key: "a" },
          { op: "upsert", row: pod("b", "CrashLoopBackOff") },
        ],
      }),
    );
    expect(result.current.rows.map((r) => `${r.name}:${r.status}`)).toEqual(["b:CrashLoopBackOff", "c:Running"]);
    act(() => emit({ type: "polling" }));
    expect(result.current.status).toBe("polling");
    const error = { code: "CONNECTION_FAILED" as const, message: "x", retryable: true };
    act(() => emit({ type: "error", error, fatal: false }));
    expect(result.current.status).toBe("polling");
    expect(result.current.error).toEqual(error);
    act(() => emit({ type: "error", error, fatal: true }));
    expect(result.current.status).toBe("error");
  });

  it("останавливает сессию при размонтировании и перезапускает после входа заново", async () => {
    const { unmount } = renderHook(() => useWatchList(PARAMS, podKey));
    await waitFor(() => expect(api.startWatch).toHaveBeenCalledTimes(1));
    await act(async () => {
      useConnectionEpoch.getState().bump("c1");
    });
    await waitFor(() => expect(api.startWatch).toHaveBeenCalledTimes(2));
    expect(api.stopSession).toHaveBeenCalledWith("s1");
    unmount();
    expect(api.stopSession).toHaveBeenCalledWith("s2");
  });

  it("без параметров сессии нет", () => {
    renderHook(() => useWatchList(null, podKey));
    expect(api.startWatch).not.toHaveBeenCalled();
  });
});
