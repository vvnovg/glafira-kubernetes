import { act, renderHook } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { useNow } from "./useNow";

beforeEach(() => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-10-26T10:00:00Z"));
});
afterEach(() => vi.useRealTimers());

describe("useNow", () => {
  it("обновляется каждые intervalMs и перестаёт после размонтирования", () => {
    const { result, unmount } = renderHook(() => useNow(60_000));
    const start = result.current;
    expect(start).toBe(Date.now());
    act(() => vi.advanceTimersByTime(59_000));
    expect(result.current).toBe(start);
    act(() => vi.advanceTimersByTime(1_000));
    expect(result.current).toBe(start + 60_000);
    act(() => vi.advanceTimersByTime(3 * 60_000));
    expect(result.current).toBe(start + 4 * 60_000);
    unmount();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("смена интервала перезапускает таймер без утечки", () => {
    const { result, rerender } = renderHook(({ ms }: { ms: number }) => useNow(ms), { initialProps: { ms: 60_000 } });
    rerender({ ms: 1_000 });
    expect(vi.getTimerCount()).toBe(1);
    const before = result.current;
    act(() => vi.advanceTimersByTime(2_000));
    expect(result.current).toBe(before + 2_000);
  });
});
