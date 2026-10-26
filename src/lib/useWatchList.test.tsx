import { act, renderHook, waitFor } from "@testing-library/react";
import { type ReactNode, StrictMode, useLayoutEffect, useState } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../api/client")>();
  return { ...orig, api: { startWatch: vi.fn(), stopSession: vi.fn() } };
});

import { api } from "../api/client";
import type { PodRow, WatchEvent, WatchParams } from "../api/types";
import { useConnectionEpoch } from "../state/connectionEpoch";
import { TabActiveContext } from "./tabActive";
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

/** Обработчик событий последней начатой сессии. */
let emit: (e: WatchEvent) => void = () => undefined;
/** Обработчики всех начатых сессий по порядку: у перезапущенной сессии прежний обработчик ещё жив. */
let emits: Array<(e: WatchEvent) => void> = [];

/** Ответ `startWatch`, который приходит, когда его решит тест. */
function deferred<T>() {
  let resolve: (v: T) => void = () => undefined;
  const promise = new Promise<T>((r) => (resolve = r));
  return { promise, resolve };
}

beforeEach(() => {
  vi.clearAllMocks();
  emits = [];
  vi.mocked(api.startWatch).mockImplementation(async (_p, onEvent) => {
    emit = onEvent;
    emits.push(onEvent);
    return `s${vi.mocked(api.startWatch).mock.calls.length}`;
  });
  vi.mocked(api.stopSession).mockResolvedValue(true);
});

describe("useWatchList", () => {
  it("список целиком, затем изменения по порядку, опрос и ошибки", async () => {
    const { result } = renderHook(() => useWatchList(PARAMS, podKey));
    expect(result.current.status).toBe("connecting");
    await waitFor(() => expect(api.startWatch).toHaveBeenCalledWith(PARAMS, expect.any(Function)));
    act(() => emit({ type: "reset", items: [pod("a"), pod("b")], errors: [], truncated: false }));
    expect(result.current.rows.map((r) => r.name)).toEqual(["a", "b"]);
    expect(result.current.status).toBe("live");
    expect(result.current.truncated).toBe(false);
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

  it("truncated: ставит каждый сброс, изменения его сохраняют, перезапуск сбрасывает", async () => {
    const { result } = renderHook(() => useWatchList(PARAMS, podKey));
    await waitFor(() => expect(api.startWatch).toHaveBeenCalledTimes(1));
    expect(result.current.truncated).toBe(false);
    act(() => emit({ type: "reset", items: [pod("a")], errors: [], truncated: true }));
    expect(result.current.truncated).toBe(true);
    act(() => emit({ type: "changes", changes: [{ op: "upsert", row: pod("b") }] }));
    act(() => emit({ type: "changes", changes: [{ op: "delete", key: "a" }] }));
    act(() => emit({ type: "polling" }));
    expect(result.current.rows.map((r) => r.name)).toEqual(["b"]);
    expect(result.current.truncated).toBe(true);
    // Опрос присылает полный список сбросом: предел снимается, когда строк стало меньше.
    act(() => emit({ type: "reset", items: [pod("b")], errors: [], truncated: false }));
    expect(result.current.truncated).toBe(false);
    act(() => emit({ type: "reset", items: [pod("b")], errors: [], truncated: true }));
    expect(result.current.truncated).toBe(true);
    await act(async () => {
      useConnectionEpoch.getState().bump("c1");
    });
    expect(result.current.truncated).toBe(false);
    expect(result.current.rows).toEqual([]);
    await waitFor(() => expect(api.startWatch).toHaveBeenCalledTimes(2));
  });

  it("сброс без truncated — не обрезан", async () => {
    const { result } = renderHook(() => useWatchList(PARAMS, podKey));
    await waitFor(() => expect(api.startWatch).toHaveBeenCalledTimes(1));
    act(() => emit({ type: "reset", items: [pod("a")], errors: [], truncated: true }));
    expect(result.current.truncated).toBe(true);
    act(() => emit({ type: "reset", items: [pod("a")], errors: [] }));
    expect(result.current.truncated).toBe(false);
    expect(result.current.rows.map((r) => r.name)).toEqual(["a"]);
  });

  it("пустой пакет изменений снимает ошибку: сессия восстановилась", async () => {
    const { result } = renderHook(() => useWatchList(PARAMS, podKey));
    await waitFor(() => expect(api.startWatch).toHaveBeenCalledTimes(1));
    act(() => emit({ type: "reset", items: [pod("a")], errors: [], truncated: false }));
    const error = { code: "CONNECTION_FAILED" as const, message: "x", retryable: true };
    act(() => emit({ type: "error", error, fatal: false }));
    expect(result.current.error).toEqual(error);
    expect(result.current.status).toBe("live");
    act(() => emit({ type: "changes", changes: [] }));
    expect(result.current.error).toBeNull();
    expect(result.current.status).toBe("live");
    expect(result.current.rows.map((r) => r.name)).toEqual(["a"]);
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

  describe("гонки сессий", () => {
    it("id сессии пришёл после размонтирования: сессия останавливается, состояние не трогается", async () => {
      const errors = vi.spyOn(console, "error").mockImplementation(() => undefined);
      const late = deferred<string>();
      vi.mocked(api.startWatch).mockImplementationOnce(() => late.promise);
      const { result, unmount } = renderHook(() => useWatchList(PARAMS, podKey));
      await waitFor(() => expect(api.startWatch).toHaveBeenCalledTimes(1));
      const before = result.current;
      unmount();
      expect(api.stopSession).not.toHaveBeenCalled();
      await act(async () => late.resolve("late"));
      expect(api.stopSession).toHaveBeenCalledTimes(1);
      expect(api.stopSession).toHaveBeenCalledWith("late");
      expect(result.current).toBe(before);
      expect(errors).not.toHaveBeenCalled();
      errors.mockRestore();
    });

    it("id сессии пришёл после перезапуска: старая сессия останавливается, строки новой на месте", async () => {
      const errors = vi.spyOn(console, "error").mockImplementation(() => undefined);
      const late = deferred<string>();
      vi.mocked(api.startWatch).mockImplementationOnce(() => late.promise);
      const { result } = renderHook(() => useWatchList(PARAMS, podKey));
      await waitFor(() => expect(api.startWatch).toHaveBeenCalledTimes(1));
      await act(async () => {
        useConnectionEpoch.getState().bump("c1");
      });
      await waitFor(() => expect(api.startWatch).toHaveBeenCalledTimes(2));
      act(() => emit({ type: "reset", items: [pod("new")], errors: [], truncated: false }));
      const before = result.current;
      expect(api.stopSession).not.toHaveBeenCalled();
      await act(async () => late.resolve("late"));
      expect(api.stopSession).toHaveBeenCalledTimes(1);
      expect(api.stopSession).toHaveBeenCalledWith("late");
      expect(result.current).toBe(before);
      expect(result.current.rows.map((r) => r.name)).toEqual(["new"]);
      expect(errors).not.toHaveBeenCalled();
      errors.mockRestore();
    });

    it("события прежней сессии после перезапуска и размонтирования игнорируются", async () => {
      const errors = vi.spyOn(console, "error").mockImplementation(() => undefined);
      const { result, unmount } = renderHook(() => useWatchList(PARAMS, podKey));
      await waitFor(() => expect(emits).toHaveLength(1));
      const [first] = emits;
      act(() => first({ type: "reset", items: [pod("old")], errors: [], truncated: true }));
      expect(result.current.rows.map((r) => r.name)).toEqual(["old"]);
      await act(async () => {
        useConnectionEpoch.getState().bump("c1");
      });
      await waitFor(() => expect(emits).toHaveLength(2));
      act(() => emits[1]({ type: "reset", items: [pod("new")], errors: [], truncated: false }));
      const before = result.current;
      // Запоздавшие события первой сессии всех видов.
      act(() => first({ type: "reset", items: [pod("old-2")], errors: [], truncated: true }));
      act(() => first({ type: "changes", changes: [{ op: "upsert", row: pod("old-3") }] }));
      act(() => first({ type: "polling" }));
      act(() => first({ type: "error", error: { code: "CONNECTION_FAILED", message: "x", retryable: true }, fatal: true }));
      expect(result.current).toBe(before);
      expect(result.current.rows.map((r) => r.name)).toEqual(["new"]);
      expect(result.current.truncated).toBe(false);
      expect(result.current.status).toBe("live");
      unmount();
      act(() => emits[1]({ type: "reset", items: [pod("after-unmount")], errors: [], truncated: false }));
      expect(errors).not.toHaveBeenCalled();
      errors.mockRestore();
    });
  });

  it("StrictMode: события первого, отключённого запуска эффекта с тем же ключом игнорируются", async () => {
    // В разработке React запускает эффект, отключает и запускает снова: у обоих запусков один ключ сессии.
    const { result } = renderHook(() => useWatchList(PARAMS, podKey), { wrapper: StrictMode });
    await waitFor(() => expect(emits).toHaveLength(2));
    act(() => emits[1]({ type: "reset", items: [pod("live")], errors: [], truncated: false }));
    const before = result.current;
    act(() => emits[0]({ type: "reset", items: [pod("stale")], errors: [], truncated: true }));
    act(() => emits[0]({ type: "changes", changes: [{ op: "upsert", row: pod("stale-2") }] }));
    expect(result.current).toBe(before);
    expect(result.current.rows.map((r) => r.name)).toEqual(["live"]);
    await waitFor(() => expect(api.stopSession).toHaveBeenCalledWith("s1"));
  });

  describe("смена параметров", () => {
    it("событие прежней сессии между коммитом нового ключа и отключением её эффекта строки не оставляет", async () => {
      // Слой-эффект выполняется сразу после коммита, а эффект прежней сессии отключается позже (пассивные эффекты),
      // так что её обработчик ещё жив: из него приходит сообщение прежнего проекта.
      const { result, rerender } = renderHook(
        ({ params }: { params: WatchParams }) => {
          const state = useWatchList(params, podKey);
          useLayoutEffect(() => {
            if (params.namespace === "team-b") emits[0]({ type: "reset", items: [pod("OLD")], errors: [], truncated: true });
          }, [params.namespace]);
          return state;
        },
        { initialProps: { params: PARAMS } },
      );
      await waitFor(() => expect(emits).toHaveLength(1));
      act(() => emits[0]({ type: "reset", items: [pod("a")], errors: [], truncated: false }));
      expect(result.current.status).toBe("live");
      // Новая сессия так и не отвечает: прежние строки ничто другое не сотрёт.
      vi.mocked(api.startWatch).mockImplementationOnce(() => new Promise(() => undefined));
      rerender({ params: { ...PARAMS, namespace: "team-b" } });
      await waitFor(() => expect(api.startWatch).toHaveBeenCalledTimes(2));
      expect(result.current.rows).toEqual([]);
      expect(result.current.status).toBe("connecting");
      expect(result.current.truncated).toBe(false);
      expect(api.stopSession).toHaveBeenCalledWith("s1");
    });

    it("другой проект: ни один кадр не показывает строки прежнего", async () => {
      const seen: string[][] = [];
      const { rerender } = renderHook(
        ({ params }: { params: WatchParams | null }) => {
          const state = useWatchList(params, podKey);
          seen.push(state.rows.map((r) => r.name));
          return state;
        },
        { initialProps: { params: PARAMS as WatchParams | null } },
      );
      await waitFor(() => expect(api.startWatch).toHaveBeenCalledTimes(1));
      act(() => emit({ type: "reset", items: [pod("a-1")], errors: [], truncated: true }));
      expect(seen[seen.length - 1]).toEqual(["a-1"]);
      const rendersBefore = seen.length;
      rerender({ params: { ...PARAMS, namespace: "team-b" } });
      expect(seen.slice(rendersBefore).flat()).toEqual([]);
      await waitFor(() => expect(api.startWatch).toHaveBeenCalledTimes(2));
      expect(api.stopSession).toHaveBeenCalledWith("s1");
    });

    it("params null: строки и статус сбрасываются, сессия останавливается", async () => {
      const { result, rerender } = renderHook(({ params }: { params: WatchParams | null }) => useWatchList(params, podKey), {
        initialProps: { params: PARAMS as WatchParams | null },
      });
      await waitFor(() => expect(api.startWatch).toHaveBeenCalledTimes(1));
      act(() => emit({ type: "reset", items: [pod("a")], errors: [], truncated: true }));
      expect(result.current.status).toBe("live");
      rerender({ params: null });
      expect(result.current.rows).toEqual([]);
      expect(result.current.status).toBe("connecting");
      expect(result.current.truncated).toBe(false);
      await waitFor(() => expect(api.stopSession).toHaveBeenCalledWith("s1"));
      // Возврат к тем же параметрам начинает с чистого состояния, а не со строк до `null`.
      rerender({ params: PARAMS });
      expect(result.current.rows).toEqual([]);
      await waitFor(() => expect(api.startWatch).toHaveBeenCalledTimes(2));
    });
  });

  describe("скрытая вкладка", () => {
    /** Оболочка-«вкладка»: `setActive` показывает и скрывает её, как это делает `Layout`. */
    function tab(initial = true) {
      let set: (active: boolean) => void = () => undefined;
      const wrapper = ({ children }: { children: ReactNode }) => {
        const [active, setActive] = useState(initial);
        set = setActive;
        return <TabActiveContext.Provider value={active}>{children}</TabActiveContext.Provider>;
      };
      return { wrapper, setActive: (active: boolean) => act(() => set(active)) };
    }

    it("сессия останавливается, когда вкладку скрыли, и начинается заново с новым сбросом, когда её показали", async () => {
      const { wrapper, setActive } = tab();
      const { result } = renderHook(() => useWatchList(PARAMS, podKey), { wrapper });
      await waitFor(() => expect(api.startWatch).toHaveBeenCalledTimes(1));
      act(() => emits[0]({ type: "reset", items: [pod("a"), pod("b")], errors: [], truncated: true }));
      expect(result.current.status).toBe("live");

      await setActive(false);
      expect(api.stopSession).toHaveBeenCalledTimes(1);
      expect(api.stopSession).toHaveBeenCalledWith("s1");
      // События остановленной сессии, пришедшие позже, ничего не меняют.
      const hidden = result.current;
      act(() => emits[0]({ type: "changes", changes: [{ op: "upsert", row: pod("late") }] }));
      expect(result.current).toBe(hidden);
      expect(api.startWatch).toHaveBeenCalledTimes(1);

      await setActive(true);
      await waitFor(() => expect(api.startWatch).toHaveBeenCalledTimes(2));
      expect(api.startWatch).toHaveBeenLastCalledWith(PARAMS, expect.any(Function));
      // Пустого кадра нет: прежние строки на месте, но список уже не «живой», пока не пришёл новый `Reset`.
      expect(result.current.rows.map((r) => r.name)).toEqual(["a", "b"]);
      expect(result.current.status).toBe("connecting");
      expect(result.current.truncated).toBe(true);
      act(() => emits[1]({ type: "reset", items: [pod("c")], errors: [], truncated: false }));
      expect(result.current.rows.map((r) => r.name)).toEqual(["c"]);
      expect(result.current.status).toBe("live");
      expect(result.current.truncated).toBe(false);

      await setActive(false);
      expect(api.stopSession).toHaveBeenLastCalledWith("s2");
    });

    it("вкладка скрыта с самого начала: сессии нет, пока её не покажут", async () => {
      const { wrapper, setActive } = tab(false);
      const { result } = renderHook(() => useWatchList(PARAMS, podKey), { wrapper });
      await act(async () => undefined);
      expect(api.startWatch).not.toHaveBeenCalled();
      expect(result.current.status).toBe("connecting");
      await setActive(true);
      await waitFor(() => expect(api.startWatch).toHaveBeenCalledTimes(1));
      act(() => emit({ type: "reset", items: [pod("a")], errors: [], truncated: false }));
      expect(result.current.rows.map((r) => r.name)).toEqual(["a"]);
    });

    it("id сессии пришёл, когда вкладку уже скрыли: сессия останавливается", async () => {
      const late = deferred<string>();
      vi.mocked(api.startWatch).mockImplementationOnce(() => late.promise);
      const { wrapper, setActive } = tab();
      renderHook(() => useWatchList(PARAMS, podKey), { wrapper });
      await waitFor(() => expect(api.startWatch).toHaveBeenCalledTimes(1));
      await setActive(false);
      expect(api.stopSession).not.toHaveBeenCalled();
      await act(async () => late.resolve("late"));
      expect(api.stopSession).toHaveBeenCalledWith("late");
    });

    it("вход заново в скрытой вкладке сбрасывает строки: прежняя сессия кончилась", async () => {
      const { wrapper, setActive } = tab();
      const { result } = renderHook(() => useWatchList(PARAMS, podKey), { wrapper });
      await waitFor(() => expect(api.startWatch).toHaveBeenCalledTimes(1));
      act(() => emits[0]({ type: "reset", items: [pod("a")], errors: [], truncated: false }));
      await setActive(false);
      await act(async () => {
        useConnectionEpoch.getState().bump("c1");
      });
      expect(result.current.rows).toEqual([]);
      expect(api.startWatch).toHaveBeenCalledTimes(1);
      await setActive(true);
      await waitFor(() => expect(api.startWatch).toHaveBeenCalledTimes(2));
    });
  });

  it("без параметров сессии нет", () => {
    renderHook(() => useWatchList(null, podKey));
    expect(api.startWatch).not.toHaveBeenCalled();
  });
});
