import { act, fireEvent, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../test/render";

const scrollToIndex = vi.hoisted(() => vi.fn());

vi.mock("@tanstack/react-virtual", () => ({
  useVirtualizer: (o: { count: number }) => ({
    getVirtualItems: () => Array.from({ length: o.count }, (_, index) => ({ index, start: index * 18 })),
    getTotalSize: () => o.count * 18,
    scrollToIndex,
  }),
}));

vi.mock("@tauri-apps/plugin-dialog", () => ({ save: vi.fn() }));

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return { ...orig, api: { startLogs: vi.fn(), stopSession: vi.fn(), saveLogs: vi.fn() } };
});

import { save } from "@tauri-apps/plugin-dialog";
import { api } from "../../api/client";
import type { AppError, LogEvent, LogLine } from "../../api/types";
import { LogsView } from "./LogsView";

let emit: (e: LogEvent) => void = () => undefined;
const CONTAINERS = [
  { name: "migrate", init: true },
  { name: "web", init: false },
];

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(api.startLogs).mockImplementation(async (_p, onEvent) => {
    emit = onEvent;
    return `s${vi.mocked(api.startLogs).mock.calls.length}`;
  });
  vi.mocked(api.stopSession).mockResolvedValue(true);
});

const lastParams = () => {
  const calls = vi.mocked(api.startLogs).mock.calls;
  return calls[calls.length - 1]?.[0];
};

const ui = (extra: { maxLines?: number } = {}) => <LogsView connectionId="c1" namespace="team-a" pod="web-1" containers={CONTAINERS} {...extra} />;

const line = (text: string, ts: string | null = null): LogLine => ({ ts, text, truncated: false });
const lines = (...texts: string[]): LogEvent => ({ type: "lines", lines: texts.map((x) => line(x)), scanned: texts.length, shown: texts.length });
const appError = (key: string, text: string, extra: Partial<AppError> = {}): AppError => ({
  code: "INVALID_INPUT",
  key,
  params: { error: text },
  message: text,
  retryable: false,
  ...extra,
});

/** Ответ `startLogs`, который приходит, когда тест решит. */
function pendingStart() {
  let resolve: (id: string) => void = () => undefined;
  vi.mocked(api.startLogs).mockImplementation((_p, onEvent) => {
    emit = onEvent;
    return new Promise<string>((r) => {
      resolve = r;
    });
  });
  return { resolve: (id: string) => resolve(id) };
}

describe("LogsView", () => {
  it("хвост основного контейнера с follow, строки, счётчики и состояние", async () => {
    renderWithClient(ui());
    await waitFor(() =>
      expect(lastParams()).toEqual({
        connectionId: "c1",
        namespace: "team-a",
        pod: "web-1",
        container: "web",
        previous: false,
        follow: true,
        tailLines: 500,
        sinceSeconds: null,
        filter: null,
      }),
    );
    act(() =>
      emit({
        type: "lines",
        lines: [
          { ts: "2026-10-25T10:00:00.1Z", text: "GET /api 500", truncated: false },
          { ts: "2026-10-25T10:00:01Z", text: "x".repeat(10), truncated: true },
        ],
        scanned: 3,
        shown: 2,
      }),
    );
    expect(screen.getByText("GET /api 500")).toBeInTheDocument();
    expect(screen.getByText("(строка обрезана)")).toBeInTheDocument();
    expect(screen.getByText("показано 2 из 3")).toBeInTheDocument();
    expect(screen.getByText("идёт поток")).toBeInTheDocument();
    act(() => emit({ type: "reconnecting", since: "2026-10-25T10:00:01Z" }));
    expect(screen.getByText("соединение потеряно — переподключение…")).toHaveClass("warn");
    act(() => emit({ type: "end", scanned: 3, shown: 2 }));
    expect(screen.getByText("конец лога")).toBeInTheDocument();
  });

  it("фильтр и предыдущий запуск перезапускают сессию; время строк — без перезапуска", async () => {
    const user = userEvent.setup();
    renderWithClient(ui());
    await waitFor(() => expect(api.startLogs).toHaveBeenCalledTimes(1));
    act(() => emit({ type: "lines", lines: [{ ts: "2026-10-25T10:00:00.1Z", text: "hello", truncated: false }], scanned: 1, shown: 1 }));
    await user.click(screen.getByLabelText("Время строк"));
    expect(screen.getByText("2026-10-25T10:00:00.1Z")).toBeInTheDocument();
    expect(api.startLogs).toHaveBeenCalledTimes(1);
    await user.type(screen.getByLabelText("Фильтр"), "err");
    await user.click(screen.getByLabelText("regex"));
    await waitFor(() => expect(lastParams()?.filter).toEqual({ text: "err", regex: true, caseSensitive: false }));
    expect(api.stopSession).toHaveBeenCalledWith("s1");
    await user.click(screen.getByLabelText("Предыдущий запуск"));
    await waitFor(() => expect(lastParams()).toMatchObject({ previous: true, follow: false }));
    expect(screen.getByLabelText("Следить за новыми строками")).toBeDisabled();
  });

  it("ошибка regex показывается, сохранение пишет файл с параметрами без follow", async () => {
    vi.mocked(save).mockResolvedValue("/tmp/web-1.log");
    vi.mocked(api.saveLogs).mockResolvedValue(42);
    const user = userEvent.setup();
    renderWithClient(ui());
    await waitFor(() => expect(api.startLogs).toHaveBeenCalledTimes(1));
    act(() => emit({ type: "error", error: appError("errors.logs.badRegex", "x") }));
    expect(screen.getByRole("alert")).toHaveTextContent("Неверное регулярное выражение: x");
    await user.click(screen.getByRole("button", { name: "Сохранить в файл…" }));
    await waitFor(() => expect(api.saveLogs).toHaveBeenCalledWith(expect.objectContaining({ pod: "web-1", follow: false }), "/tmp/web-1.log", false));
    expect(await screen.findByText("Сохранено строк: 42 — /tmp/web-1.log")).toBeInTheDocument();
  });

  it("regex, который отклонил сам startLogs, показывается ошибкой", async () => {
    vi.mocked(api.startLogs).mockRejectedValue(appError("errors.logs.badRegex", "unclosed group"));
    renderWithClient(ui());
    expect(await screen.findByRole("alert")).toHaveTextContent("Неверное регулярное выражение: unclosed group");
    expect(screen.getByText("ошибка")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Повторить" }), "повтор не поможет: надо исправить выражение").not.toBeInTheDocument();
  });

  describe("буфер и счётчики", () => {
    it("переполненный буфер: в списке последние строки, над ним — сколько отброшено", async () => {
      renderWithClient(ui({ maxLines: 3 }));
      await waitFor(() => expect(api.startLogs).toHaveBeenCalledTimes(1));
      expect(screen.queryByText(/отброшен/)).not.toBeInTheDocument();
      act(() => emit({ type: "lines", lines: ["l1", "l2", "l3"].map((x) => line(x)), scanned: 8, shown: 3 }));
      expect(screen.queryByText(/отброшен/), "пока всё помещается — записи нет").not.toBeInTheDocument();
      act(() => emit({ type: "lines", lines: ["l4", "l5"].map((x) => line(x)), scanned: 10, shown: 5 }));
      // Счётчики Rust — по сессии, а в окне три строки: расхождение объяснено.
      expect(screen.getByText("показано 5 из 10")).toBeInTheDocument();
      expect(screen.getByText("В окне последние 3 строк, отброшено 2 более ранние строки")).toBeInTheDocument();
      expect(screen.queryByText("l1")).not.toBeInTheDocument();
      expect(screen.queryByText("l2")).not.toBeInTheDocument();
      expect(screen.getAllByText(/^l\d$/).map((e) => e.textContent)).toEqual(["l3", "l4", "l5"]);
      // Пакет больше самого окна.
      act(() => emit({ type: "lines", lines: ["a", "b", "c", "d", "e"].map((x) => line(x)), scanned: 15, shown: 10 }));
      expect(screen.getByText("В окне последние 3 строк, отброшено 7 более ранних строк")).toBeInTheDocument();
      expect(screen.getAllByText(/^[a-e]$/).map((e) => e.textContent)).toEqual(["c", "d", "e"]);
    });

    it("пустой пакет обновляет счётчики и выводит из переподключения, список не трогает", async () => {
      renderWithClient(ui());
      await waitFor(() => expect(api.startLogs).toHaveBeenCalledTimes(1));
      act(() => emit(lines("one")));
      act(() => emit({ type: "reconnecting", since: null }));
      expect(screen.getByText("соединение потеряно — переподключение…")).toBeInTheDocument();
      scrollToIndex.mockClear();
      act(() => emit({ type: "lines", lines: [], scanned: 40, shown: 1 }));
      expect(screen.queryByText("соединение потеряно — переподключение…")).not.toBeInTheDocument();
      expect(screen.getByText("идёт поток")).toBeInTheDocument();
      expect(screen.getByText("показано 1 из 40")).toBeInTheDocument();
      expect(screen.getAllByText("one")).toHaveLength(1);
      expect(screen.queryByText(/отброшен/)).not.toBeInTheDocument();
    });

    it("фильтр, под который ничего не подходит: счётчик растёт без строк", async () => {
      renderWithClient(ui());
      await waitFor(() => expect(api.startLogs).toHaveBeenCalledTimes(1));
      act(() => emit({ type: "lines", lines: [], scanned: 120, shown: 0 }));
      expect(screen.getByText("показано 0 из 120")).toBeInTheDocument();
      expect(screen.getByRole("log").querySelectorAll(".log-line")).toHaveLength(0);
    });

    it("ответ startLogs выводит из «подключения» даже без событий, но не затирает ошибку", async () => {
      const start = pendingStart();
      renderWithClient(ui());
      await waitFor(() => expect(api.startLogs).toHaveBeenCalledTimes(1));
      expect(screen.getByText("подключение…")).toBeInTheDocument();
      await act(async () => start.resolve("s1"));
      expect(screen.getByText("идёт поток")).toBeInTheDocument();
      expect(screen.getByText("показано 0 из 0")).toBeInTheDocument();
    });

    it("ошибка, пришедшая раньше ответа startLogs, остаётся", async () => {
      const start = pendingStart();
      renderWithClient(ui());
      await waitFor(() => expect(api.startLogs).toHaveBeenCalledTimes(1));
      act(() => emit({ type: "error", error: appError("errors.logs.badRegex", "x") }));
      await act(async () => start.resolve("s1"));
      expect(screen.getByText("ошибка")).toBeInTheDocument();
      expect(screen.queryByText("идёт поток")).not.toBeInTheDocument();
    });
  });

  describe("сессия", () => {
    it("события прежней сессии после смены параметров игнорируются", async () => {
      const user = userEvent.setup();
      renderWithClient(ui());
      await waitFor(() => expect(api.startLogs).toHaveBeenCalledTimes(1));
      const old = emit;
      await user.click(screen.getByLabelText("Предыдущий запуск"));
      await waitFor(() => expect(api.startLogs).toHaveBeenCalledTimes(2));
      expect(api.stopSession).toHaveBeenCalledWith("s1");
      act(() => old(lines("stale")));
      act(() => old({ type: "error", error: appError("errors.logs.badRegex", "stale") }));
      act(() => old({ type: "end", scanned: 9, shown: 9 }));
      expect(screen.queryByText("stale")).not.toBeInTheDocument();
      expect(screen.queryByRole("alert")).not.toBeInTheDocument();
      expect(screen.getByText("показано 0 из 0")).toBeInTheDocument();
      act(() => emit(lines("fresh")));
      expect(screen.getByText("fresh")).toBeInTheDocument();
      expect(screen.getByText("показано 1 из 1")).toBeInTheDocument();
    });

    it("startLogs, ответивший после закрытия вкладки, останавливает свою сессию", async () => {
      const start = pendingStart();
      const { unmount } = renderWithClient(ui());
      await waitFor(() => expect(api.startLogs).toHaveBeenCalledTimes(1));
      unmount();
      expect(api.stopSession).not.toHaveBeenCalled();
      await act(async () => start.resolve("late"));
      expect(api.stopSession).toHaveBeenCalledWith("late");
    });

    it("хвост применяется через паузу после ввода, а не на каждую клавишу", async () => {
      const user = userEvent.setup();
      renderWithClient(ui());
      await waitFor(() => expect(api.startLogs).toHaveBeenCalledTimes(1));
      const tail = screen.getByLabelText("Последних строк");
      await user.clear(tail);
      await user.type(tail, "20");
      expect(api.startLogs, "пока набирают, сессия не перезапускается").toHaveBeenCalledTimes(1);
      await waitFor(() => expect(lastParams()?.tailLines).toBe(20));
      expect(api.startLogs).toHaveBeenCalledTimes(2);
    });

    it("«Начиная с» подписано как 5 мин, 1 ч, 6 ч, 24 ч", async () => {
      renderWithClient(ui());
      await waitFor(() => expect(api.startLogs).toHaveBeenCalledTimes(1));
      const since = screen.getByLabelText("Начиная с");
      expect(within(since).getAllByRole("option").map((o) => o.textContent)).toEqual(["начала", "5 мин", "1 ч", "6 ч", "24 ч"]);
    });

    it("после смены параметров список снова идёт за концом, даже если прокрутили вверх", async () => {
      const user = userEvent.setup();
      renderWithClient(ui());
      await waitFor(() => expect(api.startLogs).toHaveBeenCalledTimes(1));
      act(() => emit(lines("a", "b")));
      expect(scrollToIndex).toHaveBeenLastCalledWith(1);
      // Прокрутили вверх: до конца далеко.
      const box = screen.getByRole("log");
      Object.defineProperty(box, "scrollHeight", { configurable: true, value: 5000 });
      Object.defineProperty(box, "clientHeight", { configurable: true, value: 300 });
      box.scrollTop = 0;
      fireEvent.scroll(box);
      scrollToIndex.mockClear();
      act(() => emit(lines("a", "b", "c")));
      expect(scrollToIndex, "читают выше — список не дёргается").not.toHaveBeenCalled();
      await user.click(screen.getByLabelText("Предыдущий запуск"));
      await waitFor(() => expect(api.startLogs).toHaveBeenCalledTimes(2));
      act(() => emit(lines("x", "y", "z")));
      expect(scrollToIndex).toHaveBeenCalledWith(2);
    });

    it("потерянный поток: «Повторить» начинает сессию заново, а ошибка выражения повтора не предлагает", async () => {
      const user = userEvent.setup();
      renderWithClient(ui());
      await waitFor(() => expect(api.startLogs).toHaveBeenCalledTimes(1));
      act(() => emit(lines("one")));
      act(() =>
        emit({
          type: "error",
          error: appError("errors.logs.lost", "lost", { code: "CONNECTION_FAILED", retryable: true, message: "The log stream was lost and did not recover in 60 s" }),
        }),
      );
      expect(screen.getByRole("alert")).toHaveTextContent("Поток логов прервался");
      await user.click(screen.getByRole("button", { name: "Повторить" }));
      await waitFor(() => expect(api.startLogs).toHaveBeenCalledTimes(2));
      expect(api.stopSession).toHaveBeenCalledWith("s1");
      expect(screen.queryByRole("alert")).not.toBeInTheDocument();
      expect(screen.queryByText("one"), "новая сессия читает лог заново").not.toBeInTheDocument();
      expect(screen.getByText("показано 0 из 0")).toBeInTheDocument();
    });
  });

  describe("доступность", () => {
    it("список строк доступен с клавиатуры и не объявляется скринридером на каждую строку", async () => {
      renderWithClient(ui());
      await waitFor(() => expect(api.startLogs).toHaveBeenCalledTimes(1));
      const box = screen.getByRole("log", { name: "Строки лога: web-1 / web" });
      expect(box).toHaveAttribute("tabindex", "0");
      expect(box).toHaveAttribute("aria-live", "off");
      box.focus();
      expect(box).toHaveFocus();
    });

    it("состояние и итог сохранения — всегда смонтированные области status", async () => {
      vi.mocked(save).mockResolvedValue("/tmp/web-1.log");
      vi.mocked(api.saveLogs).mockResolvedValue(3);
      const user = userEvent.setup();
      renderWithClient(ui());
      const regions = screen.getAllByRole("status");
      expect(regions).toHaveLength(2);
      expect(regions[0]).toHaveTextContent("подключение…");
      expect(regions[1]).toBeEmptyDOMElement();
      await user.click(screen.getByRole("button", { name: "Сохранить в файл…" }));
      await waitFor(() => expect(regions[1]).toHaveTextContent("Сохранено строк: 3 — /tmp/web-1.log"));
      expect(screen.getAllByRole("status")[1], "тот же элемент, текст подставлен в него").toBe(regions[1]);
    });

    it("предыдущий запуск: у выключенного «Следить» есть объяснение", async () => {
      const user = userEvent.setup();
      renderWithClient(ui());
      await waitFor(() => expect(api.startLogs).toHaveBeenCalledTimes(1));
      const follow = screen.getByLabelText("Следить за новыми строками");
      expect(follow).not.toHaveAttribute("title");
      expect(follow).not.toHaveAccessibleDescription();
      await user.click(screen.getByLabelText("Предыдущий запуск"));
      expect(follow).toBeDisabled();
      expect(follow).toHaveAttribute("title", "Предыдущий запуск уже завершён — следить не за чем");
      expect(follow).toHaveAccessibleDescription("Предыдущий запуск уже завершён — следить не за чем");
    });
  });

  describe("сохранение в файл", () => {
    it("кнопка занята, пока идёт сохранение", async () => {
      vi.mocked(save).mockResolvedValue("/tmp/web-1.log");
      let done: (n: number) => void = () => undefined;
      vi.mocked(api.saveLogs).mockReturnValue(
        new Promise<number>((r) => {
          done = r;
        }),
      );
      const user = userEvent.setup();
      renderWithClient(ui());
      await waitFor(() => expect(api.startLogs).toHaveBeenCalledTimes(1));
      const button = screen.getByRole("button", { name: "Сохранить в файл…" });
      expect(button).toHaveAttribute("aria-busy", "false");
      await user.click(button);
      await waitFor(() => expect(button).toBeDisabled());
      expect(button).toHaveAttribute("aria-busy", "true");
      await act(async () => done(7));
      expect(button).toBeEnabled();
      expect(button).toHaveAttribute("aria-busy", "false");
      expect(screen.getByText("Сохранено строк: 7 — /tmp/web-1.log")).toBeInTheDocument();
    });

    it("отмена диалога ничего не пишет и не показывает", async () => {
      vi.mocked(save).mockResolvedValue(null);
      const user = userEvent.setup();
      renderWithClient(ui());
      await waitFor(() => expect(api.startLogs).toHaveBeenCalledTimes(1));
      const button = screen.getByRole("button", { name: "Сохранить в файл…" });
      await user.click(button);
      await waitFor(() => expect(save).toHaveBeenCalledTimes(1));
      await waitFor(() => expect(button).toBeEnabled());
      expect(api.saveLogs).not.toHaveBeenCalled();
      expect(screen.queryByText(/Сохранено строк/)).not.toBeInTheDocument();
      expect(screen.queryByRole("alert")).not.toBeInTheDocument();
    });

    it("ошибка записи показывается, кнопка снова доступна", async () => {
      vi.mocked(save).mockResolvedValue("/tmp/web-1.log");
      vi.mocked(api.saveLogs).mockRejectedValue(appError("errors.logs.saveStopped", "stopped", { code: "CONNECTION_FAILED", message: "Saving the log was stopped: the connection was closed" }));
      const user = userEvent.setup();
      renderWithClient(ui());
      await waitFor(() => expect(api.startLogs).toHaveBeenCalledTimes(1));
      const button = screen.getByRole("button", { name: "Сохранить в файл…" });
      await user.click(button);
      expect(await screen.findByRole("alert")).toHaveTextContent("Сохранение лога остановлено");
      expect(button).toBeEnabled();
      expect(screen.queryByText(/Сохранено строк/)).not.toBeInTheDocument();
    });

    it("сохраняет с фильтром и хвостом, набранными сейчас, а не отложенными", async () => {
      vi.mocked(save).mockResolvedValue("/tmp/web-1.log");
      vi.mocked(api.saveLogs).mockResolvedValue(1);
      const user = userEvent.setup();
      renderWithClient(ui());
      await waitFor(() => expect(api.startLogs).toHaveBeenCalledTimes(1));
      await user.type(screen.getByLabelText("Фильтр"), "err");
      const tail = screen.getByLabelText("Последних строк");
      await user.clear(tail);
      await user.type(tail, "50");
      // Пауза ввода ещё не прошла: сессия идёт со старыми параметрами…
      expect(lastParams()).toMatchObject({ filter: null, tailLines: 500 });
      await user.click(screen.getByRole("button", { name: "Сохранить в файл…" }));
      // …а файл пишется с теми, что на экране.
      await waitFor(() => expect(api.saveLogs).toHaveBeenCalledTimes(1));
      expect(vi.mocked(api.saveLogs).mock.calls[0]?.[0]).toMatchObject({
        filter: { text: "err", regex: false, caseSensitive: false },
        tailLines: 50,
        follow: false,
      });
    });
  });
});
