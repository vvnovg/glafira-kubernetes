import { act, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../test/render";

vi.mock("@tanstack/react-virtual", () => ({
  useVirtualizer: (o: { count: number }) => ({
    getVirtualItems: () => Array.from({ length: o.count }, (_, index) => ({ index, start: index * 18 })),
    getTotalSize: () => o.count * 18,
    scrollToIndex: vi.fn(),
  }),
}));

vi.mock("@tauri-apps/plugin-dialog", () => ({ save: vi.fn() }));

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return { ...orig, api: { startLogs: vi.fn(), stopSession: vi.fn(), saveLogs: vi.fn() } };
});

import { save } from "@tauri-apps/plugin-dialog";
import { api } from "../../api/client";
import type { LogEvent } from "../../api/types";
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

describe("LogsView", () => {
  it("хвост основного контейнера с follow, строки, счётчики и состояние", async () => {
    renderWithClient(<LogsView connectionId="c1" namespace="team-a" pod="web-1" containers={CONTAINERS} />);
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
    renderWithClient(<LogsView connectionId="c1" namespace="team-a" pod="web-1" containers={CONTAINERS} />);
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
    renderWithClient(<LogsView connectionId="c1" namespace="team-a" pod="web-1" containers={CONTAINERS} />);
    await waitFor(() => expect(api.startLogs).toHaveBeenCalledTimes(1));
    act(() => emit({ type: "error", error: { code: "INVALID_INPUT", key: "errors.logs.badRegex", params: { error: "x" }, message: "x", retryable: false } }));
    expect(screen.getByRole("alert")).toHaveTextContent("Неверное регулярное выражение: x");
    await user.click(screen.getByRole("button", { name: "Сохранить в файл…" }));
    await waitFor(() => expect(api.saveLogs).toHaveBeenCalledWith(expect.objectContaining({ pod: "web-1", follow: false }), "/tmp/web-1.log", false));
    expect(await screen.findByText("Сохранено строк: 42 — /tmp/web-1.log")).toBeInTheDocument();
  });
});
