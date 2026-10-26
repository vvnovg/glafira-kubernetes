import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { TabActiveContext } from "../../lib/tabActive";
import { renderWithClient } from "../../test/render";

vi.mock("@tanstack/react-virtual", () => ({
  useVirtualizer: (o: { count: number }) => ({
    getVirtualItems: () => Array.from({ length: o.count }, (_, index) => ({ index, start: index * 28 })),
    getTotalSize: () => o.count * 28,
  }),
}));

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return { ...orig, api: { getHealth: vi.fn().mockResolvedValue({ lastCheck: null, ignored: [], annotations: [] }), startWatch: vi.fn(), stopSession: vi.fn(), listPods: vi.fn() } };
});

import { api } from "../../api/client";
import type { EventRow, WatchEvent } from "../../api/types";
import { useWorkspace } from "../../state/workspace";
import { podRow } from "../../test/objects";
import { EventsTable } from "./EventsTable";
import { PodList } from "./PodList";

let emit: (e: WatchEvent) => void = () => undefined;

beforeEach(() => {
  vi.clearAllMocks();
  useWorkspace.setState({ tabs: [], activeId: null });
  vi.mocked(api.startWatch).mockImplementation(async (_p, onEvent) => {
    emit = onEvent;
    return "s1";
  });
  vi.mocked(api.stopSession).mockResolvedValue(true);
});

describe("PodList", () => {
  it("поды workload'а через watch: только проблемные, фильтр по имени, опрос, открытие пода", async () => {
    const user = userEvent.setup();
    renderWithClient(<PodList connectionId="c1" namespace="team-a" workload={{ kind: "Deployment", name: "web" }} />);
    await waitFor(() =>
      expect(api.startWatch).toHaveBeenCalledWith(
        { connectionId: "c1", namespace: "team-a", kind: "pods", workload: { kind: "Deployment", name: "web" } },
        expect.any(Function),
      ),
    );
    act(() =>
      emit({
        type: "reset",
        items: [podRow("web-b"), podRow("web-a", { status: "CrashLoopBackOff", problem: "crashLoop", restarts: 7, lastRestart: 1 })],
        errors: [],
      }),
    );
    expect(screen.getAllByRole("row").slice(1).map((r) => r.firstChild?.textContent)).toEqual(["web-a", "web-b"]);
    await user.click(screen.getByLabelText("Только проблемные (1)"));
    expect(screen.queryByText("web-b")).not.toBeInTheDocument();
    expect(screen.getByText("CrashLoopBackOff")).toHaveClass("problem");
    act(() => emit({ type: "polling" }));
    expect(screen.getByText("Нет прав на watch — список обновляется раз в 10 с")).toBeInTheDocument();
    await user.click(screen.getByText("web-a"));
    expect(useWorkspace.getState().tabs[0]).toMatchObject({ kind: "pod", connectionId: "c1", namespace: "team-a", name: "web-a" });
  });

  it("ошибка до первого списка видна рядом с «Загрузка…», пока watcher пробует снова", async () => {
    renderWithClient(<PodList connectionId="c1" namespace="team-a" />);
    await waitFor(() => expect(api.startWatch).toHaveBeenCalledTimes(1));
    act(() => emit({ type: "error", error: { code: "TIMEOUT", key: "errors.k8s.timeout", message: "t", retryable: true }, fatal: false }));
    expect(screen.getByText("Загрузка…")).toBeInTheDocument();
    expect(screen.getByText("Сервер не ответил вовремя")).toHaveClass("warn");
    act(() => emit({ type: "reset", items: [podRow("web-a")], errors: [] }));
    expect(screen.queryByText("Загрузка…")).not.toBeInTheDocument();
    expect(screen.queryByText("Сервер не ответил вовремя")).not.toBeInTheDocument();
  });

  it("поды CronJob читаются списком, без watch", async () => {
    vi.mocked(api.listPods).mockResolvedValue({ items: [podRow("report-1-x", { owner: { kind: "Job", name: "report-1" } })], truncated: false });
    renderWithClient(<PodList connectionId="c1" namespace="team-a" workload={{ kind: "CronJob", name: "report" }} />);
    expect(await screen.findByText("report-1-x")).toBeInTheDocument();
    expect(api.listPods).toHaveBeenCalledWith("c1", "team-a", { kind: "CronJob", name: "report" });
    expect(api.startWatch).not.toHaveBeenCalled();
  });
});

const TRUNCATED = "Список неполный: показаны первые 10 000 объектов";

describe("PodList: список обрезан", () => {
  it("watch: заметка про первые 10 000 стоит, пока сессия держит предел", async () => {
    renderWithClient(<PodList connectionId="c1" namespace="team-a" />);
    await waitFor(() => expect(api.startWatch).toHaveBeenCalledTimes(1));
    act(() => emit({ type: "reset", items: [podRow("web-a")], errors: [], truncated: false }));
    expect(screen.queryByText(TRUNCATED)).not.toBeInTheDocument();
    act(() => emit({ type: "reset", items: [podRow("web-a")], errors: [], truncated: true }));
    expect(screen.getByText(TRUNCATED)).toBeInTheDocument();
    act(() => emit({ type: "reset", items: [podRow("web-a")], errors: [], truncated: false }));
    expect(screen.queryByText(TRUNCATED)).not.toBeInTheDocument();
  });

  it("поды CronJob (список): заметка, когда прочитано не всё", async () => {
    vi.mocked(api.listPods).mockResolvedValue({ items: [podRow("report-1-x")], truncated: true });
    renderWithClient(<PodList connectionId="c1" namespace="team-a" workload={{ kind: "CronJob", name: "report" }} />);
    expect(await screen.findByText("report-1-x")).toBeInTheDocument();
    expect(screen.getByText(TRUNCATED)).toBeInTheDocument();
  });
});

describe("PodList: вкладка скрыта", () => {
  afterEach(() => vi.useRealTimers());

  const inTab = (client: QueryClient, active: boolean, workload?: { kind: "CronJob" | "Deployment"; name: string }) => (
    <QueryClientProvider client={client}>
      <TabActiveContext.Provider value={active}>
        <PodList connectionId="c1" namespace="team-a" workload={workload} />
      </TabActiveContext.Provider>
    </QueryClientProvider>
  );

  it("watch не начинается в скрытой вкладке и останавливается, когда вкладку скрыли", async () => {
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const view = render(inTab(client, false));
    await act(async () => undefined);
    expect(api.startWatch).not.toHaveBeenCalled();
    view.rerender(inTab(client, true));
    await waitFor(() => expect(api.startWatch).toHaveBeenCalledTimes(1));
    act(() => emit({ type: "reset", items: [podRow("web-a")], errors: [], truncated: false }));
    view.rerender(inTab(client, false));
    expect(api.stopSession).toHaveBeenCalledWith("s1");
    // Строки на месте: при возврате нет пустого списка.
    expect(screen.getByText("web-a")).toBeInTheDocument();
  });

  it("поды CronJob опрашиваются раз в 10 с, только пока вкладка показана", async () => {
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval"] });
    vi.mocked(api.listPods).mockResolvedValue({ items: [podRow("report-1-x")], truncated: false });
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const cron = { kind: "CronJob" as const, name: "report" };
    const tick = () =>
      act(async () => {
        vi.advanceTimersByTime(10_000);
      });
    const view = render(inTab(client, true, cron));
    await act(async () => undefined);
    expect(api.listPods).toHaveBeenCalledTimes(1);
    await tick();
    expect(api.listPods).toHaveBeenCalledTimes(2);

    view.rerender(inTab(client, false, cron));
    await tick();
    await tick();
    expect(api.listPods).toHaveBeenCalledTimes(2);

    view.rerender(inTab(client, true, cron));
    await tick();
    expect(api.listPods).toHaveBeenCalledTimes(3);
  });
});

describe("EventsTable", () => {
  const ev = (key: string, type: string, reason: string, name: string, lastSeen: number): EventRow => ({
    key,
    type,
    reason,
    object: { kind: "Pod", name },
    message: `${reason} message`,
    count: 1,
    firstSeen: lastSeen,
    lastSeen,
    source: "kubelet",
  });

  it("новые сверху, фильтры по типу, причине и объекту", async () => {
    const user = userEvent.setup();
    renderWithClient(
      <EventsTable rows={[ev("1", "Normal", "Pulled", "web-a", 10), ev("2", "Warning", "BackOff", "web-a", 20), ev("3", "Warning", "FailedMount", "db-0", 15)]} />,
    );
    const reasons = () => screen.getAllByRole("row").slice(1).map((r) => r.children[2].textContent);
    expect(reasons()).toEqual(["BackOff", "FailedMount", "Pulled"]);
    await user.selectOptions(screen.getByLabelText("Тип"), "Warning");
    expect(reasons()).toEqual(["BackOff", "FailedMount"]);
    await user.type(screen.getByLabelText("Объект"), "db-");
    expect(reasons()).toEqual(["FailedMount"]);
    await user.clear(screen.getByLabelText("Объект"));
    await user.selectOptions(screen.getByLabelText("Причина"), "BackOff");
    expect(reasons()).toEqual(["BackOff"]);
  });
});
