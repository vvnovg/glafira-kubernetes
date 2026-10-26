import { act, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../test/render";

vi.mock("@tanstack/react-virtual", () => ({
  useVirtualizer: (o: { count: number }) => ({
    getVirtualItems: () => Array.from({ length: o.count }, (_, index) => ({ index, start: index * 28 })),
    getTotalSize: () => o.count * 28,
  }),
}));

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return { ...orig, api: { startWatch: vi.fn(), stopSession: vi.fn(), listPods: vi.fn() } };
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

  it("поды CronJob читаются списком, без watch", async () => {
    vi.mocked(api.listPods).mockResolvedValue({ items: [podRow("report-1-x", { owner: { kind: "Job", name: "report-1" } })], truncated: false });
    renderWithClient(<PodList connectionId="c1" namespace="team-a" workload={{ kind: "CronJob", name: "report" }} />);
    expect(await screen.findByText("report-1-x")).toBeInTheDocument();
    expect(api.listPods).toHaveBeenCalledWith("c1", "team-a", { kind: "CronJob", name: "report" });
    expect(api.startWatch).not.toHaveBeenCalled();
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
