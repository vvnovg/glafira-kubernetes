import { act, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../test/render";

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return { ...orig, api: { startProjectUsage: vi.fn(), stopSession: vi.fn().mockResolvedValue(true) } };
});

import { api } from "../../api/client";
import type { MetricsRejected, ProjectUsage, ProjectUsageEvent, WorkloadResources } from "../../api/types";
import { useWorkspace } from "../../state/workspace";
import { ProjectUsageView } from "./ProjectUsageView";
import { projectRows } from "./projectRows";

const MI = 1024 * 1024;
const workload = (name: string, patch: Partial<WorkloadResources> = {}): WorkloadResources => ({
  kind: "Deployment",
  name,
  pods: 2,
  requests: { cpu: 1, memory: 512 * MI },
  limits: { cpu: 2, memory: 1024 * MI },
  usage: { cpu: 0.2, memory: 256 * MI },
  reserve: { cpu: 0.8, memory: 256 * MI },
  risks: [],
  excess: [],
  ...patch,
});
const usage = (workloads: WorkloadResources[]): ProjectUsage => ({
  namespace: "team-a",
  windowDays: 7,
  source: { kind: "prometheus", origin: "auto", location: "monitoring/prometheus-k8s:9090", history: true },
  sourceStatus: { mode: "auto", selected: null, rejected: [], podMetrics: false, checkedAtMs: 1 },
  workloads,
  errors: [],
  truncated: false,
  quotas: [{ quota: "compute", resource: "requests.cpu", hard: 4, used: 3 }],
  collectedAtMs: Date.now(),
});

/** Thanos отвергнут: нет прав. Текст причины приходит из Rust ключом и параметром, начинается с имени источника. */
const thanosForbidden: MetricsRejected = {
  source: { kind: "thanos", origin: "auto", location: "openshift-monitoring/thanos-querier", history: true },
  reason: "forbidden",
  message: {
    key: "messages.metrics.thanosForbidden",
    params: { source: "Thanos Querier openshift-monitoring/thanos-querier" },
    text: "Thanos Querier openshift-monitoring/thanos-querier: no rights to cluster monitoring",
  },
  error: { code: "FORBIDDEN", message: "forbidden", retryable: false },
};

let send: (e: ProjectUsageEvent) => void = () => {};
beforeEach(() => {
  vi.mocked(api.startProjectUsage).mockReset();
  vi.mocked(api.startProjectUsage).mockImplementation(async (_c, _n, _d, _r, onEvent) => {
    send = onEvent;
    return "s1";
  });
  vi.mocked(api.stopSession).mockClear();
  useWorkspace.setState({ tabs: [], activeId: null });
});

describe("ProjectUsageView", () => {
  it("сессия сбора, квоты, риски и избыток; «Обновить» — мимо кэша; закрытие останавливает сессию", async () => {
    const user = userEvent.setup();
    const { unmount } = renderWithClient(<ProjectUsageView connectionId="c1" namespace="team-a" />);
    expect(screen.getByText(/Собираю потребление проекта/)).toBeInTheDocument();
    expect(api.startProjectUsage).toHaveBeenCalledWith("c1", "team-a", 7, false, expect.any(Function));
    act(() =>
      send({
        kind: "done",
        usage: usage([
          workload("idle", { excess: [{ container: "app", resource: "cpu", request: 2, usage: 0.3 }] }),
          workload("hot", { risks: [{ code: "throttled", container: "app", mean: 0.4 }] }),
        ]),
      }),
    );
    expect(await screen.findByText("app: троттлинг 40 %")).toBeInTheDocument();
    expect(screen.getByText("app: CPU request 2 при p95 300m")).toBeInTheDocument();
    expect(screen.getByText(/compute requests.cpu: 3 \/ 4 \(75 %\)/)).toBeInTheDocument();
    const rows = screen.getAllByRole("row").slice(1);
    expect(within(rows[0]).getByRole("button", { name: "hot" })).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "hot" }));
    expect(useWorkspace.getState().tabs[0]).toMatchObject({ kind: "workload", workloadKind: "Deployment", name: "hot" });
    await user.click(screen.getByRole("button", { name: "Обновить" }));
    expect(api.startProjectUsage).toHaveBeenLastCalledWith("c1", "team-a", 7, true, expect.any(Function));
    unmount();
    expect(api.stopSession).toHaveBeenCalledWith("s1");
  });

  it("сортировка по резерву, «только с рекомендациями», не больше 100 строк", () => {
    const list = [workload("a", { reserve: { cpu: 0.1, memory: 0 } }), workload("b", { reserve: { cpu: 0.5, memory: 0 }, risks: [{ code: "oom", container: "app", count: 1 }] })];
    expect(projectRows(list, "reserve", false, "").map((w) => w.name)).toEqual(["b", "a"]);
    expect(projectRows(list, "risk", true, "").map((w) => w.name)).toEqual(["b"]);
    expect(projectRows(list, "risk", false, "A").map((w) => w.name)).toEqual(["a"]);
  });

  it("причина, по которой источник отвергнут, видна всегда и переведена", async () => {
    renderWithClient(<ProjectUsageView connectionId="c1" namespace="team-a" />);
    await vi.waitFor(() => expect(api.startProjectUsage).toHaveBeenCalled());
    const done = usage([workload("web")]);
    act(() => send({ kind: "done", usage: { ...done, sourceStatus: { ...done.sourceStatus, rejected: [thanosForbidden] } } }));
    expect(
      await screen.findByText("Thanos Querier openshift-monitoring/thanos-querier: нет прав на мониторинг кластера (нужна роль cluster-monitoring-view или аналог)"),
    ).toBeInTheDocument();
  });

  it("больше 100 workloads — пометка", async () => {
    renderWithClient(<ProjectUsageView connectionId="c1" namespace="team-a" />);
    await vi.waitFor(() => expect(api.startProjectUsage).toHaveBeenCalled());
    act(() => send({ kind: "done", usage: usage(Array.from({ length: 120 }, (_, i) => workload(`w${i}`))) }));
    expect(await screen.findByText("Показаны 100 из 120: сузьте список по имени")).toBeInTheDocument();
    expect(screen.getAllByRole("row")).toHaveLength(101);
  });
});
