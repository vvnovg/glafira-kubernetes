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
  return {
    ...orig,
    api: {
      getNamespaceAccess: vi.fn(),
      listPods: vi.fn(),
      listWorkloads: vi.fn(),
      getQuotas: vi.fn(),
      listEvents: vi.fn(),
      startWatch: vi.fn(),
      stopSession: vi.fn(),
    },
  };
});

import { api } from "../../api/client";
import type { NamespaceAccess, WatchEvent } from "../../api/types";
import { useWorkspace } from "../../state/workspace";
import { podRow, workloadRow } from "../../test/objects";
import { ProjectPanel } from "./ProjectPanel";

let emit: (e: WatchEvent) => void = () => undefined;

const ALL: NamespaceAccess = { resources: {}, incomplete: false };
const now = () => Math.floor(Date.now() / 1000);

beforeEach(() => {
  vi.clearAllMocks();
  useWorkspace.setState({ tabs: [], activeId: null });
  vi.mocked(api.getNamespaceAccess).mockResolvedValue(ALL);
  vi.mocked(api.listPods).mockResolvedValue({
    items: [podRow("web-a"), podRow("web-b", { phase: "Pending", status: "ImagePullBackOff", problem: "imagePull" })],
    truncated: false,
  });
  vi.mocked(api.listWorkloads).mockResolvedValue({ items: [workloadRow("web", { problem: "unavailable" }), workloadRow("api")], errors: [], truncated: false });
  vi.mocked(api.getQuotas).mockResolvedValue({
    quotas: { data: [{ name: "compute", scopes: [], entries: [{ resource: "requests.cpu", used: "3600m", hard: "4", ratio: 0.9 }, { resource: "pods", used: "2", hard: "20", ratio: 0.1 }] }] },
    limitRanges: { data: [] },
  });
  vi.mocked(api.listEvents).mockResolvedValue({
    items: [
      { key: "e1", type: "Warning", reason: "Failed", object: { kind: "Pod", name: "web-b" }, message: "image not found", count: 3, firstSeen: now() - 600, lastSeen: now() - 60, source: "kubelet" },
      { key: "e2", type: "Warning", reason: "Old", object: { kind: "Pod", name: "x" }, message: "long ago", count: 1, firstSeen: now() - 7200, lastSeen: now() - 7200, source: null },
    ],
    truncated: false,
  });
  vi.mocked(api.startWatch).mockImplementation(async (_p, onEvent) => {
    emit = onEvent;
    return "s1";
  });
  vi.mocked(api.stopSession).mockResolvedValue(true);
});

describe("ProjectPanel", () => {
  it("обзор: поды по фазам и проблемам, workloads не в норме, квоты у предела, Warning за час", async () => {
    const user = userEvent.setup();
    renderWithClient(<ProjectPanel connectionId="c1" namespace="team-a" />);
    expect(await screen.findByRole("button", { name: "web-b" })).toBeInTheDocument();
    expect(screen.getByText("ImagePullBackOff")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Deployment/web" })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Deployment/api" })).not.toBeInTheDocument();
    expect(await screen.findByText(/3600m \/ 4 \(90 %\)/)).toBeInTheDocument();
    expect(screen.getByText(/image not found/)).toBeInTheDocument();
    expect(screen.queryByText(/long ago/)).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "web-b" }));
    expect(useWorkspace.getState().tabs[0]).toMatchObject({ kind: "pod", name: "web-b" });
  });

  it("workloads через watch, виды без прав — отдельно; открытие workload'а", async () => {
    const user = userEvent.setup();
    renderWithClient(<ProjectPanel connectionId="c1" namespace="team-a" />);
    await user.click(await screen.findByRole("tab", { name: "Workloads" }));
    await waitFor(() => expect(api.startWatch).toHaveBeenCalledWith({ connectionId: "c1", namespace: "team-a", kind: "workloads" }, expect.any(Function)));
    act(() =>
      emit({
        type: "reset",
        items: [workloadRow("web"), workloadRow("report", { kind: "CronJob", schedule: "0 3 * * *" })],
        errors: [{ kind: "DaemonSet", error: { code: "FORBIDDEN", key: "errors.k8s.forbiddenIn", params: { verb: "list", resource: "daemonsets", namespace: "team-a" }, message: "x", retryable: false } }],
      }),
    );
    expect(screen.getByText("DaemonSet: — Нет прав: list daemonsets в проекте «team-a»")).toBeInTheDocument();
    expect(screen.getByText("0 3 * * *")).toBeInTheDocument();
    await user.selectOptions(screen.getByLabelText("Вид"), "CronJob");
    expect(screen.queryByText("web")).not.toBeInTheDocument();
    await user.click(screen.getByText("report"));
    expect(useWorkspace.getState().tabs[0]).toMatchObject({ kind: "workload", workloadKind: "CronJob", name: "report" });
  });

  it("подвкладки без прав на чтение не показываются", async () => {
    vi.mocked(api.getNamespaceAccess).mockResolvedValue({
      resources: { pods: { get: true, list: true, watch: false }, events: { get: false, list: false, watch: false } },
      incomplete: false,
    });
    renderWithClient(<ProjectPanel connectionId="c1" namespace="team-a" />);
    await waitFor(() => expect(screen.queryByRole("tab", { name: "События" })).not.toBeInTheDocument());
    expect(screen.getByRole("tab", { name: "Поды" })).toBeInTheDocument();
  });
});
