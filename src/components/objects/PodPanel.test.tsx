import { screen, waitFor } from "@testing-library/react";
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
  return { ...orig, api: { getPod: vi.fn(), listEvents: vi.fn(), getYaml: vi.fn() } };
});

import { api } from "../../api/client";
import type { PodDetails } from "../../api/types";
import { useWorkspace } from "../../state/workspace";
import { container, podRow } from "../../test/objects";
import { PodPanel } from "./PodPanel";

const DETAILS: PodDetails = {
  pod: podRow("web-7d9f-a", { status: "CrashLoopBackOff", problem: "crashLoop", ready: 0, restarts: 7 }),
  conditions: [{ type: "Ready", status: "False", reason: "ContainersNotReady" }],
  podIp: "10.1.0.5",
  hostIp: "10.0.0.1",
  serviceAccount: "default",
  started: 1,
  containers: [container({ init: true, name: "migrate", state: { state: "terminated", reason: "Completed", message: null, exitCode: 0, started: 1, finished: 2 }, lastState: null }), container()],
  labels: { app: "web" },
  annotations: { note: "x" },
  volumes: [{ name: "data", kind: "persistentVolumeClaim", source: "web-data" }],
};

beforeEach(() => {
  vi.clearAllMocks();
  useWorkspace.setState({ tabs: [], activeId: null });
  vi.mocked(api.getPod).mockResolvedValue(DETAILS);
  vi.mocked(api.listEvents).mockResolvedValue({ items: [], truncated: false });
});

describe("PodPanel", () => {
  it("обзор: статус, контейнеры с прошлым состоянием, тома; владелец открывает workload", async () => {
    const user = userEvent.setup();
    renderWithClient(<PodPanel connectionId="c1" namespace="team-a" name="web-7d9f-a" />);
    expect(await screen.findByText("CrashLoopBackOff")).toHaveClass("problem");
    expect(screen.getByText("migrate")).toBeInTheDocument();
    expect(screen.getByText("(init-контейнер)")).toBeInTheDocument();
    expect(screen.getByText(/завершён: Error, код 1/)).toBeInTheDocument();
    expect(screen.getAllByText("registry/web@sha256:abc", { selector: "code" })).toHaveLength(2);
    expect(screen.getByText("data: persistentVolumeClaim web-data")).toBeInTheDocument();
    expect(screen.getByText("ContainersNotReady")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Deployment/web" }));
    expect(useWorkspace.getState().tabs[0]).toMatchObject({ kind: "workload", workloadKind: "Deployment", name: "web", namespace: "team-a" });
  });

  it("события пода", async () => {
    const user = userEvent.setup();
    renderWithClient(<PodPanel connectionId="c1" namespace="team-a" name="web-7d9f-a" />);
    await user.click(screen.getByRole("tab", { name: "События" }));
    await waitFor(() => expect(api.listEvents).toHaveBeenCalledWith("c1", "team-a", { kind: "Pod", name: "web-7d9f-a" }));
    expect(screen.queryByText("Список неполный: показаны первые 10 000 объектов")).not.toBeInTheDocument();
  });

  it("события пода: прочитано не всё — заметка про первые 10 000", async () => {
    vi.mocked(api.listEvents).mockResolvedValue({ items: [], truncated: true });
    const user = userEvent.setup();
    renderWithClient(<PodPanel connectionId="c1" namespace="team-a" name="web-7d9f-a" />);
    await user.click(screen.getByRole("tab", { name: "События" }));
    expect(await screen.findByText("Список неполный: показаны первые 10 000 объектов")).toBeInTheDocument();
  });
});
