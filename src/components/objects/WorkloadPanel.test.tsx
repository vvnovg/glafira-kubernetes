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
  return { ...orig, api: { getHealth: vi.fn().mockResolvedValue({ lastCheck: null, ignored: [], annotations: [] }), getWorkload: vi.fn(), startWatch: vi.fn(), stopSession: vi.fn(), listEvents: vi.fn(), getYaml: vi.fn() } };
});

import { api } from "../../api/client";
import type { WorkloadDetails } from "../../api/types";
import { container, workloadRow } from "../../test/objects";
import { WorkloadPanel } from "./WorkloadPanel";

const DETAILS: WorkloadDetails = {
  workload: workloadRow("web", { ready: 2, available: 2, problem: "unavailable" }),
  conditions: [{ type: "Available", status: "False", reason: "MinimumReplicasUnavailable" }],
  selector: "app=web",
  strategy: "RollingUpdate (maxSurge 25%, maxUnavailable 25%)",
  containers: [container()],
  labels: { app: "web" },
  annotations: {},
  activeJobs: [],
  hpa: { data: { name: "web", minReplicas: 2, maxReplicas: 10, currentReplicas: 3, desiredReplicas: 3, metrics: ["cpu 80%"], conditions: [] } },
  pdb: { data: null },
  history: {
    data: [
      { revision: 3, name: "web-7d9f", images: ["registry/web:3"], created: 1, replicas: 3, current: true },
      { revision: 2, name: "web-5c4b", images: ["registry/web:2"], created: 1, replicas: 0, current: false },
    ],
  },
};

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(api.getWorkload).mockResolvedValue(DETAILS);
  vi.mocked(api.startWatch).mockResolvedValue("s1");
  vi.mocked(api.stopSession).mockResolvedValue(true);
  vi.mocked(api.listEvents).mockResolvedValue({ items: [], truncated: false });
  vi.mocked(api.getYaml).mockResolvedValue("kind: Deployment\n");
});

describe("WorkloadPanel", () => {
  it("обзор: реплики, проблема, HPA, история, контейнеры без значений переменных", async () => {
    renderWithClient(<WorkloadPanel connectionId="c1" namespace="team-a" kind="Deployment" name="web" />);
    expect(await screen.findByText("Доступно меньше реплик, чем желаемо")).toBeInTheDocument();
    expect(api.getWorkload).toHaveBeenCalledWith("c1", "team-a", "Deployment", "web");
    expect(screen.getAllByText("app=web"), "селектор и метка").toHaveLength(2);
    expect(screen.getByText("MinimumReplicasUnavailable")).toBeInTheDocument();
    expect(screen.getByText(/2–10, реплик сейчас 3, нужно 3 · cpu 80%/)).toBeInTheDocument();
    expect(screen.getByText("3 (текущая)")).toBeInTheDocument();
    expect(screen.getByText("registry/web:2")).toBeInTheDocument();
    expect(screen.getByText("ждёт: CrashLoopBackOff — back-off 5m0s")).toBeInTheDocument();
    expect(screen.getByText("DB_PASSWORD ← secretKeyRef db/password")).toBeInTheDocument();
    expect(screen.getByText("100m / —")).toBeInTheDocument();
  });

  it("подвкладки: поды workload'а через watch, события вместе с подами, YAML", async () => {
    const user = userEvent.setup();
    renderWithClient(<WorkloadPanel connectionId="c1" namespace="team-a" kind="Deployment" name="web" />);
    await user.click(screen.getByRole("tab", { name: "Поды" }));
    await waitFor(() => expect(api.startWatch).toHaveBeenCalledWith(expect.objectContaining({ kind: "pods", workload: { kind: "Deployment", name: "web" } }), expect.any(Function)));
    await user.click(screen.getByRole("tab", { name: "События" }));
    await waitFor(() => expect(api.listEvents).toHaveBeenCalledWith("c1", "team-a", { kind: "Deployment", name: "web" }));
    expect(api.stopSession).toHaveBeenCalledWith("s1");
    await user.click(screen.getByRole("tab", { name: "YAML" }));
    expect(await screen.findByText("kind: Deployment")).toBeInTheDocument();
  });

  it("CronJob: расписание и идущие Job вместо реплик", async () => {
    vi.mocked(api.getWorkload).mockResolvedValue({
      ...DETAILS,
      workload: workloadRow("report", { kind: "CronJob", desired: null, ready: 1, schedule: "0 3 * * *", suspended: true, lastSchedule: 1, problem: null }),
      activeJobs: ["report-1"],
      history: null,
    });
    renderWithClient(<WorkloadPanel connectionId="c1" namespace="team-a" kind="CronJob" name="report" />);
    expect(await screen.findByText("0 3 * * *")).toBeInTheDocument();
    expect(screen.getByText("report-1")).toBeInTheDocument();
    expect(screen.getByText(/приостановлен/)).toBeInTheDocument();
    expect(screen.queryByText("История выкладки")).not.toBeInTheDocument();
  });
});
