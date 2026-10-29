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
  return {
    ...orig,
    api: {
      getHealth: vi.fn().mockResolvedValue({ lastCheck: null, ignored: [], annotations: [], proposals: [] }),
      getWorkload: vi.fn(),
      listConnections: vi.fn().mockResolvedValue([]),
      startWatch: vi.fn(),
      stopSession: vi.fn(),
      listEvents: vi.fn(),
      getYaml: vi.fn(),
      previewChange: vi.fn(),
      applyChange: vi.fn(),
    },
  };
});

import { api } from "../../api/client";
import type { Change, Proposal, WorkloadDetails } from "../../api/types";
import { container, workloadRow } from "../../test/objects";
import { newProfile } from "../connections/profile";
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
  // Кнопки изменений читают профиль подключения из кэша ["connections"]; тест применения подставляет свой.
  vi.mocked(api.listConnections).mockResolvedValue([]);
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

  const proposal: Proposal = {
    id: "p1",
    severity: "warning",
    title: "Two replicas",
    rationale: "r",
    target: { kind: "workload", namespace: "team-a", name: "web", workloadKind: "Deployment" },
    change: { kind: "scale", namespace: "team-a", workload: { kind: "Deployment", name: "web" }, replicas: 2 },
    client: null,
    createdAtMs: 1,
    updatedAtMs: 1,
    status: "open",
  };

  it("«Открыть в форме» предложения LLM: форма поверх вкладки, «Отмена» её закрывает", async () => {
    vi.mocked(api.getWorkload).mockResolvedValue(DETAILS);
    const user = userEvent.setup();
    renderWithClient(<WorkloadPanel connectionId="c1" namespace="team-a" kind="Deployment" name="web" proposal={{ proposal, nonce: 1 }} />);
    const dialog = screen.getByRole("dialog", { name: "Реплики: Deployment web" });
    expect(dialog).toHaveTextContent("Применяется предложение LLM: Two replicas");
    expect(screen.getByLabelText("Реплики")).toHaveValue(2);
    await user.click(screen.getByRole("button", { name: "Отмена" }));
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  });

  it("предложение LLM: «Проверить» → «Применить» отправляет applyChange с id предложения", async () => {
    vi.mocked(api.listConnections).mockResolvedValue([{ ...newProfile(), id: "c1", name: "dev", environment: "dev" }]);
    vi.mocked(api.previewChange).mockImplementation(async (_id, change) => ({
      change,
      version: "g4",
      diff: [{ container: null, field: "replicas", before: "3", after: "2" }],
      warnings: [],
      dryRun: { status: "passed" },
      confirmPodName: false,
    }));
    vi.mocked(api.applyChange).mockImplementation(async (_id, change) => change);
    const user = userEvent.setup();
    renderWithClient(<WorkloadPanel connectionId="c1" namespace="team-a" kind="Deployment" name="web" proposal={{ proposal, nonce: 1 }} />);
    await user.click(screen.getByRole("button", { name: "Проверить" }));
    await user.click(await screen.findByRole("button", { name: "Применить" }));
    const sent: Change = proposal.change;
    await waitFor(() => expect(api.applyChange).toHaveBeenCalledWith("c1", sent, "g4", null, null, "p1"));
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
  });
});
