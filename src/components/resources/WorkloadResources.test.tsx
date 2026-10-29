import { screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../test/render";

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return {
    ...orig,
    api: { listConnections: vi.fn().mockResolvedValue([]), getNamespaceAccess: vi.fn(), recommendResources: vi.fn(), getPodUsage: vi.fn(), previewChange: vi.fn() },
  };
});
vi.mock("./UsageChart", () => ({ UsageChart: ({ title }: { title: string }) => <p>график: {title}</p> }));

import { api } from "../../api/client";
import type { Analysis, MetricsSourceInfo } from "../../api/types";
import { PodResources, WorkloadResources } from "./WorkloadResources";

const MI = 1024 * 1024;
const source: MetricsSourceInfo = { kind: "prometheus", origin: "auto", location: "monitoring/prometheus-k8s:9090", history: true };
const none = { cpu: null, memory: null };

function analysis(confidence: Analysis["recommendation"]["confidence"]): Analysis {
  return {
    usage: {
      namespace: "team-a",
      kind: "Deployment",
      name: "web",
      windowDays: 7,
      source,
      sourceStatus: { mode: "auto", selected: source, rejected: [], podMetrics: false, checkedAtMs: 1 },
      coverage: 1,
      dataSeconds: 31 * 86_400,
      ownerMetrics: true,
      replicas: 2,
      surge: 1,
      containers: [
        {
          name: "app",
          init: false,
          sidecar: false,
          requests: { cpu: 0.01, memory: 16 * MI },
          limits: { cpu: 0.05, memory: 32 * MI },
          limitRangeRequests: none,
          limitRangeLimits: none,
          envNames: [],
          stats: {
            cpu: { p50: 0.008, p95: 0.012, p99: 0.015, max: 0.02 },
            memory: { p95: 20 * MI, p99: 22 * MI, max: 30 * MI },
            throttling: { mean: 0.4, p95: 0.6 },
            oomCount: 0,
            oomLast: null,
            restarts: 0,
          },
        },
      ],
      charts: { start: 0, end: 1, stepSeconds: 300, cpu: [], memory: [], restarts: [], ooms: [] },
      podsNow: [],
    },
    recommendation: {
      namespace: "team-a",
      kind: "Deployment",
      name: "web",
      windowDays: 7,
      source,
      confidence,
      coverage: 1,
      dataSeconds: 31 * 86_400,
      replicas: 2,
      containers: [
        {
          name: "app",
          current: { requests: { cpu: 0.01, memory: 16 * MI }, limits: { cpu: 0.05, memory: 32 * MI } },
          recommended: { requests: { cpu: 0.02, memory: 32 * MI }, limits: { cpu: 0.08, memory: 48 * MI } },
          notes: [{ code: "cpuThrottled", p95: 0.6 }],
        },
      ],
      violations: [],
      hpa: [],
      totals: { requests: { cpu: 0.02, memory: 32 * MI }, limits: { cpu: 0.06, memory: 32 * MI } },
    },
  };
}

beforeEach(() => {
  vi.mocked(api.getNamespaceAccess).mockResolvedValue({ resources: {}, incomplete: true });
});

describe("WorkloadResources", () => {
  it("источник, уверенность, «сейчас → рекомендация», статистика, итог и графики", async () => {
    vi.mocked(api.recommendResources).mockResolvedValue(analysis("high"));
    renderWithClient(<WorkloadResources connectionId="c1" namespace="team-a" kind="Deployment" name="web" />);
    expect(await screen.findByText("10m → 20m")).toBeInTheDocument();
    expect(api.recommendResources).toHaveBeenCalledWith("c1", "team-a", "Deployment", "web", 7);
    expect(screen.getByText(/Prometheus monitoring\/prometheus-k8s:9090/)).toHaveTextContent("покрытие 100 %");
    expect(screen.getByText("уверенность: высокая")).toBeInTheDocument();
    const row = screen.getByRole("row", { name: /app/ });
    expect(within(row).getByText("16Mi → 32Mi")).toBeInTheDocument();
    expect(within(row).getByText("12m / 20m")).toBeInTheDocument();
    expect(within(row).getByText("40 %")).toBeInTheDocument();
    expect(screen.getByText(/app: Троттлинг p95 — 60 %/)).toBeInTheDocument();
    expect(screen.getByText(/На весь workload \(2 реплик\): requests CPU \+20m, память \+32Mi/)).toBeInTheDocument();
    expect(await screen.findByText("график: CPU, ядра")).toBeInTheDocument();
  });

  it("«Применить рекомендацию» открывает форму с рекомендованными значениями; низкая уверенность — предупреждение", async () => {
    vi.mocked(api.recommendResources).mockResolvedValue(analysis("low"));
    const user = userEvent.setup();
    renderWithClient(<WorkloadResources connectionId="c1" namespace="team-a" kind="Deployment" name="web" />);
    await user.click(await screen.findByRole("button", { name: "Применить рекомендацию" }));
    const dialog = screen.getByRole("dialog", { name: "Ресурсы контейнеров: Deployment web" });
    expect(within(dialog).getByText(/Уверенность рекомендации низкая/)).toBeInTheDocument();
    expect(within(dialog).getByLabelText("app CPU request")).toHaveValue("20m");
    expect(within(dialog).getByLabelText("app Memory limit")).toHaveValue("48Mi");
  });

  it("окно меняет запрос; без источника метрик — ошибка с причиной", async () => {
    vi.mocked(api.recommendResources).mockRejectedValue({ code: "METRICS_UNAVAILABLE", key: "errors.metrics.unavailable", message: "No metrics source is available", retryable: false });
    const user = userEvent.setup();
    renderWithClient(<WorkloadResources connectionId="c1" namespace="team-a" kind="Deployment" name="web" />);
    expect(await screen.findByText("Ни один источник метрик не доступен", { exact: false })).toBeInTheDocument();
    await user.selectOptions(screen.getByLabelText("Окно"), "30");
    expect(api.recommendResources).toHaveBeenLastCalledWith("c1", "team-a", "Deployment", "web", 30);
  });
});

describe("PodResources", () => {
  it("текущее потребление контейнеров пода", async () => {
    vi.mocked(api.getPodUsage).mockResolvedValue({ source: { kind: "metricsServer", origin: "auto", location: "metrics.k8s.io", history: false }, containers: [{ name: "app", cpu: 0.25, memory: 512 * MI }] });
    renderWithClient(<PodResources connectionId="c1" namespace="team-a" pod="web-1" />);
    expect(await screen.findByText("512Mi")).toBeInTheDocument();
    expect(screen.getByText("250m")).toBeInTheDocument();
    expect(screen.getByText("Источник: metrics.k8s.io")).toBeInTheDocument();
  });
});
