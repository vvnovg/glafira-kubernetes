import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../test/render";

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return {
    ...orig,
    api: { listConnections: vi.fn().mockResolvedValue([]), getNamespaceAccess: vi.fn(), recommendResources: vi.fn(), getPodUsage: vi.fn(), getMetricsSourceStatus: vi.fn(), previewChange: vi.fn() },
  };
});
vi.mock("./UsageChart", () => ({ UsageChart: ({ title }: { title: string }) => <p>график: {title}</p> }));

import { api } from "../../api/client";
import type { Analysis, MetricsRejected, MetricsSourceInfo, MetricsSourceStatus } from "../../api/types";
import { PodResources, WorkloadResources } from "./WorkloadResources";

const MI = 1024 * 1024;
const source: MetricsSourceInfo = { kind: "prometheus", origin: "auto", location: "monitoring/prometheus-k8s:9090", history: true };
const none = { cpu: null, memory: null };
const podMetrics: MetricsSourceInfo = { kind: "metricsServer", origin: "auto", location: "metrics.k8s.io", history: false };
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
const THANOS_RU = "Thanos Querier openshift-monitoring/thanos-querier: нет прав на мониторинг кластера (нужна роль cluster-monitoring-view или аналог)";
const status = (rejected: MetricsRejected[], selected: MetricsSourceInfo | null = null): MetricsSourceStatus => ({ mode: "auto", selected, rejected, podMetrics: selected === null, checkedAtMs: 1 });

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

/** Только metrics.k8s.io: Thanos не подошёл, истории нет, окно не применяется. */
function currentOnly(a: Analysis): Analysis {
  return {
    usage: { ...a.usage, source: podMetrics, sourceStatus: status([thanosForbidden]), coverage: null, dataSeconds: null, charts: null },
    recommendation: { ...a.recommendation, source: podMetrics, coverage: null, dataSeconds: null },
  };
}

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(api.getNamespaceAccess).mockResolvedValue({ resources: {}, incomplete: true });
  vi.mocked(api.getMetricsSourceStatus).mockResolvedValue(status([]));
});

describe("WorkloadResources", () => {
  it("источник, уверенность, «сейчас → рекомендация», статистика, итог и графики", async () => {
    vi.mocked(api.recommendResources).mockResolvedValue(analysis("high"));
    renderWithClient(<WorkloadResources connectionId="c1" namespace="team-a" kind="Deployment" name="web" />);
    expect(await screen.findByText("10m → 20m")).toBeInTheDocument();
    expect(api.recommendResources).toHaveBeenCalledWith("c1", "team-a", "Deployment", "web", 7, false);
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
    expect(api.recommendResources).toHaveBeenLastCalledWith("c1", "team-a", "Deployment", "web", 30, false);
  });

  it("причины отказа источников: на успехе — под строкой источника, переведённые", async () => {
    vi.mocked(api.recommendResources).mockResolvedValue(currentOnly(analysis("low")));
    renderWithClient(<WorkloadResources connectionId="c1" namespace="team-a" kind="Deployment" name="web" />);
    expect(await screen.findByText(THANOS_RU)).toBeInTheDocument();
    expect(screen.getByText(/Источник: metrics\.k8s\.io/)).toBeInTheDocument();
    expect(screen.queryByText(/no rights to cluster monitoring/)).not.toBeInTheDocument();
  });

  it("нет ни одного источника: вместо английских подробностей — переведённые причины из выбора источника", async () => {
    vi.mocked(api.recommendResources).mockRejectedValue({
      code: "METRICS_UNAVAILABLE",
      key: "errors.metrics.unavailable",
      message: "No metrics source is available",
      details: "Thanos Querier openshift-monitoring/thanos-querier: no rights to cluster monitoring",
      retryable: false,
    });
    vi.mocked(api.getMetricsSourceStatus).mockResolvedValue(status([thanosForbidden]));
    renderWithClient(<WorkloadResources connectionId="c1" namespace="team-a" kind="Deployment" name="web" />);
    expect(await screen.findByText("Ни один источник метрик не доступен")).toBeInTheDocument();
    expect(await screen.findByText(THANOS_RU)).toBeInTheDocument();
    expect(api.getMetricsSourceStatus).toHaveBeenCalledWith("c1", false);
    expect(screen.queryByText("Подробности")).not.toBeInTheDocument();
    expect(screen.queryByText(/no rights to cluster monitoring/)).not.toBeInTheDocument();
  });

  it("нет ни одного источника и выбор не прочитался: английские подробности остаются", async () => {
    vi.mocked(api.recommendResources).mockRejectedValue({
      code: "METRICS_UNAVAILABLE",
      key: "errors.metrics.unavailable",
      message: "No metrics source is available",
      details: "Thanos Querier: no rights",
      retryable: false,
    });
    vi.mocked(api.getMetricsSourceStatus).mockRejectedValue({ code: "INTERNAL", message: "boom", retryable: false });
    renderWithClient(<WorkloadResources connectionId="c1" namespace="team-a" kind="Deployment" name="web" />);
    expect(await screen.findByText("Подробности")).toBeInTheDocument();
    expect(screen.getByText("Thanos Querier: no rights")).toBeInTheDocument();
  });

  it("«Обновить» и «Повторить» выбирают источник заново, обычная загрузка и смена окна — нет", async () => {
    vi.mocked(api.recommendResources).mockResolvedValue(analysis("high"));
    const user = userEvent.setup();
    renderWithClient(<WorkloadResources connectionId="c1" namespace="team-a" kind="Deployment" name="web" />);
    await screen.findByText("10m → 20m");
    expect(api.recommendResources).toHaveBeenLastCalledWith("c1", "team-a", "Deployment", "web", 7, false);
    await user.click(screen.getByRole("button", { name: "Обновить" }));
    await waitFor(() => expect(api.recommendResources).toHaveBeenLastCalledWith("c1", "team-a", "Deployment", "web", 7, true));
    // Флаг одноразовый: следующая загрузка (другое окно) снова берёт выбор из подключения.
    await user.selectOptions(screen.getByLabelText("Окно"), "30");
    await waitFor(() => expect(api.recommendResources).toHaveBeenLastCalledWith("c1", "team-a", "Deployment", "web", 30, false));
    // «Повторить» после ошибки.
    vi.mocked(api.recommendResources).mockRejectedValueOnce({ code: "TIMEOUT", message: "timeout", retryable: true });
    await user.click(screen.getByRole("button", { name: "Обновить" }));
    await user.click(await screen.findByRole("button", { name: "Повторить" }));
    await waitFor(() => expect(api.recommendResources).toHaveBeenCalledTimes(5));
    expect(api.recommendResources).toHaveBeenLastCalledWith("c1", "team-a", "Deployment", "web", 30, true);
  });

  it("только metrics.k8s.io: строка вместо дней, окно недоступно с причиной в подсказке", async () => {
    vi.mocked(api.recommendResources).mockResolvedValue(currentOnly(analysis("low")));
    renderWithClient(<WorkloadResources connectionId="c1" namespace="team-a" kind="Deployment" name="web" />);
    const line = await screen.findByText(/Источник: metrics\.k8s\.io/);
    expect(line).toHaveTextContent("Только текущие значения metrics.k8s.io: истории нет, окно не применяется");
    expect(line).not.toHaveTextContent("дн.");
    const select = screen.getByLabelText("Окно");
    expect(select).toBeDisabled();
    expect(select).toHaveAttribute("title", "Только текущие значения metrics.k8s.io: истории нет, окно не применяется");
  });

  it("с историей окно доступно, в строке источника — дни", async () => {
    vi.mocked(api.recommendResources).mockResolvedValue(analysis("high"));
    renderWithClient(<WorkloadResources connectionId="c1" namespace="team-a" kind="Deployment" name="web" />);
    expect(await screen.findByText(/Источник: Prometheus/)).toHaveTextContent("7 дн.");
    expect(screen.getByLabelText("Окно")).toBeEnabled();
  });
});

describe("PodResources", () => {
  it("текущее потребление контейнеров пода", async () => {
    vi.mocked(api.getPodUsage).mockResolvedValue({ source: podMetrics, sourceStatus: status([thanosForbidden]), containers: [{ name: "app", cpu: 0.25, memory: 512 * MI }] });
    renderWithClient(<PodResources connectionId="c1" namespace="team-a" pod="web-1" />);
    expect(await screen.findByText("512Mi")).toBeInTheDocument();
    expect(screen.getByText("250m")).toBeInTheDocument();
    expect(screen.getByText("Источник: metrics.k8s.io")).toBeInTheDocument();
    expect(api.getPodUsage).toHaveBeenCalledWith("c1", "team-a", "web-1", false);
    // Почему Thanos не подошёл — под строкой источника.
    expect(screen.getByText(THANOS_RU)).toBeInTheDocument();
  });

  it("под без метрик: сообщение вместо пустой таблицы", async () => {
    vi.mocked(api.getPodUsage).mockResolvedValue({ source: podMetrics, sourceStatus: status([]), containers: [] });
    renderWithClient(<PodResources connectionId="c1" namespace="team-a" pod="web-1" />);
    expect(await screen.findByText("Нет данных о потреблении пода")).toBeInTheDocument();
    expect(screen.queryByRole("table")).not.toBeInTheDocument();
  });

  it("«Обновить» выбирает источник заново; нет источников — переведённые причины", async () => {
    vi.mocked(api.getPodUsage).mockResolvedValue({ source: podMetrics, sourceStatus: status([]), containers: [] });
    const user = userEvent.setup();
    renderWithClient(<PodResources connectionId="c1" namespace="team-a" pod="web-1" />);
    await screen.findByText("Нет данных о потреблении пода");
    vi.mocked(api.getPodUsage).mockRejectedValueOnce({ code: "METRICS_UNAVAILABLE", key: "errors.metrics.unavailable", message: "No metrics source is available", details: "x", retryable: false });
    vi.mocked(api.getMetricsSourceStatus).mockResolvedValue(status([thanosForbidden]));
    await user.click(screen.getByRole("button", { name: "Обновить" }));
    expect(api.getPodUsage).toHaveBeenLastCalledWith("c1", "team-a", "web-1", true);
    expect(await screen.findByText(THANOS_RU)).toBeInTheDocument();
  });
});
