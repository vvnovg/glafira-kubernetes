import { act, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../test/render";

vi.mock("@tanstack/react-virtual", () => ({
  useVirtualizer: (o: { count: number }) => ({
    getVirtualItems: () => Array.from({ length: o.count }, (_, index) => ({ index, start: index * 28 })),
    getTotalSize: () => o.count * 28,
  }),
}));

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return { ...orig, api: { listNodes: vi.fn(), getNode: vi.fn() } };
});

import { api } from "../../api/client";
import type { NodeDetails, NodeRow } from "../../api/types";
import { NodesPanel } from "./NodesPanel";

const node = (name: string, ready: boolean, problems: string[] = []): NodeRow => ({
  name,
  roles: ["worker"],
  ready,
  problems,
  unschedulable: false,
  kubeletVersion: "v1.32.5",
  internalIp: "10.0.0.1",
  cpu: "3500m",
  memory: "15Gi",
  pods: "250",
  created: 1,
  taints: 0,
});

/** Сведения о ноде для тестов, где важны только имя и условия. */
const details = (name: string): NodeDetails => ({
  node: node(name, true),
  conditions: [{ type: "Ready", status: "True", reason: "KubeletReady" }],
  taints: [],
  labels: {},
  capacity: { cpu: "4" },
  allocatable: { cpu: "3500m" },
  addresses: [],
  osImage: null,
  kernelVersion: null,
  containerRuntime: null,
  architecture: null,
});

beforeEach(() => vi.clearAllMocks());
afterEach(() => vi.useRealTimers());

describe("NodesPanel", () => {
  it("список нод и сведения о выбранной", async () => {
    vi.mocked(api.listNodes).mockResolvedValue({ items: [node("w1", true), node("w2", false, ["NotReady", "DiskPressure"])], truncated: false });
    vi.mocked(api.getNode).mockResolvedValue({
      node: node("w2", false),
      conditions: [{ type: "DiskPressure", status: "True", reason: "KubeletHasDiskPressure" }],
      taints: ["node.kubernetes.io/disk-pressure:NoSchedule"],
      labels: { "kubernetes.io/os": "linux" },
      capacity: { cpu: "4", memory: "16Gi" },
      allocatable: { cpu: "3500m", memory: "15Gi" },
      addresses: ["InternalIP: 10.0.0.2"],
      osImage: "RHCOS",
      kernelVersion: "5.14",
      containerRuntime: "cri-o://1.32",
      architecture: "amd64",
    });
    const user = userEvent.setup();
    renderWithClient(<NodesPanel connectionId="c1" />);
    expect(await screen.findByText("w1")).toBeInTheDocument();
    expect(screen.getByText("NotReady, DiskPressure")).toBeInTheDocument();
    await user.click(screen.getByText("w2"));
    expect(await screen.findByText("KubeletHasDiskPressure")).toBeInTheDocument();
    expect(api.getNode).toHaveBeenCalledWith("c1", "w2");
    expect(screen.getByText("node.kubernetes.io/disk-pressure:NoSchedule")).toBeInTheDocument();
    expect(screen.getByText("cri-o://1.32")).toBeInTheDocument();
    expect(screen.getByText("16Gi")).toBeInTheDocument();
  });

  it("пустой список нод — «Пусто»", async () => {
    vi.mocked(api.listNodes).mockResolvedValue({ items: [], truncated: false });
    renderWithClient(<NodesPanel connectionId="c1" />);
    expect(await screen.findByText("Пусто")).toBeInTheDocument();
    expect(screen.queryByText("Загрузка…")).not.toBeInTheDocument();
  });

  it("список обрезан — предупреждение", async () => {
    vi.mocked(api.listNodes).mockResolvedValue({ items: [node("w1", true)], truncated: true });
    renderWithClient(<NodesPanel connectionId="c1" />);
    expect(await screen.findByText("Список неполный: показаны первые 10 000 объектов")).toBeInTheDocument();
  });

  it("ошибка чтения: прочая — с «Повторить», нет прав — без него, сессия истекла — «Войти снова»", async () => {
    const user = userEvent.setup();
    vi.mocked(api.listNodes).mockRejectedValueOnce({ code: "CONNECTION_FAILED", message: "No route to host", retryable: true });
    const first = renderWithClient(<NodesPanel connectionId="c1" />);
    expect(await screen.findByText("No route to host")).toBeInTheDocument();
    vi.mocked(api.listNodes).mockResolvedValue({ items: [node("w1", true)], truncated: false });
    await user.click(screen.getByRole("button", { name: "Повторить" }));
    expect(await screen.findByText("w1")).toBeInTheDocument();
    expect(screen.queryByText("No route to host")).not.toBeInTheDocument();
    first.unmount();

    vi.mocked(api.listNodes).mockRejectedValue({
      code: "FORBIDDEN",
      key: "errors.k8s.forbiddenCluster",
      params: { verb: "list", resource: "nodes" },
      message: "x",
      retryable: false,
    });
    const second = renderWithClient(<NodesPanel connectionId="c1" />);
    expect(await screen.findByText("Нет прав: list nodes на уровне кластера")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Повторить" })).not.toBeInTheDocument();
    second.unmount();

    vi.mocked(api.listNodes).mockRejectedValue({ code: "SESSION_EXPIRED", key: "errors.k8s.unauthorized", message: "x", retryable: false });
    renderWithClient(<NodesPanel connectionId="c1" />);
    expect(await screen.findByRole("button", { name: "Войти снова" })).toBeInTheDocument();
  });

  it("сведения о ноде: «Загрузка…», пока читаются", async () => {
    vi.mocked(api.listNodes).mockResolvedValue({ items: [node("w1", true)], truncated: false });
    let finish: (d: NodeDetails) => void = () => undefined;
    vi.mocked(api.getNode).mockReturnValueOnce(new Promise((r) => (finish = r)));
    const user = userEvent.setup();
    renderWithClient(<NodesPanel connectionId="c1" />);
    await user.click(await screen.findByText("w1"));
    const pane = await screen.findByLabelText("w1");
    expect(pane).toHaveTextContent("Загрузка…");
    finish(details("w1"));
    expect(await screen.findByText("KubeletReady")).toBeInTheDocument();
    expect(pane).not.toHaveTextContent("Загрузка…");
  });

  it("автообновление списка обновляет и сведения о выбранной ноде", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    vi.mocked(api.listNodes).mockResolvedValue({ items: [node("w1", true)], truncated: false });
    vi.mocked(api.getNode).mockResolvedValue(details("w1"));
    const user = userEvent.setup({ advanceTimers: vi.advanceTimersByTime });
    renderWithClient(<NodesPanel connectionId="c1" />);
    await user.click(await screen.findByText("w1"));
    await screen.findByText("KubeletReady");
    expect(api.getNode).toHaveBeenCalledTimes(1);
    // Автообновление выключено: сведения не перечитываются.
    await act(async () => {
      await vi.advanceTimersByTimeAsync(31_000);
    });
    expect(api.getNode).toHaveBeenCalledTimes(1);
    await user.selectOptions(screen.getByLabelText("Автообновление"), "каждые 10 с");
    vi.mocked(api.getNode).mockResolvedValue({ ...details("w1"), conditions: [{ type: "Ready", status: "True", reason: "KubeletStillReady" }] });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(10_500);
    });
    await waitFor(() => expect(api.getNode).toHaveBeenCalledTimes(2));
    expect(await screen.findByText("KubeletStillReady")).toBeInTheDocument();
    expect(api.listNodes).toHaveBeenCalledTimes(2);
  });
});
