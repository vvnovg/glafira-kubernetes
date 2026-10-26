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
import type { AppError, ConnectionInfo, KindError, NamespaceAccess, WatchEvent } from "../../api/types";
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

const forbiddenKind = (kind: KindError["kind"], resource: string): KindError => ({
  kind,
  error: { code: "FORBIDDEN", key: "errors.k8s.forbiddenIn", params: { verb: "list", resource, namespace: "team-a" }, message: "x", retryable: false },
});

describe("обзор проекта: виды workloads, которые не прочитались", () => {
  it("ни один вид не прочитан — причины по видам, а не «Все workloads в норме»", async () => {
    vi.mocked(api.listWorkloads).mockResolvedValue({
      items: [],
      errors: [forbiddenKind("Deployment", "deployments"), forbiddenKind("StatefulSet", "statefulsets"), forbiddenKind("Job", "jobs")],
      truncated: false,
    });
    renderWithClient(<ProjectPanel connectionId="c1" namespace="team-a" />);
    expect(await screen.findByText("Deployment: — Нет прав: list deployments в проекте «team-a»")).toBeInTheDocument();
    expect(screen.getByText("StatefulSet: — Нет прав: list statefulsets в проекте «team-a»")).toBeInTheDocument();
    expect(screen.getByText("Job: — Нет прав: list jobs в проекте «team-a»")).toBeInTheDocument();
    expect(screen.queryByText("Все workloads в норме.")).not.toBeInTheDocument();
  });

  it("часть видов прочитана: проблемные workloads и причины рядом, «в норме» нет", async () => {
    vi.mocked(api.listWorkloads).mockResolvedValue({
      items: [workloadRow("web", { problem: "unavailable" }), workloadRow("api")],
      errors: [forbiddenKind("DaemonSet", "daemonsets")],
      truncated: false,
    });
    renderWithClient(<ProjectPanel connectionId="c1" namespace="team-a" />);
    expect(await screen.findByRole("button", { name: "Deployment/web" })).toBeInTheDocument();
    expect(screen.getByText("DaemonSet: — Нет прав: list daemonsets в проекте «team-a»")).toBeInTheDocument();
    expect(screen.queryByText("Все workloads в норме.")).not.toBeInTheDocument();
  });

  it("часть видов не прочитана, прочитанные в порядке: причины без «в норме»", async () => {
    vi.mocked(api.listWorkloads).mockResolvedValue({ items: [workloadRow("api")], errors: [forbiddenKind("CronJob", "cronjobs")], truncated: false });
    renderWithClient(<ProjectPanel connectionId="c1" namespace="team-a" />);
    expect(await screen.findByText("CronJob: — Нет прав: list cronjobs в проекте «team-a»")).toBeInTheDocument();
    expect(screen.queryByText("Все workloads в норме.")).not.toBeInTheDocument();
  });

  it("все виды прочитаны и в порядке — «Все workloads в норме»", async () => {
    vi.mocked(api.listWorkloads).mockResolvedValue({ items: [workloadRow("api")], errors: [], truncated: false });
    renderWithClient(<ProjectPanel connectionId="c1" namespace="team-a" />);
    expect(await screen.findByText("Все workloads в норме.")).toBeInTheDocument();
  });
});

describe("права проекта: перечитывание", () => {
  afterEach(() => vi.useRealTimers());

  const READ_EVENTS_DENIED: NamespaceAccess = {
    resources: { pods: { get: true, list: true, watch: true }, events: { get: false, list: false, watch: false } },
    incomplete: false,
  };
  const READ_EVENTS_GRANTED: NamespaceAccess = {
    resources: { pods: { get: true, list: true, watch: true }, events: { get: true, list: true, watch: true } },
    incomplete: false,
  };

  it("права перечитываются, когда кэш Rust (5 минут) уже истёк, скрытая подвкладка возвращается", async () => {
    vi.mocked(api.getNamespaceAccess).mockResolvedValueOnce(READ_EVENTS_DENIED).mockResolvedValue(READ_EVENTS_GRANTED);
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval"] });
    renderWithClient(<ProjectPanel connectionId="c1" namespace="team-a" />);
    await screen.findByRole("tab", { name: "Поды" });
    await waitFor(() => expect(screen.queryByRole("tab", { name: "События" })).not.toBeInTheDocument());
    expect(api.getNamespaceAccess).toHaveBeenCalledTimes(1);
    // Ровно 5 минут — срок кэша Rust: тик в этот момент получил бы закэшированные права, поэтому его ещё нет.
    await act(async () => {
      vi.advanceTimersByTime(5 * 60_000);
    });
    expect(api.getNamespaceAccess).toHaveBeenCalledTimes(1);
    await act(async () => {
      vi.advanceTimersByTime(5_000);
    });
    expect(await screen.findByRole("tab", { name: "События" })).toBeInTheDocument();
    expect(api.getNamespaceAccess).toHaveBeenCalledTimes(2);
    // Автоматическое чтение — без ручного обновления (`force`): кэш Rust к этому времени истёк.
    expect(api.getNamespaceAccess).toHaveBeenLastCalledWith("c1", "team-a");
    // Следующий тик — через тот же интервал, а не через два.
    await act(async () => {
      vi.advanceTimersByTime(5 * 60_000 + 5_000);
    });
    expect(api.getNamespaceAccess).toHaveBeenCalledTimes(3);
  });

  it("«Обновить» в обзоре читает права заново и возвращает подвкладку", async () => {
    vi.mocked(api.getNamespaceAccess).mockResolvedValueOnce(READ_EVENTS_DENIED).mockResolvedValue(READ_EVENTS_GRANTED);
    const user = userEvent.setup();
    renderWithClient(<ProjectPanel connectionId="c1" namespace="team-a" />);
    await screen.findByRole("tab", { name: "Поды" });
    await waitFor(() => expect(screen.queryByRole("tab", { name: "События" })).not.toBeInTheDocument());
    expect(api.getNamespaceAccess).toHaveBeenCalledTimes(1);
    await user.click(screen.getByRole("button", { name: "Обновить" }));
    expect(await screen.findByRole("tab", { name: "События" })).toBeInTheDocument();
    expect(api.getNamespaceAccess).toHaveBeenCalledTimes(2);
    expect(api.getNamespaceAccess).toHaveBeenLastCalledWith("c1", "team-a", true);
    // Обзор обновился вместе с правами.
    expect(api.listPods).toHaveBeenCalledTimes(2);
  });

  it("права не прочитались при обновлении — ошибка показывается, подвкладки остаются", async () => {
    vi.mocked(api.getNamespaceAccess)
      .mockResolvedValueOnce(READ_EVENTS_GRANTED)
      .mockRejectedValue({ code: "CONNECTION_FAILED", key: "errors.k8s.connect", message: "x", retryable: true });
    const user = userEvent.setup();
    renderWithClient(<ProjectPanel connectionId="c1" namespace="team-a" />);
    await screen.findByRole("tab", { name: "События" });
    await user.click(screen.getByRole("button", { name: "Обновить" }));
    expect(await screen.findByText("Не удаётся подключиться к серверу")).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "События" })).toBeInTheDocument();
  });
});

describe("подключение недоступно: одна ошибка на вкладку", () => {
  const DOWN: AppError = { code: "CONNECTION_FAILED", key: "errors.k8s.connect", message: "x", retryable: true };

  it("все запросы и права отказали одинаково — один баннер, у разделов «—»", async () => {
    vi.mocked(api.getNamespaceAccess).mockRejectedValue(DOWN);
    vi.mocked(api.listPods).mockRejectedValue(DOWN);
    vi.mocked(api.listWorkloads).mockRejectedValue(DOWN);
    vi.mocked(api.getQuotas).mockRejectedValue(DOWN);
    vi.mocked(api.listEvents).mockRejectedValue(DOWN);
    renderWithClient(<ProjectPanel connectionId="c1" namespace="team-a" />);
    await waitFor(() => expect(screen.getAllByText("—")).toHaveLength(4));
    expect(screen.getAllByRole("alert")).toHaveLength(1);
    expect(screen.getAllByText("Не удаётся подключиться к серверу")).toHaveLength(1);
  });

  it("права прочитались, а обзор нет — баннер у первого раздела, у остальных «—»", async () => {
    vi.mocked(api.listPods).mockRejectedValue(DOWN);
    vi.mocked(api.listWorkloads).mockRejectedValue(DOWN);
    vi.mocked(api.getQuotas).mockRejectedValue(DOWN);
    vi.mocked(api.listEvents).mockRejectedValue(DOWN);
    const user = userEvent.setup();
    renderWithClient(<ProjectPanel connectionId="c1" namespace="team-a" />);
    await waitFor(() => expect(screen.getAllByText("—")).toHaveLength(3));
    expect(screen.getAllByRole("alert")).toHaveLength(1);
    // «Повторить» перечитывает обзор целиком.
    vi.mocked(api.listPods).mockResolvedValue({ items: [podRow("web-a")], truncated: false });
    vi.mocked(api.listWorkloads).mockResolvedValue({ items: [workloadRow("api")], errors: [], truncated: false });
    vi.mocked(api.getQuotas).mockResolvedValue({ quotas: { data: [] }, limitRanges: { data: [] } });
    vi.mocked(api.listEvents).mockResolvedValue({ items: [], truncated: false });
    await user.click(screen.getByRole("button", { name: "Повторить" }));
    expect(await screen.findByText("Все workloads в норме.")).toBeInTheDocument();
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
  });

  it("разные ошибки разделов показываются каждая", async () => {
    vi.mocked(api.listPods).mockRejectedValue(DOWN);
    vi.mocked(api.getQuotas).mockRejectedValue({ code: "INTERNAL", message: "Quotas are broken", retryable: false });
    renderWithClient(<ProjectPanel connectionId="c1" namespace="team-a" />);
    expect(await screen.findByText("Не удаётся подключиться к серверу")).toBeInTheDocument();
    expect(await screen.findByText("Quotas are broken")).toBeInTheDocument();
    expect(screen.getAllByRole("alert")).toHaveLength(2);
  });
});

describe("таблица workloads: вид DeploymentConfig", () => {
  const info = (deploymentConfigs: boolean) =>
    ({ apiGroups: { routes: true, projects: true, deploymentConfigs, gatewayApi: false } }) as Partial<ConnectionInfo>;
  const openWorkloads = async (prepare?: (client: ReturnType<typeof renderWithClient>["client"]) => void) => {
    const user = userEvent.setup();
    const { client } = renderWithClient(<ProjectPanel connectionId="c1" namespace="team-a" />);
    prepare?.(client);
    await user.click(await screen.findByRole("tab", { name: "Workloads" }));
    await waitFor(() => expect(api.startWatch).toHaveBeenCalled());
    return screen.getByLabelText("Вид");
  };
  const kinds = (select: HTMLElement) => Array.from(select.querySelectorAll("option")).map((o) => o.textContent);

  it("в кластере нет apps.openshift.io — DeploymentConfig не предлагается", async () => {
    const select = await openWorkloads((client) => client.setQueryData(["connection", "c1"], info(false)));
    expect(kinds(select)).toEqual(["все", "Deployment", "StatefulSet", "DaemonSet", "Job", "CronJob"]);
  });

  it("группа apps.openshift.io есть — предлагается", async () => {
    const select = await openWorkloads((client) => client.setQueryData(["connection", "c1"], info(true)));
    expect(kinds(select)).toContain("DeploymentConfig");
  });

  it("сведений о подключении нет: вид появляется, когда пришла строка DeploymentConfig", async () => {
    const select = await openWorkloads();
    expect(kinds(select)).not.toContain("DeploymentConfig");
    act(() => emit({ type: "reset", items: [workloadRow("legacy", { kind: "DeploymentConfig" })], errors: [], truncated: false }));
    expect(kinds(screen.getByLabelText("Вид"))).toContain("DeploymentConfig");
  });
});

describe("списки обрезаны на 10 000: заметка на экранах", () => {
  const TRUNCATED = "Список неполный: показаны первые 10 000 объектов";

  it("обзор: числа, проблемные поды и workloads, Warning — у каждого раздела своя заметка", async () => {
    vi.mocked(api.listPods).mockResolvedValue({ items: [podRow("web-a")], truncated: true });
    vi.mocked(api.listWorkloads).mockResolvedValue({ items: [workloadRow("api")], errors: [], truncated: true });
    vi.mocked(api.listEvents).mockResolvedValue({ items: [], truncated: true });
    renderWithClient(<ProjectPanel connectionId="c1" namespace="team-a" />);
    await waitFor(() => expect(screen.getAllByText(TRUNCATED)).toHaveLength(3));
    // Заметка рядом с числами по фазам, а не вместо них.
    expect(screen.getByText("Running").closest("div")).toHaveTextContent("1");
  });

  it("обзор: списки прочитаны целиком — заметок нет", async () => {
    renderWithClient(<ProjectPanel connectionId="c1" namespace="team-a" />);
    expect(await screen.findByRole("button", { name: "web-b" })).toBeInTheDocument();
    await screen.findByText(/image not found/);
    expect(screen.queryByText(TRUNCATED)).not.toBeInTheDocument();
  });

  it("обзор: обрезан только список событий — заметка одна, у Warning", async () => {
    vi.mocked(api.listEvents).mockResolvedValue({ items: [], truncated: true });
    renderWithClient(<ProjectPanel connectionId="c1" namespace="team-a" />);
    await screen.findByRole("button", { name: "web-b" });
    const note = await screen.findByText(TRUNCATED);
    expect(screen.getAllByText(TRUNCATED)).toHaveLength(1);
    expect(note.previousElementSibling).toHaveTextContent("События Warning за последний час");
  });

  it("workloads (watch): заметка по признаку сброса", async () => {
    const user = userEvent.setup();
    renderWithClient(<ProjectPanel connectionId="c1" namespace="team-a" />);
    await user.click(await screen.findByRole("tab", { name: "Workloads" }));
    await waitFor(() => expect(api.startWatch).toHaveBeenCalled());
    act(() => emit({ type: "reset", items: [workloadRow("web")], errors: [], truncated: true }));
    expect(screen.getByText(TRUNCATED)).toBeInTheDocument();
    act(() => emit({ type: "reset", items: [workloadRow("web")], errors: [], truncated: false }));
    expect(screen.queryByText(TRUNCATED)).not.toBeInTheDocument();
  });

  it("события (watch): заметка по признаку сброса", async () => {
    const user = userEvent.setup();
    renderWithClient(<ProjectPanel connectionId="c1" namespace="team-a" />);
    await user.click(await screen.findByRole("tab", { name: "События" }));
    await waitFor(() => expect(api.startWatch).toHaveBeenCalledWith({ connectionId: "c1", namespace: "team-a", kind: "events" }, expect.any(Function)));
    act(() => emit({ type: "reset", items: [], errors: [], truncated: true }));
    expect(screen.getByText(TRUNCATED)).toBeInTheDocument();
  });
});

describe("квоты: один порог 90 % (спец. §8.2, quota.nearLimit)", () => {
  const quotas = (...ratios: number[]) => ({
    quotas: {
      data: [{ name: "compute", scopes: [], entries: ratios.map((ratio, i) => ({ resource: `res-${i}`, used: `${i}`, hard: "100", ratio })) }],
    },
    limitRanges: { data: [] },
  });

  it("обзор показывает ресурс от 90 % и не показывает 89 %", async () => {
    vi.mocked(api.getQuotas).mockResolvedValue(quotas(0.89, 0.9, 0.95));
    renderWithClient(<ProjectPanel connectionId="c1" namespace="team-a" />);
    expect(await screen.findByText("res-1")).toBeInTheDocument();
    expect(screen.getByText("res-2")).toBeInTheDocument();
    expect(screen.queryByText("res-0")).not.toBeInTheDocument();
    expect(screen.queryByText(/занята меньше чем на/)).not.toBeInTheDocument();
  });

  it("все ресурсы ниже порога — «меньше чем на 90 %»", async () => {
    vi.mocked(api.getQuotas).mockResolvedValue(quotas(0.85, 0.5));
    renderWithClient(<ProjectPanel connectionId="c1" namespace="team-a" />);
    expect(await screen.findByText("Каждая квота занята меньше чем на 90 %.")).toBeInTheDocument();
  });

  it("вкладка «Квоты» выделяет те же строки, что и обзор", async () => {
    vi.mocked(api.getQuotas).mockResolvedValue(quotas(0.85, 0.9));
    const user = userEvent.setup();
    renderWithClient(<ProjectPanel connectionId="c1" namespace="team-a" />);
    await user.click(await screen.findByRole("tab", { name: "Квоты" }));
    const row = async (name: string) => (await screen.findByText(name)).closest("tr");
    expect(await row("res-1")).toHaveClass("warn");
    expect(await row("res-0")).not.toHaveClass("warn");
  });
});

describe("скрытая вкладка проекта", () => {
  afterEach(() => vi.useRealTimers());

  it("права не перечитываются по таймеру, пока вкладка скрыта; вернулась — таймер идёт снова", async () => {
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval"] });
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const inTab = (active: boolean) => (
      <QueryClientProvider client={client}>
        <TabActiveContext.Provider value={active}>
          <ProjectPanel connectionId="c1" namespace="team-a" />
        </TabActiveContext.Provider>
      </QueryClientProvider>
    );
    const advance = (ms: number) =>
      act(async () => {
        vi.advanceTimersByTime(ms);
      });
    const view = render(inTab(false));
    await act(async () => undefined);
    expect(api.getNamespaceAccess).toHaveBeenCalledTimes(1);
    await advance(30 * 60_000);
    expect(api.getNamespaceAccess).toHaveBeenCalledTimes(1);

    view.rerender(inTab(true));
    await advance(5 * 60_000);
    expect(api.getNamespaceAccess).toHaveBeenCalledTimes(1);
    await advance(5_000);
    expect(api.getNamespaceAccess).toHaveBeenCalledTimes(2);
  });
});
