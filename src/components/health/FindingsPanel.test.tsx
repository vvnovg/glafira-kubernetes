import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { Annotation, HealthView, Proposal } from "../../api/types";
import { useWorkspace } from "../../state/workspace";
import { finding } from "../../test/health";
import { renderWithClient } from "../../test/render";
import { newProfile } from "../connections/profile";

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return {
    ...orig,
    api: {
      listConnections: vi.fn().mockResolvedValue([]),
      getHealth: vi.fn(),
      runHealthCheck: vi.fn(),
      setFindingIgnored: vi.fn(),
      setAnnotationStatus: vi.fn(),
      deleteAnnotation: vi.fn(),
      deleteAllAnnotations: vi.fn(),
      setProposalStatus: vi.fn(),
      deleteProposal: vi.fn(),
      deleteAllProposals: vi.fn(),
    },
  };
});

import { api } from "../../api/client";
import { FindingsPanel } from "./FindingsPanel";

const latest = (ns: string, name: string) =>
  finding("container.latestTag", "warning", { kind: "workload", namespace: ns, name, workloadKind: "Deployment" }, { images: ["app:latest"] });
const orphan = finding("service.noEndpoints", "warning", { kind: "service", namespace: "team-b", name: "orphan" }, { referencedBy: [] });
const crash = finding("pod.crashLoop", "critical", { kind: "pod", namespace: "team-a", name: "crash" }, { containers: ["crash"], reason: "Error", exitCode: 1, restarts: 4 });

const note = (id: string, status: Annotation["status"] = "open"): Annotation => ({
  id,
  target: { kind: "workload", namespace: "team-a", name: "web", workloadKind: "Deployment" },
  severity: "warning",
  title: `Note ${id}`,
  body: "line 1\nline 2",
  relatedRuleId: "container.latestTag",
  client: "claude-ai",
  createdAtMs: 1,
  updatedAtMs: 1,
  status,
});

function view(p: Partial<HealthView> = {}): HealthView {
  return {
    lastCheck: {
      runId: "r1",
      startedAtMs: Date.now() - 5 * 60_000,
      durationMs: 1200,
      trigger: { kind: "mcp", client: "claude-ai" },
      environment: "prod",
      incomplete: true,
      scope: { projects: ["team-a", "team-b"], projectsTotal: 2, pods: 7, nodes: 3 },
      findings: [crash, latest("team-a", "web"), latest("team-b", "legacy"), orphan],
    },
    ignored: [],
    annotations: [],
    proposals: [],
    ...p,
  };
}

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(api.listConnections).mockResolvedValue([{ ...newProfile(), id: "c1", name: "dev-1", environment: "prod" }]);
  useWorkspace.setState({ tabs: [], activeId: null });
});

describe("FindingsPanel", () => {
  it("запускает проверку и группирует находки по правилу", async () => {
    vi.mocked(api.getHealth).mockResolvedValue({ lastCheck: null, ignored: [], annotations: [], proposals: [] });
    vi.mocked(api.runHealthCheck).mockResolvedValue(view());
    const user = userEvent.setup();
    renderWithClient(<FindingsPanel connectionId="c1" />);
    expect(await screen.findByText("Кластер ещё не проверялся.")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Проверить кластер" }));
    expect(api.runHealthCheck).toHaveBeenCalledWith("c1");
    expect(await screen.findByText("Тег latest")).toBeInTheDocument();
    expect(screen.getByText("объектов: 2")).toBeInTheDocument();
    expect(screen.getByText("Deployment team-a/web: образы с тегом latest")).toBeInTheDocument();
    expect(screen.getByText("Под team-a/crash: контейнеры перезапускаются по кругу")).toBeInTheDocument();
    expect(screen.getByText(/Последняя проверка: 5 минут назад · claude-ai/)).toBeInTheDocument();
    expect(screen.getByText(/проверено частично/)).toBeInTheDocument();
  });

  it("ссылка на объект открывает его вкладку", async () => {
    vi.mocked(api.getHealth).mockResolvedValue(view());
    const user = userEvent.setup();
    renderWithClient(<FindingsPanel connectionId="c1" />);
    await user.click(await screen.findByRole("button", { name: "team-a/web" }));
    expect(useWorkspace.getState().tabs[0]).toMatchObject({ kind: "workload", namespace: "team-a", workloadKind: "Deployment", name: "web" });
    await user.click(screen.getByRole("button", { name: "team-b/orphan" }));
    expect(useWorkspace.getState().tabs[1]).toMatchObject({ kind: "project", namespace: "team-b" });
    await user.click(screen.getByRole("button", { name: "team-a/crash" }));
    expect(useWorkspace.getState().tabs[2]).toMatchObject({ kind: "pod", namespace: "team-a", name: "crash" });
  });

  it("скрывает находку для объекта и показывает скрытые по запросу", async () => {
    vi.mocked(api.getHealth).mockResolvedValue(view());
    vi.mocked(api.setFindingIgnored).mockResolvedValue(view({ ignored: [latest("team-b", "legacy").key] }));
    const user = userEvent.setup();
    renderWithClient(<FindingsPanel connectionId="c1" />);
    const title = "Deployment team-b/legacy: образы с тегом latest";
    const item = (await screen.findByText(title)).closest("li")!;
    await user.click(within(item).getByRole("button", { name: "Скрыть для этого объекта" }));
    expect(api.setFindingIgnored).toHaveBeenCalledWith("c1", latest("team-b", "legacy").key, true);
    await waitFor(() => expect(screen.queryByText(title)).not.toBeInTheDocument());
    // Подсказка «?» стоит рядом с подписью, а не внутри неё: имя флажка — только подпись.
    await user.click(screen.getByRole("checkbox", { name: "Показать скрытые (1)" }));
    const hidden = (await screen.findByText(title)).closest("li")!;
    expect(within(hidden).getByRole("button", { name: "Вернуть" })).toBeInTheDocument();
  });

  it("когда все находки скрыты, говорит об этом, а не «находок нет»", async () => {
    const all = view().lastCheck!.findings.map((f) => f.key);
    vi.mocked(api.getHealth).mockResolvedValue(view({ ignored: all }));
    renderWithClient(<FindingsPanel connectionId="c1" />);
    expect(await screen.findByText("Все находки скрыты. Их видно с флажком «Показать скрытые».")).toBeInTheDocument();
    expect(screen.queryByText("Находок нет: правила не нашли проблем.")).not.toBeInTheDocument();
  });

  it("фильтрует по источнику, виду объекта и проекту", async () => {
    vi.mocked(api.getHealth).mockResolvedValue(view({ annotations: [note("a")] }));
    const user = userEvent.setup();
    renderWithClient(<FindingsPanel connectionId="c1" />);
    await screen.findByText("Note a");
    await user.selectOptions(screen.getByRole("combobox", { name: "Источник" }), "llm");
    expect(screen.queryByText("Deployment team-a/web: образы с тегом latest")).not.toBeInTheDocument();
    expect(screen.getByText("Note a")).toBeInTheDocument();
    await user.selectOptions(screen.getByRole("combobox", { name: "Источник" }), "all");
    await user.selectOptions(screen.getByRole("combobox", { name: "Объект" }), "service");
    expect(screen.getByText("Service team-b/orphan: нет готовых endpoints")).toBeInTheDocument();
    expect(screen.queryByText("Deployment team-a/web: образы с тегом latest")).not.toBeInTheDocument();
    await user.selectOptions(screen.getByRole("combobox", { name: "Объект" }), "all");
    await user.type(screen.getByRole("textbox", { name: "Поиск по имени или проекту" }), "team-b");
    expect(screen.getByText("Deployment team-b/legacy: образы с тегом latest")).toBeInTheDocument();
    expect(screen.queryByText("Deployment team-a/web: образы с тегом latest")).not.toBeInTheDocument();
    // Пусто и у предложений, и у замечаний.
    expect(screen.getAllByText("Под фильтры ничего не подходит.")).toHaveLength(2);
  });

  it("закрывает и удаляет замечания LLM", async () => {
    vi.mocked(api.getHealth).mockResolvedValue(view({ annotations: [note("a"), note("b", "resolved")] }));
    vi.mocked(api.setAnnotationStatus).mockResolvedValue(view({ annotations: [note("a", "dismissed"), note("b", "resolved")] }));
    vi.mocked(api.deleteAllAnnotations).mockResolvedValue(view());
    const user = userEvent.setup();
    renderWithClient(<FindingsPanel connectionId="c1" />);
    const a = (await screen.findByText("Note a")).closest("li")!;
    expect(within(a).getByText("Замечание LLM · claude-ai · Тег latest")).toBeInTheDocument();
    expect(within(a).getByText(/line 1/)).toHaveClass("note-body");
    expect(screen.getByText("Закрытые (1)")).toBeInTheDocument();
    await user.click(within(a).getByRole("button", { name: "Не актуально" }));
    expect(api.setAnnotationStatus).toHaveBeenCalledWith("c1", "a", "dismissed");
    await waitFor(() => expect(screen.getByText("Закрытые (2)")).toBeInTheDocument());
    await user.click(screen.getByRole("button", { name: "Удалить все замечания LLM" }));
    expect(screen.getByRole("alert")).toHaveTextContent("Удалить все замечания LLM у этого подключения?");
    await user.click(within(screen.getByRole("alert")).getByRole("button", { name: "Удалить все замечания LLM" }));
    expect(api.deleteAllAnnotations).toHaveBeenCalledWith("c1");
    await waitFor(() => expect(screen.getByText("Замечаний LLM нет.")).toBeInTheDocument());
  });

  it("предложения LLM: отклонение с причиной и «Открыть в форме» — вкладка workload'а с формой", async () => {
    const proposal: Proposal = {
      id: "p1",
      severity: "warning",
      title: "Two replicas",
      rationale: "one replica is a risk",
      target: { kind: "workload", namespace: "team-a", name: "web", workloadKind: "Deployment" },
      change: { kind: "scale", namespace: "team-a", workload: { kind: "Deployment", name: "web" }, replicas: 2 },
      client: "claude-ai",
      createdAtMs: 1,
      updatedAtMs: 1,
      status: "open",
    };
    vi.mocked(api.getHealth).mockResolvedValue(view({ proposals: [proposal] }));
    vi.mocked(api.setProposalStatus).mockResolvedValue(view({ proposals: [{ ...proposal, status: "rejected", rejectReason: "later" }] }));
    useWorkspace.setState({ tabs: [], activeId: null });
    const user = userEvent.setup();
    renderWithClient(<FindingsPanel connectionId="c1" />);
    const item = (await screen.findByText("Two replicas")).closest("li")!;
    expect(within(item).getByText("Реплик: 2")).toBeInTheDocument();
    await user.click(within(item).getByRole("button", { name: "Открыть в форме" }));
    expect(useWorkspace.getState().tabs[0]).toMatchObject({ kind: "workload", name: "web", proposal: { proposal } });
    await user.click(within(item).getByRole("button", { name: "Отклонить" }));
    await user.type(within(item).getByLabelText("Причина (необязательно)"), "later");
    await user.click(within(item).getByRole("button", { name: "Отклонить предложение" }));
    expect(api.setProposalStatus).toHaveBeenCalledWith("c1", "p1", "rejected", "later");
    await waitFor(() => expect(screen.getByText(/отклонено: later/)).toBeInTheDocument());
  });
});
