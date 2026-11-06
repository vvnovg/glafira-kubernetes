import { act, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { checkedView, finding } from "../../../test/health";
import { renderWithClient } from "../../../test/render";

vi.mock("../../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../../api/client")>();
  return { ...orig, api: { getHealth: vi.fn().mockResolvedValue({ lastCheck: null, ignored: [], annotations: [], proposals: [] }), listProjects: vi.fn(), connect: vi.fn() } };
});

import { api } from "../../../api/client";
import type { ConnectionInfo, ProjectRow } from "../../../api/types";
import { useWorkspace } from "../../../state/workspace";
import { newProfile } from "../profile";
import { ConnectionNode } from "./ConnectionNode";

const conn = { ...newProfile(), id: "c1", name: "dev" };
const project = (name: string, favorite = false, fromProfile = false): ProjectRow => ({ name, displayName: null, phase: "Active", favorite, fromProfile });

beforeEach(() => {
  vi.clearAllMocks();
  useWorkspace.setState({ tabs: [], activeId: null });
});

describe("ConnectionNode", () => {
  it("проекты по щелчку: избранные сверху, фильтр, открытие проекта", async () => {
    vi.mocked(api.listProjects).mockResolvedValue({
      items: [project("alpha"), project("team-b", true), ...["c", "d", "e", "f", "g", "h"].map((x) => project(`p-${x}`))],
      truncated: false,
    });
    const user = userEvent.setup();
    renderWithClient(
      <ul>
        <ConnectionNode connection={conn} />
      </ul>,
    );
    expect(api.listProjects).not.toHaveBeenCalled();
    await user.click(screen.getByRole("button", { name: /Проекты/ }));
    // Избранный проект — со значком `star` (спец. U1), сверху списка.
    const teamB = await screen.findByRole("button", { name: "team-b" });
    expect(teamB.querySelector('svg[data-icon="star"]')).not.toBeNull();
    const names = screen.getAllByRole("button").map((b) => b.textContent);
    expect(names.indexOf("team-b")).toBeLessThan(names.indexOf("alpha"));
    await user.type(screen.getByLabelText("Фильтр проектов"), "alp");
    expect(screen.queryByRole("button", { name: "team-b" })).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "alpha" }));
    expect(useWorkspace.getState().tabs[0]).toMatchObject({ kind: "project", connectionId: "c1", namespace: "alpha" });
    expect(api.connect).not.toHaveBeenCalled();
  });

  it("без прав на список — проекты из профиля с пояснением; ноды — при правах из сведений подключения", async () => {
    vi.mocked(api.listProjects).mockResolvedValue({
      items: [project("team-a", true, true)],
      truncated: false,
      error: { code: "FORBIDDEN", key: "errors.k8s.forbiddenCluster", params: { verb: "list", resource: "namespaces" }, message: "x", retryable: false },
    });
    const user = userEvent.setup();
    const { client } = renderWithClient(
      <ul>
        <ConnectionNode connection={conn} />
      </ul>,
    );
    expect(screen.queryByRole("button", { name: "Ноды" })).not.toBeInTheDocument();
    client.setQueryData<Partial<ConnectionInfo>>(["connection", "c1"], {
      capabilities: { listProjects: false, listNodes: true, listAllPods: false, readClusterOperators: false, readClusterVersion: false },
    });
    await user.click(await screen.findByRole("button", { name: "Ноды" }));
    expect(useWorkspace.getState().tabs[0]).toMatchObject({ kind: "nodes", connectionId: "c1" });
    await user.click(screen.getByRole("button", { name: /Проекты/ }));
    expect(await screen.findByText("Кластер не даёт список проектов — показаны проекты из профиля")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "team-a" }).querySelector('svg[data-icon="star"]')).not.toBeNull();
  });

  const FORBIDDEN = { code: "FORBIDDEN" as const, key: "errors.k8s.forbiddenCluster", params: { verb: "list", resource: "namespaces" }, message: "x", retryable: false };

  const renderNode = () => {
    const user = userEvent.setup();
    renderWithClient(
      <ul>
        <ConnectionNode connection={conn} />
      </ul>,
    );
    return user;
  };

  it("ошибка чтения проектов — сообщение и «Повторить»", async () => {
    vi.mocked(api.listProjects)
      .mockRejectedValueOnce({ code: "CONNECTION_FAILED", key: "errors.k8s.connect", message: "x", retryable: true })
      .mockResolvedValue({ items: [project("alpha")], truncated: false });
    const user = renderNode();
    await user.click(screen.getByRole("button", { name: /Проекты/ }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Не удаётся подключиться к серверу");
    await user.click(screen.getByRole("button", { name: "Повторить" }));
    expect(await screen.findByRole("button", { name: "alpha" })).toBeInTheDocument();
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Повторить" })).not.toBeInTheDocument();
    expect(api.listProjects).toHaveBeenCalledTimes(2);
  });

  it("истёк токен — сообщение и «Войти снова»: подключается заново и открывает вкладку подключения", async () => {
    vi.mocked(api.listProjects).mockRejectedValue({ code: "SESSION_EXPIRED", key: "errors.k8s.unauthorized", message: "x", retryable: false });
    vi.mocked(api.connect).mockReturnValue(new Promise(() => {}));
    const user = renderNode();
    await user.click(screen.getByRole("button", { name: /Проекты/ }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Кластер отклонил учётные данные");
    expect(screen.queryByRole("button", { name: "Повторить" })).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Войти снова" }));
    expect(useWorkspace.getState().tabs[0]).toMatchObject({ kind: "cluster", connectionId: "c1" });
    expect(api.connect).toHaveBeenCalledWith("c1", true);
    // Пока идёт подключение, второе не запускается.
    expect(screen.getByRole("button", { name: "Войти снова" })).toBeDisabled();
  });

  it("нет прав на список и в профиле пусто — одно сообщение, без «Проектов нет»", async () => {
    vi.mocked(api.listProjects).mockResolvedValue({ items: [], truncated: false, error: FORBIDDEN });
    const user = renderNode();
    await user.click(screen.getByRole("button", { name: /Проекты/ }));
    expect(await screen.findByText("Кластер не даёт список проектов, а в профиле их нет")).toBeInTheDocument();
    expect(screen.queryByText("Проектов нет")).not.toBeInTheDocument();
    expect(screen.queryByText(/показаны проекты из профиля/)).not.toBeInTheDocument();
  });

  it("список проектов обрезан — заметка про первые 10 000, у полного списка её нет", async () => {
    vi.mocked(api.listProjects).mockResolvedValue({ items: [project("alpha")], truncated: true });
    const user = renderNode();
    await user.click(screen.getByRole("button", { name: /Проекты/ }));
    expect(await screen.findByText("Список неполный: показаны первые 10 000 объектов")).toHaveClass("tree-notice");
    expect(screen.getByRole("button", { name: "alpha" })).toBeInTheDocument();
  });

  it("список проектов пуст без ошибки — «Проектов нет»", async () => {
    vi.mocked(api.listProjects).mockResolvedValue({ items: [], truncated: false });
    const user = renderNode();
    await user.click(screen.getByRole("button", { name: /Проекты/ }));
    expect(await screen.findByText("Проектов нет")).toBeInTheDocument();
    expect(screen.queryByText(/Кластер не даёт список проектов/)).not.toBeInTheDocument();
  });

  it("фильтр без совпадений — «Ничего не найдено», очистка возвращает проекты", async () => {
    vi.mocked(api.listProjects).mockResolvedValue({ items: ["a", "b", "c", "d", "e", "f", "g", "h"].map((x) => project(`p-${x}`)), truncated: false });
    const user = renderNode();
    await user.click(screen.getByRole("button", { name: /Проекты/ }));
    const filter = await screen.findByLabelText("Фильтр проектов");
    expect(screen.queryByText("Ничего не найдено")).not.toBeInTheDocument();
    await user.type(filter, "zzz");
    expect(screen.getByText("Ничего не найдено")).toBeInTheDocument();
    await user.clear(filter);
    expect(screen.queryByText("Ничего не найдено")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "p-a" })).toBeInTheDocument();
  });

  it("значок LLM — только у подключения с доступом LLM", () => {
    renderWithClient(
      <ul>
        <ConnectionNode connection={conn} />
        <ConnectionNode connection={{ ...conn, id: "c2", name: "prod", llmAccess: "metadata" }} />
      </ul>,
    );
    const badges = screen.getAllByTitle("Открыто LLM-ассистентам по MCP");
    expect(badges).toHaveLength(1);
    expect(badges[0]).toHaveTextContent("LLM");
    expect(badges[0].closest(".tree-conn")).toHaveTextContent("prod");
  });

  it("значок LLM уровня логов говорит о логах", () => {
    renderWithClient(
      <ul>
        <ConnectionNode connection={{ ...conn, id: "c3", name: "stage", llmAccess: "logs" }} />
      </ul>,
    );
    expect(screen.getByTitle("Открыто LLM-ассистентам по MCP, с логами")).toHaveTextContent("LLM");
  });

  it("«Замечания» открывает вкладку находок подключения", async () => {
    const user = renderNode();
    await user.click(screen.getByRole("button", { name: "Замечания" }));
    expect(useWorkspace.getState().tabs[0]).toMatchObject({ kind: "findings", connectionId: "c1" });
  });

  it("значки замечаний: у пункта «Замечания» — всё подключение, у проекта — его находки", async () => {
    vi.mocked(api.getHealth).mockResolvedValue(
      checkedView([finding("pod.imagePull", "critical", { kind: "pod", namespace: "team-b", name: "pull" }, { image: "x" })]),
    );
    vi.mocked(api.listProjects).mockResolvedValue({ items: [project("alpha"), project("team-b")], truncated: false });
    const user = renderNode();
    const findings = screen.getByRole("button", { name: "Замечания" }).closest("li")!;
    expect(await within(findings).findByRole("img", { name: "Критично: 1" })).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: /Проекты/ }));
    const teamB = (await screen.findByRole("button", { name: /^team-b/ })).closest("li")!;
    expect(within(teamB).getByRole("img", { name: "Критично: 1" })).toBeInTheDocument();
    const alpha = screen.getByRole("button", { name: "alpha" }).closest("li")!;
    expect(within(alpha).queryByRole("img")).not.toBeInTheDocument();
  });
});

describe("ConnectionNode: подсветка узла активной вкладки (спец. U1 §6)", () => {
  const current = () => screen.queryAllByRole("button", { current: "page" }).map((b) => b.textContent);

  it("«Обзор», «Замечания» и проект подсвечены для своей вкладки; вкладки workload'а и пода подсвечивают проект", async () => {
    // Без находок: значок замечаний у проекта входит в имя его кнопки.
    vi.mocked(api.getHealth).mockResolvedValue({ lastCheck: null, ignored: [], annotations: [], proposals: [] });
    vi.mocked(api.listProjects).mockResolvedValue({ items: [project("alpha"), project("team-b")], truncated: false });
    const user = userEvent.setup();
    renderWithClient(
      <ul>
        <ConnectionNode connection={conn} />
      </ul>,
    );
    expect(current()).toEqual([]);
    act(() => useWorkspace.getState().open({ kind: "cluster", connectionId: "c1" }));
    expect(current()).toEqual(["Обзор"]);
    act(() => useWorkspace.getState().open({ kind: "findings", connectionId: "c1" }));
    expect(current()).toEqual(["Замечания"]);

    // Пока проекты свёрнуты, вкладку проекта отмечает строка «Проекты».
    act(() => useWorkspace.getState().open({ kind: "project", connectionId: "c1", namespace: "team-b" }));
    expect(current()).toEqual([]);
    expect(screen.getByRole("button", { name: "Развернуть или свернуть Проекты" }).closest(".tree-row")).toHaveClass("current");
    await user.click(screen.getByRole("button", { name: "Развернуть или свернуть Проекты" }));
    expect(await screen.findByRole("button", { name: "team-b" })).toHaveAttribute("aria-current", "page");
    expect(screen.getByRole("button", { name: "Развернуть или свернуть Проекты" }).closest(".tree-row")).not.toHaveClass("current");

    act(() => useWorkspace.getState().open({ kind: "workload", connectionId: "c1", namespace: "alpha", workloadKind: "Deployment", name: "web" }));
    expect(current()).toEqual(["alpha"]);
    act(() => useWorkspace.getState().open({ kind: "pod", connectionId: "c1", namespace: "team-b", name: "web-1" }));
    expect(current()).toEqual(["team-b"]);

    // Вкладка другого подключения узел не подсвечивает.
    act(() => useWorkspace.getState().open({ kind: "cluster", connectionId: "c2" }));
    expect(current()).toEqual([]);
  });
});
