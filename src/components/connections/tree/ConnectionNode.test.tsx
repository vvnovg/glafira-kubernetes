import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../../test/render";

vi.mock("../../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../../api/client")>();
  return { ...orig, api: { listProjects: vi.fn(), connect: vi.fn() } };
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
    expect(await screen.findByRole("button", { name: "★ team-b" })).toBeInTheDocument();
    const names = screen.getAllByRole("button").map((b) => b.textContent);
    expect(names.indexOf("★ team-b")).toBeLessThan(names.indexOf("alpha"));
    await user.type(screen.getByLabelText("Фильтр проектов"), "alp");
    expect(screen.queryByRole("button", { name: "★ team-b" })).not.toBeInTheDocument();
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
    expect(screen.getByRole("button", { name: "★ team-a" })).toBeInTheDocument();
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

  it("нет прав на список и в профиле пусто — одно сообщение, без «Проектов нет»", async () => {
    vi.mocked(api.listProjects).mockResolvedValue({ items: [], truncated: false, error: FORBIDDEN });
    const user = renderNode();
    await user.click(screen.getByRole("button", { name: /Проекты/ }));
    expect(await screen.findByText("Кластер не даёт список проектов, а в профиле их нет")).toBeInTheDocument();
    expect(screen.queryByText("Проектов нет")).not.toBeInTheDocument();
    expect(screen.queryByText(/показаны проекты из профиля/)).not.toBeInTheDocument();
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
});
