import { screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { HealthView } from "../../api/types";
import { useWorkspace } from "../../state/workspace";
import { checkedView, finding } from "../../test/health";
import { renderWithClient } from "../../test/render";

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return {
    ...orig,
    api: {
      listConnections: vi.fn().mockResolvedValue([]),
      getHealth: vi.fn(),
      setFindingIgnored: vi.fn(),
      setAnnotationStatus: vi.fn(),
      deleteAnnotation: vi.fn(),
    },
  };
});

import { api } from "../../api/client";
import { matchCluster, matchPod, matchProject, matchWorkload } from "./findings";
import { HealthBadge, HealthBlock } from "./HealthBlock";

const web = { kind: "workload" as const, namespace: "team-a", name: "web", workloadKind: "Deployment" as const };
const crash = finding("pod.crashLoop", "critical", web, { pods: ["web-1"], count: 1, total: 2, containers: ["web"], reason: "Error", exitCode: 1, restarts: 5 });
const latest = finding("container.latestTag", "warning", { ...web, namespace: "team-b" }, { images: ["web:latest"] });
const node = finding("node.unschedulable", "info", { kind: "node", name: "w1" });

const view: HealthView = {
  ...checkedView([crash, latest, node], [], [latest.key]),
  annotations: [
    {
      id: "n1",
      target: web,
      severity: "warning",
      title: "Web restarts after deploys",
      body: "Started after the last release.",
      client: null,
      createdAtMs: 1,
      updatedAtMs: 1,
      status: "open",
    },
  ],
};

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(api.getHealth).mockResolvedValue(view);
  useWorkspace.setState({ tabs: [], activeId: null });
});

describe("HealthBlock", () => {
  it("показывает находки и замечания объекта и ведёт на вкладку «Замечания»", async () => {
    const user = userEvent.setup();
    renderWithClient(<HealthBlock connectionId="c1" match={matchWorkload("team-a", "Deployment", "web")} />);
    const block = await screen.findByRole("region", { name: "Замечания" });
    expect(within(block).getByText("Deployment team-a/web: контейнеры перезапускаются по кругу")).toBeInTheDocument();
    expect(within(block).getByText("Web restarts after deploys")).toBeInTheDocument();
    expect(within(block).getByText("Замечание LLM · Неизвестный MCP-клиент")).toBeInTheDocument();
    await user.click(within(block).getByRole("button", { name: "Все замечания" }));
    expect(useWorkspace.getState().activeId).toBe("findings:c1");
  });

  it("под видит находку своего workload'а, чужой под — нет", async () => {
    renderWithClient(
      <>
        <HealthBlock connectionId="c1" match={matchPod("team-a", "web-1")} />
        <HealthBlock connectionId="c1" match={matchPod("team-a", "web-2")} />
      </>,
    );
    const block = await screen.findByRole("region", { name: "Замечания" });
    expect(within(block).getByText(/контейнеры перезапускаются по кругу/)).toBeInTheDocument();
    expect(screen.getAllByRole("region", { name: "Замечания" })).toHaveLength(1);
  });

  it("не показывается, если по объекту ничего нет (скрытые не считаются)", async () => {
    // Два блока в одном дереве: первый дождётся данных, значит, ко второму они тоже дошли — и он остался пустым.
    renderWithClient(
      <>
        <HealthBlock connectionId="c1" match={matchProject("team-a")} />
        <HealthBlock connectionId="c1" match={matchProject("team-b")} />
      </>,
    );
    await screen.findByRole("region", { name: "Замечания" });
    expect(screen.getAllByRole("region", { name: "Замечания" })).toHaveLength(1);
  });

  it("обзор кластера показывает ноды и объект находки", async () => {
    renderWithClient(<HealthBlock connectionId="c1" match={matchCluster} showTarget />);
    const block = await screen.findByRole("region", { name: "Замечания" });
    expect(within(block).getByText("Нода w1 закрыта для новых подов")).toBeInTheDocument();
    expect(within(block).getByRole("button", { name: "w1" })).toBeInTheDocument();
  });

  it("«Скрыть» отправляет ключ находки", async () => {
    vi.mocked(api.setFindingIgnored).mockResolvedValue({ ...view, ignored: [latest.key, crash.key] });
    const user = userEvent.setup();
    renderWithClient(<HealthBlock connectionId="c1" match={matchWorkload("team-a", "Deployment", "web")} />);
    const block = await screen.findByRole("region", { name: "Замечания" });
    await user.click(within(block).getAllByRole("button", { name: "Скрыть для этого объекта" })[0]);
    expect(api.setFindingIgnored).toHaveBeenCalledWith("c1", crash.key, true);
  });
});

describe("HealthBadge", () => {
  it("считает находки и замечания уровня «критично» и «предупреждение» объекта; подсказка — их заголовки", async () => {
    renderWithClient(<HealthBadge connectionId="c1" match={matchWorkload("team-a", "Deployment", "web")} />);
    const badge = await screen.findByTitle(/Требуют внимания: 2/);
    expect(badge).toHaveAttribute(
      "title",
      ["Требуют внимания: 2", "Deployment team-a/web: контейнеры перезапускаются по кругу", "Web restarts after deploys"].join("\n"),
    );
    expect(within(badge).getByRole("img", { name: "Критично: 2" })).toBeInTheDocument();
  });

  it("без match — всё подключение; информационные находки и скрытые не считаются", async () => {
    renderWithClient(<HealthBadge connectionId="c1" />);
    expect(await screen.findByTitle(/Требуют внимания: 2/)).toBeInTheDocument();
  });
});
