import { act, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { MCP_STATUS_KEY } from "../../lib/useMcpStatus";
import { mcpStatus } from "../../test/mcp";
import { renderWithClient } from "../../test/render";

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return {
    ...orig,
    api: {
      getMcpStatus: vi.fn(),
      setMcpEnabled: vi.fn(),
      revokeMcpClient: vi.fn(),
      disconnectMcpSessions: vi.fn(),
      getMcpAudit: vi.fn(),
      getMcpClientConfigs: vi.fn(),
      revealMcpAudit: vi.fn(),
    },
  };
});

import { api } from "../../api/client";
import { McpSection } from "./McpSection";

const configs = {
  command: "/Applications/Glafira for Kubernetes.app/Contents/MacOS/glafira-kubernetes",
  args: ["mcp"],
  claudeDesktop: '{"mcpServers":{"glafira-kubernetes":{}}}',
  claudeCode: "claude mcp add glafira-kubernetes -- /x mcp",
  otherClient: "'/Applications/Glafira for Kubernetes.app/Contents/MacOS/glafira-kubernetes' mcp",
};

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(api.getMcpClientConfigs).mockResolvedValue(configs);
  vi.mocked(api.getMcpAudit).mockResolvedValue([]);
});

describe("McpSection", () => {
  it("включает сервер и показывает конфиги клиентов", async () => {
    vi.mocked(api.getMcpStatus).mockResolvedValue(mcpStatus({ state: "off", enabled: false }));
    vi.mocked(api.setMcpEnabled).mockResolvedValue(mcpStatus());
    const user = userEvent.setup();
    renderWithClient(<McpSection />);
    const toggle = await screen.findByLabelText("Разрешить MCP-сервер");
    expect(toggle).not.toBeChecked();
    expect(screen.getByRole("status")).toHaveTextContent("Сервер выключен");
    await user.click(toggle);
    expect(api.setMcpEnabled).toHaveBeenCalledWith(true);
    await waitFor(() => expect(screen.getByRole("status")).toHaveTextContent("Сервер работает"));
    expect(screen.getByText("claude mcp add glafira-kubernetes -- /x mcp")).toBeInTheDocument();
    // Путь с пробелами — в кавычках, как у Claude Code.
    expect(screen.getByText(configs.otherClient)).toBeInTheDocument();
  });

  it("политика запрещает MCP", async () => {
    vi.mocked(api.getMcpStatus).mockResolvedValue(
      mcpStatus({ state: "disabledByPolicy", enabled: true, policyAllows: false }),
    );
    renderWithClient(<McpSection />);
    const toggle = await screen.findByLabelText("Разрешить MCP-сервер");
    expect(toggle).toBeDisabled();
    expect(toggle).not.toBeChecked();
    expect(screen.getByText("MCP отключён вашей организацией")).toBeInTheDocument();
    expect(screen.queryByText("Подключение клиента")).not.toBeInTheDocument();
  });

  it("ошибка получения фрагментов конфига показывается, а не скрывается молча", async () => {
    vi.mocked(api.getMcpStatus).mockResolvedValue(mcpStatus());
    vi.mocked(api.getMcpClientConfigs).mockRejectedValue({
      code: "INTERNAL",
      key: "errors.mcp.noExe",
      params: { error: "x" },
      message: "Cannot find the Glafira executable: x",
      retryable: false,
    });
    renderWithClient(<McpSection />);
    expect(await screen.findByText("Не удалось найти исполняемый файл Glafira: x")).toBeInTheDocument();
    expect(screen.queryByText("Подключение клиента")).not.toBeInTheDocument();
  });

  it("сессии, отзыв клиента и журнал", async () => {
    const status = mcpStatus({
      sessions: [{ id: "s1", clientName: "claude-ai", clientVersion: "1.2", startedMs: 0, calls: 7, active: false, logged: 0, approved: true }],
      approvedClients: [{ name: "claude-ai", version: "1.2", firstSeenMs: 0, lastSeenMs: 0 }],
    });
    vi.mocked(api.getMcpStatus).mockResolvedValue(status);
    vi.mocked(api.revokeMcpClient).mockResolvedValue(status);
    vi.mocked(api.disconnectMcpSessions).mockResolvedValue(status);
    vi.mocked(api.getMcpAudit).mockResolvedValue([
      { ts: 0, event: "call", client: "claude-ai 1.2", tool: "describe_workload", connection: { id: "c1", name: "dev" }, outcome: "error", code: "not_allowed", ms: 3 },
      { ts: 0, event: "approved", session: "s1", client: "claude-ai" },
      { ts: 0, event: "call", client: "claude-ai 1.2", tool: "get_pod_logs", outcome: "ok", ms: 9, scanned: 1500, returned: 200, masked: 3 },
    ]);
    const user = userEvent.setup();
    renderWithClient(<McpSection />);
    const sessions = (await screen.findByText("Активные сессии")).nextElementSibling as HTMLElement;
    expect(within(sessions).getByText("7")).toBeInTheDocument();
    await user.click(within(sessions).getByRole("button", { name: "Отключить" }));
    expect(api.disconnectMcpSessions).toHaveBeenCalledWith("s1");
    expect(await screen.findByText("describe_workload")).toBeInTheDocument();
    expect(screen.getByText("not_allowed")).toBeInTheDocument();
    expect(screen.getByText("Клиент подтверждён")).toBeInTheDocument();
    expect(screen.getByText(/^get_pod_logs — 200 из 1[\s\u00a0]?500 строк, замаскировано: 3$/)).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Отозвать" }));
    expect(api.revokeMcpClient).toHaveBeenCalledWith("claude-ai");
    // отзыв и отключение обновляют журнал: первая загрузка + отключение + отзыв
    await waitFor(() => expect(api.getMcpAudit).toHaveBeenCalledTimes(3));
  });

  it("журнал обновляется сам, когда статус сообщает о новых вызовах", async () => {
    const session = { id: "s1", clientName: "claude-ai", clientVersion: "1.2", startedMs: 0, calls: 0, active: false, logged: 0, approved: true };
    vi.mocked(api.getMcpStatus).mockResolvedValue(mcpStatus({ sessions: [session] }));
    const { client } = renderWithClient(<McpSection />);
    expect(await screen.findByText("Журнал пуст.")).toBeInTheDocument();
    expect(api.getMcpAudit).toHaveBeenCalledTimes(1);
    vi.mocked(api.getMcpAudit).mockResolvedValue([
      { ts: 0, event: "call", client: "claude-ai 1.2", tool: "get_pod_logs", outcome: "ok", ms: 9 },
    ]);
    // Событие статуса из Rust: вызов записан в журнал, а другой ещё идёт (`active`, `calls` те же).
    act(() => client.setQueryData(MCP_STATUS_KEY, mcpStatus({ sessions: [{ ...session, calls: 2, active: true }] })));
    act(() => client.setQueryData(MCP_STATUS_KEY, mcpStatus({ sessions: [{ ...session, calls: 2, active: true, logged: 1 }] })));
    expect(await screen.findByText("get_pod_logs")).toBeInTheDocument();
    // Отказ до вызова (rate_limited): меняется только `logged`.
    act(() => client.setQueryData(MCP_STATUS_KEY, mcpStatus({ sessions: [{ ...session, calls: 2, active: true, logged: 2 }] })));
    await waitFor(() => expect(api.getMcpAudit).toHaveBeenCalledTimes(3));
  });
});
