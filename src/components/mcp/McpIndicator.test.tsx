import { act, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { McpStatus } from "../../api/types";
import { MCP_STATUS_EVENT, MCP_STATUS_KEY, useMcpEvents } from "../../lib/useMcpStatus";
import { useWorkspace } from "../../state/workspace";
import { mcpStatus } from "../../test/mcp";
import { renderWithClient } from "../../test/render";

let emit: ((e: { payload: McpStatus }) => void) | undefined;
vi.mock("@tauri-apps/api/event", () => ({
  listen: vi.fn(async (name: string, cb: (e: { payload: McpStatus }) => void) => {
    if (name === "mcp://state") emit = cb;
    return () => {};
  }),
}));

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return { ...orig, api: { getMcpStatus: vi.fn() } };
});

import { api } from "../../api/client";
import { McpIndicator } from "./McpIndicator";

function WithEvents() {
  useMcpEvents();
  return <McpIndicator />;
}

const session = { id: "s1", clientName: "claude-ai", clientVersion: "1", startedMs: 0, calls: 0, active: false, approved: true };

beforeEach(() => {
  vi.clearAllMocks();
  useWorkspace.setState({ tabs: [], activeId: null });
});

describe("McpIndicator", () => {
  it("появляется с сессией, подсвечивается во время вызова и открывает настройки", async () => {
    expect(MCP_STATUS_EVENT).toBe("mcp://state");
    vi.mocked(api.getMcpStatus).mockResolvedValue(mcpStatus());
    const user = userEvent.setup();
    const { client } = renderWithClient(<WithEvents />);
    // Сначала первый ответ get_mcp_status, затем события: иначе ответ затёр бы событие.
    await waitFor(() => expect(client.getQueryData(MCP_STATUS_KEY)).toBeDefined());
    await waitFor(() => expect(emit).toBeDefined());
    expect(screen.queryByRole("button")).not.toBeInTheDocument();
    act(() => emit!({ payload: mcpStatus({ sessions: [session] }) }));
    const button = await screen.findByRole("button", { name: "LLM: claude-ai" });
    expect(button).not.toHaveClass("active");
    act(() => emit!({ payload: mcpStatus({ sessions: [{ ...session, active: true }, { ...session, id: "s2", clientName: "cursor" }] }) }));
    expect(await screen.findByRole("button", { name: "LLM-клиентов: 2" })).toHaveClass("active");
    await user.click(screen.getByRole("button"));
    expect(useWorkspace.getState().tabs.map((t) => t.kind)).toEqual(["settings"]);
  });
});
