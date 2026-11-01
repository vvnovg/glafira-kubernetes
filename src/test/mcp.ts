import type { McpStatus } from "../api/types";

/** Статус MCP для тестов: сервер работает, сессий и запросов нет. */
export function mcpStatus(patch: Partial<McpStatus> = {}): McpStatus {
  return {
    state: "running",
    enabled: true,
    policyAllows: true,
    error: null,
    sessions: [],
    pending: [],
    approvedClients: [],
    responseLimitKb: 100,
    allowedConnections: 2,
    ...patch,
  };
}
