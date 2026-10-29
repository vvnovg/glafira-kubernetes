import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { mcpStatus } from "../test/mcp";
import { renderWithClient } from "../test/render";
import { newProfile } from "./connections/profile";

vi.mock("@tauri-apps/plugin-opener", () => ({ openUrl: vi.fn() }));
vi.mock("../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../api/client")>();
  return {
    ...orig,
    api: {
      getHealth: vi.fn().mockResolvedValue({ lastCheck: null, ignored: [], annotations: [], proposals: [] }),
      listConnections: vi.fn().mockResolvedValue([{ ...newProfile(), id: "c", name: "local" }]),
      listFolders: vi.fn().mockResolvedValue([]),
      getMcpStatus: vi.fn().mockResolvedValue(mcpStatus()),
    },
  };
});

import { openUrl } from "@tauri-apps/plugin-opener";
import { SUPPORT_URL } from "../lib/links";
import { Sidebar } from "./Sidebar";

describe("Sidebar", () => {
  it("внизу панели — «Поддержать проект», открывает страницу поддержки; ссылки «Лицензия» нет", async () => {
    const user = userEvent.setup();
    const { container } = renderWithClient(<Sidebar />);
    expect(await screen.findByRole("button", { name: "Обзор" })).toBeEnabled();
    // McpIndicator без сессий MCP ничего не рисует; на случай сессий исключаем его кнопку.
    const footer = [...container.querySelectorAll<HTMLButtonElement>(".sidebar-footer button:not(.mcp-indicator)")].map((b) => b.textContent);
    expect(footer).toEqual(["Настройки", "Поддержать проект", "Справка", "О программе"]);
    const links = screen.getAllByRole("button").map((b) => b.textContent);
    expect(links.some((l) => l?.startsWith("Лицензия"))).toBe(false);
    await user.click(screen.getByRole("button", { name: "Поддержать проект" }));
    expect(openUrl).toHaveBeenCalledWith(SUPPORT_URL);
  });
});
