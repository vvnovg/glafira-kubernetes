import { act, fireEvent, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { LanguageSettings } from "../../api/types";
import { i18n } from "../../i18n";
import { mcpStatus } from "../../test/mcp";
import { renderWithClient } from "../../test/render";
import { useHelpContext } from "../../state/helpContext";

const base: LanguageSettings = {
  system: "ru-RU", systemMatch: "ru", chosen: null, policy: null, effective: "ru", reason: "system",
  languages: ["en", "ru"],
};

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return {
    ...orig,
    api: {
      getLanguageSettings: vi.fn(),
      setLanguage: vi.fn(),
      getMcpStatus: vi.fn().mockResolvedValue(mcpStatus({ state: "off", enabled: false })),
      getMcpClientConfigs: vi.fn().mockResolvedValue({ command: "/x", args: ["mcp"], claudeDesktop: "{}", claudeCode: "claude", otherClient: "/x mcp" }),
      getMcpAudit: vi.fn().mockResolvedValue([]),
    },
  };
});

import { api } from "../../api/client";
import { SettingsPanel } from "./SettingsPanel";

beforeEach(() => {
  vi.clearAllMocks();
});

// Выбор «en» переключает глобальный язык; сброс — в act, пока компоненты смонтированы.
afterEach(() => act(() => i18n.changeLanguage("ru")));

// SettingsPanel монтирует асинхронный раздел MCP: дожидаемся, пока его запросы
// (статус, фрагменты конфига, журнал) отработают, чтобы тест не кончился раньше.
async function mcpSettled() {
  await screen.findByLabelText("Разрешить MCP-сервер");
  await screen.findByText("claude");
  await screen.findByText("Журнал пуст.");
}

describe("SettingsPanel", () => {
  it("показывает язык системы и меняет язык", async () => {
    vi.mocked(api.getLanguageSettings).mockResolvedValue(base);
    vi.mocked(api.setLanguage).mockResolvedValue({ ...base, chosen: "en", effective: "en", reason: "user" });
    const user = userEvent.setup();
    renderWithClient(<SettingsPanel />);
    const select = await screen.findByLabelText("Язык интерфейса");
    await mcpSettled();
    expect(screen.getByRole("option", { name: "Как в системе (Русский)" })).toBeInTheDocument();
    expect(screen.getAllByRole("option").map((o) => o.textContent)).toEqual(
      expect.arrayContaining(["English", "Русский"]),
    );
    await user.selectOptions(select, "en");
    expect(api.setLanguage).toHaveBeenCalledWith("en");
    expect(await screen.findByLabelText("Interface language")).toBeInTheDocument();
  });

  it("язык из политики нельзя сменить", async () => {
    vi.mocked(api.getLanguageSettings).mockResolvedValue({ ...base, policy: "ru", reason: "policy" });
    renderWithClient(<SettingsPanel />);
    expect(await screen.findByLabelText("Язык интерфейса")).toBeDisabled();
    expect(screen.getByText("Язык задан вашей организацией")).toBeInTheDocument();
    await mcpSettled();
  });

  it("F1 в настройках — глава «Начало работы»", async () => {
    vi.mocked(api.getLanguageSettings).mockResolvedValue(base);
    renderWithClient(<SettingsPanel />);
    await screen.findByLabelText("Язык интерфейса");
    expect(useHelpContext.getState().byTab.settings).toBe("getting-started");
    await mcpSettled();
  });

  it("F1 в разделе MCP открывает главу «LLM-ассистенты»", async () => {
    vi.mocked(api.getLanguageSettings).mockResolvedValue(base);
    renderWithClient(<SettingsPanel />);
    const mcp = await screen.findByLabelText("Разрешить MCP-сервер");
    act(() => mcp.focus());
    expect(useHelpContext.getState().byTab.settings).toBe("llm-mcp");
    act(() => screen.getByLabelText("Язык интерфейса").focus());
    expect(useHelpContext.getState().byTab.settings).toBe("getting-started");
    await mcpSettled();
  });

  it("раздел для F1 выбирает и щелчок без фокуса (флажки и кнопки в WebKit на macOS)", async () => {
    vi.mocked(api.getLanguageSettings).mockResolvedValue(base);
    renderWithClient(<SettingsPanel />);
    const mcp = await screen.findByLabelText("Разрешить MCP-сервер");
    fireEvent.pointerDown(mcp);
    expect(useHelpContext.getState().byTab.settings).toBe("llm-mcp");
    fireEvent.pointerDown(screen.getByLabelText("Язык интерфейса"));
    expect(useHelpContext.getState().byTab.settings).toBe("getting-started");
    await mcpSettled();
  });
});
