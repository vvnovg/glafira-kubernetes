import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../test/render";

vi.mock("@tauri-apps/plugin-opener", () => ({ openUrl: vi.fn() }));
vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return { ...orig, api: { getAppInfo: vi.fn(), getPolicyStatus: vi.fn() } };
});

import { openUrl } from "@tauri-apps/plugin-opener";
import { api } from "../../api/client";
import { LICENSE_URL, SUPPORT_URL } from "../../lib/links";
import { AboutPanel } from "./AboutPanel";

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(api.getAppInfo).mockResolvedValue({
    name: "Glafira for Kubernetes", version: "0.1.0", commit: "abc1234", os: "macos", arch: "aarch64",
    dataDir: "/data", logDir: "/logs",
  });
  vi.mocked(api.getPolicyStatus).mockResolvedValue({
    managed: true, source: "/etc/x", updatesEnabled: true, updateUrl: null, language: null, mcpEnabled: true,
    allowInsecureTls: false,
    allowExecAuth: false,
    errors: [{ key: "errors.policy.invalidBool", params: { key: "mcpEnabled", value: "2" }, text: "x" }],
  });
});

describe("AboutPanel", () => {
  it("версия, сборка, политика, ссылки и товарные знаки", async () => {
    const user = userEvent.setup();
    renderWithClient(<AboutPanel />);
    expect(await screen.findByText("Glafira for Kubernetes")).toBeInTheDocument();
    expect(screen.getByText("0.1.0")).toBeInTheDocument();
    expect(screen.getByText("abc1234")).toBeInTheDocument();
    expect(await screen.findByText("задана")).toBeInTheDocument();
    expect(screen.getByText("Политика организации: mcpEnabled: недопустимое логическое 2")).toBeInTheDocument();
    expect(screen.getByText(/Kubernetes — зарегистрированный товарный знак/)).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Apache 2.0" }));
    expect(openUrl).toHaveBeenCalledWith(LICENSE_URL);
    await user.click(screen.getByRole("button", { name: "Поддержать проект" }));
    expect(openUrl).toHaveBeenCalledWith(SUPPORT_URL);
  });
});
