import { beforeEach, describe, expect, it, vi } from "vitest";

const invoke = vi.fn();
vi.mock("@tauri-apps/api/core", () => ({
  invoke: (...args: unknown[]) => invoke(...args),
}));

import { api, toAppError } from "./client";
import type { AppError, ConnectionProfile } from "./types";

beforeEach(() => {
  invoke.mockReset();
});

describe("api client", () => {
  it("saveConnection оборачивает профиль, секреты и confirmExec в input", async () => {
    invoke.mockResolvedValue({});
    const profile = { id: "a" } as ConnectionProfile;
    await api.saveConnection(profile, { token: "t" }, true);
    expect(invoke).toHaveBeenCalledWith("save_connection", {
      input: { profile, secrets: { token: "t" }, confirmExec: true },
    });
  });

  it("saveConnection по умолчанию: пустые секреты и confirmExec = false", async () => {
    invoke.mockResolvedValue({});
    const profile = { id: "a" } as ConnectionProfile;
    await api.saveConnection(profile);
    expect(invoke).toHaveBeenCalledWith("save_connection", {
      input: { profile, secrets: {}, confirmExec: false },
    });
  });

  it("testConnection оборачивает профиль в input, confirmExec по умолчанию false", async () => {
    invoke.mockResolvedValue({ ok: true, steps: [] });
    const profile = { id: "", name: "x" } as ConnectionProfile;
    await api.testConnection(profile, { token: "t" });
    expect(invoke).toHaveBeenCalledWith("test_connection", {
      input: { profile, secrets: { token: "t" }, confirmExec: false },
    });
    await api.testConnection(profile, {}, true);
    expect(invoke).toHaveBeenLastCalledWith("test_connection", {
      input: { profile, secrets: {}, confirmExec: true },
    });
  });

  it("connect передаёт аргументы в camelCase, fresh по умолчанию false", async () => {
    invoke.mockResolvedValue({});
    await api.connect("c1");
    expect(invoke).toHaveBeenCalledWith("connect", { connectionId: "c1", fresh: false });
    await api.connect("c1", true);
    expect(invoke).toHaveBeenLastCalledWith("connect", { connectionId: "c1", fresh: true });
  });

  it("applyKubeconfigImport: approveExec — отпечатки строк запуска, по умолчанию пустой", async () => {
    invoke.mockResolvedValue([]);
    await api.applyKubeconfigImport("/kube/config", ["ctx"], null);
    expect(invoke).toHaveBeenCalledWith("apply_kubeconfig_import", {
      path: "/kube/config",
      contexts: ["ctx"],
      folderId: null,
      approveExec: [],
    });
    await api.applyKubeconfigImport("/kube/config", ["a", "b"], "f1", ["fp-b"]);
    expect(invoke).toHaveBeenLastCalledWith("apply_kubeconfig_import", {
      path: "/kube/config",
      contexts: ["a", "b"],
      folderId: "f1",
      approveExec: ["fp-b"],
    });
  });

  it("previewImport передаёт цель и флаг обновления", async () => {
    invoke.mockResolvedValue({ added: [], updated: [], unchanged: [], skipped: [], warnings: [], needsSecrets: 0 });
    await api.previewImport("/f.json", null, true);
    expect(invoke).toHaveBeenCalledWith("preview_import", {
      path: "/f.json",
      targetFolderId: null,
      updateExisting: true,
    });
  });

  it("resolveExecCommand передаёт строку команды и возвращает путь", async () => {
    invoke.mockResolvedValue("/usr/local/bin/kubelogin");
    await expect(api.resolveExecCommand("kubelogin")).resolves.toBe("/usr/local/bin/kubelogin");
    expect(invoke).toHaveBeenCalledWith("resolve_exec_command", { command: "kubelogin" });
  });

  it("пробрасывает AppError как есть", async () => {
    const err: AppError = { code: "NOT_FOUND", message: "нет", retryable: false };
    invoke.mockRejectedValue(err);
    await expect(api.listConnections()).rejects.toEqual(err);
  });

  it("строковые ошибки превращает в INTERNAL", async () => {
    invoke.mockRejectedValue("boom");
    await expect(api.listConnections()).rejects.toEqual({ code: "INTERNAL", message: "boom", retryable: false });
  });
});

describe("toAppError", () => {
  it("AppError проходит без изменений: key и params сохраняются", () => {
    const e: AppError = {
      code: "READ_ONLY",
      key: "errors.guard.readOnly",
      params: { name: "p" },
      message: "Connection “p” is read-only",
      retryable: false,
    };
    expect(toAppError(e)).toBe(e);
  });

  it("строка становится INTERNAL", () => {
    expect(toAppError("boom")).toEqual({ code: "INTERNAL", message: "boom", retryable: false });
  });

  it("Error становится INTERNAL с текстом сообщения", () => {
    expect(toAppError(new Error("x"))).toEqual({ code: "INTERNAL", message: "x", retryable: false });
  });
});
