import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../test/render";

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return {
    ...orig,
    api: {
      testConnection: vi.fn(),
      saveConnection: vi.fn(),
      deleteConnection: vi.fn(),
      listConnections: vi.fn().mockResolvedValue([]),
      listFolders: vi.fn().mockResolvedValue([]),
      moveItems: vi.fn(),
      getPolicyStatus: vi.fn(),
      resolveExecCommand: vi.fn(),
      cancelOauthLogin: vi.fn(),
    },
  };
});

import { api } from "../../api/client";
import type { ConnectionProfile, PolicyStatus } from "../../api/types";
import { useWorkspace } from "../../state/workspace";
import { ConnectionForm } from "./ConnectionForm";
import { newProfile } from "./profile";

const POLICY: PolicyStatus = {
  managed: false,
  source: null,
  updatesEnabled: true,
  updateUrl: null,
  language: null,
  mcpEnabled: true,
  allowInsecureTls: true,
  allowExecAuth: true,
  errors: [],
};

const msg = (key: string, text: string, params: Record<string, string> = {}) => ({ key, params, text });

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(api.listFolders).mockResolvedValue([]);
  vi.mocked(api.getPolicyStatus).mockResolvedValue(POLICY);
  useWorkspace.setState({ tabs: [], activeId: null });
});

describe("ConnectionForm", () => {
  it("проверяет подключение по токену и показывает шаги", async () => {
    vi.mocked(api.testConnection).mockResolvedValue({
      ok: false,
      steps: [
        { name: "config", ok: true, message: msg("messages.connectionTest.configOk", "Configuration is valid") },
        { name: "tcp", ok: true, message: msg("messages.connectionTest.tcpOk", "TCP", { address: "10.0.0.1:6443" }) },
        { name: "tls", ok: true, message: msg("messages.connectionTest.tlsOk", "trusted") },
        {
          name: "auth",
          ok: false,
          message: msg("errors.k8s.unauthorized", "rejected"),
          error: { code: "SESSION_EXPIRED", key: "errors.k8s.unauthorized", message: "rejected", details: "401 Unauthorized", retryable: false },
        },
      ],
    });
    const user = userEvent.setup();
    renderWithClient(<ConnectionForm initial={newProfile()} tabId="t" />);

    await user.type(screen.getByLabelText("Имя"), "dev");
    await user.type(screen.getByLabelText("Адрес API-сервера"), "https://api.dev:6443");
    await user.selectOptions(screen.getByLabelText("Способ входа"), "token");
    await user.type(screen.getByLabelText("Токен"), "sha256~abc");
    await user.type(screen.getByLabelText("Избранные проекты"), "team-a, team-b");
    await user.click(screen.getByRole("button", { name: "Проверить подключение" }));

    expect(api.testConnection).toHaveBeenCalledWith(
      expect.objectContaining({
        name: "dev",
        apiUrl: "https://api.dev:6443",
        auth: { method: "token" },
        favoriteNamespaces: ["team-a", "team-b"],
      }),
      { token: "sha256~abc", clientKey: null },
      false,
    );
    expect(await screen.findByText("TCP-соединение с 10.0.0.1:6443 установлено")).toBeInTheDocument();
    expect(screen.getByText("Вход:")).toBeInTheDocument();
    expect(screen.getByText("Кластер отклонил учётные данные: токен истёк или неверен")).toBeInTheDocument();
    expect(screen.getByText("401 Unauthorized")).toBeInTheDocument();
  });

  it("сохраняет и открывает вкладку подключения", async () => {
    const saved: ConnectionProfile = { ...newProfile(), id: "new-id", name: "dev", apiUrl: "https://api.dev:6443" };
    vi.mocked(api.saveConnection).mockResolvedValue(saved);
    useWorkspace.getState().open({ kind: "connection-form", connectionId: null });
    const user = userEvent.setup();
    renderWithClient(<ConnectionForm initial={newProfile()} tabId="connection-form:new" />);
    await user.type(screen.getByLabelText("Имя"), "dev");
    await user.click(screen.getByRole("button", { name: "Сохранить" }));
    await waitFor(() => expect(useWorkspace.getState().activeId).toBe("cluster:new-id"));
    expect(api.saveConnection).toHaveBeenCalledWith(
      expect.objectContaining({ auth: { method: "browser" } }),
      {
        token: null,
        clientKey: null,
      },
      false,
    );
    expect(useWorkspace.getState().tabs.map((t) => t.id)).toEqual(["cluster:new-id"]);
  });

  it("клиентский сертификат: поля сертификата и ключа, ключ уходит секретом", async () => {
    vi.mocked(api.testConnection).mockResolvedValue({ ok: true, steps: [] });
    const user = userEvent.setup();
    renderWithClient(<ConnectionForm initial={newProfile()} tabId="t" />);
    await user.selectOptions(screen.getByLabelText("Способ входа"), "clientCert");
    await user.type(screen.getByLabelText("Сертификат (PEM)"), "CERT");
    await user.type(screen.getByLabelText("Ключ (PEM)"), "KEY");
    await user.click(screen.getByRole("button", { name: "Проверить подключение" }));
    expect(api.testConnection).toHaveBeenCalledWith(
      expect.objectContaining({ auth: { method: "clientCert", certificatePem: "CERT" } }),
      {
        token: null,
        clientKey: "KEY",
      },
      false,
    );
    // Переключение на токен и обратно не теряет сертификат.
    await user.selectOptions(screen.getByLabelText("Способ входа"), "token");
    expect(screen.queryByLabelText("Сертификат (PEM)")).toBeNull();
    await user.selectOptions(screen.getByLabelText("Способ входа"), "clientCert");
    expect(screen.getByLabelText("Сертификат (PEM)")).toHaveValue("CERT");
  });

  it("CA: сертификаты PEM, предупреждение и запрет небезопасного режима политикой", async () => {
    const user = userEvent.setup();
    const { unmount } = renderWithClient(<ConnectionForm initial={newProfile()} tabId="t" />);
    await user.selectOptions(screen.getByLabelText("Доверенный CA"), "pem");
    expect(screen.getByLabelText("Сертификаты CA (PEM)")).toBeInTheDocument();
    await user.selectOptions(screen.getByLabelText("Доверенный CA"), "insecure");
    expect(screen.getByRole("alert")).toHaveTextContent("не будет проверяться");
    unmount();

    vi.mocked(api.getPolicyStatus).mockResolvedValue({ ...POLICY, allowInsecureTls: false });
    renderWithClient(<ConnectionForm initial={newProfile()} tabId="t" />);
    const option = await screen.findByRole("option", { name: "Не проверять (небезопасно)" });
    await waitFor(() => expect(option).toBeDisabled());
  });

  it("новое prod-подключение — только для чтения", async () => {
    const user = userEvent.setup();
    renderWithClient(<ConnectionForm initial={newProfile()} tabId="t" />);
    await user.selectOptions(screen.getByLabelText("Окружение"), "prod");
    expect(screen.getByLabelText("Только чтение")).toBeChecked();
  });

  const existing: ConnectionProfile = { ...newProfile(), id: "c1", name: "prod-1", auth: { method: "token" } };

  it("существующее подключение: пустой токен не меняется", async () => {
    vi.mocked(api.saveConnection).mockResolvedValue(existing);
    const user = userEvent.setup();
    renderWithClient(<ConnectionForm initial={existing} tabId="connection-form:c1" />);
    expect(screen.getByLabelText("Токен")).toHaveAttribute("placeholder", "оставьте пустым, чтобы не менять");
    await user.click(screen.getByRole("button", { name: "Сохранить" }));
    expect(api.saveConnection).toHaveBeenCalledWith(expect.objectContaining({ id: "c1" }), { token: null, clientKey: null }, false);
  });

  it("exec-плагин: «Найти», строка запуска и подтверждение перед проверкой", async () => {
    vi.mocked(api.resolveExecCommand).mockResolvedValue("/opt/homebrew/bin/kubelogin");
    vi.mocked(api.testConnection).mockResolvedValue({ ok: true, steps: [] });
    const user = userEvent.setup();
    renderWithClient(<ConnectionForm initial={newProfile()} tabId="t" />);
    await user.selectOptions(screen.getByLabelText("Способ входа"), "exec");
    await user.type(screen.getByLabelText("Команда"), "kubelogin");
    await user.click(screen.getByRole("button", { name: "Найти" }));
    await waitFor(() => expect(screen.getByLabelText("Команда")).toHaveValue("/opt/homebrew/bin/kubelogin"));
    expect(api.resolveExecCommand).toHaveBeenCalledWith("kubelogin");
    await user.type(screen.getByLabelText("Аргументы (по одному в строке)"), "get-token{Enter}--oidc-issuer-url=https://sso.corp");
    await user.type(screen.getByLabelText("Переменные окружения (ИМЯ=значение, по одной в строке)"), "OIDC_CLIENT_SECRET=s3cr3t");
    expect(screen.getByLabelText("Строка запуска")).toHaveTextContent(
      "OIDC_CLIENT_SECRET=•••• /opt/homebrew/bin/kubelogin get-token --oidc-issuer-url=https://sso.corp",
    );
    const test = screen.getByRole("button", { name: "Проверить подключение" });
    expect(test).toBeDisabled();
    await user.click(screen.getByLabelText("Разрешаю Glafira запускать эту команду"));
    await user.click(test);
    expect(api.testConnection).toHaveBeenCalledWith(
      expect.objectContaining({
        auth: expect.objectContaining({
          method: "exec",
          command: "/opt/homebrew/bin/kubelogin",
          args: ["get-token", "--oidc-issuer-url=https://sso.corp"],
          env: [{ name: "OIDC_CLIENT_SECRET", value: "s3cr3t" }],
        }),
      }),
      { token: null, clientKey: null },
      true,
    );
  });

  it("exec-плагин: правка строки после галочки снимает подтверждение", async () => {
    vi.mocked(api.testConnection).mockResolvedValue({ ok: true, steps: [] });
    const user = userEvent.setup();
    renderWithClient(<ConnectionForm initial={newProfile()} tabId="t" />);
    await user.selectOptions(screen.getByLabelText("Способ входа"), "exec");
    await user.type(screen.getByLabelText("Команда"), "/usr/local/bin/aws");
    await user.type(screen.getByLabelText("Аргументы (по одному в строке)"), "eks");
    const confirm = screen.getByLabelText("Разрешаю Glafira запускать эту команду");
    const test = screen.getByRole("button", { name: "Проверить подключение" });
    await user.click(confirm);
    expect(confirm).toBeChecked();
    expect(test).toBeEnabled();

    await user.type(screen.getByLabelText("Аргументы (по одному в строке)"), "{Enter}get-token");
    expect(confirm).not.toBeChecked();
    expect(test).toBeDisabled();
    expect(api.testConnection).not.toHaveBeenCalled();

    // Новая галочка относится к новой строке.
    await user.click(confirm);
    await user.click(test);
    expect(api.testConnection).toHaveBeenCalledWith(
      expect.objectContaining({ auth: expect.objectContaining({ args: ["eks", "get-token"] }) }),
      { token: null, clientKey: null },
      true,
    );
  });

  it("exec-плагин: после смены способа туда и обратно подтверждение нужно снова", async () => {
    const user = userEvent.setup();
    renderWithClient(<ConnectionForm initial={newProfile()} tabId="t" />);
    await user.selectOptions(screen.getByLabelText("Способ входа"), "exec");
    await user.type(screen.getByLabelText("Команда"), "/usr/local/bin/aws");
    await user.click(screen.getByLabelText("Разрешаю Glafira запускать эту команду"));
    expect(screen.getByRole("button", { name: "Проверить подключение" })).toBeEnabled();

    await user.selectOptions(screen.getByLabelText("Способ входа"), "token");
    await user.selectOptions(screen.getByLabelText("Способ входа"), "exec");
    // Строка вернулась та же, но подтверждение сбросилось.
    expect(screen.getByLabelText("Команда")).toHaveValue("/usr/local/bin/aws");
    expect(screen.getByLabelText("Разрешаю Glafira запускать эту команду")).not.toBeChecked();
    expect(screen.getByRole("button", { name: "Проверить подключение" })).toBeDisabled();
  });

  it("exec-плагин: подтверждённая строка не требует галочки, политика запрещает способ", async () => {
    const approved: ConnectionProfile = {
      ...newProfile(),
      id: "e1",
      name: "eks",
      auth: { method: "exec", command: "/usr/local/bin/aws", args: ["eks", "get-token"], env: [], apiVersion: "client.authentication.k8s.io/v1beta1", provideClusterInfo: false, approved: "fp" },
    };
    const user = userEvent.setup();
    const { unmount } = renderWithClient(<ConnectionForm initial={approved} tabId="t" />);
    expect(screen.getByText("Строка запуска подтверждена")).toBeInTheDocument();
    await user.type(screen.getByLabelText("Аргументы (по одному в строке)"), "{Enter}--region=eu-west-1");
    expect(screen.getByLabelText("Разрешаю Glafira запускать эту команду")).not.toBeChecked();
    unmount();

    vi.mocked(api.getPolicyStatus).mockResolvedValue({ ...POLICY, allowExecAuth: false });
    renderWithClient(<ConnectionForm initial={newProfile()} tabId="t" />);
    const option = await screen.findByRole("option", { name: "Exec-плагин" });
    await waitFor(() => expect(option).toBeDisabled());
  });

  it("удаление с подтверждением закрывает вкладки подключения", async () => {
    vi.mocked(api.deleteConnection).mockResolvedValue();
    useWorkspace.getState().open({ kind: "cluster", connectionId: "c1" });
    useWorkspace.getState().open({ kind: "connection-form", connectionId: "c1" });
    const user = userEvent.setup();
    renderWithClient(<ConnectionForm initial={existing} tabId="connection-form:c1" />);
    await user.click(screen.getByRole("button", { name: "Удалить" }));
    expect(screen.getByText("Удалить подключение «prod-1»?")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Да, удалить" }));
    await waitFor(() => expect(api.deleteConnection).toHaveBeenCalledWith("c1"));
    await waitFor(() => expect(useWorkspace.getState().tabs).toEqual([]));
  });

  it("ошибка сохранения показывается баннером", async () => {
    vi.mocked(api.saveConnection).mockRejectedValue({
      code: "INVALID_INPUT",
      key: "errors.connections.httpsRequired",
      message: "x",
      retryable: false,
    });
    const user = userEvent.setup();
    renderWithClient(<ConnectionForm initial={newProfile()} tabId="t" />);
    await user.click(screen.getByRole("button", { name: "Сохранить" }));
    expect(await screen.findByText("Адрес API-сервера должен начинаться с https:// и содержать хост")).toBeInTheDocument();
  });
});
