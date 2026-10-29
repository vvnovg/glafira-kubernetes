import { act, screen, waitFor, within } from "@testing-library/react";
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
      cancelConnectionTest: vi.fn(),
      listProjects: vi.fn(),
    },
  };
});

import { api } from "../../api/client";
import type { ConnectionProfile, PolicyStatus } from "../../api/types";
import { useConnectionEpoch } from "../../state/connectionEpoch";
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
  useConnectionEpoch.setState({ epochs: {} });
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

  it("сохранение существующего подключения перезапускает его watch и логи: Rust остановил сессии без события", async () => {
    const c1: ConnectionProfile = { ...newProfile(), id: "c1", name: "prod-1", auth: { method: "token" } };
    vi.mocked(api.saveConnection).mockResolvedValue(c1);
    const user = userEvent.setup();
    renderWithClient(<ConnectionForm initial={c1} tabId="connection-form:c1" />);
    await user.click(screen.getByRole("button", { name: "Сохранить" }));
    await waitFor(() => expect(useWorkspace.getState().activeId).toBe("cluster:c1"));
    // Другие подключения не затрагиваются.
    expect(useConnectionEpoch.getState().epochs).toEqual({ c1: 1 });
  });

  it("сбой keychain после остановки сессий тоже перезапускает списки, отказ проверки профиля — нет", async () => {
    const c1: ConnectionProfile = { ...newProfile(), id: "c1", name: "prod-1", auth: { method: "token" } };
    const user = userEvent.setup();
    renderWithClient(<ConnectionForm initial={c1} tabId="connection-form:c1" />);
    // Профиль не принят до остановки сессий — они живы.
    vi.mocked(api.saveConnection).mockRejectedValueOnce({ code: "INVALID_INPUT", key: "errors.connections.httpsRequired", message: "x", retryable: false });
    await user.click(screen.getByRole("button", { name: "Сохранить" }));
    expect(await screen.findByText("Адрес API-сервера должен начинаться с https:// и содержать хост")).toBeInTheDocument();
    expect(useConnectionEpoch.getState().epochs).toEqual({});
    // Профиль записан и откатан, но сессии остановлены.
    vi.mocked(api.saveConnection).mockRejectedValueOnce({ code: "INTERNAL", key: "errors.secrets.keychain", message: "x", retryable: false });
    await user.click(screen.getByRole("button", { name: "Сохранить" }));
    await waitFor(() => expect(useConnectionEpoch.getState().epochs).toEqual({ c1: 1 }));
    expect(useWorkspace.getState().tabs).toEqual([]);
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

  it("exec-плагин: «Отмена» во время проверки отменяет только проверку", async () => {
    const approved: ConnectionProfile = {
      ...newProfile(),
      id: "e1",
      name: "eks",
      auth: { method: "exec", command: "/usr/local/bin/aws", args: ["eks", "get-token"], env: [], apiVersion: "client.authentication.k8s.io/v1beta1", provideClusterInfo: false, approved: "fp" },
    };
    vi.mocked(api.testConnection).mockReturnValue(new Promise(() => undefined));
    vi.mocked(api.cancelConnectionTest).mockResolvedValue(true);
    const user = userEvent.setup();
    renderWithClient(<ConnectionForm initial={approved} tabId="t" />);
    await user.click(screen.getByRole("button", { name: "Проверить подключение" }));
    await user.click(await screen.findByRole("button", { name: "Отмена" }));
    expect(api.cancelConnectionTest).toHaveBeenCalledWith("e1");
  });

  it("exec-плагин: значения секретов скрыты до «Показать», поля до этого только для чтения", async () => {
    const withSecrets: ConnectionProfile = {
      ...newProfile(),
      id: "e2",
      name: "oidc",
      auth: {
        method: "exec",
        command: "/opt/homebrew/bin/kubectl",
        args: ["oidc-login", "get-token", "--oidc-client-secret=c1i3nt"],
        env: [
          { name: "AWS_PROFILE", value: "dev" },
          { name: "AWS_SECRET_ACCESS_KEY", value: "s3cr3t" },
        ],
        apiVersion: "client.authentication.k8s.io/v1",
        provideClusterInfo: false,
        approved: "fp",
      },
    };
    const user = userEvent.setup();
    renderWithClient(<ConnectionForm initial={withSecrets} tabId="t" />);
    const args = screen.getByLabelText("Аргументы (по одному в строке)");
    const env = screen.getByLabelText("Переменные окружения (ИМЯ=значение, по одной в строке)");
    expect(args).toHaveValue("oidc-login\nget-token\n--oidc-client-secret=••••");
    expect(env).toHaveValue("AWS_PROFILE=dev\nAWS_SECRET_ACCESS_KEY=••••");
    expect(args).toHaveAttribute("readonly");
    expect(screen.queryByDisplayValue(/s3cr3t|c1i3nt/)).not.toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Показать" }));
    expect(env).toHaveValue("AWS_PROFILE=dev\nAWS_SECRET_ACCESS_KEY=s3cr3t");
    expect(args).toHaveValue("oidc-login\nget-token\n--oidc-client-secret=c1i3nt");
    expect(args).not.toHaveAttribute("readonly");
    expect(screen.queryByRole("button", { name: "Показать" })).not.toBeInTheDocument();
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

  it("доступ LLM: уровень и проекты уходят в профиль, «все доступные» — null", async () => {
    vi.mocked(api.saveConnection).mockImplementation(async (p) => ({ ...p, id: "c1" }));
    const user = userEvent.setup();
    const initial = { ...newProfile(), id: "c1", name: "dev", apiUrl: "https://api.dev:6443" };
    renderWithClient(<ConnectionForm initial={initial} tabId="t" />);
    expect(screen.queryByLabelText("Проекты для LLM")).toBeNull();
    await user.selectOptions(screen.getByLabelText("Доступ LLM (MCP)"), "metadata");
    await user.click(screen.getByRole("button", { name: "Сохранить" }));
    await waitFor(() => expect(api.saveConnection).toHaveBeenCalledTimes(1));
    expect(vi.mocked(api.saveConnection).mock.calls[0][0]).toMatchObject({ llmAccess: "metadata", llmNamespaces: null });

    await user.selectOptions(screen.getByLabelText("Проекты для LLM"), "list");
    await user.type(screen.getByLabelText("Проекты LLM"), "team-a, team-b, team-a");
    await user.click(screen.getByRole("button", { name: "Сохранить" }));
    await waitFor(() => expect(api.saveConnection).toHaveBeenCalledTimes(2));
    expect(vi.mocked(api.saveConnection).mock.calls[1][0]).toMatchObject({
      llmAccess: "metadata",
      llmNamespaces: ["team-a", "team-b"],
    });
  });

  it("источник метрик: свой Prometheus с токеном — адрес в профиле, токен секретом; «Автоматически» — без них", async () => {
    vi.mocked(api.saveConnection).mockImplementation(async (p) => ({ ...p, id: "c1" }));
    const user = userEvent.setup();
    const initial = { ...newProfile(), id: "c1", name: "dev", apiUrl: "https://api.dev:6443" };
    renderWithClient(<ConnectionForm initial={initial} tabId="t" />);
    expect(screen.queryByLabelText("Адрес источника")).toBeNull();
    await user.selectOptions(screen.getByLabelText("Метрики"), "prometheus");
    await user.type(screen.getByLabelText("Адрес источника"), "https://prometheus.example.com");
    await user.selectOptions(screen.getByLabelText("Авторизация источника"), "bearer");
    await user.type(screen.getByLabelText("Токен источника"), "prom-token");
    await user.selectOptions(screen.getByLabelText("CA источника"), "pem");
    await user.type(screen.getByLabelText("Сертификаты CA источника (PEM)"), "PEM");
    await user.click(screen.getByRole("button", { name: "Сохранить" }));
    await waitFor(() => expect(api.saveConnection).toHaveBeenCalledTimes(1));
    const [saved, secrets] = vi.mocked(api.saveConnection).mock.calls[0];
    expect(saved.metrics).toEqual({ mode: "prometheus", url: "https://prometheus.example.com", auth: "bearer", ca: { mode: "pem", pem: "PEM" } });
    expect(secrets?.metricsToken).toBe("prom-token");

    // Вернуться к «Автоматически» и обратно: введённый адрес не теряется.
    await user.selectOptions(screen.getByLabelText("Метрики"), "auto");
    expect(screen.queryByLabelText("Адрес источника")).toBeNull();
    await user.selectOptions(screen.getByLabelText("Метрики"), "thanos");
    expect(screen.getByLabelText("Адрес источника")).toHaveValue("https://prometheus.example.com");
    await user.selectOptions(screen.getByLabelText("Метрики"), "auto");
    await user.click(screen.getByRole("button", { name: "Сохранить" }));
    await waitFor(() => expect(api.saveConnection).toHaveBeenCalledTimes(2));
    const [auto, autoSecrets] = vi.mocked(api.saveConnection).mock.calls[1];
    expect(auto.metrics).toEqual({ mode: "auto" });
    expect(autoSecrets).not.toHaveProperty("metricsToken");
  });

  it("поля проектов подсказывают проекты, которые уже прочитало дерево, без нового запроса", async () => {
    const user = userEvent.setup();
    const initial = { ...newProfile(), id: "c1", name: "dev", apiUrl: "https://api.dev:6443", favoriteNamespaces: ["team-a"], llmAccess: "metadata" as const, llmNamespaces: [] };
    const { client } = renderWithClient(<ConnectionForm initial={initial} tabId="t" />);
    act(() => client.setQueryData(["projects", "c1"], { items: [{ name: "default" }, { name: "team-b" }], truncated: false }));
    await user.type(screen.getByLabelText("Проекты LLM"), "te");
    expect(within(screen.getByRole("listbox")).getAllByRole("option").map((o) => o.textContent)).toEqual(["team-a", "team-b"]);
    // В избранных уже team-a; недописанное «te» из другого поля подсказкой не становится.
    await user.type(screen.getByLabelText("Избранные проекты"), ", ");
    expect(within(screen.getByRole("listbox")).getAllByRole("option").map((o) => o.textContent)).toEqual(["default", "team-b"]);
    expect(api.listProjects).not.toHaveBeenCalled();
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
