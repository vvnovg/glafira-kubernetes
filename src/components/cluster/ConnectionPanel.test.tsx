import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../test/render";

vi.mock("@tauri-apps/plugin-opener", () => ({ openUrl: vi.fn() }));

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return {
    ...orig,
    api: {
      listConnections: vi.fn(),
      connect: vi.fn(),
      startOauthLogin: vi.fn(),
      cancelOauthLogin: vi.fn(),
      setConnectionToken: vi.fn(),
      oauthTokenPage: vi.fn(),
    },
  };
});

import { openUrl } from "@tauri-apps/plugin-opener";
import { api } from "../../api/client";
import type { ConnectionInfo, ConnectionProfile } from "../../api/types";
import { useWorkspace } from "../../state/workspace";
import { newProfile } from "../connections/profile";
import { ConnectionPanel } from "./ConnectionPanel";

const INFO: ConnectionInfo = {
  user: "dev1",
  groups: ["system:authenticated", "team-a"],
  serverVersion: "v1.32.5",
  platform: { kind: "openshift", distribution: null, version: "4.18", label: "OpenShift 4.18" },
  apiGroups: { routes: true, projects: true, deploymentConfigs: true, gatewayApi: false },
  capabilities: { listProjects: true, listNodes: false, listAllPods: false, readClusterOperators: true, readClusterVersion: false },
};

const profile = (auth: ConnectionProfile["auth"]): ConnectionProfile => ({
  ...newProfile(),
  id: "c1",
  name: "dev-cluster",
  apiUrl: "https://api.dev:6443",
  auth,
});

const EXPIRED = { code: "SESSION_EXPIRED", key: "errors.k8s.unauthorized", message: "x", retryable: false };

const EXEC: ConnectionProfile["auth"] = {
  method: "exec",
  command: "/opt/homebrew/bin/kubelogin",
  args: ["get-token"],
  env: [],
  apiVersion: "client.authentication.k8s.io/v1",
  provideClusterInfo: false,
  approved: "fp",
};

const EXEC_FAILED = { code: "AUTH_FAILED", key: "errors.exec.failed", params: { command: "kubelogin", code: "1" }, message: "x", retryable: false };

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(api.cancelOauthLogin).mockResolvedValue(true);
  useWorkspace.setState({ tabs: [], activeId: null });
});

describe("ConnectionPanel", () => {
  it("подключается и показывает пользователя, платформу, группы API и права", async () => {
    vi.mocked(api.listConnections).mockResolvedValue([profile({ method: "token" })]);
    vi.mocked(api.connect).mockResolvedValue(INFO);
    renderWithClient(<ConnectionPanel connectionId="c1" />);
    expect(await screen.findByText("dev1")).toBeInTheDocument();
    expect(api.connect).toHaveBeenCalledWith("c1", false);
    expect(screen.getByText("OpenShift 4.18")).toBeInTheDocument();
    expect(screen.getByText("Route, Project, DeploymentConfig")).toBeInTheDocument();
    expect(screen.getByText("✅ Операторы кластера")).toBeInTheDocument();
    expect(screen.getByText("v1.32.5")).toBeInTheDocument();
    expect(screen.getByText("✅ Список проектов")).toBeInTheDocument();
    expect(screen.getByText("— Ноды")).toBeInTheDocument();
    expect(screen.getByText("2 группы")).toBeInTheDocument();
  });

  it("Kubernetes: платформа с дистрибутивом, Gateway API, без прав OpenShift", async () => {
    vi.mocked(api.listConnections).mockResolvedValue([profile({ method: "token" })]);
    vi.mocked(api.connect).mockResolvedValue({
      ...INFO,
      platform: { kind: "kubernetes", distribution: "eks", version: "1.31", label: "Kubernetes 1.31 (EKS)" },
      apiGroups: { routes: false, projects: false, deploymentConfigs: false, gatewayApi: true },
    });
    renderWithClient(<ConnectionPanel connectionId="c1" />);
    expect(await screen.findByText("Kubernetes 1.31 (EKS)")).toBeInTheDocument();
    expect(screen.getByText("Gateway API")).toBeInTheDocument();
    expect(screen.queryByText(/Операторы кластера/)).toBeNull();
  });

  it("exec-плагин: ожидание с отменой, ошибка плагина и запуск снова", async () => {
    vi.mocked(api.listConnections).mockResolvedValue([profile(EXEC)]);
    let fail: (e: unknown) => void = () => {};
    vi.mocked(api.connect).mockReturnValueOnce(new Promise((_, reject) => (fail = reject)));
    const user = userEvent.setup();
    renderWithClient(<ConnectionPanel connectionId="c1" />);
    expect(await screen.findByText(/Ожидание exec-плагина/)).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Отменить" }));
    expect(api.cancelOauthLogin).toHaveBeenCalledWith("c1");
    fail(EXEC_FAILED);
    vi.mocked(api.connect).mockResolvedValue(INFO);
    await user.click(await screen.findByRole("button", { name: "Запустить exec-плагин снова" }));
    expect(api.connect).toHaveBeenLastCalledWith("c1", true);
    expect(await screen.findByText("dev1")).toBeInTheDocument();
  });

  it("exec-плагин: «Подключиться заново» показывает ожидание с отменой", async () => {
    vi.mocked(api.listConnections).mockResolvedValue([profile(EXEC)]);
    vi.mocked(api.connect).mockResolvedValueOnce(INFO);
    const user = userEvent.setup();
    renderWithClient(<ConnectionPanel connectionId="c1" />);
    expect(await screen.findByText("dev1")).toBeInTheDocument();
    expect(screen.queryByText(/Ожидание exec-плагина/)).toBeNull();

    let finish: (i: ConnectionInfo) => void = () => {};
    vi.mocked(api.connect).mockReturnValueOnce(new Promise((r) => (finish = r)));
    await user.click(screen.getByRole("button", { name: "Подключиться заново" }));
    expect(api.connect).toHaveBeenLastCalledWith("c1", true);
    expect(await screen.findByText(/Ожидание exec-плагина/)).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Отменить" }));
    expect(api.cancelOauthLogin).toHaveBeenCalledWith("c1");

    finish(INFO);
    await waitFor(() => expect(screen.queryByText(/Ожидание exec-плагина/)).toBeNull());
  });

  it("exec-плагин: после ошибки повторного запуска ожидание показывается один раз", async () => {
    vi.mocked(api.listConnections).mockResolvedValue([profile(EXEC)]);
    vi.mocked(api.connect).mockResolvedValueOnce(INFO).mockRejectedValueOnce(EXEC_FAILED);
    const user = userEvent.setup();
    renderWithClient(<ConnectionPanel connectionId="c1" />);
    expect(await screen.findByText("dev1")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Подключиться заново" }));
    // Прежние сведения остаются, рядом с ошибкой — действия входа.
    expect(await screen.findByRole("button", { name: "Запустить exec-плагин снова" })).toBeInTheDocument();

    let finish: (i: ConnectionInfo) => void = () => {};
    vi.mocked(api.connect).mockReturnValueOnce(new Promise((r) => (finish = r)));
    await user.click(screen.getByRole("button", { name: "Подключиться заново" }));
    expect(await screen.findByText(/Ожидание exec-плагина/)).toBeInTheDocument();
    expect(screen.getAllByText(/Ожидание exec-плагина/)).toHaveLength(1);
    expect(screen.getAllByRole("button", { name: "Отменить" })).toHaveLength(1);
    expect(screen.queryByRole("button", { name: "Запустить exec-плагин снова" })).toBeNull();

    finish(INFO);
    await waitFor(() => expect(screen.queryByText(/Ожидание exec-плагина/)).toBeNull());
    expect(screen.queryByRole("alert")).toBeNull();
  });

  it.each([
    ["первое подключение не удалось", false],
    ["после успешного подключения", true],
  ])("exec-плагин: пока идёт «Запустить exec-плагин снова» (%s), «Подключиться заново» недоступна", async (_name, hadData) => {
    vi.mocked(api.listConnections).mockResolvedValue([profile(EXEC)]);
    if (hadData) vi.mocked(api.connect).mockResolvedValueOnce(INFO);
    vi.mocked(api.connect).mockRejectedValueOnce(EXEC_FAILED);
    const user = userEvent.setup();
    renderWithClient(<ConnectionPanel connectionId="c1" />);
    if (hadData) {
      expect(await screen.findByText("dev1")).toBeInTheDocument();
      await user.click(screen.getByRole("button", { name: "Подключиться заново" }));
    }
    const rerun = await screen.findByRole("button", { name: "Запустить exec-плагин снова" });
    const before = hadData ? 2 : 1;
    expect(api.connect).toHaveBeenCalledTimes(before);

    let finish: (i: ConnectionInfo) => void = () => {};
    vi.mocked(api.connect).mockReturnValueOnce(new Promise((r) => (finish = r)));
    await user.click(rerun);
    expect(api.connect).toHaveBeenCalledTimes(before + 1);
    expect(api.connect).toHaveBeenLastCalledWith("c1", true);
    const reconnect = screen.getByRole("button", { name: "Подключиться заново" });
    expect(reconnect).toBeDisabled();
    // Второй запуск плагина в очередь не встаёт.
    await user.click(reconnect);
    expect(api.connect).toHaveBeenCalledTimes(before + 1);
    await user.click(screen.getByRole("button", { name: "Отменить" }));
    expect(api.cancelOauthLogin).toHaveBeenCalledTimes(1);

    finish(INFO);
    await waitFor(() => expect(screen.getByRole("button", { name: "Подключиться заново" })).toBeEnabled());
    expect(screen.getByText("dev1")).toBeInTheDocument();
    expect(api.connect).toHaveBeenCalledTimes(before + 1);
  });

  it("истёкшая сессия у входа через браузер: вход и успешное подключение", async () => {
    vi.mocked(api.listConnections).mockResolvedValue([profile({ method: "browser" })]);
    vi.mocked(api.connect).mockRejectedValue(EXPIRED);
    let finish: (i: ConnectionInfo) => void = () => {};
    vi.mocked(api.startOauthLogin).mockReturnValue(new Promise((r) => (finish = r)));
    const user = userEvent.setup();
    renderWithClient(<ConnectionPanel connectionId="c1" />);
    await user.click(await screen.findByRole("button", { name: "Войти через браузер" }));
    expect(api.startOauthLogin).toHaveBeenCalledWith("c1");
    expect(screen.getByText("Ожидание входа в браузере…")).toBeInTheDocument();
    // Пока ждём вход, подключение заново не запускаем.
    expect(screen.getByRole("button", { name: "Подключиться заново" })).toBeDisabled();
    await user.click(screen.getByRole("button", { name: "Отменить" }));
    expect(api.cancelOauthLogin).toHaveBeenCalledWith("c1");
    finish(INFO);
    expect(await screen.findByText("dev1")).toBeInTheDocument();
  });

  it("отмена входа через браузер не выглядит ошибкой", async () => {
    vi.mocked(api.listConnections).mockResolvedValue([profile({ method: "browser" })]);
    vi.mocked(api.connect).mockRejectedValue(EXPIRED);
    let cancelled: (e: unknown) => void = () => {};
    vi.mocked(api.startOauthLogin).mockReturnValue(new Promise((_, reject) => (cancelled = reject)));
    const user = userEvent.setup();
    renderWithClient(<ConnectionPanel connectionId="c1" />);
    await user.click(await screen.findByRole("button", { name: "Войти через браузер" }));
    await user.click(screen.getByRole("button", { name: "Отменить" }));
    cancelled({ code: "AUTH_FAILED", key: "errors.oauth.cancelled", message: "x", retryable: false });
    expect(await screen.findByRole("button", { name: "Войти через браузер" })).toBeInTheDocument();
    expect(screen.queryByText("Вход отменён")).toBeNull();
    // Остался только баннер истёкшей сессии.
    expect(screen.getAllByRole("alert")).toHaveLength(1);
  });

  it("пока идёт вход через браузер, «Сохранить токен» недоступна", async () => {
    vi.mocked(api.listConnections).mockResolvedValue([profile({ method: "browser" })]);
    vi.mocked(api.connect).mockRejectedValue(EXPIRED);
    let signedIn: (i: ConnectionInfo) => void = () => {};
    vi.mocked(api.startOauthLogin).mockReturnValue(new Promise<ConnectionInfo>((r) => (signedIn = r)));
    const user = userEvent.setup();
    renderWithClient(<ConnectionPanel connectionId="c1" />);
    await user.click(await screen.findByRole("button", { name: "Войти через браузер" }));
    expect(await screen.findByText("Ожидание входа в браузере…")).toBeInTheDocument();
    await user.type(screen.getByLabelText("Токен"), "sha256~new");
    expect(screen.getByRole("button", { name: "Сохранить токен" })).toBeDisabled();
    expect(api.setConnectionToken).not.toHaveBeenCalled();

    signedIn(INFO);
    expect(await screen.findByText("dev1")).toBeInTheDocument();
  });

  it("пока сохраняется токен, «Войти через браузер» недоступна", async () => {
    vi.mocked(api.listConnections).mockResolvedValue([profile({ method: "browser" })]);
    vi.mocked(api.connect).mockRejectedValueOnce(EXPIRED);
    let saved: () => void = () => {};
    vi.mocked(api.setConnectionToken).mockReturnValue(new Promise<void>((r) => (saved = r)));
    let finish: (i: ConnectionInfo) => void = () => {};
    vi.mocked(api.connect).mockReturnValueOnce(new Promise((r) => (finish = r)));
    const user = userEvent.setup();
    renderWithClient(<ConnectionPanel connectionId="c1" />);
    await user.type(await screen.findByLabelText("Токен"), "sha256~new");
    await user.click(screen.getByRole("button", { name: "Сохранить токен" }));
    await waitFor(() => expect(api.setConnectionToken).toHaveBeenCalledWith("c1", "sha256~new"));
    expect(screen.getByRole("button", { name: "Войти через браузер" })).toBeDisabled();

    // После сохранения токена идёт подключение: блок входа скрыт, пока оно не закончится.
    saved();
    await waitFor(() => expect(api.connect).toHaveBeenCalledTimes(2));
    finish(INFO);
    expect(await screen.findByText("dev1")).toBeInTheDocument();
  });

  it("вставка токена сохраняет его и подключается заново", async () => {
    vi.mocked(api.listConnections).mockResolvedValue([profile({ method: "token" })]);
    vi.mocked(api.connect).mockRejectedValueOnce(EXPIRED).mockResolvedValue(INFO);
    vi.mocked(api.setConnectionToken).mockResolvedValue();
    const user = userEvent.setup();
    renderWithClient(<ConnectionPanel connectionId="c1" />);
    const field = await screen.findByLabelText("Токен");
    expect(screen.queryByRole("button", { name: "Войти через браузер" })).toBeNull();
    await user.type(field, "sha256~new");
    await user.click(screen.getByRole("button", { name: "Сохранить токен" }));
    await waitFor(() => expect(api.setConnectionToken).toHaveBeenCalledWith("c1", "sha256~new"));
    expect(api.connect).toHaveBeenLastCalledWith("c1", true);
    expect(await screen.findByText("dev1")).toBeInTheDocument();
  });

  it("вставка токена: пока токен сохраняется и идёт подключение, «Подключиться заново» недоступна", async () => {
    vi.mocked(api.listConnections).mockResolvedValue([profile({ method: "token" })]);
    vi.mocked(api.connect).mockRejectedValueOnce(EXPIRED);
    let saved: () => void = () => {};
    vi.mocked(api.setConnectionToken).mockReturnValue(new Promise<void>((r) => (saved = r)));
    let finish: (i: ConnectionInfo) => void = () => {};
    vi.mocked(api.connect).mockReturnValueOnce(new Promise((r) => (finish = r)));
    const user = userEvent.setup();
    renderWithClient(<ConnectionPanel connectionId="c1" />);
    await user.type(await screen.findByLabelText("Токен"), "sha256~new");
    await user.click(screen.getByRole("button", { name: "Сохранить токен" }));
    await waitFor(() => expect(api.setConnectionToken).toHaveBeenCalledWith("c1", "sha256~new"));
    expect(screen.getByRole("button", { name: "Подключиться заново" })).toBeDisabled();
    expect(api.connect).toHaveBeenCalledTimes(1);

    saved();
    await waitFor(() => expect(api.connect).toHaveBeenCalledTimes(2));
    expect(api.connect).toHaveBeenLastCalledWith("c1", true);
    expect(screen.getByRole("button", { name: "Подключиться заново" })).toBeDisabled();

    finish(INFO);
    expect(await screen.findByText("dev1")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Подключиться заново" })).toBeEnabled();
  });

  it("страница токена кластера открывается в браузере", async () => {
    vi.mocked(api.listConnections).mockResolvedValue([profile({ method: "browser" })]);
    vi.mocked(api.connect).mockRejectedValue(EXPIRED);
    vi.mocked(api.oauthTokenPage).mockResolvedValue("https://oauth-openshift.apps.dev/oauth/token/request");
    const user = userEvent.setup();
    renderWithClient(<ConnectionPanel connectionId="c1" />);
    await user.click(await screen.findByRole("button", { name: "Получить токен на странице кластера" }));
    await waitFor(() => expect(openUrl).toHaveBeenCalledWith("https://oauth-openshift.apps.dev/oauth/token/request"));
  });

  it("прочая ошибка — баннер с повтором; сертификат — кнопка изменения подключения", async () => {
    vi.mocked(api.listConnections).mockResolvedValue([profile({ method: "clientCert", certificatePem: "PEM" })]);
    vi.mocked(api.connect).mockRejectedValueOnce({ code: "CONNECTION_FAILED", key: "errors.k8s.connect", message: "x", retryable: true });
    const user = userEvent.setup();
    const { unmount } = renderWithClient(<ConnectionPanel connectionId="c1" />);
    expect(await screen.findByText("Не удаётся подключиться к серверу")).toBeInTheDocument();
    vi.mocked(api.connect).mockResolvedValue(INFO);
    await user.click(screen.getByRole("button", { name: "Повторить" }));
    expect(await screen.findByText("dev1")).toBeInTheDocument();
    unmount();

    vi.mocked(api.connect).mockRejectedValue({ code: "AUTH_FAILED", key: "errors.connections.clientKeyMissing", message: "x", retryable: false });
    renderWithClient(<ConnectionPanel connectionId="c1" />);
    // Две кнопки «Изменить подключение»: на панели инструментов и в блоке входа — нажимаем вторую, когда блок уже показан.
    const heading = await screen.findByRole("heading", { name: "Войдите снова" });
    expect(screen.getAllByRole("button", { name: "Изменить подключение" })).toHaveLength(2);
    await user.click(within(heading.nextElementSibling as HTMLElement).getByRole("button", { name: "Изменить подключение" }));
    expect(useWorkspace.getState().activeId).toBe("connection-form:c1");
  });
});
