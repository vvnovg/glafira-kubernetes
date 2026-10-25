import { screen, waitFor } from "@testing-library/react";
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

beforeEach(() => {
  vi.clearAllMocks();
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
    const exec: ConnectionProfile["auth"] = {
      method: "exec",
      command: "/opt/homebrew/bin/kubelogin",
      args: ["get-token"],
      env: [],
      apiVersion: "client.authentication.k8s.io/v1",
      provideClusterInfo: false,
      approved: "fp",
    };
    vi.mocked(api.listConnections).mockResolvedValue([profile(exec)]);
    let fail: (e: unknown) => void = () => {};
    vi.mocked(api.connect).mockReturnValueOnce(new Promise((_, reject) => (fail = reject)));
    const user = userEvent.setup();
    renderWithClient(<ConnectionPanel connectionId="c1" />);
    expect(await screen.findByText(/Ожидание exec-плагина/)).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Отменить" }));
    expect(api.cancelOauthLogin).toHaveBeenCalledWith("c1");
    fail({ code: "AUTH_FAILED", key: "errors.exec.failed", params: { command: "kubelogin", code: "1" }, message: "x", retryable: false });
    vi.mocked(api.connect).mockResolvedValue(INFO);
    await user.click(await screen.findByRole("button", { name: "Запустить exec-плагин снова" }));
    expect(api.connect).toHaveBeenLastCalledWith("c1", true);
    expect(await screen.findByText("dev1")).toBeInTheDocument();
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
    await user.click(screen.getByRole("button", { name: "Отменить" }));
    expect(api.cancelOauthLogin).toHaveBeenCalledWith("c1");
    finish(INFO);
    expect(await screen.findByText("dev1")).toBeInTheDocument();
  });

  it("вставка токена сохраняет его и подключается заново", async () => {
    vi.mocked(api.listConnections).mockResolvedValue([profile({ method: "token" })]);
    vi.mocked(api.connect).mockRejectedValueOnce(EXPIRED).mockResolvedValue(INFO);
    vi.mocked(api.setConnectionToken).mockResolvedValue();
    const user = userEvent.setup();
    renderWithClient(<ConnectionPanel connectionId="c1" />);
    expect(screen.queryByRole("button", { name: "Войти через браузер" })).toBeNull();
    await user.type(await screen.findByLabelText("Токен"), "sha256~new");
    await user.click(screen.getByRole("button", { name: "Сохранить токен" }));
    await waitFor(() => expect(api.setConnectionToken).toHaveBeenCalledWith("c1", "sha256~new"));
    expect(api.connect).toHaveBeenLastCalledWith("c1", true);
    expect(await screen.findByText("dev1")).toBeInTheDocument();
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
    const buttons = await screen.findAllByRole("button", { name: "Изменить подключение" });
    await user.click(buttons[buttons.length - 1]);
    expect(useWorkspace.getState().activeId).toBe("connection-form:c1");
  });
});
