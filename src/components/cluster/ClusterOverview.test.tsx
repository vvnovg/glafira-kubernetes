import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../test/render";

vi.mock("@tauri-apps/plugin-opener", () => ({ openUrl: vi.fn() }));

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return { ...orig, api: { getClusterOverview: vi.fn(), listClusterOperators: vi.fn(), connect: vi.fn() } };
});

import { openUrl } from "@tauri-apps/plugin-opener";
import { api } from "../../api/client";
import type { AppError, ClusterOverview, ConnectionInfo, OpenShiftOverview, OperatorRow } from "../../api/types";
import { ClusterOverviewView } from "./ClusterOverview";

const forbidden = (resource: string): AppError => ({
  code: "FORBIDDEN",
  key: "errors.k8s.forbiddenCluster",
  params: { verb: "list", resource },
  message: "x",
  retryable: false,
});

const op = (name: string, degraded: boolean, message: string | null = null): OperatorRow => ({
  name,
  version: "4.18.3",
  available: true,
  progressing: false,
  degraded,
  message,
});

const CONSOLE = "https://console-openshift-console.apps.c.example.com";

/** Блок OpenShift обзора; тесты меняют его части через `withDetails`. */
const DETAILS: OpenShiftOverview = {
  kind: "openshift",
  clusterVersion: {
    data: {
      version: "4.18.3",
      channel: "stable-4.18",
      availableUpdates: 2,
      failing: { type: "Failing", status: "False" },
      progressing: { type: "Progressing", status: "True", message: "Working towards 4.18.4" },
    },
  },
  infrastructure: { data: "AWS" },
  consoleUrl: { data: CONSOLE },
  operators: { data: { total: 32, problems: [op("ingress", true, "Some ingresscontrollers are degraded")] } },
  machineConfigPools: {
    data: [{ name: "worker", degraded: false, updating: true, machines: 3, ready: 2, updated: 1, degradedMachines: 0 }],
  },
};

const OPENSHIFT: ClusterOverview = {
  platform: { kind: "openshift", distribution: null, version: "4.18", label: "OpenShift 4.18" },
  serverVersion: "v1.31.6",
  readyz: { ok: false, failed: ["etcd"] },
  nodes: { data: { total: 6, ready: 5, roles: [{ role: "master", total: 3, ready: 3 }, { role: "worker", total: 3, ready: 2 }] } },
  ingressClasses: { data: [{ name: "openshift-default", controller: "openshift.io/ingress-to-route", default: true }] },
  gatewayClasses: null,
  projects: { data: { count: 500, more: true } },
  platformDetails: DETAILS,
};

/** Обзор OpenShift с изменёнными частями блока OpenShift. */
const withDetails = (patch: Partial<OpenShiftOverview>): ClusterOverview => ({ ...OPENSHIFT, platformDetails: { ...DETAILS, ...patch } });

const INFO: ConnectionInfo = {
  user: "dev1",
  groups: [],
  serverVersion: "v1.31.6",
  platform: OPENSHIFT.platform,
  apiGroups: { routes: true, projects: true, deploymentConfigs: true, gatewayApi: false },
  capabilities: { listProjects: true, listNodes: true, listAllPods: false, readClusterOperators: true, readClusterVersion: true },
  tokenExpiresAt: null,
};

const SESSION_EXPIRED: AppError = { code: "SESSION_EXPIRED", key: "errors.k8s.unauthorized", message: "Unauthorized", retryable: false };

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(openUrl).mockResolvedValue();
});

describe("ClusterOverviewView", () => {
  it("OpenShift: API-сервер, ноды по ролям, обновления, консоль, операторы и пулы машин", async () => {
    vi.mocked(api.getClusterOverview).mockResolvedValue(OPENSHIFT);
    vi.mocked(api.listClusterOperators).mockResolvedValue([op("dns", false), op("ingress", true)]);
    const user = userEvent.setup();
    renderWithClient(<ClusterOverviewView connectionId="c1" />);
    expect(await screen.findByText("⚠ не готов")).toBeInTheDocument();
    expect(screen.getByText("etcd")).toBeInTheDocument();
    expect(screen.getByText("5 / 6")).toBeInTheDocument();
    expect(screen.getByText("500+")).toBeInTheDocument();
    expect(screen.getByText("По ролям: master 3/3, worker 2/3")).toBeInTheDocument();
    expect(screen.getByText("по умолчанию")).toBeInTheDocument();
    expect(screen.getByText("stable-4.18")).toBeInTheDocument();
    expect(screen.getByText("Working towards 4.18.4")).toBeInTheDocument();
    expect(screen.getByText("AWS")).toBeInTheDocument();
    expect(screen.getByText("Some ingresscontrollers are degraded")).toBeInTheDocument();
    expect(screen.getByText(/Не в норме: 1 из 32/)).toBeInTheDocument();
    expect(screen.getByText(/1\/3 машин обновлено · обновляется/)).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: CONSOLE }));
    expect(openUrl).toHaveBeenCalledWith(CONSOLE);
    await user.click(screen.getByRole("button", { name: "Показать все" }));
    expect(await screen.findByText("dns")).toBeInTheDocument();
    expect(api.listClusterOperators).toHaveBeenCalledWith("c1");
    expect(screen.queryByText("Остальное скрыто правами.")).not.toBeInTheDocument();
  });

  /** Значение строки «<подпись>» из списка фактов блока OpenShift. */
  const fact = (label: string) => screen.getByText(label).nextElementSibling as HTMLElement;

  it("OpenShift: инфраструктура со значением, консоль — ссылка", async () => {
    vi.mocked(api.getClusterOverview).mockResolvedValue(withDetails({ infrastructure: { data: "VSphere" } }));
    renderWithClient(<ClusterOverviewView connectionId="c1" />);
    await screen.findByText("Инфраструктура");
    expect(fact("Инфраструктура")).toHaveTextContent("VSphere");
    expect(within(fact("Веб-консоль")).getByRole("button")).toHaveTextContent(CONSOLE);
  });

  it("OpenShift: объект есть, значения нет ({data: null}) — «—», не ошибка", async () => {
    vi.mocked(api.getClusterOverview).mockResolvedValue(withDetails({ infrastructure: { data: null }, consoleUrl: { data: null } }));
    renderWithClient(<ClusterOverviewView connectionId="c1" />);
    await screen.findByText("Инфраструктура");
    expect(fact("Инфраструктура")).toHaveTextContent(/^—$/);
    expect(fact("Веб-консоль")).toHaveTextContent(/^—$/);
    expect(within(fact("Веб-консоль")).queryByRole("button")).not.toBeInTheDocument();
    expect(screen.queryByText("Остальное скрыто правами.")).not.toBeInTheDocument();
  });

  it("OpenShift без пулов машин (ROSA HCP) — «—», не ошибка", async () => {
    vi.mocked(api.getClusterOverview).mockResolvedValue(withDetails({ machineConfigPools: { data: [] } }));
    renderWithClient(<ClusterOverviewView connectionId="c1" />);
    const heading = await screen.findByRole("heading", { name: "Пулы машин (MachineConfigPool)" });
    expect(heading.nextElementSibling).toHaveTextContent(/^—$/);
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
  });

  it("OpenShift: консоль не прочитана из-за прав — причина вместо ссылки, остальной обзор виден", async () => {
    vi.mocked(api.getClusterOverview).mockResolvedValue(withDetails({ consoleUrl: { error: forbidden("consoles") } }));
    renderWithClient(<ClusterOverviewView connectionId="c1" />);
    await screen.findByText("Инфраструктура");
    expect(fact("Веб-консоль")).toHaveTextContent("— Нет прав: list consoles на уровне кластера");
    expect(within(fact("Веб-консоль")).queryByRole("button")).not.toBeInTheDocument();
    expect(fact("Инфраструктура")).toHaveTextContent("AWS");
    expect(screen.getByText("Остальное скрыто правами.")).toBeInTheDocument();
  });

  it("OpenShift: ошибка чтения инфраструктуры не права — баннер, консоль на месте", async () => {
    vi.mocked(api.getClusterOverview).mockResolvedValue(
      withDetails({ infrastructure: { error: { code: "INTERNAL", message: "Infrastructure is broken", retryable: false } } }),
    );
    renderWithClient(<ClusterOverviewView connectionId="c1" />);
    await screen.findByText("Инфраструктура");
    expect(fact("Инфраструктура")).toHaveTextContent("Infrastructure is broken");
    expect(within(fact("Веб-консоль")).getByRole("button")).toBeInTheDocument();
    expect(screen.queryByText("Остальное скрыто правами.")).not.toBeInTheDocument();
  });

  describe("адрес консоли", () => {
    it.each(["javascript:alert(1)", "mailto:x@y", "tel:+10000000000", "file:///etc/passwd"])("%s — не http(s), текст без кнопки, система не вызывается", async (raw) => {
      vi.mocked(api.getClusterOverview).mockResolvedValue(withDetails({ consoleUrl: { data: raw } }));
      renderWithClient(<ClusterOverviewView connectionId="c1" />);
      await screen.findByText("Веб-консоль");
      expect(fact("Веб-консоль")).toHaveTextContent(raw);
      expect(within(fact("Веб-консоль")).queryByRole("button")).not.toBeInTheDocument();
      expect(within(fact("Веб-консоль")).queryByRole("link")).not.toBeInTheDocument();
      expect(openUrl).not.toHaveBeenCalled();
    });

    it("адрес, который не разобрать, — «—»", async () => {
      vi.mocked(api.getClusterOverview).mockResolvedValue(withDetails({ consoleUrl: { data: "не адрес" } }));
      renderWithClient(<ClusterOverviewView connectionId="c1" />);
      await screen.findByText("Веб-консоль");
      expect(fact("Веб-консоль")).toHaveTextContent(/^—$/);
    });

    it("http и https открываются; отказ системы открыть адрес не остаётся необработанным", async () => {
      vi.mocked(api.getClusterOverview).mockResolvedValue(withDetails({ consoleUrl: { data: "http://console.local:8443/" } }));
      vi.mocked(openUrl).mockRejectedValue(new Error("not allowed"));
      const user = userEvent.setup();
      renderWithClient(<ClusterOverviewView connectionId="c1" />);
      await user.click(await screen.findByRole("button", { name: "http://console.local:8443/" }));
      expect(openUrl).toHaveBeenCalledWith("http://console.local:8443/");
      // Кнопка по-прежнему на месте и работает.
      await user.click(screen.getByRole("button", { name: "http://console.local:8443/" }));
      expect(openUrl).toHaveBeenCalledTimes(2);
    });
  });

  describe("сессия истекла", () => {
    it("ошибка обзора — баннер и «Войти снова» подключает заново, а после входа обзор перечитывается", async () => {
      vi.mocked(api.getClusterOverview).mockRejectedValueOnce(SESSION_EXPIRED).mockResolvedValue(OPENSHIFT);
      vi.mocked(api.connect).mockResolvedValue(INFO);
      const user = userEvent.setup();
      renderWithClient(<ClusterOverviewView connectionId="c1" />);
      expect(await screen.findByText("Кластер отклонил учётные данные: токен истёк или неверен")).toBeInTheDocument();
      await user.click(screen.getByRole("button", { name: "Войти снова" }));
      await waitFor(() => expect(api.connect).toHaveBeenCalledWith("c1", true));
      expect(await screen.findByText("⚠ не готов")).toBeInTheDocument();
      expect(api.getClusterOverview).toHaveBeenCalledTimes(2);
    });

    it("кнопка недоступна, пока идёт подключение", async () => {
      vi.mocked(api.getClusterOverview).mockRejectedValue(SESSION_EXPIRED);
      let finish: (i: ConnectionInfo) => void = () => undefined;
      vi.mocked(api.connect).mockReturnValue(new Promise((r) => (finish = r)));
      const user = userEvent.setup();
      renderWithClient(<ClusterOverviewView connectionId="c1" />);
      await user.click(await screen.findByRole("button", { name: "Войти снова" }));
      await waitFor(() => expect(screen.getByRole("button", { name: "Войти снова" })).toBeDisabled());
      expect(api.connect).toHaveBeenCalledTimes(1);
      finish(INFO);
      await waitFor(() => expect(screen.getByRole("button", { name: "Войти снова" })).toBeEnabled());
    });

    it("прочая ошибка обзора — баннер с «Повторить», без «Войти снова»", async () => {
      vi.mocked(api.getClusterOverview).mockRejectedValueOnce({ code: "CONNECTION_FAILED", message: "No route to host", retryable: true }).mockResolvedValue(OPENSHIFT);
      const user = userEvent.setup();
      renderWithClient(<ClusterOverviewView connectionId="c1" />);
      expect(await screen.findByText("No route to host")).toBeInTheDocument();
      expect(screen.queryByRole("button", { name: "Войти снова" })).not.toBeInTheDocument();
      await user.click(screen.getByRole("button", { name: "Повторить" }));
      expect(await screen.findByText("⚠ не готов")).toBeInTheDocument();
    });
  });

  describe("/readyz без ответа", () => {
    it("ошибка не права: «—» вместо «не готов», причина баннером", async () => {
      vi.mocked(api.getClusterOverview).mockResolvedValue({
        ...OPENSHIFT,
        readyz: { ok: false, failed: [], error: { code: "CONNECTION_FAILED", message: "readyz timed out", retryable: true } },
      });
      renderWithClient(<ClusterOverviewView connectionId="c1" />);
      expect(await screen.findByText("readyz timed out")).toBeInTheDocument();
      expect(screen.queryByText(/не готов/)).not.toBeInTheDocument();
      expect(screen.queryByText(/✅ готов/)).not.toBeInTheDocument();
      expect(screen.getByText("API-сервер").closest("div")?.querySelector("dd")).toHaveTextContent(/^—$/);
      expect(screen.queryByText("Остальное скрыто правами.")).not.toBeInTheDocument();
    });

    it("нет прав на /readyz: «—» с причиной и пояснение о правах", async () => {
      vi.mocked(api.getClusterOverview).mockResolvedValue({ ...OPENSHIFT, readyz: { ok: false, failed: [], error: forbidden("readyz") } });
      renderWithClient(<ClusterOverviewView connectionId="c1" />);
      expect(await screen.findByText("— Нет прав: list readyz на уровне кластера")).toBeInTheDocument();
      expect(screen.queryByText(/не готов/)).not.toBeInTheDocument();
      expect(screen.getByText("Остальное скрыто правами.")).toBeInTheDocument();
    });
  });

  it("«Показать все»: пока операторы читаются — «Загрузка…», затем список", async () => {
    vi.mocked(api.getClusterOverview).mockResolvedValue(OPENSHIFT);
    let finish: (rows: OperatorRow[]) => void = () => undefined;
    vi.mocked(api.listClusterOperators).mockReturnValueOnce(new Promise((r) => (finish = r)));
    const user = userEvent.setup();
    renderWithClient(<ClusterOverviewView connectionId="c1" />);
    await user.click(await screen.findByRole("button", { name: "Показать все" }));
    expect(await screen.findByText("Загрузка…")).toBeInTheDocument();
    finish([op("dns", false)]);
    expect(await screen.findByText("dns")).toBeInTheDocument();
    expect(screen.queryByText("Загрузка…")).not.toBeInTheDocument();
  });

  it("«Показать все»: ошибка чтения операторов — баннер с «Повторить»", async () => {
    vi.mocked(api.getClusterOverview).mockResolvedValue(OPENSHIFT);
    vi.mocked(api.listClusterOperators).mockRejectedValueOnce({ code: "CONNECTION_FAILED", message: "operators timed out", retryable: true }).mockResolvedValue([op("dns", false)]);
    const user = userEvent.setup();
    renderWithClient(<ClusterOverviewView connectionId="c1" />);
    await user.click(await screen.findByRole("button", { name: "Показать все" }));
    expect(await screen.findByText("operators timed out")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Повторить" }));
    expect(await screen.findByText("dns")).toBeInTheDocument();
  });

  it("без прав — «—» с причиной и пояснение; Gateway API — классы Gateway", async () => {
    vi.mocked(api.getClusterOverview).mockResolvedValue({
      ...OPENSHIFT,
      platform: { kind: "kubernetes", distribution: null, version: "1.32", label: "Kubernetes 1.32" },
      readyz: { ok: true, failed: [] },
      nodes: { error: forbidden("nodes") },
      projects: { error: forbidden("namespaces") },
      gatewayClasses: { data: [{ name: "cilium", controller: "io.cilium/gateway-controller", accepted: true }] },
      platformDetails: null,
    });
    renderWithClient(<ClusterOverviewView connectionId="c1" />);
    expect(await screen.findByText("✅ готов")).toBeInTheDocument();
    expect(screen.getByText("— Нет прав: list nodes на уровне кластера")).toBeInTheDocument();
    expect(screen.getByText("cilium")).toBeInTheDocument();
    expect(screen.getByText("Остальное скрыто правами.")).toBeInTheDocument();
    expect(screen.queryByText("OpenShift")).not.toBeInTheDocument();
  });
});
