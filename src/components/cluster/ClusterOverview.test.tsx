import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../test/render";

vi.mock("@tauri-apps/plugin-opener", () => ({ openUrl: vi.fn() }));

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return { ...orig, api: { getClusterOverview: vi.fn(), listClusterOperators: vi.fn() } };
});

import { openUrl } from "@tauri-apps/plugin-opener";
import { api } from "../../api/client";
import type { AppError, ClusterOverview, OperatorRow } from "../../api/types";
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

const OPENSHIFT: ClusterOverview = {
  platform: { kind: "openshift", distribution: null, version: "4.18", label: "OpenShift 4.18" },
  serverVersion: "v1.31.6",
  readyz: { ok: false, failed: ["etcd"] },
  nodes: { data: { total: 6, ready: 5, roles: [{ role: "master", total: 3, ready: 3 }, { role: "worker", total: 3, ready: 2 }] } },
  ingressClasses: { data: [{ name: "openshift-default", controller: "openshift.io/ingress-to-route", default: true }] },
  gatewayClasses: null,
  projects: { data: { count: 500, more: true } },
  platformDetails: {
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
    infrastructure: "AWS",
    consoleUrl: "https://console-openshift-console.apps.c.example.com",
    operators: { data: { total: 32, problems: [op("ingress", true, "Some ingresscontrollers are degraded")] } },
    machineConfigPools: {
      data: [{ name: "worker", degraded: false, updating: true, machines: 3, ready: 2, updated: 1, degradedMachines: 0 }],
    },
  },
};

beforeEach(() => vi.clearAllMocks());

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
    await user.click(screen.getByRole("button", { name: "https://console-openshift-console.apps.c.example.com" }));
    expect(openUrl).toHaveBeenCalledWith("https://console-openshift-console.apps.c.example.com");
    await user.click(screen.getByRole("button", { name: "Показать все" }));
    expect(await screen.findByText("dns")).toBeInTheDocument();
    expect(api.listClusterOperators).toHaveBeenCalledWith("c1");
    expect(screen.queryByText("Остальное скрыто правами.")).not.toBeInTheDocument();
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
