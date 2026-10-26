import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../test/render";

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return { ...orig, api: { getNetwork: vi.fn(), getConfig: vi.fn(), getConfigMapValues: vi.fn(), listPvcs: vi.fn(), getQuotas: vi.fn() } };
});

import { api } from "../../api/client";
import type { AppError, NetworkView as Network } from "../../api/types";
import { ConfigView } from "./ConfigView";
import { NetworkView } from "./NetworkView";
import { QuotasView } from "./QuotasView";
import { StorageView } from "./StorageView";

const forbidden = (resource: string): AppError => ({
  code: "FORBIDDEN",
  key: "errors.k8s.forbiddenIn",
  params: { verb: "list", resource, namespace: "team-a" },
  message: "x",
  retryable: false,
});

const soon = () => Math.floor(Date.now() / 1000) + 86_400;

const NETWORK: Network = {
  services: { data: [{ name: "web", type: "ClusterIP", clusterIp: "10.0.0.10", external: [], ports: ["80/TCP → 8080"], selector: "app=web", endpoints: { ready: 0, total: 2 }, created: 1 }] },
  endpointsError: null,
  routes: {
    data: [
      { name: "web", host: "web.apps.example.com", path: "/", service: "web", port: "8080", tls: "edge", insecurePolicy: "Redirect", admitted: false, admittedMessage: "host already claimed", certExpires: soon(), ingress: null, created: 1 },
    ],
  },
  ingresses: {
    data: [
      {
        name: "web",
        class: "nginx",
        rules: [{ host: "web.example.com", path: "/api", service: "api", port: "http" }],
        defaultBackend: null,
        tls: [{ hosts: ["web.example.com"], secret: "web-tls" }],
        addresses: ["203.0.113.5"],
        routes: ["web-abcde"],
        created: 1,
      },
    ],
  },
  gateways: { data: [{ name: "gw", class: "cilium", listeners: [{ name: "https", port: 443, protocol: "HTTPS", hostname: null }], addresses: ["198.51.100.7"], accepted: true, programmed: false, created: 1 }] },
  httpRoutes: {
    data: [
      {
        kind: "HTTPRoute",
        name: "web",
        hostnames: ["web.example.com"],
        parents: [{ name: "gw", namespace: null, section: "https", accepted: { type: "Accepted", status: "True" }, resolvedRefs: { type: "ResolvedRefs", status: "False", reason: "BackendNotFound" } }],
        backends: ["web:80"],
        created: 1,
      },
    ],
  },
  grpcRoutes: null,
};

beforeEach(() => vi.clearAllMocks());

describe("подвкладки проекта", () => {
  it("сеть: endpoints, Route с отказом и скорым сроком, Ingress с созданными Routes, Gateway API", async () => {
    vi.mocked(api.getNetwork).mockResolvedValue(NETWORK);
    renderWithClient(<NetworkView connectionId="c1" namespace="team-a" />);
    expect(await screen.findByText("0/2")).toHaveClass("problem");
    expect(screen.getByText(/host already claimed/)).toBeInTheDocument();
    expect(screen.getByText("Routes из этого Ingress: web-abcde")).toBeInTheDocument();
    expect(screen.getByText("web-tls")).toBeInTheDocument();
    expect(screen.getByText("https HTTPS:443")).toBeInTheDocument();
    expect(screen.getByText(/BackendNotFound/)).toBeInTheDocument();
    expect(screen.queryByText("GRPCRoute")).not.toBeInTheDocument();
  });

  it("конфигурация: значения ConfigMap по щелчку, Secrets без значений и без прав", async () => {
    vi.mocked(api.getConfig).mockResolvedValue({
      configMaps: { data: [{ name: "web", keys: ["LOG_LEVEL"], binaryKeys: ["logo.png"], created: 1 }] },
      secrets: { error: forbidden("secrets") },
    });
    vi.mocked(api.getConfigMapValues).mockResolvedValue({ LOG_LEVEL: { text: "debug", binarySize: null }, "logo.png": { text: null, binarySize: 4 } });
    const user = userEvent.setup();
    renderWithClient(<ConfigView connectionId="c1" namespace="team-a" />);
    expect(await screen.findByText("LOG_LEVEL, logo.png")).toBeInTheDocument();
    expect(api.getConfigMapValues).not.toHaveBeenCalled();
    await user.click(screen.getByRole("button", { name: "Показать значения" }));
    expect(await screen.findByText("debug")).toBeInTheDocument();
    expect(screen.getByText("двоичные данные, 4 байт")).toBeInTheDocument();
    expect(screen.getByText("— Нет прав: list secrets в проекте «team-a»")).toBeInTheDocument();
  });

  it("хранилище и квоты", async () => {
    vi.mocked(api.listPvcs).mockResolvedValue({
      items: [
        { name: "data", phase: "Bound", class: "standard", capacity: "10Gi", requested: "10Gi", accessModes: ["ReadWriteOnce"], volumeMode: "Filesystem", volume: "pvc-1", mountedBy: ["db-0"], created: 1 },
        { name: "cache", phase: "Pending", class: null, capacity: null, requested: "1Gi", accessModes: [], volumeMode: null, volume: null, mountedBy: null, created: 1 },
      ],
      truncated: false,
    });
    vi.mocked(api.getQuotas).mockResolvedValue({
      quotas: { data: [{ name: "compute", scopes: [], entries: [{ resource: "requests.cpu", used: "3600m", hard: "4", ratio: 0.9 }] }] },
      limitRanges: { data: [{ name: "defaults", limits: [{ type: "Container", resource: "cpu", min: null, max: null, default: "500m", defaultRequest: "100m", maxLimitRequestRatio: null }] }] },
    });
    renderWithClient(
      <>
        <StorageView connectionId="c1" namespace="team-a" />
        <QuotasView connectionId="c1" namespace="team-a" />
      </>,
    );
    expect(await screen.findByText("db-0")).toBeInTheDocument();
    expect(screen.getByText("Pending")).toHaveClass("problem");
    expect(screen.getByText("запрошено 1Gi")).toBeInTheDocument();
    expect(screen.getByText("неизвестно")).toBeInTheDocument();
    await waitFor(() => expect(screen.getByText("3600m").closest("tr")).toHaveClass("warn"));
    expect(screen.getByText("500m")).toBeInTheDocument();
  });
});
