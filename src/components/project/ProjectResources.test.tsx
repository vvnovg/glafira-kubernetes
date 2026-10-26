import { act, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../test/render";

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return { ...orig, api: { getNetwork: vi.fn(), getConfig: vi.fn(), getConfigMapValues: vi.fn(), listPvcs: vi.fn(), getQuotas: vi.fn() } };
});

import { api } from "../../api/client";
import type { AppError, ConfigView as ConfigData, NetworkView as Network, RouteRow } from "../../api/types";
import { ConfigView } from "./ConfigView";
import { NetworkView } from "./NetworkView";
import { QuotasView } from "./QuotasView";
import { StorageView } from "./StorageView";

const forbidden = (resource: string, verb = "list"): AppError => ({
  code: "FORBIDDEN",
  key: "errors.k8s.forbiddenIn",
  params: { verb, resource, namespace: "team-a" },
  message: "x",
  retryable: false,
});

const DAY = 86_400;
const soon = () => Math.floor(Date.now() / 1000) + DAY;

const ROUTE: RouteRow = {
  name: "web",
  host: "web.apps.example.com",
  path: "/",
  service: "web",
  port: "8080",
  tls: "edge",
  insecurePolicy: "Redirect",
  admitted: true,
  admittedMessage: null,
  certExpires: null,
  ingress: null,
  created: 1,
};

const NETWORK: Network = {
  services: { data: [{ name: "web", type: "ClusterIP", clusterIp: "10.0.0.10", external: [], ports: ["80/TCP → 8080"], selector: "app=web", endpoints: { ready: 0, total: 2 }, created: 1 }] },
  endpointsError: null,
  routes: { data: [{ ...ROUTE, admitted: false, admittedMessage: "host already claimed", certExpires: soon() }] },
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

const withRoutes = (routes: RouteRow[]): Network => ({ ...NETWORK, routes: { data: routes } });
/** Ячейка «Сертификат до» в строке Route с этим именем. */
const certCell = (route: string) => within(screen.getByText(route).closest("tr") as HTMLElement).getAllByRole("cell")[5];

beforeEach(() => vi.clearAllMocks());

describe("подвкладки проекта", () => {
  it("сеть: endpoints, Route с отказом, Ingress с созданными Routes, Gateway API", async () => {
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

  it("сеть: заголовки и значки Gateway переведены, у значка есть текст", async () => {
    vi.mocked(api.getNetwork).mockResolvedValue(NETWORK);
    renderWithClient(<NetworkView connectionId="c1" namespace="team-a" />);
    await screen.findByText("https HTTPS:443");
    expect(screen.getByRole("columnheader", { name: "принят" })).toBeInTheDocument();
    expect(screen.getByRole("columnheader", { name: "настроен" })).toBeInTheDocument();
    expect(screen.queryByRole("columnheader", { name: "Accepted" })).not.toBeInTheDocument();
    expect(screen.queryByRole("columnheader", { name: "Programmed" })).not.toBeInTheDocument();
    // accepted: true, programmed: false.
    const [accepted, programmed] = within(screen.getByText("gw").closest("tr") as HTMLElement)
      .getAllByRole("cell")
      .slice(4);
    expect(within(accepted).getByRole("img", { name: "в порядке" })).toHaveTextContent("✅");
    expect(within(programmed).getByRole("img", { name: "проблема" })).toHaveTextContent("⚠");
    // Условия родителей HTTPRoute: принят — в порядке, ссылки не разрешены — проблема.
    expect(within(screen.getByText("True")).getByRole("img", { name: "в порядке" })).toBeInTheDocument();
    expect(within(screen.getByText(/BackendNotFound/)).getByRole("img", { name: "проблема" })).toBeInTheDocument();
  });

  it("сеть: срок сертификата Route — меньше 30 дней предупреждение, 60 дней без выделения, истёк — ошибка со словом", async () => {
    const t0 = Math.floor(Date.now() / 1000);
    vi.mocked(api.getNetwork).mockResolvedValue(
      withRoutes([
        { ...ROUTE, name: "soon-cert", certExpires: t0 + 10 * DAY },
        { ...ROUTE, name: "far-cert", certExpires: t0 + 60 * DAY },
        { ...ROUTE, name: "old-cert", certExpires: t0 - DAY },
        { ...ROUTE, name: "none-cert", certExpires: null },
      ]),
    );
    renderWithClient(<NetworkView connectionId="c1" namespace="team-a" />);
    await screen.findByText("soon-cert");
    expect(certCell("soon-cert")).toHaveClass("warn");
    expect(certCell("soon-cert")).not.toHaveClass("problem");
    expect(certCell("soon-cert")).not.toHaveTextContent("истёк");
    expect(certCell("far-cert")).not.toHaveClass("warn");
    expect(certCell("far-cert")).not.toHaveClass("problem");
    expect(certCell("far-cert")).not.toHaveTextContent("истёк");
    // Истёкший строже скорого: другой класс и слово, а не только цвет.
    expect(certCell("old-cert")).toHaveClass("problem");
    expect(certCell("old-cert")).not.toHaveClass("warn");
    expect(certCell("old-cert")).toHaveTextContent("истёк");
    expect(certCell("none-cert")).toHaveTextContent("—");
    expect(certCell("none-cert")).not.toHaveClass("warn");
    expect(certCell("none-cert")).not.toHaveClass("problem");
  });

  describe("конфигурация", () => {
    const WEB = { name: "web", keys: ["LOG_LEVEL"], binaryKeys: ["logo.png"], created: 1 };
    const CONFIG: ConfigData = { configMaps: { data: [WEB] }, secrets: { data: [] } };

    it("значения ConfigMap читаются по щелчку один раз, Secrets без значений и без прав", async () => {
      vi.mocked(api.getConfig).mockResolvedValue({ ...CONFIG, secrets: { error: forbidden("secrets") } });
      vi.mocked(api.getConfigMapValues).mockResolvedValue({ LOG_LEVEL: { text: "debug", binarySize: null }, "logo.png": { text: null, binarySize: 4 } });
      const user = userEvent.setup();
      renderWithClient(<ConfigView connectionId="c1" namespace="team-a" />);
      expect(await screen.findByText("LOG_LEVEL, logo.png")).toBeInTheDocument();
      expect(api.getConfigMapValues).not.toHaveBeenCalled();
      const show = screen.getByRole("button", { name: "Показать значения" });
      expect(show).toHaveAttribute("aria-expanded", "false");
      await user.click(show);
      expect(await screen.findByText("debug")).toBeInTheDocument();
      expect(screen.getByText("двоичные данные, 4 байта")).toBeInTheDocument();
      expect(screen.getByRole("button", { name: "Скрыть значения" })).toHaveAttribute("aria-expanded", "true");
      expect(api.getConfigMapValues).toHaveBeenCalledTimes(1);
      expect(api.getConfigMapValues).toHaveBeenCalledWith("c1", "team-a", "web");
      expect(screen.getByText("— Нет прав: list secrets в проекте «team-a»")).toBeInTheDocument();
    });

    it("размер двоичного значения склоняется: 1 байт, 2 байта, 5 байт, 21 байт", async () => {
      vi.mocked(api.getConfig).mockResolvedValue({ ...CONFIG, configMaps: { data: [{ ...WEB, keys: [], binaryKeys: ["a", "b", "c", "d"] }] } });
      vi.mocked(api.getConfigMapValues).mockResolvedValue({
        a: { text: null, binarySize: 1 },
        b: { text: null, binarySize: 2 },
        c: { text: null, binarySize: 5 },
        d: { text: null, binarySize: 21 },
      });
      const user = userEvent.setup();
      renderWithClient(<ConfigView connectionId="c1" namespace="team-a" />);
      await user.click(await screen.findByRole("button", { name: "Показать значения" }));
      for (const text of ["1 байт", "2 байта", "5 байт", "21 байт"]) expect(await screen.findByText(`двоичные данные, ${text}`)).toBeInTheDocument();
    });

    it("значения скрываются повторным щелчком и показываются снова", async () => {
      vi.mocked(api.getConfig).mockResolvedValue(CONFIG);
      vi.mocked(api.getConfigMapValues).mockResolvedValue({ LOG_LEVEL: { text: "debug", binarySize: null } });
      const user = userEvent.setup();
      renderWithClient(<ConfigView connectionId="c1" namespace="team-a" />);
      await user.click(await screen.findByRole("button", { name: "Показать значения" }));
      expect(await screen.findByText("debug")).toBeInTheDocument();
      await user.click(screen.getByRole("button", { name: "Скрыть значения" }));
      expect(screen.queryByText("debug")).not.toBeInTheDocument();
      expect(screen.getByRole("button", { name: "Показать значения" })).toHaveAttribute("aria-expanded", "false");
      expect(api.getConfigMapValues).toHaveBeenCalledTimes(1);
      await user.click(screen.getByRole("button", { name: "Показать значения" }));
      expect(await screen.findByText("debug")).toBeInTheDocument();
      expect(screen.getByRole("button", { name: "Скрыть значения" })).toHaveAttribute("aria-expanded", "true");
    });

    it("нет прав на чтение ConfigMap — причина вместо значений, без «Повторить» и без повторного запроса", async () => {
      vi.mocked(api.getConfig).mockResolvedValue(CONFIG);
      vi.mocked(api.getConfigMapValues).mockRejectedValue(forbidden("configmaps", "get"));
      const user = userEvent.setup();
      renderWithClient(<ConfigView connectionId="c1" namespace="team-a" />);
      await user.click(await screen.findByRole("button", { name: "Показать значения" }));
      expect(await screen.findByText("Нет прав: get configmaps в проекте «team-a»")).toBeInTheDocument();
      expect(screen.queryByRole("button", { name: "Повторить" })).not.toBeInTheDocument();
      expect(api.getConfigMapValues).toHaveBeenCalledTimes(1);
    });
  });

  it("хранилище и квоты", async () => {
    vi.mocked(api.listPvcs).mockResolvedValue({
      items: [
        { name: "data", phase: "Bound", class: "standard", capacity: "10Gi", requested: "10Gi", accessModes: ["ReadWriteOnce"], volumeMode: "Filesystem", volume: "pvc-1", mountedBy: ["db-0"], created: 1 },
        { name: "cache", phase: "Pending", class: null, capacity: null, requested: "1Gi", accessModes: [], volumeMode: null, volume: null, mountedBy: null, created: 1 },
        { name: "old", phase: "Lost", class: "standard", capacity: "5Gi", requested: "5Gi", accessModes: [], volumeMode: null, volume: "pvc-2", mountedBy: [], created: 1 },
        { name: "fresh", phase: null, class: null, capacity: null, requested: null, accessModes: [], volumeMode: null, volume: null, mountedBy: [], created: 1 },
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
    // Ошибка — только Lost; Pending у WaitForFirstConsumer — норма, поэтому лишь предупреждение; пустая фаза не красная.
    expect(screen.getByText("Lost")).toHaveClass("problem");
    expect(screen.getByText("Pending")).toHaveClass("warn");
    expect(screen.getByText("Pending")).not.toHaveClass("problem");
    expect(screen.getByText("Bound")).not.toHaveClass("problem");
    expect(screen.getByText("Bound")).not.toHaveClass("warn");
    const unknownPhase = within(screen.getByText("fresh").closest("tr") as HTMLElement).getAllByRole("cell")[1];
    expect(unknownPhase).toHaveTextContent("—");
    expect(unknownPhase).not.toHaveClass("problem");
    expect(unknownPhase).not.toHaveClass("warn");
    expect(screen.getByText("запрошено 1Gi")).toBeInTheDocument();
    expect(screen.getByText("неизвестно")).toBeInTheDocument();
    await waitFor(() => expect(screen.getByText("3600m").closest("tr")).toHaveClass("warn"));
    expect(screen.getByText("500m")).toBeInTheDocument();
  });
});

describe("срок сертификата Route идёт со временем", () => {
  const T0 = new Date("2026-10-26T10:00:00Z");
  const t0 = T0.getTime() / 1000;

  beforeEach(() => {
    // Подменяются только часы и setInterval (по ним считает `useNow`): setTimeout остаётся настоящим, на нём
    // держатся ожидание запросов и тестовые ожидания (как в ConnectionPanel.test).
    vi.useFakeTimers({ toFake: ["Date", "setInterval", "clearInterval"] });
    vi.setSystemTime(T0);
  });
  afterEach(() => vi.useRealTimers());

  const advance = (ms: number) =>
    act(() => {
      vi.advanceTimersByTime(ms);
    });
  // Часы переведены вперёд, и один тик `useNow` (раз в минуту) подхватывает новое время.
  const tick = (aheadSec: number) => {
    vi.setSystemTime(new Date((t0 + aheadSec) * 1000));
    advance(60_000);
  };

  it("выделение появляется, когда до срока остаётся меньше 30 дней, и сменяется «истёк»", async () => {
    // До порога 30 дней — ещё 2 часа; до истечения — 30 дней и 2 часа.
    vi.mocked(api.getNetwork).mockResolvedValue(withRoutes([{ ...ROUTE, name: "aging-cert", certExpires: t0 + 30 * DAY + 2 * 3_600 }]));
    renderWithClient(<NetworkView connectionId="c1" namespace="team-a" />);
    await screen.findByText("aging-cert");
    expect(certCell("aging-cert")).not.toHaveClass("warn");
    expect(certCell("aging-cert")).not.toHaveClass("problem");

    // Минута не меняет вывод: до порога ещё почти два часа.
    tick(60);
    expect(certCell("aging-cert")).not.toHaveClass("warn");

    // Прошло 3 часа: меньше 30 дней — предупреждение (без перезапроса и перерисовки данными).
    tick(3 * 3_600);
    expect(certCell("aging-cert")).toHaveClass("warn");
    expect(certCell("aging-cert")).not.toHaveClass("problem");
    expect(certCell("aging-cert")).not.toHaveTextContent("истёк");

    // Прошло 31 день: сертификат истёк.
    tick(31 * DAY);
    expect(certCell("aging-cert")).toHaveClass("problem");
    expect(certCell("aging-cert")).not.toHaveClass("warn");
    expect(certCell("aging-cert")).toHaveTextContent("истёк");
    expect(api.getNetwork).toHaveBeenCalledTimes(1);
  });
});
