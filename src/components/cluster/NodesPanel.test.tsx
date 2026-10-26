import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../test/render";

vi.mock("@tanstack/react-virtual", () => ({
  useVirtualizer: (o: { count: number }) => ({
    getVirtualItems: () => Array.from({ length: o.count }, (_, index) => ({ index, start: index * 28 })),
    getTotalSize: () => o.count * 28,
  }),
}));

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return { ...orig, api: { listNodes: vi.fn(), getNode: vi.fn() } };
});

import { api } from "../../api/client";
import type { NodeRow } from "../../api/types";
import { NodesPanel } from "./NodesPanel";

const node = (name: string, ready: boolean, problems: string[] = []): NodeRow => ({
  name,
  roles: ["worker"],
  ready,
  problems,
  unschedulable: false,
  kubeletVersion: "v1.32.5",
  internalIp: "10.0.0.1",
  cpu: "3500m",
  memory: "15Gi",
  pods: "250",
  created: 1,
  taints: 0,
});

beforeEach(() => vi.clearAllMocks());

describe("NodesPanel", () => {
  it("список нод и сведения о выбранной", async () => {
    vi.mocked(api.listNodes).mockResolvedValue({ items: [node("w1", true), node("w2", false, ["NotReady", "DiskPressure"])], truncated: false });
    vi.mocked(api.getNode).mockResolvedValue({
      node: node("w2", false),
      conditions: [{ type: "DiskPressure", status: "True", reason: "KubeletHasDiskPressure" }],
      taints: ["node.kubernetes.io/disk-pressure:NoSchedule"],
      labels: { "kubernetes.io/os": "linux" },
      capacity: { cpu: "4", memory: "16Gi" },
      allocatable: { cpu: "3500m", memory: "15Gi" },
      addresses: ["InternalIP: 10.0.0.2"],
      osImage: "RHCOS",
      kernelVersion: "5.14",
      containerRuntime: "cri-o://1.32",
      architecture: "amd64",
    });
    const user = userEvent.setup();
    renderWithClient(<NodesPanel connectionId="c1" />);
    expect(await screen.findByText("w1")).toBeInTheDocument();
    expect(screen.getByText("NotReady, DiskPressure")).toBeInTheDocument();
    await user.click(screen.getByText("w2"));
    expect(await screen.findByText("KubeletHasDiskPressure")).toBeInTheDocument();
    expect(api.getNode).toHaveBeenCalledWith("c1", "w2");
    expect(screen.getByText("node.kubernetes.io/disk-pressure:NoSchedule")).toBeInTheDocument();
    expect(screen.getByText("cri-o://1.32")).toBeInTheDocument();
    expect(screen.getByText("16Gi")).toBeInTheDocument();
  });
});
