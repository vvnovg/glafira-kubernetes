import { screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { Proposal } from "../../api/types";
import { useWorkspace } from "../../state/workspace";
import { renderWithClient } from "../../test/render";

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return { ...orig, api: { listConnections: vi.fn().mockResolvedValue([]) } };
});

import { ProposalItem } from "./ProposalItem";

const applied = (p: Partial<Proposal> = {}): Proposal => ({
  id: "p1",
  severity: "warning",
  title: "Две реплики",
  rationale: "одна реплика — простой при выкладке",
  target: { kind: "workload", namespace: "team-a", name: "web", workloadKind: "Deployment" },
  change: { kind: "scale", namespace: "team-a", workload: { kind: "Deployment", name: "web" }, replicas: 2 },
  client: "claude-code",
  createdAtMs: 1,
  updatedAtMs: 1,
  status: "applied",
  closedAtMs: Date.now() - 60_000,
  appliedAsProposed: false,
  ...p,
});

function renderItem(proposal: Proposal) {
  renderWithClient(
    <ul>
      <ProposalItem connectionId="c1" proposal={proposal} onStatus={vi.fn()} onDelete={vi.fn()} />
    </ul>,
  );
}

beforeEach(() => {
  useWorkspace.setState({ tabs: [], activeId: null });
});

describe("ProposalItem", () => {
  it("у применённого с правками показывает, что применено фактически", () => {
    renderItem(applied({ appliedChange: { kind: "scale", namespace: "team-a", workload: { kind: "Deployment", name: "web" }, replicas: 3 } }));
    expect(screen.getByText("Реплик: 2")).toBeInTheDocument();
    const line = screen.getByText("Применено: Реплик: 3");
    expect(line).toHaveClass("proposal-change");
    expect(screen.getByText(/применено с правками/)).toBeInTheDocument();
  });

  it("без фактически применённого строки нет", () => {
    renderItem(applied());
    expect(screen.getByText("Реплик: 2")).toBeInTheDocument();
    expect(screen.queryByText(/^Применено:/)).not.toBeInTheDocument();
  });
});
