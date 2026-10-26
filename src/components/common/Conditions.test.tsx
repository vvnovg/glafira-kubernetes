import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import type { ConditionView } from "../../api/types";
import { Conditions } from "./Conditions";

const cond = (type: string, status: string): ConditionView => ({ type, status, reason: `${type}Reason` });

/** Ячейка статуса строки условия `type`. */
function statusCell(type: string): HTMLElement {
  const row = screen.getByText(`${type}Reason`).closest("tr") as HTMLElement;
  return row.querySelectorAll("td")[1];
}

describe("Conditions", () => {
  it("проблема — False у Ready, Available, Initialized и прочих; True — норма", () => {
    render(
      <Conditions
        conditions={[
          cond("Ready", "False"),
          cond("Available", "False"),
          cond("Initialized", "False"),
          cond("ContainersReady", "True"),
          cond("PodScheduled", "True"),
        ]}
      />,
    );
    for (const type of ["Ready", "Available", "Initialized"]) expect(statusCell(type)).toHaveClass("problem");
    for (const type of ["ContainersReady", "PodScheduled"]) {
      expect(statusCell(type)).not.toHaveClass("problem");
      expect(statusCell(type)).not.toHaveClass("warn");
      expect(statusCell(type)).not.toHaveClass("muted");
    }
  });

  it("проблема — True у *Pressure, NetworkUnavailable, ReplicaFailure и Failed", () => {
    const types = ["MemoryPressure", "DiskPressure", "PIDPressure", "NetworkUnavailable", "ReplicaFailure", "Failed"];
    render(<Conditions conditions={types.map((t) => cond(t, "True"))} />);
    for (const type of types) expect(statusCell(type)).toHaveClass("problem");
  });

  it("False у *Pressure и NetworkUnavailable — норма, без выделения", () => {
    render(<Conditions conditions={[cond("MemoryPressure", "False"), cond("NetworkUnavailable", "False"), cond("Ready", "True")]} />);
    for (const type of ["MemoryPressure", "NetworkUnavailable", "Ready"]) {
      expect(statusCell(type)).not.toHaveClass("problem");
      expect(statusCell(type)).not.toHaveClass("warn");
    }
  });

  it("Unknown — предупреждение для любого типа", () => {
    render(<Conditions conditions={[cond("Ready", "Unknown"), cond("DiskPressure", "Unknown")]} />);
    for (const type of ["Ready", "DiskPressure"]) {
      expect(statusCell(type)).toHaveClass("warn");
      expect(statusCell(type)).not.toHaveClass("problem");
    }
  });

  it("без условий — прочерк", () => {
    render(<Conditions conditions={[]} />);
    expect(screen.getByText("—")).toBeInTheDocument();
  });
});
