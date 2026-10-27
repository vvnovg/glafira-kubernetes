import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { describe, expect, it } from "vitest";
import { ProjectListInput } from "./ProjectListInput";

const KNOWN = ["default", "kube-system", "team-a", "team-b"];

function Field({ initial = "" }: { initial?: string }) {
  const [value, setValue] = useState(initial);
  return <ProjectListInput id="f" value={value} onChange={setValue} known={KNOWN} placeholder="team-a, team-b" />;
}

const options = () => screen.queryAllByRole("option").map((o) => o.textContent);

describe("ProjectListInput", () => {
  it("suggests the projects matching what is being typed and completes it", async () => {
    render(<Field />);
    const input = screen.getByRole("combobox");
    await userEvent.type(input, "tea");
    expect(options()).toEqual(["team-a", "team-b"]);
    await userEvent.keyboard("{ArrowDown}{Enter}");
    expect(input).toHaveValue("team-b, ");
    expect(options()).toEqual(["default", "kube-system", "team-a"]);
  });

  it("does not offer the projects already listed and completes only the last one", async () => {
    render(<Field initial="team-a, kube" />);
    const input = screen.getByRole("combobox");
    await userEvent.click(input);
    expect(options()).toEqual(["kube-system"]);
    await userEvent.click(screen.getByRole("option", { name: "kube-system" }));
    expect(input).toHaveValue("team-a, kube-system, ");
    expect(input).toHaveFocus();
  });

  it("Escape closes the list and leaving the field hides it", async () => {
    render(<Field />);
    const input = screen.getByRole("combobox");
    await userEvent.click(input);
    expect(input).toHaveAttribute("aria-expanded", "true");
    await userEvent.keyboard("{Escape}");
    expect(options()).toEqual([]);
    await userEvent.keyboard("{ArrowDown}");
    expect(options()).toHaveLength(4);
    await userEvent.tab();
    expect(options()).toEqual([]);
  });
});
