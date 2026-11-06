import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { SubTabs } from "./SubTabs";

describe("SubTabs", () => {
  it("подвкладки — сегменты со значком типа; активная отмечена aria-selected", async () => {
    const onChange = vi.fn();
    render(
      <SubTabs
        tabs={[
          { id: "overview", label: "Обзор", icon: "grid" },
          { id: "pods", label: "Поды", icon: "pod" },
          { id: "yaml", label: "YAML" },
        ]}
        active="overview"
        onChange={onChange}
      />,
    );
    const tabs = screen.getAllByRole("tab");
    expect(tabs.map((t) => t.textContent)).toEqual(["Обзор", "Поды", "YAML"]);
    expect(tabs[0]).toHaveAttribute("aria-selected", "true");
    expect(tabs[0].querySelector('svg[data-icon="grid"]')).not.toBeNull();
    expect(tabs[1].querySelector('svg[data-icon="pod"]')).not.toBeNull();
    expect(tabs[2].querySelector("svg")).toBeNull();
    await userEvent.click(screen.getByRole("tab", { name: "Поды" }));
    expect(onChange).toHaveBeenCalledWith("pods");
  });
});
