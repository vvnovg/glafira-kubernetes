import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../test/render";

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return { ...orig, api: { getYaml: vi.fn() } };
});

import { api } from "../../api/client";
import { Conditions } from "./Conditions";
import { KeyValues } from "./KeyValues";
import { SubTabs } from "./SubTabs";
import { YamlView } from "./YamlView";

beforeEach(() => vi.clearAllMocks());

describe("YamlView и мелкие части экранов", () => {
  it("YAML объекта и копирование", async () => {
    vi.mocked(api.getYaml).mockResolvedValue("kind: Deployment\nmetadata:\n  name: web\n");
    const user = userEvent.setup();
    const writeText = vi.spyOn(navigator.clipboard, "writeText").mockResolvedValue();
    renderWithClient(<YamlView connectionId="c1" namespace="team-a" kind="Deployment" name="web" />);
    expect(await screen.findByText(/kind: Deployment/)).toBeInTheDocument();
    expect(api.getYaml).toHaveBeenCalledWith("c1", "team-a", "Deployment", "web");
    await user.click(screen.getByRole("button", { name: "Копировать" }));
    expect(writeText).toHaveBeenCalledWith("kind: Deployment\nmetadata:\n  name: web\n");
    await waitFor(() => expect(screen.getByText("Скопировано")).toBeInTheDocument());
  });

  it("условия, метки и подвкладки", async () => {
    const onChange = vi.fn();
    const user = userEvent.setup();
    renderWithClient(
      <>
        <Conditions conditions={[{ type: "Ready", status: "False", reason: "ContainersNotReady", message: "web not ready" }]} />
        <KeyValues values={{ app: "web" }} />
        <SubTabs tabs={[{ id: "a", label: "Обзор" }, { id: "b", label: "YAML" }]} active="a" onChange={onChange} />
      </>,
    );
    expect(screen.getByText("ContainersNotReady")).toBeInTheDocument();
    expect(screen.getByText("app=web")).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Обзор" })).toHaveAttribute("aria-selected", "true");
    await user.click(screen.getByRole("tab", { name: "YAML" }));
    expect(onChange).toHaveBeenCalledWith("b");
  });
});
