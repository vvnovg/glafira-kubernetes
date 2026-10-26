import { act, screen, waitFor } from "@testing-library/react";
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

beforeEach(() => {
  vi.clearAllMocks();
  vi.useRealTimers();
});

describe("YamlView и мелкие части экранов", () => {
  it("YAML объекта и копирование", async () => {
    vi.mocked(api.getYaml).mockResolvedValue("kind: Deployment\nmetadata:\n  name: web\n");
    const user = userEvent.setup();
    const writeText = vi.spyOn(navigator.clipboard, "writeText").mockResolvedValue();
    renderWithClient(<YamlView connectionId="c1" namespace="team-a" kind="Deployment" name="web" />);
    expect(await screen.findByText(/kind: Deployment/)).toBeInTheDocument();
    // Область уведомления смонтирована до копирования, текст появляется в ней же.
    const status = screen.getByRole("status");
    expect(status).toBeEmptyDOMElement();
    expect(api.getYaml).toHaveBeenCalledWith("c1", "team-a", "Deployment", "web");
    await user.click(screen.getByRole("button", { name: "Копировать" }));
    expect(writeText).toHaveBeenCalledWith("kind: Deployment\nmetadata:\n  name: web\n");
    await waitFor(() => expect(status).toHaveTextContent("Скопировано"));
    expect(screen.getByRole("status")).toBe(status);
  });

  it("«Скопировано» исчезает через 2 с", async () => {
    vi.mocked(api.getYaml).mockResolvedValue("kind: Pod\n");
    vi.useFakeTimers({ shouldAdvanceTime: true });
    const user = userEvent.setup({ advanceTimers: vi.advanceTimersByTime });
    vi.spyOn(navigator.clipboard, "writeText").mockResolvedValue();
    renderWithClient(<YamlView connectionId="c1" namespace="team-a" kind="Pod" name="web" />);
    await screen.findByText(/kind: Pod/);
    await user.click(screen.getByRole("button", { name: "Копировать" }));
    expect(await screen.findByText("Скопировано")).toBeInTheDocument();
    await act(async () => {
      await vi.advanceTimersByTimeAsync(1500);
    });
    expect(screen.getByText("Скопировано")).toBeInTheDocument();
    await act(async () => {
      await vi.advanceTimersByTimeAsync(600);
    });
    expect(screen.queryByText("Скопировано")).not.toBeInTheDocument();
  });

  it("новый текст YAML снимает «Скопировано»: скопирован был прежний", async () => {
    vi.mocked(api.getYaml).mockResolvedValueOnce("kind: Pod\nmetadata:\n  resourceVersion: \"1\"\n").mockResolvedValue("kind: Pod\nmetadata:\n  resourceVersion: \"2\"\n");
    const user = userEvent.setup();
    vi.spyOn(navigator.clipboard, "writeText").mockResolvedValue();
    renderWithClient(<YamlView connectionId="c1" namespace="team-a" kind="Pod" name="web" />);
    await screen.findByText(/resourceVersion: "1"/);
    await user.click(screen.getByRole("button", { name: "Копировать" }));
    expect(await screen.findByText("Скопировано")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Обновить" }));
    await screen.findByText(/resourceVersion: "2"/);
    expect(screen.queryByText("Скопировано")).not.toBeInTheDocument();
  });

  it("отказ буфера обмена виден: асинхронный и синхронный", async () => {
    vi.mocked(api.getYaml).mockResolvedValue("kind: Pod\n");
    const user = userEvent.setup();
    const writeText = vi.spyOn(navigator.clipboard, "writeText").mockRejectedValue(new Error("denied"));
    renderWithClient(<YamlView connectionId="c1" namespace="team-a" kind="Pod" name="web" />);
    await screen.findByText(/kind: Pod/);
    const status = screen.getByRole("status");
    expect(status).toBeEmptyDOMElement();
    await user.click(screen.getByRole("button", { name: "Копировать" }));
    await waitFor(() => expect(status).toHaveTextContent("Не удалось скопировать в буфер обмена"));
    expect(status).toHaveClass("warn");
    expect(screen.queryByText("Скопировано")).not.toBeInTheDocument();
    // Вторая попытка удалась: отказ заменяется итогом.
    writeText.mockResolvedValue();
    await user.click(screen.getByRole("button", { name: "Копировать" }));
    await waitFor(() => expect(status).toHaveTextContent("Скопировано"));
    expect(status).not.toHaveClass("warn");
    // Буфера нет вовсе (`writeText` бросает сразу, не промисом).
    writeText.mockImplementation(() => {
      throw new TypeError("clipboard unavailable");
    });
    await user.click(screen.getByRole("button", { name: "Копировать" }));
    await waitFor(() => expect(status).toHaveTextContent("Не удалось скопировать в буфер обмена"));
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
