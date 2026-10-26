import { act, fireEvent, screen, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { ConnectionProfile } from "../api/types";
import { useHelpContext } from "../state/helpContext";
import { useWorkspace } from "../state/workspace";
import { renderWithClient } from "../test/render";
import { Layout } from "./Layout";

vi.mock("../api/client", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../api/client")>();
  const conn = {
    id: "c1",
    name: "prod-1",
    color: "#888",
    environment: "prod",
    readOnly: true,
    apiUrl: "https://api.prod:6443",
    auth: { method: "browser" },
    ca: { mode: "system" },
    proxy: { mode: "system" },
    favoriteNamespaces: [],
    manualNamespaces: [],
    requestTimeoutMs: 5000,
    connectTimeoutMs: 5000,
  } as unknown as ConnectionProfile;
  return { ...actual, api: { ...actual.api, listConnections: vi.fn().mockResolvedValue([conn]), listFolders: vi.fn().mockResolvedValue([]) } };
});

// Вместо экрана — метка, показана ли его вкладка (контекст `Layout` для опросов и watch).
vi.mock("./TabContent", async () => {
  const { useTabActive } = await import("../lib/tabActive");
  return { TabContent: ({ tab }: { tab: { id: string } }) => <div data-testid={tab.id} data-tab-active={String(useTabActive())} /> };
});

describe("Layout tabs", () => {
  afterEach(() => useHelpContext.setState({ byTab: {} }));

  beforeEach(() => {
    useHelpContext.setState({ byTab: {} });
    useWorkspace.setState({
      tabs: [
        { kind: "cluster", connectionId: "c1", id: "cluster:c1" },
        { kind: "connection-form", connectionId: "c1", id: "connection-form:c1" },
      ],
      activeId: "cluster:c1",
    });
  });

  it("shows env badge and lock in the tab, with accessible tab roles", async () => {
    renderWithClient(<Layout />);
    const list = screen.getByRole("tablist");
    const tabs = within(list).getAllByRole("tab");
    expect(tabs[0].getAttribute("aria-selected")).toBe("true");
    expect(tabs[1].getAttribute("aria-selected")).toBe("false");
    expect((await within(list).findAllByText("PROD")).length).toBe(2);
    expect(within(list).getAllByRole("img", { name: "Только чтение" }).length).toBe(2);
  });

  it("activates a tab via its button", () => {
    renderWithClient(<Layout />);
    const tabs = within(screen.getByRole("tablist")).getAllByRole("tab");
    fireEvent.click(tabs[1]);
    expect(tabs[1].getAttribute("aria-selected")).toBe("true");
    expect(tabs[0].getAttribute("aria-selected")).toBe("false");
  });

  it("tells each screen whether its tab is shown: hidden tabs stay mounted but must stop polling", () => {
    renderWithClient(<Layout />);
    const shown = (id: string) => screen.getByTestId(id).getAttribute("data-tab-active");
    expect([shown("cluster:c1"), shown("connection-form:c1")]).toEqual(["true", "false"]);
    expect(screen.getByTestId("connection-form:c1").closest("section")).toHaveAttribute("hidden");
    fireEvent.click(within(screen.getByRole("tablist")).getAllByRole("tab")[1]);
    expect([shown("cluster:c1"), shown("connection-form:c1")]).toEqual(["false", "true"]);
  });

  it("F1 opens the guide chapter of the active screen", () => {
    useWorkspace.setState({
      tabs: [{ kind: "connection-form", connectionId: "c1", id: "connection-form:c1" }],
      activeId: "connection-form:c1",
    });
    renderWithClient(<Layout />);
    const ev = new KeyboardEvent("keydown", { key: "F1", bubbles: true, cancelable: true });
    act(() => {
      window.dispatchEvent(ev);
    });
    expect(ev.defaultPrevented).toBe(true);
    const { tabs, activeId } = useWorkspace.getState();
    expect(activeId).toBe("help");
    expect(tabs.find((t) => t.id === "help")).toMatchObject({ kind: "help", chapter: "connections" });
    expect(within(screen.getByRole("tablist")).getByRole("tab", { name: "Справка" })).toBeInTheDocument();
  });

  it("F1 prefers the chapter the active screen reports", () => {
    useWorkspace.setState({
      tabs: [{ kind: "settings", id: "settings" }],
      activeId: "settings",
    });
    useHelpContext.setState({ byTab: { settings: "connections" } });
    renderWithClient(<Layout />);
    fireEvent.keyDown(window, { key: "F1" });
    expect(useWorkspace.getState().tabs.find((t) => t.id === "help")).toMatchObject({ chapter: "connections" });
  });

  it("opens the guide from the sidebar", () => {
    renderWithClient(<Layout />);
    fireEvent.click(screen.getByRole("button", { name: "Справка" }));
    expect(useWorkspace.getState().activeId).toBe("help");
  });
});
