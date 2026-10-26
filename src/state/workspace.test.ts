import { beforeEach, describe, expect, it } from "vitest";
import { tabId, useWorkspace } from "./workspace";

beforeEach(() => {
  useWorkspace.setState({ tabs: [], activeId: null });
});

describe("workspace", () => {
  it("повторное открытие активирует существующую вкладку", () => {
    const { open } = useWorkspace.getState();
    open({ kind: "cluster", connectionId: "c" });
    open({ kind: "settings" });
    open({ kind: "cluster", connectionId: "c" });
    const s = useWorkspace.getState();
    expect(s.tabs).toHaveLength(2);
    expect(s.activeId).toBe("cluster:c");
  });

  it("закрытие активной вкладки активирует соседнюю", () => {
    const { open, close, activate } = useWorkspace.getState();
    open({ kind: "cluster", connectionId: "a" });
    open({ kind: "cluster", connectionId: "b" });
    open({ kind: "cluster", connectionId: "c" });
    activate("cluster:b");
    close("cluster:b");
    expect(useWorkspace.getState().activeId).toBe("cluster:c");
    close("cluster:c");
    expect(useWorkspace.getState().activeId).toBe("cluster:a");
    close("cluster:a");
    expect(useWorkspace.getState().activeId).toBeNull();
  });

  it("id вкладок", () => {
    expect(tabId({ kind: "connection-form", connectionId: null })).toBe("connection-form:new");
    expect(tabId({ kind: "connection-form", connectionId: "c" })).toBe("connection-form:c");
    expect(tabId({ kind: "cluster", connectionId: "c" })).toBe("cluster:c");
    expect(tabId({ kind: "help", chapter: "connections" })).toBe("help");
    expect(tabId({ kind: "nodes", connectionId: "c" })).toBe("nodes:c");
    expect(tabId({ kind: "project", connectionId: "c", namespace: "team-a" })).toBe("project:c:team-a");
    expect(tabId({ kind: "workload", connectionId: "c", namespace: "team-a", workloadKind: "Deployment", name: "web" })).toBe("workload:c:team-a:Deployment:web");
    expect(tabId({ kind: "pod", connectionId: "c", namespace: "team-a", name: "web-1" })).toBe("pod:c:team-a:web-1");
  });

  it("вкладки объектов закрываются вместе с подключением", () => {
    const { open, closeForConnection } = useWorkspace.getState();
    open({ kind: "pod", connectionId: "a", namespace: "team-a", name: "web-1" });
    open({ kind: "workload", connectionId: "b", namespace: "team-a", workloadKind: "Job", name: "migrate" });
    closeForConnection("a");
    expect(useWorkspace.getState().tabs.map((t) => t.id)).toEqual(["workload:b:team-a:Job:migrate"]);
  });

  it("closeForConnection закрывает все вкладки подключения", () => {
    const { open, activate, closeForConnection } = useWorkspace.getState();
    open({ kind: "cluster", connectionId: "a" });
    open({ kind: "cluster", connectionId: "b" });
    open({ kind: "connection-form", connectionId: "b" });
    open({ kind: "settings" });
    activate("connection-form:b");
    closeForConnection("b");
    const s = useWorkspace.getState();
    expect(s.tabs.map((t) => t.id)).toEqual(["cluster:a", "settings"]);
    expect(s.activeId).toBe("settings");
    closeForConnection("a");
    expect(useWorkspace.getState().tabs.map((t) => t.id)).toEqual(["settings"]);
  });

  it("повторное open обновляет spec открытой вкладки", () => {
    const { open } = useWorkspace.getState();
    open({ kind: "help", chapter: "getting-started" });
    open({ kind: "help", chapter: "connections", anchor: "kubeconfig" });
    const s = useWorkspace.getState();
    expect(s.tabs).toHaveLength(1);
    expect(s.tabs[0]).toMatchObject({ chapter: "connections", anchor: "kubeconfig" });
  });
});
