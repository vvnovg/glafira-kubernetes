import { create } from "zustand";
import type { WorkloadKind } from "../api/types";

export type TabSpec =
  /** Обзор подключения и кластера: кто вошёл, права, API-сервер, ноды, у OpenShift — обновления и операторы. */
  | { kind: "cluster"; connectionId: string }
  | { kind: "nodes"; connectionId: string }
  | { kind: "project"; connectionId: string; namespace: string }
  | { kind: "workload"; connectionId: string; namespace: string; workloadKind: WorkloadKind; name: string }
  | { kind: "pod"; connectionId: string; namespace: string; name: string }
  /** folderId — папка нового подключения («Новое подключение здесь»). */
  | { kind: "connection-form"; connectionId: string | null; folderId?: string | null }
  | { kind: "about" }
  /** Справка: глава и раздел руководства (F1, ссылки). */
  | { kind: "help"; chapter?: string; anchor?: string }
  | { kind: "settings" };

export type Tab = TabSpec & { id: string };

export function tabId(spec: TabSpec): string {
  switch (spec.kind) {
    case "connection-form":
      return `connection-form:${spec.connectionId ?? "new"}`;
    case "about":
      return "about";
    case "help":
      return "help";
    case "settings":
      return "settings";
    case "project":
      return `project:${spec.connectionId}:${spec.namespace}`;
    case "workload":
      return `workload:${spec.connectionId}:${spec.namespace}:${spec.workloadKind}:${spec.name}`;
    case "pod":
      return `pod:${spec.connectionId}:${spec.namespace}:${spec.name}`;
    default:
      return `${spec.kind}:${spec.connectionId}`;
  }
}

interface WorkspaceState {
  tabs: Tab[];
  activeId: string | null;
  open: (spec: TabSpec) => void;
  close: (id: string) => void;
  /** Закрывает все вкладки подключения (например, после его удаления). */
  closeForConnection: (connectionId: string) => void;
  activate: (id: string) => void;
}

export const useWorkspace = create<WorkspaceState>((set) => ({
  tabs: [],
  activeId: null,
  open: (spec) =>
    set((s) => {
      const id = tabId(spec);
      const tab = { ...spec, id } as Tab;
      // Уже открытая вкладка получает новый spec (например, главу справки).
      if (s.tabs.some((t) => t.id === id)) return { tabs: s.tabs.map((t) => (t.id === id ? tab : t)), activeId: id };
      return { tabs: [...s.tabs, tab], activeId: id };
    }),
  close: (id) =>
    set((s) => {
      const idx = s.tabs.findIndex((t) => t.id === id);
      if (idx < 0) return s;
      const tabs = s.tabs.filter((t) => t.id !== id);
      const activeId = s.activeId !== id ? s.activeId : (tabs[idx] ?? tabs[idx - 1])?.id ?? null;
      return { tabs, activeId };
    }),
  closeForConnection: (connectionId) =>
    set((s) => {
      const firstIdx = s.tabs.findIndex((t) => ("connectionId" in t ? t.connectionId : null) === connectionId);
      if (firstIdx < 0) return s;
      const tabs = s.tabs.filter((t) => ("connectionId" in t ? t.connectionId : null) !== connectionId);
      const activeId = tabs.some((t) => t.id === s.activeId)
        ? s.activeId
        : (tabs[firstIdx] ?? tabs[tabs.length - 1])?.id ?? null;
      return { tabs, activeId };
    }),
  activate: (id) => set({ activeId: id }),
}));
