import { useEffect } from "react";
import { getCurrentWindow } from "@tauri-apps/api/window";
import { useWorkspace } from "../state/workspace";
import { isMacOS } from "./platform";

/** Элемент видимой вкладки с нужным действием (`data-action="refresh"` у кнопки «Обновить», `"filter"` у фильтра). */
function inActiveTab(action: string): HTMLElement | null {
  return document.querySelector<HTMLElement>(`.tab-content:not([hidden]) [data-action="${action}"]`);
}

/**
 * Действия сочетаний клавиш (спец. U1 §8): на macOS их вызывает меню приложения, на других ОС — `useShortcuts`.
 * Одна таблица, чтобы меню и обработчик клавиш не расходились.
 */
export const shortcutActions = {
  newConnection: () => useWorkspace.getState().open({ kind: "connection-form", connectionId: null }),
  closeTab: () => {
    const { activeId, close } = useWorkspace.getState();
    if (activeId) close(activeId);
  },
  closeWindow: () => void getCurrentWindow().close(),
  settings: () => useWorkspace.getState().open({ kind: "settings" }),
  refresh: () => inActiveTab("refresh")?.click(),
  /** Фильтр или поиск на экране; без него — фильтр дерева подключений. */
  find: () => (inActiveTab("filter") ?? document.querySelector<HTMLElement>(".tree-filter"))?.focus(),
  filterConnections: () => document.querySelector<HTMLElement>(".tree-filter")?.focus(),
  nextTab: () => stepTab(1),
  prevTab: () => stepTab(-1),
  tab: (n: number) => {
    const { tabs, activate } = useWorkspace.getState();
    const tab = tabs[n - 1];
    if (tab) activate(tab.id);
  },
};

function stepTab(delta: number) {
  const { tabs, activeId, activate } = useWorkspace.getState();
  if (tabs.length === 0) return;
  const i = tabs.findIndex((t) => t.id === activeId);
  const next = tabs[(i + delta + tabs.length) % tabs.length];
  if (next) activate(next.id);
}

/** Сочетания, которые на macOS занимает меню приложения: там `keydown` их не трогает (иначе действие сработает дважды). */
function inMenu(e: KeyboardEvent): boolean {
  return isMacOS() && (e.metaKey || e.ctrlKey) && !/^[1-9]$/.test(e.key);
}

/** Что делать по клавише; `null` — не наше сочетание. */
export function resolveShortcut(e: KeyboardEvent, mac = isMacOS()): (() => void) | null {
  const mod = mac ? e.metaKey : e.ctrlKey;
  if (mac && e.ctrlKey && e.key === "Tab") return e.shiftKey ? shortcutActions.prevTab : shortcutActions.nextTab;
  if (!mod || e.altKey) return null;
  if (/^[1-9]$/.test(e.key)) {
    const n = Number(e.key);
    return () => shortcutActions.tab(n);
  }
  if (!mac && e.key === "Tab") return e.shiftKey ? shortcutActions.prevTab : shortcutActions.nextTab;
  const key = e.key.toLowerCase();
  if (e.shiftKey) {
    if (key === "w") return shortcutActions.closeWindow;
    if (key === "f") return shortcutActions.filterConnections;
    return null;
  }
  switch (key) {
    case "n":
      return shortcutActions.newConnection;
    case "w":
      return shortcutActions.closeTab;
    case "r":
      return shortcutActions.refresh;
    case "f":
      return shortcutActions.find;
    case ",":
      return shortcutActions.settings;
    default:
      return null;
  }
}

/** Сочетания клавиш окна: ⌘1…⌘9 везде, остальное — там, где нет меню приложения (Windows, Linux). */
export function useShortcuts(): void {
  useEffect(() => {
    const onKeyDown = (e: KeyboardEvent) => {
      if (e.defaultPrevented || e.isComposing || inMenu(e)) return;
      const action = resolveShortcut(e);
      if (!action) return;
      e.preventDefault();
      action();
    };
    // capture — раньше обработчиков полей, которые останавливают всплытие (как F1 в useHelpShortcut).
    window.addEventListener("keydown", onKeyDown, { capture: true });
    return () => window.removeEventListener("keydown", onKeyDown, { capture: true });
  }, []);
}
