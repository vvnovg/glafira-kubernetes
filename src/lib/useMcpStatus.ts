import { useQuery, useQueryClient } from "@tanstack/react-query";
import { listen } from "@tauri-apps/api/event";
import { useEffect } from "react";
import { api } from "../api/client";
import type { McpStatus } from "../api/types";

/** Событие статуса из Rust (`crate::mcp::STATUS_EVENT`). */
export const MCP_STATUS_EVENT = "mcp://state";
export const MCP_STATUS_KEY = ["mcp-status"] as const;

/** Статус MCP-шлюза: первое значение — командой, дальше его обновляют события из Rust. */
export function useMcpStatus() {
  return useQuery({ queryKey: MCP_STATUS_KEY, queryFn: api.getMcpStatus });
}

/** Подписка на события статуса — одна на приложение (в Layout). Вне Tauri (тесты) молча не работает. */
export function useMcpEvents(): void {
  const qc = useQueryClient();
  useEffect(() => {
    let stop: (() => void) | undefined;
    let unmounted = false;
    listen<McpStatus>(MCP_STATUS_EVENT, (e) => qc.setQueryData(MCP_STATUS_KEY, e.payload))
      .then((unlisten) => (unmounted ? unlisten() : (stop = unlisten)))
      .catch(() => {});
    return () => {
      unmounted = true;
      stop?.();
    };
  }, [qc]);
}
