import { useEffect, useState } from "react";
import { api, toAppError } from "../api/client";
import type { AppError, EventRow, KindError, PodRow, WatchParams, WatchRow, WorkloadRow } from "../api/types";
import { useEpoch } from "../state/connectionEpoch";

/** Ключи строк — те же, что у Rust (`Row::key`). */
export const podKey = (p: PodRow) => p.name;
export const workloadKey = (w: WorkloadRow) => `${w.kind}/${w.name}`;
export const eventKey = (e: EventRow) => e.key;

export type WatchStatus = "connecting" | "live" | "polling" | "error";

export interface WatchState<T> {
  rows: T[];
  status: WatchStatus;
  /** Последняя ошибка: при `error` сессия завершена, иначе watcher пробует снова. */
  error: AppError | null;
  /** Виды workloads, которые не читаются. */
  kindErrors: KindError[];
}

const INITIAL = { rows: [], status: "connecting" as const, error: null, kindErrors: [] };

/**
 * Список проекта, который обновляется сам (спец. §6.7): сессия watch в Rust, а без прав на watch —
 * опрос раз в 10 с. Сессия перезапускается при смене параметров и после входа заново и
 * останавливается при размонтировании. `params` null — сессии нет. `keyOf` — функция модуля.
 */
export function useWatchList<T extends WatchRow>(params: WatchParams | null, keyOf: (row: T) => string): WatchState<T> {
  const epoch = useEpoch(params?.connectionId ?? "");
  const paramsKey = params ? JSON.stringify(params) : null;
  const [state, setState] = useState<WatchState<T>>(INITIAL);

  useEffect(() => {
    if (!paramsKey) return;
    const p = JSON.parse(paramsKey) as WatchParams;
    let active = true;
    let sessionId: string | null = null;
    const rows = new Map<string, T>();
    setState(INITIAL);
    api
      .startWatch(p, (e) => {
        if (!active) return;
        switch (e.type) {
          case "reset":
            rows.clear();
            for (const r of e.items as T[]) rows.set(keyOf(r), r);
            setState((s) => ({ rows: [...rows.values()], status: s.status === "polling" ? "polling" : "live", error: null, kindErrors: e.errors }));
            break;
          case "changes":
            for (const c of e.changes) {
              if (c.op === "upsert") rows.set(keyOf(c.row as T), c.row as T);
              else rows.delete(c.key);
            }
            setState((s) => ({ ...s, rows: [...rows.values()], error: null }));
            break;
          case "polling":
            setState((s) => ({ ...s, status: "polling" }));
            break;
          case "error":
            setState((s) => ({ ...s, status: e.fatal ? "error" : s.status, error: e.error }));
            break;
        }
      })
      .then((id) => {
        if (active) sessionId = id;
        else api.stopSession(id).catch(() => undefined);
      })
      .catch((err) => {
        if (active) setState((s) => ({ ...s, status: "error", error: toAppError(err) }));
      });
    return () => {
      active = false;
      if (sessionId) api.stopSession(sessionId).catch(() => undefined);
    };
    // keyOf — функция модуля; сессия зависит только от параметров и входа.
  }, [paramsKey, epoch]);

  return state;
}
