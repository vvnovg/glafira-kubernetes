import { useState } from "react";
import { useTranslation } from "react-i18next";

/** Варианты автообновления (спец. §6.7): выключено, 10 с, 30 с. */
export const REFRESH_INTERVALS = [0, 10_000, 30_000] as const;

/** Интервал автообновления экрана (0 — выключено) для `refetchInterval`. */
export function useRefreshInterval(): [number, (ms: number) => void] {
  return useState<number>(0);
}

/** «Обновить» и автообновление для экранов без watch. */
export function RefreshControls({
  interval,
  onInterval,
  onRefresh,
  busy,
}: {
  interval: number;
  onInterval: (ms: number) => void;
  onRefresh: () => void;
  busy: boolean;
}) {
  const { t } = useTranslation();
  return (
    <span className="toolbar">
      <button type="button" disabled={busy} onClick={onRefresh}>
        {t("common.refresh")}
      </button>
      <label className="inline">
        {t("common.autoRefresh")}
        <select value={interval} onChange={(e) => onInterval(Number(e.target.value))}>
          {REFRESH_INTERVALS.map((ms) => (
            <option key={ms} value={ms}>
              {ms === 0 ? t("common.autoRefreshOff") : t("common.everySeconds", { n: ms / 1000 })}
            </option>
          ))}
        </select>
      </label>
    </span>
  );
}
