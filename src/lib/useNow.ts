import { useEffect, useState } from "react";

/**
 * Текущее время в мс, обновляется раз в `intervalMs`: экран, зависящий от времени (срок токена), иначе остался бы
 * снимком на момент последней перерисовки.
 */
export function useNow(intervalMs: number): number {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    // Время могло уйти вперёд, пока таймер не работал (первый показ, смена интервала).
    setNow(Date.now());
    const timer = setInterval(() => setNow(Date.now()), intervalMs);
    return () => clearInterval(timer);
  }, [intervalMs]);
  return now;
}
