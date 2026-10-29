import type { ChartMarker, ChartSeries } from "../../api/types";

/** Горизонтальная линия графика: текущий request или limit контейнера. */
export interface ChartLine {
  label: string;
  value: number;
  dash: "request" | "limit";
}

/**
 * Данные uPlot: общая ось времени (объединение точек всех рядов), ряды контейнеров с `null` там, где точки нет,
 * линии request и limit по всей ширине и отметки (рестарты, OOM) — точкой на высоте худшего значения в этот момент.
 */
export function chartData(series: ChartSeries[], lines: ChartLine[], markers: ChartMarker[]): (number | null)[][] {
  const xs = [...new Set([...series.flatMap((s) => s.points.map(([t]) => t)), ...markers.map((m) => m.time)])].sort((a, b) => a - b);
  const rows = series.map((s) => {
    const at = new Map(s.points);
    return xs.map((x) => at.get(x) ?? null);
  });
  const top = xs.map((_, i) => Math.max(0, ...rows.map((r) => r[i] ?? 0)));
  const marked = new Set(markers.map((m) => m.time));
  return [xs, ...rows, ...lines.map((l) => xs.map(() => l.value)), xs.map((x, i) => (marked.has(x) ? top[i] : null))];
}

/** Цвет из CSS-переменной темы (`--chart-1`…); вне браузера — запасной. */
export function themeColor(name: string, fallback: string): string {
  const v = typeof document === "undefined" ? "" : getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  return v || fallback;
}
