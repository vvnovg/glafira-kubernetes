import { useEffect, useRef } from "react";
import uPlot from "uplot";
import "uplot/dist/uPlot.min.css";
import type { ChartMarker, ChartSeries } from "../../api/types";
import { type ChartLine, chartData, themeColor } from "./chart";

const PALETTE: [string, string][] = [
  ["--chart-1", "#3b82f6"],
  ["--chart-2", "#10b981"],
  ["--chart-3", "#8b5cf6"],
  ["--chart-4", "#f97316"],
  ["--chart-5", "#ec4899"],
  ["--chart-6", "#14b8a6"],
];

const HEIGHT = 220;

interface Props {
  title: string;
  series: ChartSeries[];
  lines: ChartLine[];
  markers: ChartMarker[];
  /** Подпись отметок: «Рестарты и OOM». */
  markerLabel: string;
  format: (v: number) => string;
}

/**
 * График потребления (спец. §7.4): ряд худшего пода по каждому контейнеру, линии текущих request и limit, отметки
 * рестартов и OOM. Цвета — из CSS-переменных темы; ширина следует за контейнером.
 */
export function UsageChart({ title, series, lines, markers, markerLabel, format }: Props) {
  const ref = useRef<HTMLDivElement>(null);
  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const color = (i: number) => themeColor(...PALETTE[i % PALETTE.length]);
    const muted = themeColor("--muted", "#6b7280");
    const axis = { stroke: muted, grid: { stroke: themeColor("--border", "#e3e5e8") }, ticks: { stroke: muted } };
    const plot = new uPlot(
      {
        width: el.clientWidth || 600,
        height: HEIGHT,
        title,
        axes: [axis, { ...axis, size: 60, values: (_u: uPlot, vals: number[]) => vals.map(format) }],
        series: [
          {},
          ...series.map((s, i) => ({ label: s.container, stroke: color(i), width: 1.5, value: (_u: uPlot, v: number | null) => (v == null ? "—" : format(v)) })),
          ...lines.map((l) => ({
            label: l.label,
            stroke: l.dash === "limit" ? themeColor("--danger", "#dc2626") : muted,
            dash: l.dash === "limit" ? [6, 4] : [2, 4],
            width: 1,
            value: (_u: uPlot, v: number | null) => (v == null ? "—" : format(v)),
          })),
          { label: markerLabel, stroke: themeColor("--warn", "#f59e0b"), paths: () => null, points: { show: true, size: 8 } },
        ],
      },
      chartData(series, lines, markers) as uPlot.AlignedData,
      el,
    );
    const resize = new ResizeObserver(() => plot.setSize({ width: el.clientWidth || 600, height: HEIGHT }));
    resize.observe(el);
    return () => {
      resize.disconnect();
      plot.destroy();
    };
  }, [title, series, lines, markers, markerLabel, format]);
  return <div className="usage-chart" ref={ref} />;
}
